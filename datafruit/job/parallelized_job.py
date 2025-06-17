import ray
import pandas as pd
import functools
import logging
from typing import List, Dict, Callable, Any, Type, Optional
import uuid
from sqlalchemy import create_engine, text
import cloudpickle
import time

logger = logging.getLogger(__name__)

@ray.remote
def process_data_chunk(chunk_data: List[Any], func_bytes: bytes, chunk_id: str) -> List[Dict]:
    """
    Ray remote function to process a chunk of input data.

    This function is executed on Ray workers. It deserializes the transformation
    function using cloudpickle and applies it to each item in the chunk.
    It handles individual item processing failures by logging warnings and
    skipping to the next item, allowing for 'partial' failure strategies.

    Args:
        chunk_data: A list of data items to be processed in this chunk.
        func_bytes: The serialized bytes of the transformation function.
        chunk_id: A unique identifier for the chunk, used for logging.

    Returns:
        A list of dictionaries, where each dictionary represents a processed item.
        If a processed item is not a dictionary, it's wrapped in {'result': item}.

    Raises:
        Exception: If the transformation function cannot be deserialized or
                   if there's a critical error processing the entire chunk.
    """
    try:
        transform_func = cloudpickle.loads(func_bytes)
        
        results = []
        for i, item in enumerate(chunk_data):
            try:
                transformed_item = transform_func(item)
                
                if isinstance(transformed_item, dict):
                    result_dict = transformed_item
                else:
                    result_dict = {'result': transformed_item}
                
                results.append(result_dict)
                    
            except Exception as e:
                logger.warning(f"Failed to process item {i} in {chunk_id}: {e}")
                continue
        
        return results
        
    except Exception as e:
        logger.error(f"Chunk {chunk_id} failed: {e}")
        raise


def chunk_data(data: List[Any], chunk_size: int = 1000) -> List[List[Any]]:
    """Split data into fixed-size chunks."""
    if not data:
        return []
    
    chunks = []
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        chunks.append(chunk)
    
    return chunks


def write_results_to_table(connection_string: str, table_name: str, results: List[Dict], 
                          write_mode: str = 'append', use_transaction: bool = False) -> bool:
    """Write processed results to the output table."""
    
    if not results:
        return True
    
    try:
        engine = create_engine(connection_string)
        df = pd.DataFrame(results)
        
        with engine.connect() as conn:
            if use_transaction:
                trans = conn.begin()
                try:
                    _perform_write_operation(conn, df, table_name, write_mode)
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    raise
            else:
                _perform_write_operation(conn, df, table_name, write_mode)
                conn.commit()
            
            return True
            
    except Exception as e:
        logger.error(f"Failed to write results to table: {e}")
        return False


def _perform_write_operation(conn, df: pd.DataFrame, table_name: str, write_mode: str):
    """Perform the actual database write operation."""
    
    if write_mode == 'append':
        df.to_sql(table_name, conn, if_exists='append', index=False)
        
    elif write_mode == 'replace':
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
    elif write_mode == 'upsert':
        temp_table = f"{table_name}_temp_{uuid.uuid4().hex[:8]}"
        df.to_sql(temp_table, conn, if_exists='replace', index=False)
        
        columns = df.columns.tolist()
        pk_col = columns[0] if columns else 'id'
        other_cols = columns[1:] if len(columns) > 1 else []
        
        if other_cols:
            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in other_cols])
            upsert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {temp_table}
            ON CONFLICT ({pk_col}) DO UPDATE SET {set_clause}
            """
        else:
            upsert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {temp_table}
            ON CONFLICT ({pk_col}) DO NOTHING
            """
        
        conn.execute(text(upsert_query))
        conn.execute(text(f"DROP TABLE {temp_table}"))


def create_simple_transform_function(transform_logic: Callable[[Dict], Dict]) -> Callable:
    """Create a simple transform function that can be safely pickled."""
    
    def simple_transform(item: Dict) -> Dict:
        return transform_logic(item)
    
    return simple_transform


def parallelized_job(output_table: Optional[Type] = None,
                    workers: int = 2, 
                    chunk_size: int = 1000,
                    write_mode: str = 'append',
                    connection_string: Optional[str] = None,
                    db: Optional['PostgresDB'] = None,
                    show_progress: bool = True,
                    failure_strategy: str = 'partial',
                    max_retries: int = 2):
    """
    Decorator for parallel data processing jobs with fault tolerance.
    
    Args:
        output_table: SQLModel table class to write results to
        workers: Number of Ray workers (default: 2)
        chunk_size: Items per chunk for memory safety (default: 1000)
        write_mode: 'append', 'replace', 'upsert' (default: 'append')
        connection_string: Database connection string
        db: PostgresDB object (extracts connection_string automatically)
        show_progress: Show progress during processing (default: True)
        failure_strategy: 'partial', 'all_or_nothing', 'retry_failed' (default: 'partial')
        max_retries: Number of retry attempts (default: 2)
    
    Failure Strategies:
        - 'partial': Process what we can, report failures
        - 'all_or_nothing': Rollback everything if any chunk fails
        - 'retry_failed': Retry failed chunks, then proceed with partial results
    """
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(input_data: List[Any], **kwargs) -> Dict[str, Any]:
            """Execute parallel processing job."""
            
            if not input_data:
                return {"success": True, "total_processed": 0, "message": "No data to process"}
            
            if not ray.is_initialized():
                ray.init(ignore_reinit_error=True)
            
            job_id = str(uuid.uuid4())[:8]
            
            # Determine database connection
            final_connection_string = None
            if connection_string:
                final_connection_string = connection_string
            elif db:
                final_connection_string = db.connection_string
            elif output_table:
                logger.warning("output_table specified but no database connection provided")
            
            # Get table name
            table_name = None
            if output_table:
                table_name = output_table.__tablename__
                if db and output_table not in db.tables:
                    logger.warning(f"Table {output_table.__name__} not in db.tables list")
            
            try:
                start_time = time.time()
                
                # Create simple function for pickling
                simple_func = create_simple_transform_function(func)
                
                try:
                    func_bytes = cloudpickle.dumps(simple_func)
                except Exception as e:
                    return {
                        "success": False, 
                        "job_id": job_id, 
                        "total_processed": 0,
                        "error": f"Cannot serialize function: {e}"
                    }
                
                # Create chunks
                chunks = chunk_data(input_data, chunk_size)
                num_chunks = len(chunks)
                
                if show_progress:
                    print(f"Processing {len(input_data)} items using {workers} workers...")
                    print(f"Memory-safe chunking: {num_chunks} chunks of {chunk_size} items each")
                
                # Process chunks with retry logic
                all_results = []
                failed_chunks = []
                successful_chunks = []
                
                # Initial processing
                futures = []
                for i, chunk in enumerate(chunks):
                    future = process_data_chunk.remote(
                        chunk_data=chunk,
                        func_bytes=func_bytes,
                        chunk_id=f"chunk_{i}"
                    )
                    futures.append((future, i, chunk))
                
                # Collect initial results
                for future, chunk_id, chunk_data_for_retry in futures:
                    try:
                        chunk_results = ray.get(future, timeout=300)
                        all_results.extend(chunk_results)
                        successful_chunks.append(chunk_id)
                        
                    except Exception as e:
                        failed_chunks.append({
                            'chunk_id': chunk_id,
                            'chunk_data': chunk_data_for_retry,
                            'error': str(e)
                        })
                
                # Retry logic
                retry_attempt = 1
                while failed_chunks and retry_attempt <= max_retries and failure_strategy != 'partial':
                    if show_progress:
                        print(f"Retrying {len(failed_chunks)} failed chunks (attempt {retry_attempt + 1})...")
                    
                    retry_futures = []
                    for failure in failed_chunks:
                        future = process_data_chunk.remote(
                            chunk_data=failure['chunk_data'],
                            func_bytes=func_bytes,
                            chunk_id=f"chunk_{failure['chunk_id']}_retry_{retry_attempt + 1}"
                        )
                        retry_futures.append((future, failure['chunk_id'], failure['chunk_data']))
                    
                    # Process retries
                    new_failures = []
                    for future, chunk_id, chunk_data_for_retry in retry_futures:
                        try:
                            chunk_results = ray.get(future, timeout=300)
                            all_results.extend(chunk_results)
                            successful_chunks.append(chunk_id)
                        except Exception as e:
                            new_failures.append({
                                'chunk_id': chunk_id,
                                'chunk_data': chunk_data_for_retry,
                                'error': str(e)
                            })
                    
                    failed_chunks = new_failures
                    retry_attempt += 1
                
                processing_time = time.time() - start_time
                items_per_second = len(all_results) / processing_time if processing_time > 0 else 0
                
                # Determine final success
                final_success = True
                if failure_strategy == 'all_or_nothing' and failed_chunks:
                    final_success = False
                    if show_progress:
                        print(f"Job failed: {len(failed_chunks)} chunks could not be processed")
                        print("Rolling back due to all_or_nothing strategy")
                elif failed_chunks and show_progress:
                    print(f"Partial success: {len(failed_chunks)} chunks failed after {max_retries} retries")
                
                if show_progress and all_results:
                    print(f"Processed {len(all_results)} items in {processing_time:.2f}s")
                    print(f"Performance: {items_per_second:.0f} items/second")
                
                # Write to database
                write_success = True
                if output_table and final_connection_string and all_results:
                    if failure_strategy == 'all_or_nothing' and not final_success:
                        write_success = False
                        if show_progress:
                            print("Skipping database write due to failures")
                    else:
                        if show_progress:
                            print(f"Writing {len(all_results)} results to {table_name}...")
                        
                        use_transaction = (failure_strategy == 'all_or_nothing')
                        write_success = write_results_to_table(
                            final_connection_string, 
                            table_name, 
                            all_results, 
                            write_mode,
                            use_transaction=use_transaction
                        )
                        
                        if show_progress:
                            if write_success:
                                print(f"Successfully wrote to {table_name}")
                            else:
                                print("Failed to write to database")
                
                # Prepare failure details
                failed_items = []
                for failure in failed_chunks:
                    for item in failure['chunk_data']:
                        failed_items.append({
                            'item': item,
                            'chunk_id': failure['chunk_id'],
                            'error': failure['error']
                        })
                
                return {
                    "success": write_success and final_success,
                    "job_id": job_id,
                    "input_items": len(input_data),
                    "total_processed": len(all_results),
                    "successful_chunks": len(successful_chunks),
                    "failed_chunks": len(failed_chunks),
                    "failed_items": len(failed_items),
                    "chunks_created": num_chunks,
                    "chunk_size": chunk_size,
                    "workers_used": workers,
                    "processing_time_seconds": round(processing_time, 2),
                    "items_per_second": round(items_per_second, 0),
                    "write_mode": write_mode,
                    "failure_strategy": failure_strategy,
                    "max_retries": max_retries,
                    "retries_attempted": retry_attempt - 1,
                    "results": all_results if not output_table else None,
                    "failed_item_details": failed_items if failed_items else None
                }
                
            except Exception as e:
                logger.error(f"Parallel job {job_id} failed: {e}")
                return {
                    "success": False, 
                    "job_id": job_id, 
                    "total_processed": 0,
                    "error": str(e)
                }
            finally:
                ray.shutdown()
        
        return wrapper
    return decorator


if __name__ == "__main__": 
    print("hello world")
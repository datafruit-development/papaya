import ray
import random
import time 
import math 
import psutil
import pandas as pd
import logging
from sqlmodel import SQLModel, Field
from typing import Optional, Union, Type, Dict, Callable, Any, List
from enum import Enum 
from datafruit import PostgresDB
from sqlmodel import Session, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from functools import wraps 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if not ray.is_initialized(): 
    ray.init()

# circuit breaker enum that ensure no time waste during parallel processes
class _CircuitState(Enum):
    """Circuit breaker states for fault tolerance in parallel processes."""
    CLOSED="CLOSED"
    OPEN="OPEN"
    HALF_OPEN="HALF_OPEN"
 
 

def pyjob(
    # Input/Output
    input: Optional[Union[pd.DataFrame, List, List[SQLModel], SQLModel]] = None,
    output: Optional[Union[Type[pd.DataFrame], Type[List], Type[SQLModel], str]] = None,
    
    # Database Configuration (alternative to input)
    db: Optional[PostgresDB] = None,
    table: Optional[Type[SQLModel]] = None,
    where_clause: Optional[Any] = None,
    order_by: Optional[Any] = None,
    limit: Optional[int] = None,
    
    # Processing config for ray
    batch_size: Optional[int] = None,
    num_cpus: int = 1,
    memory: Optional[int] = None,
    
    # error handling (optional, probably should update for more robust handling)
    max_retries: int = 3,
    retry_exceptions: Optional[List[Exception]] = None,
    enable_circuit_breaker: bool = True,
    circuit_failure_threshold: int = 5,
    circuit_timeout: int = 60,
    
    # added for future monitoring implementation 
    enable_monitoring: bool = True,
    auto_gc: bool = True,
    memory_threshold: float = 0.8
): 
    """
    A flexible decorator for parallel data transformations using Ray with explicit input/output support.
    
    This decorator handles all the parallelization complexity while giving you complete flexibility 
    in your transformation function. You write simple functions that process ONE item at a time, 
    and the decorator automatically applies it to ALL items in parallel.
    """ 
    circuit_breaker_state = {
        'state': _CircuitState.CLOSED,  
        'failure_count': 0, 
        'last_failure_time': None
    }
    
    def _update_circuit_breaker(success: bool):
        """Updates the circuit breaker state based on task success/failure."""
        if not enable_circuit_breaker:
            return
        
        if success:
            if circuit_breaker_state['state'] == _CircuitState.HALF_OPEN:
                circuit_breaker_state['state'] = _CircuitState.CLOSED
                circuit_breaker_state['failure_count'] = 0
                logging.info("Circuit breaker reset to CLOSED - system recovered.")
        else:
            circuit_breaker_state['failure_count'] += 1
            circuit_breaker_state['last_failure_time'] = time.time()
            if circuit_breaker_state['failure_count'] >= circuit_failure_threshold:
                if circuit_breaker_state['state'] != _CircuitState.OPEN:
                    circuit_breaker_state['state'] = _CircuitState.OPEN
                    logging.warning(f"Circuit breaker OPENED after {circuit_failure_threshold} failures.")

    def _check_circuit_breaker() -> bool:
        """Checks if requests should be allowed through the circuit breaker."""
        if not enable_circuit_breaker or circuit_breaker_state['state'] == _CircuitState.CLOSED:
            return True

        if circuit_breaker_state['state'] == _CircuitState.OPEN:
            if time.time() - circuit_breaker_state['last_failure_time'] > circuit_timeout:
                circuit_breaker_state['state'] = _CircuitState.HALF_OPEN
                logging.info("Circuit breaker HALF_OPEN - allowing test request.")
                return True
            return False
        
        return True  

    def _monitor_memory():
        """Monitors memory usage and triggers garbage collection if needed."""
        if not enable_monitoring:
            return 0
        
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > memory_threshold * 100 and auto_gc:
            import gc
            gc.collect()
            logging.warning(f"Memory usage ({memory_percent:.1f}%) exceeded threshold. Triggered GC.")
        return memory_percent
    
    def _get_optimal_batch_size(data_size: int) -> int:
        """Calculate optimal batch size based on data size and available resources."""
        if batch_size is not None:
            return batch_size
        
        # Adaptive batching based on available CPU cores
        num_workers = int(ray.available_resources().get("CPU", 4))
        optimal = max(50, data_size // (num_workers * 4))  # More granular batches
        return optimal  # might need to implement a cap for memory safety. but not now 🗿

    def _detect_input_type(data):
        """Detect the type of input data."""
        if isinstance(data, pd.DataFrame):
            return 'dataframe'
        elif isinstance(data, list):
            if data and isinstance(data[0], SQLModel):
                return 'sqlmodel_list'
            else:
                return 'list'
        elif isinstance(data, SQLModel):
            return 'sqlmodel_single'
        else:
            return 'unknown'

    def _detect_output_type(sample_result):
        """Detect the expected output type based on function result."""
        if isinstance(sample_result, pd.Series):
            return 'dataframe_row'
        elif isinstance(sample_result, pd.DataFrame):
            return 'dataframe'
        elif isinstance(sample_result, SQLModel):
            return 'sqlmodel'
        elif isinstance(sample_result, dict):
            return 'dict'
        elif isinstance(sample_result, list):
            return 'list'
        else:
            return 'other'
    def _convert_results_to_output_format(results: List[Any], expected_output):
        """Convert results to expected output format."""
        if not results:
            if expected_output == pd.DataFrame or expected_output == 'dataframe':
                return pd.DataFrame()
            else:
                return []

        # Detect actual result type
        sample = results[0]
        result_type = _detect_output_type(sample)

        # Handle output conversion based on expected output
        if expected_output == pd.DataFrame or expected_output == 'dataframe':
            if result_type == 'dataframe_row':
                # Convert Series back to DataFrame
                return pd.DataFrame(results).reset_index(drop=True)
            elif result_type == 'dict':
                # Convert dicts to DataFrame
                return pd.DataFrame(results)
            elif result_type == 'dataframe':
                # Concatenate DataFrames
                return pd.concat(results, ignore_index=True)
            else:
                # Try to convert other types to DataFrame
                return pd.DataFrame(results)
        
        elif hasattr(expected_output, '__mro__') and SQLModel in expected_output.__mro__:
            # Expected output is SQLModel type
            if result_type == 'sqlmodel':
                return results
            else:
                logging.warning(f"Expected SQLModel output but got {result_type}")
                return results
        
        elif expected_output == List or expected_output == 'list':
            # Expected output is List
            if result_type in ['list', 'dict', 'sqlmodel', 'other']:
                return results
            else:
                return results
        
        else:
            # Default: return as-is
            return results
        
    def _process_batch_with_ray(func: Callable, data: List[Any]) -> List[Any]:
        """Process a list of items in parallel batches using Ray."""
        start_time = time.time()
        initial_mem = _monitor_memory()
        
        data_size = len(data)
        optimal_batch_size = _get_optimal_batch_size(data_size)

        # Small data - process directly (no parallelization overhead)
        if data_size <= optimal_batch_size:
            logging.info(f"Processing {data_size} items directly (small dataset).")
            results = []
            for item in data:
                try:
                    result = func(item)
                    if result is not None:  # Allow filtering by returning None
                        results.append(result)
                except Exception as e:
                    logging.error(f"Processing failed for item: {e}")
                    if max_retries > 0:
                        raise
                    continue
            return results

        # Large data - split into batches for parallel processing
        batches = [data[i:i + optimal_batch_size] for i in range(0, data_size, optimal_batch_size)]
        logging.info(f"Processing {data_size} items in {len(batches)} batches of size {optimal_batch_size}.")

        # Create Ray remote function that processes one batch
        @ray.remote(
            num_cpus=num_cpus, 
            memory=memory, 
            max_retries=max_retries,
            retry_exceptions=retry_exceptions or [ConnectionError, TimeoutError, Exception]
        )
        def remote_batch_processor(batch):
            batch_results = []
            for item in batch:
                try:
                    # Rehydration Step: If a model_class was provided, convert
                    # the dictionary back into a SQLModel object.
                    if model_class:
                        # Use model_validate (more robust than **) to create the instance from the dict.
                        rehydrated_item = model_class.model_validate(item)
                    else:
                        rehydrated_item = item
                    
                    # Pass the rehydrated object to the original user function.
                    result = func(rehydrated_item)
                    
                    if result is not None:
                        batch_results.append(result)
                except Exception as e:
                    logging.error(f"Processing failed for item: {e}")
                    continue
            return batch_results
        
        # Submit all batches to Ray workers
        futures = [remote_batch_processor.remote(batch) for batch in batches]
        
        # Monitor memory during processing
        for i in range(0, len(futures), 10):
            _monitor_memory()
        
        # Collect and combine results
        batch_results = ray.get(futures)
        final_results = []
        for batch_result in batch_results:
            if batch_result:
                final_results.extend(batch_result)

        if enable_monitoring:
            duration = time.time() - start_time
            final_mem = _monitor_memory()
            logging.info(
                f"Parallel processing completed in {duration:.2f}s. "
                f"Memory: {initial_mem:.1f}% → {final_mem:.1f}%. "
                f"Processed: {data_size} → {len(final_results)} items."
            )
        
        return final_results

    def _process_dataframe(func: Callable, df: pd.DataFrame) -> Any:
        """Process DataFrame row by row and return results in expected format."""
        logging.info(f"Processing DataFrame with {len(df)} rows.")
        
        # Convert DataFrame to list of Series (rows)
        rows = [df.iloc[i] for i in range(len(df))]
        
        # Process all rows
        processed_rows = _process_batch_with_ray(func, rows)
        
        # Convert to expected output format
        result = _convert_results_to_output_format(processed_rows, output)
        
        if isinstance(result, pd.DataFrame):
            logging.info(f"DataFrame processing complete: {len(df)} → {len(result)} rows.")
        else:
            logging.info(f"DataFrame processing complete: {len(df)} → {len(result)} items.")
        
        return result

    def _process_list(func: Callable, data_list: List[Any]) -> Any:
        """Process list item by item and return results in expected format."""
        input_type = _detect_input_type(data_list)
        logging.info(f"Processing list of {len(data_list)} items (type: {input_type}).")
        
        
        model_class_for_rehydration = None
        if input_type == 'sqlmodel_list':
            model_class_for_rehydration = type(data_list[0])
            logging.info(f"Dehydrating {len(data_list)} {model_class_for_rehydration.__name__} objects for serialization.")
            # Create a new list of dictionaries.
            data_to_process = [item.model_dump() for item in data_list]
        else:
            data_to_process = data_list
        # Process all items
        
        processed_items = _process_batch_with_ray(func, data_to_process, model_class_for_rehydration)
        
        # Convert to expected output format
        result = _convert_results_to_output_format(processed_items, output)
        
        logging.info(f"List processing complete: {len(data_list)} → {len(result)} items.")
        return result

    def _process_single_sqlmodel(func: Callable, sqlmodel_obj: SQLModel) -> Any:
        """Process single SQLModel object."""
        logging.info(f"Processing single {type(sqlmodel_obj).__name__} object.")
        
        try:
            result = func(sqlmodel_obj)
            logging.info(f"Single SQLModel processing complete.")
            return result
        except Exception as e:
            logging.error(f"Processing failed for SQLModel object: {e}")
            raise

    def _process_from_database(func: Callable, db, table_class, where_clause=None, 
                             order_by=None, limit=None) -> Any:
        """Handle memory-efficient database processing with auto-save."""
        if not db or not table_class:
            raise ValueError("Both `db` and `table` must be provided for database mode.")

        start_time = time.time()
        table_name = table_class.__tablename__
        logging.info(f"Starting database processing for table: '{table_name}'")

        # Validate connection and sync schema
        if not db.validate_connection():
            raise ConnectionError("Database connection failed.")
        db.sync_schema()

        # Memory-efficient data extraction using IDs
        SessionLocal = sessionmaker(bind=db.engine)
        with SessionLocal() as session:
            # Get record IDs for batch processing
            id_query = select(table_class.id)
            if where_clause is not None:
                id_query = id_query.where(where_clause)
            if order_by is not None:
                id_query = id_query.order_by(order_by)
            if limit is not None:
                id_query = id_query.limit(limit)
            
            record_ids = list(session.exec(id_query).all())

            if not record_ids:
                logging.warning("No records found matching criteria.")
                return _convert_results_to_output_format([], output)

            # Process in memory-efficient batches
            db_batch_size = _get_optimal_batch_size(len(record_ids))
            logging.info(f"Processing {len(record_ids)} records in batches of {db_batch_size}.")
            
            all_results = []
            
            for i in range(0, len(record_ids), db_batch_size):
                batch_ids = record_ids[i:i + db_batch_size]
                
                # Fetch only this batch of records
                batch_records = list(session.exec(
                    select(table_class).where(table_class.id.in_(batch_ids))
                ).all())
                
                # Process batch
                batch_results = _process_batch_with_ray(func, batch_records)
                all_results.extend(batch_results)
                
                # Auto-save SQLModel results to database
                if batch_results and output and hasattr(output, '__mro__') and SQLModel in output.__mro__:
                    sqlmodel_results = [r for r in batch_results if isinstance(r, SQLModel)]
                    if sqlmodel_results:
                        try:
                            session.add_all(sqlmodel_results)
                            session.commit()
                            logging.info(f"Auto-saved {len(sqlmodel_results)} {output.__name__} objects to database.")
                        except Exception as e:
                            logging.error(f"Database save failed: {e}")
                            session.rollback()
                            raise
                
                # Memory management for large datasets
                if i % (db_batch_size * 10) == 0:
                    _monitor_memory()
            
            if enable_monitoring:
                duration = time.time() - start_time
                logging.info(f"Database processing completed in {duration:.2f}s.")
                
            return _convert_results_to_output_format(all_results, output)

    # =================================================================================
    # == DECORATOR WRAPPER
    # =================================================================================

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper():
            """
            Parameterless execution - all configuration comes from decorator.
            This provides a clean, declarative API where everything is specified upfront.
            """
            
            # Check circuit breaker
            if not _check_circuit_breaker():
                error_msg = f"Circuit breaker is OPEN for {func.__name__}. Call aborted."
                logging.error(error_msg)
                raise ConnectionError(error_msg)

            # Determine processing mode and execute
            try:
                result = None
                
                # Database Mode
                if db is not None and table is not None:
                    logging.info(f"Executing {func.__name__} in DATABASE mode.")
                    result = _process_from_database(
                        func, db=db, table_class=table,
                        where_clause=where_clause, order_by=order_by, limit=limit
                    )
                
                # DataFrame Mode
                elif input is not None and isinstance(input, pd.DataFrame):
                    logging.info(f"Executing {func.__name__} in DATAFRAME mode.")
                    result = _process_dataframe(func, input)
                
                # Single SQLModel Mode
                elif input is not None and isinstance(input, SQLModel):
                    logging.info(f"Executing {func.__name__} in SINGLE_SQLMODEL mode.")
                    result = _process_single_sqlmodel(func, input)
                
                # List Mode (includes SQLModel lists)
                elif input is not None and isinstance(input, list):
                    logging.info(f"Executing {func.__name__} in LIST mode.")
                    result = _process_list(func, input)
                
                # No valid input specified
                else:
                    raise ValueError(
                        "No valid input specified. Provide one of:\n"
                        "- input=dataframe for DataFrame processing\n"
                        "- input=list for list processing\n"
                        "- input=sqlmodel_object for single SQLModel processing\n"
                        "- db=postgres_db + table=TableClass for database processing"
                    )

                _update_circuit_breaker(success=True)
                return result

            except Exception as e:
                logging.error(f"Error in {func.__name__}: {e}", exc_info=True)
                _update_circuit_breaker(success=False)
                raise

        # Attach utility methods
        wrapper.get_circuit_breaker_status = lambda: circuit_breaker_state['state'].value
        wrapper.reset_circuit_breaker = lambda: circuit_breaker_state.update({
            'state': _CircuitState.CLOSED, 'failure_count': 0, 'last_failure_time': None
        })
        wrapper.get_stats = lambda: {
            'input_type': _detect_input_type(input) if input is not None else 'database',
            'output_type': output.__name__ if hasattr(output, '__name__') else str(output),
            'table': table.__tablename__ if table else None,
            'batch_size': batch_size,
            'num_cpus': num_cpus,
            'circuit_breaker': wrapper.get_circuit_breaker_status(),
            'monitoring_enabled': enable_monitoring
        }
        
        return wrapper
    return decorator
        
        
if __name__ == "__main__":
    from datetime import datetime
    from typing import Optional
    import pandas as pd
    from sqlmodel import SQLModel, Field
    
    # NOTE: The pyjob decorator and its dependencies (like ray) are assumed to be defined above.

    # --- Model Definitions ---

    class User(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        first_name: str
        last_name: str
        email: str
        age: int
        active: bool = True
        created_at: datetime = Field(default_factory=datetime.utcnow)
        # Added to prevent errors in the cleaning function
        full_name: Optional[str] = None

    class ProcessedUser(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        user_id: int
        full_name: str
        email_domain: str
        category: str
        processed_at: datetime = Field(default_factory=datetime.utcnow)

    class UserAnalytics(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        user_id: int
        engagement_score: float
        risk_level: str
        analysis_date: datetime = Field(default_factory=datetime.utcnow)

    # --- Sample Data ---

    sample_df = pd.DataFrame({
        'first_name': ['Alice', 'Bob', 'Carol', 'David'],
        'last_name': ['Johnson', 'Smith', 'Williams', 'Brown'],
        'email': ['ALICE@GMAIL.COM', 'bob@YAHOO.com', 'carol@company.org', 'david@test.COM'],
        'age': [25, 17, 30, 45],
        'price': [100, 50, 200, 75]
    })

    sample_users = [
        User(id=1, first_name="Alice", last_name="Johnson", email="alice@test.com", age=25),
        User(id=2, first_name="Bob", last_name="Smith", email="bob@test.com", age=17),
        User(id=3, first_name="Carol", last_name="Williams", email="carol@test.com", age=30)
    ]

    single_user = User(id=1, first_name="Alice", last_name="Johnson", email="alice@test.com", age=25)

    print("Running PyJob Decorator Test Suite")
    print("=" * 70)

    # --- Test 1: DataFrame to DataFrame Transformation ---
    print("\n--- Test 1: DataFrame to DataFrame ---")
    @pyjob(
        input=sample_df,
        output=pd.DataFrame,
        batch_size=2
    )
    def clean_dataframe_row(row):
        """Processes a single DataFrame row."""
        row['email'] = row['email'].lower().strip()
        row['full_name'] = f"{row['first_name']} {row['last_name']}"
        row['age_group'] = 'adult' if row['age'] >= 18 else 'minor'
        row['price_category'] = 'expensive' if row['price'] > 100 else 'affordable'
        return row

    try:
        cleaned_df = clean_dataframe_row()
        print("STATUS: SUCCEEDED")
        print(f"  Input: DataFrame (4 rows) -> Output: DataFrame ({len(cleaned_df)} rows)")
        print(f"  New columns added: {[col for col in cleaned_df.columns if col not in sample_df.columns]}")
    except Exception as e:
        print(f"STATUS: FAILED")
        print(f"  Error: {e}")

    # --- Test 2: In-place Editing of SQLModel List ---
    print("\n--- Test 2: List[SQLModel] to List[SQLModel] (In-place edit) ---")
    @pyjob(
        input=sample_users,
        output=User,
        batch_size=2
    )
    def clean_user_objects(user):
        """Cleans User objects by normalizing email and adding full_name."""
        user.email = user.email.lower().strip()
        user.full_name = f"{user.first_name} {user.last_name}"
        return user

    try:
        cleaned_users = clean_user_objects()
        print("STATUS: SUCCEEDED")
        print(f"  Input: List[User] (3 items) -> Output: List[User] ({len(cleaned_users)} items)")
        print(f"  Sample result: {cleaned_users[0].full_name} ({cleaned_users[0].email})")
    except Exception as e:
        print(f"STATUS: FAILED")
        print(f"  Error: {e}")

    # --- Test 3: Transforming List[SQLModel] to a Different SQLModel ---
    print("\n--- Test 3: List[User] to List[ProcessedUser] Transformation ---")
    @pyjob(
        input=sample_users,
        output=ProcessedUser,
        batch_size=2
    )
    def transform_to_processed_user(user):
        """Transforms a User object into a ProcessedUser object."""
        return ProcessedUser(
            user_id=user.id,
            full_name=f"{user.first_name} {user.last_name}",
            email_domain=user.email.split('@')[1],
            category='adult' if user.age >= 18 else 'minor'
        )

    try:
        processed_users = transform_to_processed_user()
        print("STATUS: SUCCEEDED")
        print(f"  Input: List[User] (3 items) -> Output: List[ProcessedUser] ({len(processed_users)} items)")
        print(f"  Sample result: {processed_users[0].full_name} (Category: {processed_users[0].category})")
    except Exception as e:
        print(f"STATUS: FAILED")
        print(f"  Error: {e}")

    # --- Test 4: Processing a Single SQLModel Object ---
    print("\n--- Test 4: Single SQLModel Transformation ---")
    @pyjob(
        input=single_user,
        output=UserAnalytics
    )
    def analyze_single_user(user):
        """Analyzes a single user and creates an analytics record."""
        return UserAnalytics(
            user_id=user.id,
            engagement_score=user.age * 0.1,
            risk_level='low' if user.age > 25 else 'high'
        )

    try:
        analytics = analyze_single_user()
        print("STATUS: SUCCEEDED")
        print(f"  Input: User -> Output: {type(analytics).__name__}")
        print(f"  Result: Engagement={analytics.engagement_score}, Risk='{analytics.risk_level}'")
    except Exception as e:
        print(f"STATUS: FAILED")
        print(f"  Error: {e}")

    # --- Test 5: DataFrame to SQLModel Transformation ---
    print("\n--- Test 5: DataFrame to List[SQLModel] Transformation ---")
    @pyjob(
        input=sample_df,
        output=User,
        batch_size=2
    )
    def validate_dataframe_to_user(row):
        """Converts a DataFrame row to a validated User object, skipping invalid rows."""
        try:
            return User(
                first_name=row['first_name'].strip().title(),
                last_name=row['last_name'].strip().title(),
                email=row['email'].lower().strip(),
                age=int(row['age'])
            )
        except (ValueError, KeyError):
            return None # Skip rows that are invalid or missing data

    try:
        validated_users = validate_dataframe_to_user()
        print("STATUS: SUCCEEDED")
        print(f"  Input: DataFrame (4 rows) -> Output: List[User] ({len(validated_users)} items)")
        print(f"  Sample result: {validated_users[0].first_name} {validated_users[0].last_name}")
    except Exception as e:
        print(f"STATUS: FAILED")
        print(f"  Error: {e}")

    # --- Final Statistics ---
    print("\n" + "=" * 70)
    print("Function Statistics Summary:")
    functions = [
        ('DataFrame->DataFrame', clean_dataframe_row),
        ('SQLModel->SQLModel', clean_user_objects),
        ('SQLModel->ProcessedUser', transform_to_processed_user),
        ('Single SQLModel', analyze_single_user),
        ('DataFrame->SQLModel', validate_dataframe_to_user),
    ]

    for name, func in functions:
        try:
            stats = func.get_stats()
            print(f"  - {name}: {stats}")
        except AttributeError:
            print(f"  - {name}: Could not retrieve stats.")
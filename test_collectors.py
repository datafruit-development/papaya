#!/usr/bin/env python3
# test_collectors.py - Test script for the Spark collectors

from pyspark.sql import SparkSession
import logging
import json
import time
import os

# Import the collectors
from base_collector import BaseCollector
from sparkmeasure_collector import SparkMeasureCollector
from httpendpoints_collector import SparkHttpCollector
from custom_collector import CustomLoggingCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_collectors")

def create_spark_session():
    """Create a Spark session for testing"""
    logger.info("Creating Spark session...")
    spark = (SparkSession.builder
             .appName("SparkDebuggerTest")
             .master("local[*]")
             .config("spark.ui.port", "4040")  # Ensure UI is accessible
             .config("spark.eventLog.enabled", "true")
             .config("spark.eventLog.dir", "/tmp/spark-events")
             .getOrCreate())
    return spark

def run_sample_job(spark):
    """Run a sample Spark job that will generate metrics and logs"""
    logger.info("Running sample Spark job...")
    
    # Create a sample dataframe
    data = [(i, f"value_{i % 10}") for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "value"])
    
    # Force some calculations to generate metrics
    logger.info("Performing transformations and actions...")
    
    # Cache the dataframe
    df.cache()
    
    # Count to materialize the cache
    count = df.count()
    logger.info(f"DataFrame count: {count}")
    
    # Group by to force shuffle
    grouped = df.groupBy("value").count()
    grouped_result = grouped.collect()
    logger.info(f"Grouped result count: {len(grouped_result)}")
    
    # Join operation to generate more complex metrics
    df2 = spark.createDataFrame([(i % 10, f"data_{i}") for i in range(20)], ["key", "data"])
    joined = df.withColumnRenamed("value", "key").join(df2, "key")
    joined_count = joined.count()
    logger.info(f"Joined result count: {joined_count}")
    
    # Generate a deliberate error (uncomment to test error handling)
    # try:
    #     spark.sql("SELECT * FROM nonexistent_table").show()
    # except Exception as e:
    #     logger.error(f"Expected error: {str(e)}")
    
    return {
        "df": df,
        "grouped": grouped,
        "joined": joined
    }

def test_sparkmeasure_collector(spark, dfs):
    """Test the SparkMeasure collector"""
    logger.info("Testing SparkMeasure collector...")
    
    try:
        # Initialize the collector
        collector = SparkMeasureCollector(mode="stage")
        
        # Set up with Spark session
        collector.setup({"spark_session": spark})
        
        # Begin collection
        collector.begin_collection(job_id="test_job_1")
        
        # Run some operations
        result = dfs["df"].groupBy("value").agg({"id": "sum"}).collect()
        logger.info(f"Aggregation result count: {len(result)}")
        
        # End collection and get metrics
        metrics = collector.end_collection()
        
        # Test getting current metrics
        current_metrics = collector.collect({"spark_session": spark})
        
        # Print the metrics
        logger.info(f"SparkMeasure metrics keys: {list(metrics.keys())}")
        logger.info(f"StageMetrics sample: {json.dumps(metrics.get('stage', {}).get('duration_stats', {}), indent=2)}")
        
        return {
            "status": "success",
            "metrics": metrics
        }
    except Exception as e:
        logger.error(f"Error testing SparkMeasure collector: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

def test_http_collector(spark):
    """Test the HTTP endpoints collector"""
    logger.info("Testing HTTP endpoints collector...")
    
    try:
        # Initialize the collector
        collector = SparkHttpCollector(base_url="http://localhost:4040")
        
        # Set up with Spark session
        collector.setup({"spark_session": spark})
        
        # Wait for UI to be fully initialized
        time.sleep(2)
        
        # Collect metrics
        metrics = collector.collect({})
        
        # Print the metrics
        logger.info(f"HTTP metrics keys: {list(metrics.keys())}")
        logger.info(f"Application info: {json.dumps(metrics.get('app', {}), indent=2)}")
        
        return {
            "status": "success",
            "metrics": metrics
        }
    except Exception as e:
        logger.error(f"Error testing HTTP collector: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

def test_custom_logging_collector(spark):
    """Test the custom logging collector"""
    logger.info("Testing custom logging collector...")
    
    try:
        # Create a temporary log file for testing
        log_dir = "/tmp/spark-debugger-test-logs"
        os.makedirs(log_dir, exist_ok=True)
        
        # Create a sample log file with test entries
        log_file = os.path.join(log_dir, "test-application.log")
        with open(log_file, "w") as f:
            f.write("2023-05-09 14:32:05 INFO  SparkContext: Running Spark version 3.3.0\n")
            f.write("2023-05-09 14:32:10 WARN  TaskSetManager: Stage 0 contains a task of very large size (1024 KB)\n")
            f.write("2023-05-09 14:32:15 ERROR SparkContext: Exception in thread main: java.lang.OutOfMemoryError: GC overhead limit exceeded\n")
            f.write("2023-05-09 14:32:16 ERROR TaskSetManager: Task 2 in stage 0.0 failed 4 times\n")
            f.write("2023-05-09 14:32:20 INFO  Streaming query made progress: batchDuration=100ms inputRowsPerSecond=1000 processedRowsPerSecond=900\n")
        
        # Initialize the collector
        collector = CustomLoggingCollector(log_dir=log_dir)
        
        # Set up with Spark session
        collector.setup({"log_dir": log_dir})
        
        # Collect metrics
        metrics = collector.collect({})
        
        # Print the metrics
        logger.info(f"Custom logging metrics keys: {list(metrics.keys())}")
        logger.info(f"Log counts: errors={metrics['logs']['error_count']}, warnings={metrics['logs']['warning_count']}")
        logger.info(f"Exceptions: {json.dumps(metrics['exceptions'], indent=2)}")
        
        return {
            "status": "success",
            "metrics": metrics
        }
    except Exception as e:
        logger.error(f"Error testing custom logging collector: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

def main():
    """Main test function"""
    logger.info("Starting Spark Debugger collector tests")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run a sample job to generate metrics
        dfs = run_sample_job(spark)
        
        # Test each collector
        sparkmeasure_results = test_sparkmeasure_collector(spark, dfs)
        http_results = test_http_collector(spark)
        logging_results = test_custom_logging_collector(spark)
        
        # Print overall results
        logger.info("Test results:")
        logger.info(f"SparkMeasure collector: {sparkmeasure_results['status']}")
        logger.info(f"HTTP endpoints collector: {http_results['status']}")
        logger.info(f"Custom logging collector: {logging_results['status']}")
        
        # Save metrics to file for inspection
        with open("/tmp/spark-debugger-test-results.json", "w") as f:
            json.dump({
                "sparkmeasure": sparkmeasure_results,
                "http": http_results,
                "logging": logging_results
            }, f, indent=2)
        
        logger.info("Test results saved to /tmp/spark-debugger-test-results.json")
        
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
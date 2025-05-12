from pyspark.sql import SparkSession
import time
import sys

def create_spark_session():
    """Create a Spark session with minimal recovery"""
    return SparkSession.builder \
        .appName("GuaranteedFailureDemo") \
        .master("local[4]") \
        .config("spark.ui.port", "4040") \
        .config("spark.task.maxFailures", "1") \
        .getOrCreate()

def run_guaranteed_failure_demo():
    spark = create_spark_session()
    sc = spark.sparkContext
    
    # Create RDDs that will definitely fail
    print("Setting up guaranteed failure jobs...")
    
    # Method 1: Directly manipulate RDD with guaranteed errors
    # Create a function that will fail for specific partition IDs
    def partition_failure(iterator, partition_id):
        # This ensures the failure happens during execution
        for i, value in enumerate(iterator):
            if partition_id == 0:  # Fail the first partition
                raise RuntimeError(f"Deliberate failure in partition {partition_id}")
            yield value
    
    # Create an RDD and apply the failing function
    print("\nRunning job with guaranteed partition failures...")
    rdd = sc.parallelize(range(1000), 4)  # 4 partitions
    
    # Use mapPartitionsWithIndex to access partition ID (this can't be optimized away)
    failing_rdd = rdd.mapPartitionsWithIndex(partition_failure)
    
    # Execute the job but catch the exception to keep the app running
    try:
        # This will trigger execution and MUST fail
        result = failing_rdd.count()
        print("ERROR: Job should have failed but returned:", result)
    except Exception as e:
        print(f"Job failed as expected: {str(e)}")
    
    # Method 2: Use a different approach with file I/O errors
    print("\nRunning job with file access failures...")
    
    # Create a function that will try to access a non-existent file
    def file_failure(x):
        if x % 25 == 0:  # Only fail for some values
            # Try to open a file that doesn't exist
            with open(f"/non/existent/path/file_{x}.txt", "r") as f:
                return f.read()
        return str(x)
    
    # Create a new RDD and apply the failing function
    file_failing_rdd = rdd.map(file_failure)
    
    try:
        # This will trigger execution and should fail
        result2 = file_failing_rdd.collect()
        print("ERROR: Job should have failed but returned data")
    except Exception as e:
        print(f"File job failed as expected: {str(e)}")
    
    # Keep the application running for UI access
    print("\nFailure jobs have been submitted. Check the Spark UI now.")
    print("Spark UI is available at http://localhost:4040")
    print("Application will remain running for 5 minutes.")
    print("Press Ctrl+C to exit earlier...")
    
    # Stay alive for 5 minutes
    timeout = time.time() + 300  # 5 minutes
    try:
        while time.time() < timeout:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
    
    # Cleanup
    spark.stop()

if __name__ == "__main__":
    run_guaranteed_failure_demo()
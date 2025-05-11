from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum
import time
import os

def run_skewed_logic(spark: SparkSession, db_url: str, db_properties: dict, regions_csv_path: str):
    """
    Contains the core logic for the skewed Spark job.
    Assumes SparkSession is provided.
    """
    print("Skewed Job Logic: Reading transactions table from PostgreSQL...")
    transactions_df = spark.read.jdbc(url=db_url, table="transactions", properties=db_properties)
    transactions_df.persist()
    print(f"Transactions count: {transactions_df.count()}")

    print(f"Skewed Job Logic: Reading regions data from CSV: {regions_csv_path}")
    regions_df = spark.read.csv(regions_csv_path, header=True, inferSchema=True)
    print(f"Regions count: {regions_df.count()}")

    print("Skewed Job Logic: Region distribution in transactions:")
    transactions_df.groupBy("region_id").count().orderBy(col("count").desc()).show(20)

    print("Skewed Job Logic: Starting skewed join and aggregation...")
    job_start_time = time.time()

    joined_df = transactions_df.join(regions_df, transactions_df["region_id"] == regions_df["region_id"])

    aggregated_df = joined_df.groupBy(regions_df["region_name"]) \
        .agg(
            spark_sum("amount").alias("total_sales_amount"),
            count("*").alias("number_of_transactions")
        ) \
        .orderBy(col("number_of_transactions").desc())

    print("Skewed Job Logic: Showing aggregated results (this might be slow or cause OOM)...")
    aggregated_df.show(truncate=False)
    result_count = aggregated_df.count() # Action to trigger computation

    # Simulate writing results
    # output_path = "/tmp/spark_skew_demo_output/skewed_results_logic" # Different path for clarity
    # print(f"Skewed Job Logic: Writing results to {output_path}...")
    # aggregated_df.write.mode("overwrite").parquet(output_path)

    job_end_time = time.time()
    duration = job_end_time - job_start_time
    print(f"Skewed Job Logic: Execution time: {duration:.2f} seconds")

    transactions_df.unpersist()
    return {"execution_time": duration, "record_count": result_count, "job_type": "skewed"}

if __name__ == "__main__":
    # This block allows running the job directly with spark-submit for testing individual job logic
    # But it won't have the collectors from run_collectors.py managed this way.
    # The run_demo.sh will use execute_spark_job.py to run this logic with collectors.
    spark_session = SparkSession.builder \
        .appName("SkewedJoinDemoJob_DirectRun") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .master("local[*]") \
        .getOrCreate()

    print("Direct Run Skewed Job: Spark session created.")

    _db_properties = {
        "user": "demo_user",
        "password": "demo_password",
        "driver": "org.postgresql.Driver"
    }
    _db_url = "jdbc:postgresql://localhost:5433/skew_db"

    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _data_dir = os.path.join(os.path.dirname(_script_dir), "data")
    _regions_csv_path = os.path.join(_data_dir, "regions.csv")

    run_skewed_logic(spark_session, _db_url, _db_properties, _regions_csv_path)

    spark_session.stop()
    print("Direct Run Skewed Job: Spark session stopped.")

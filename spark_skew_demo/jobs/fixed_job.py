from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, broadcast, floor, rand as spark_rand, lit
import time
import os

def run_fixed_logic(spark: SparkSession, db_url: str, db_properties: dict, regions_csv_path: str, solution_type: str):
    """
    Contains the core logic for the fixed Spark job.
    Assumes SparkSession is provided.
    """
    print(f"Fixed Job Logic ({solution_type}): Reading transactions table...")
    transactions_df = spark.read.jdbc(url=db_url, table="transactions", properties=db_properties)
    transactions_df.persist()
    print(f"Transactions count: {transactions_df.count()}")

    print(f"Fixed Job Logic ({solution_type}): Reading regions data from CSV: {regions_csv_path}")
    regions_df = spark.read.csv(regions_csv_path, header=True, inferSchema=True)
    print(f"Regions count: {regions_df.count()}")

    print(f"Fixed Job Logic ({solution_type}): Starting join and aggregation with {solution_type} fix...")
    job_start_time = time.time()

    if solution_type == "broadcast":
        print("Fixed Job Logic: Applying broadcast join.")
        joined_df = transactions_df.join(broadcast(regions_df), transactions_df["region_id"] == regions_df["region_id"])
    elif solution_type == "salting":
        print("Fixed Job Logic: Applying salting.")
        SALT_RANGE = 10  # Number of salt values

        salted_transactions_df = transactions_df.withColumn(
            "salt",
            floor(spark_rand(seed=42) * SALT_RANGE) # Added seed for reproducibility
        )

        exploded_regions_list = []
        for i in range(SALT_RANGE):
            exploded_regions_list.append(
                regions_df.withColumn("salt", lit(i))
            )

        current_salted_df = exploded_regions_list[0]
        for i in range(1, len(exploded_regions_list)):
            current_salted_df = current_salted_df.unionAll(exploded_regions_list[i])
        salted_regions_df = current_salted_df.distinct() # Ensure distinct salt-region pairs if regions has duplicates

        joined_df = salted_transactions_df.join(
            salted_regions_df,
            (salted_transactions_df["region_id"] == salted_regions_df["region_id"]) & \
            (salted_transactions_df["salt"] == salted_regions_df["salt"]),
            "inner"
        ).drop(salted_transactions_df.salt).drop(salted_regions_df.salt)
    else:
        raise ValueError(f"Unknown fix strategy: {solution_type}")

    aggregated_df = joined_df.groupBy(regions_df["region_name"]) \
        .agg(
            spark_sum("amount").alias("total_sales_amount"),
            count("*").alias("number_of_transactions")
        ) \
        .orderBy(col("number_of_transactions").desc())

    print(f"Fixed Job Logic ({solution_type}): Showing aggregated results...")
    aggregated_df.show(truncate=False)
    result_count = aggregated_df.count() # Action

    # output_path = f"/tmp/spark_skew_demo_output/fixed_results_{solution_type}_logic"
    # print(f"Fixed Job Logic ({solution_type}): Writing results to {output_path}...")
    # aggregated_df.write.mode("overwrite").parquet(output_path)

    job_end_time = time.time()
    duration = job_end_time - job_start_time
    print(f"Fixed Job Logic ({solution_type}): Execution time: {duration:.2f} seconds")

    transactions_df.unpersist()
    return {"execution_time": duration, "record_count": result_count, "job_type": f"fixed_{solution_type}"}

if __name__ == "__main__":
    # This block allows running the job directly with spark-submit for testing individual job logic
    strategy = "broadcast"
    if len(sys.argv) > 1 and sys.argv[1] in ["broadcast", "salting"]:
        strategy = sys.argv[1]

    spark_session = SparkSession.builder \
        .appName(f"FixedJoinDemoJob_{strategy}_DirectRun") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .master("local[*]") \
        .getOrCreate()

    print(f"Direct Run Fixed Job ({strategy}): Spark session created.")

    _db_properties = {
        "user": "demo_user",
        "password": "demo_password",
        "driver": "org.postgresql.Driver"
    }
    _db_url = "jdbc:postgresql://localhost:5433/skew_db"

    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _data_dir = os.path.join(os.path.dirname(_script_dir), "data")
    _regions_csv_path = os.path.join(_data_dir, "regions.csv")

    run_fixed_logic(spark_session, _db_url, _db_properties, _regions_csv_path, strategy)

    spark_session.stop()
    print(f"Direct Run Fixed Job ({strategy}): Spark session stopped.")

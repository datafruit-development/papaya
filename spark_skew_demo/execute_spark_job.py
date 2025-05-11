import os
import sys
import time
import argparse
import tempfile
from pyspark.sql import SparkSession

# --- Path Setup for importing collectors and job logic ---
CURRENT_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COLLECTORS_DIR = os.path.join(CURRENT_SCRIPT_DIR, 'collectors')
JOBS_DIR = os.path.join(CURRENT_SCRIPT_DIR, 'jobs')

if COLLECTORS_DIR not in sys.path:
    sys.path.insert(0, COLLECTORS_DIR)
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

try:
    from run_collectors import setup_collectors, start_job_collection, end_job_collection
    from skewed_job import run_skewed_logic
    from fixed_job import run_fixed_logic
except ImportError as e:
    print(f"Failed to import necessary modules: {e}")
    print("Ensure run_collectors.py, skewed_job.py, and fixed_job.py are in their respective directories.")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Run Spark jobs with Papaya collectors.")
    parser.add_argument("job_type", choices=["skewed", "fixed_broadcast", "fixed_salting"],
                        help="Type of Spark job to run.")
    parser.add_argument("--job-name", help="Descriptive name for the job run.", required=True)

    args = parser.parse_args()

    job_unique_id = f"{args.job_name}_{int(time.time())}"

    # Create a dynamic event log directory for this specific job run
    # This can be used by Spark and potentially by CustomLoggingCollector
    event_log_dir = tempfile.mkdtemp(prefix=f"spark-events-{args.job_name}-")
    print(f"Spark event log directory for this run: {event_log_dir}")

    spark = None
    collectors_instance = None
    job_log_dir_for_custom_collector = None # This will be set by setup_collectors

    try:
        spark_builder = SparkSession.builder \
            .appName(f"Demo-{args.job_name}") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", event_log_dir) \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
            .master("local[*]")

        if args.job_type in ["fixed_broadcast", "fixed_salting"]:
             spark_builder.config("spark.sql.adaptive.skewJoin.enabled", "true")

        spark = spark_builder.getOrCreate()
        print(f"Spark session created for {args.job_name}. App ID: {spark.sparkContext.applicationId}")
        print(f"Spark UI: {spark.sparkContext.uiWebUrl or 'Not available'}")


        # Setup Papaya collectors
        # The log_dir_base is where `setup_collectors` will create its job-specific sub-directory
        collectors_instance, job_log_dir_for_custom_collector = setup_collectors(spark, args.job_name, base_log_dir_parent="/tmp/papaya_job_logs")

        # Start collection
        start_job_collection(collectors_instance, job_unique_id)

        # Common DB and data paths
        db_properties = {
            "user": "demo_user",
            "password": "demo_password",
            "driver": "org.postgresql.Driver"
        }
        db_url = "jdbc:postgresql://localhost:5433/skew_db"

        # Path to regions.csv relative to the location of execute_spark_job.py
        data_dir = os.path.join(CURRENT_SCRIPT_DIR, "data")
        regions_csv_path = os.path.join(data_dir, "regions.csv")


        result_stats = None
        job_success_status = False
        job_error_msg = None

        try:
            print(f"Executing logic for job type: {args.job_type}")
            if args.job_type == "skewed":
                result_stats = run_skewed_logic(spark, db_url, db_properties, regions_csv_path)
            elif args.job_type == "fixed_broadcast":
                result_stats = run_fixed_logic(spark, db_url, db_properties, regions_csv_path, solution_type="broadcast")
            elif args.job_type == "fixed_salting":
                result_stats = run_fixed_logic(spark, db_url, db_properties, regions_csv_path, solution_type="salting")
            job_success_status = True
            print(f"Job logic for {args.job_name} completed successfully.")
        except Exception as e_job:
            job_success_status = False
            job_error_msg = str(e_job)
            print(f"Error during Spark job logic for {args.job_name}: {e_job}")
            # We will still try to end collection to get any partial metrics

        # End collection and save metrics
        metrics_file = end_job_collection(
            collectors_instance,
            spark,
            job_unique_id,
            args.job_name,
            job_success=job_success_status,
            job_error=job_error_msg,
            result_stats=result_stats,
            metrics_output_dir_parent="/tmp" # Base directory for "spark_skew_demo_metrics"
        )
        if metrics_file:
            print(f"Metrics for {args.job_name} (ID: {job_unique_id}) saved to: {metrics_file}")
        else:
            print(f"Failed to save metrics for {args.job_name} (ID: {job_unique_id}).")

    except Exception as e:
        print(f"An overall error occurred in execute_spark_job.py for {args.job_name}: {e}")
    finally:
        if spark:
            spark.stop()
            print(f"Spark session for {args.job_name} stopped.")
        # You might want to clean up job_log_dir_for_custom_collector if it's temporary for each run
        # For now, it will persist under /tmp/papaya_job_logs

if __name__ == "__main__":
    main()

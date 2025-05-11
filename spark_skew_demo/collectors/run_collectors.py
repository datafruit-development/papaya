import os
import sys
import json
import time
from datetime import datetime
import tempfile
import logging
from pyspark.sql import SparkSession # Import for type hinting and standalone test

# --- Path Setup ---
# This assumes 'papaya' and 'spark_skew_demo_from_scratch' are sibling directories.
# Adjust if your structure is different.
CURRENT_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEMO_ROOT_DIR = os.path.abspath(os.path.join(CURRENT_SCRIPT_DIR, '..'))
PAPAYA_PROJECT_ROOT = os.path.abspath(os.path.join(DEMO_ROOT_DIR, '..', 'papaya'))
PAPAYA_PROJECT_ROOT = os.path.abspath(os.path.join(DEMO_ROOT_DIR, '..'))
PAPAYA_COLLECTORS_DIR = os.path.join(PAPAYA_PROJECT_ROOT, 'collectors')

if PAPAYA_PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PAPAYA_PROJECT_ROOT)
if PAPAYA_COLLECTORS_DIR not in sys.path:
    sys.path.insert(0, PAPAYA_COLLECTORS_DIR)

# --- Collector Imports ---
try:
    from base_collector import BaseCollector # Expected in papaya/collectors/
    from sparkmeasure_collector import SparkMeasureCollector
    from httpendpoints_collector import SparkHttpCollector
    from custom_collector import CustomLoggingCollector
    from os_metrics_collector import OSMetricsCollector
    # from custom_app_metrics_collector import CustomAppMetricsCollector # If you plan to use it
    # from external_metrics_collector import ExternalMetricsCollector # If you plan to use it
    print("Successfully imported Papaya collectors.")
except ImportError as e:
    print(f"Error importing Papaya collectors: {e}")
    print(f"Please ensure the 'papaya' directory is correctly structured and located at: {PAPAYA_PROJECT_ROOT}")
    print(f"And its 'collectors' subdirectory is at: {PAPAYA_COLLECTORS_DIR}")
    # Fallback to dummy collectors if actual ones can't be imported



# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_collectors")


def setup_collectors(spark: SparkSession, job_name: str, base_log_dir_parent="/tmp"):
    """
    Initializes and sets up all required Papaya collectors.

    Args:
        spark: The active SparkSession.
        job_name: A name for the current job, used for creating a unique log directory.
        base_log_dir_parent: Parent directory where specific job log directories will be created.

    Returns:
        A dictionary of initialized collector instances.
    """
    logger.info("Setting up Papaya debugger collectors...")

    # Create a unique log directory for this specific job/run
    # This helps CustomLoggingCollector focus on relevant logs if it doesn't do so internally
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    dynamic_log_dir = os.path.join(base_log_dir_parent, f"spark_skew_demo_logs_{job_name}_{timestamp_str}")
    os.makedirs(dynamic_log_dir, exist_ok=True)
    logger.info(f"Log directory for CustomLoggingCollector: {dynamic_log_dir}")

    # Ensure Spark's event log is also directed if CustomLoggingCollector uses it
    # This is usually configured in SparkSession builder, but good to be aware
    # spark.conf.set("spark.eventLog.dir", dynamic_log_dir) # Example, might conflict if job already started

    collector_instances = {}
    base_context = {"spark_session": spark}

    try:
        collector_instances["sparkmeasure"] = SparkMeasureCollector(mode="task")
        collector_instances["sparkmeasure"].setup(base_context)
    except Exception as e:
        logger.error(f"Failed to setup SparkMeasureCollector: {e}")

    try:
        # SparkHttpCollector can auto-discover UI URL from SparkSession
        collector_instances["http"] = SparkHttpCollector()
        collector_instances["http"].setup(base_context)
    except Exception as e:
        logger.error(f"Failed to setup SparkHttpCollector: {e}")

    try:
        # CustomLoggingCollector needs a log directory.
        logging_context = {**base_context, "log_dir": dynamic_log_dir}
        collector_instances["logging"] = CustomLoggingCollector(log_dir=dynamic_log_dir)
        collector_instances["logging"].setup(logging_context) # Pass specific log_dir
    except Exception as e:
        logger.error(f"Failed to setup CustomLoggingCollector: {e}")

    try:
        # OSMetricsCollector can run without Prometheus (uses psutil)
        collector_instances["os_metrics"] = OSMetricsCollector()
        collector_instances["os_metrics"].setup({}) # Empty context if no specific config like Prometheus URL
    except Exception as e:
        logger.error(f"Failed to setup OSMetricsCollector: {e}")

    logger.info(f"Initialized collectors: {list(collector_instances.keys())}")
    return collector_instances, dynamic_log_dir

def start_job_collection(collectors: dict, job_id: str):
    """
    Starts the collection process for relevant collectors.

    Args:
        collectors: Dictionary of initialized collector instances.
        job_id: A unique identifier for the Spark job.
    """
    logger.info(f"Starting metrics collection for job_id: {job_id}")
    if "sparkmeasure" in collectors and hasattr(collectors["sparkmeasure"], "begin_collection"):
        collectors["sparkmeasure"].begin_collection(job_id=job_id)
    # Other collectors might have a similar 'begin' method if they track state across a job.

def end_job_collection(
    collectors: dict,
    spark: SparkSession,
    job_id: str,
    job_name: str, # Added for clearer file naming
    job_success: bool = True,
    job_error: str = None,
    result_stats: dict = None,
    metrics_output_dir_parent: str = "/tmp"
):
    """
    Ends the collection process, gathers all metrics, and saves them.

    Args:
        collectors: Dictionary of initialized collector instances.
        spark: The active SparkSession.
        job_id: The unique identifier for the Spark job.
        job_name: The descriptive name of the job (e.g., "skewed_job", "fixed_job_broadcast").
        job_success: Boolean indicating if the job completed successfully.
        job_error: String containing error message if the job failed.
        result_stats: Dictionary of additional statistics about the job's result.
        metrics_output_dir_parent: Parent directory to save the metrics JSON file.

    Returns:
        str: The path to the saved metrics JSON file, or None if saving failed.
    """
    logger.info(f"Ending metrics collection for job_id: {job_id} (Name: {job_name})")
    collected_metrics_data = {
        "job_id": job_id,
        "job_name": job_name,
        "application_id": spark.sparkContext.applicationId,
        "collection_timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "job_success": job_success,
        "result_stats": result_stats or {}
    }
    if job_error:
        collected_metrics_data["job_error"] = job_error

    base_context = {"spark_session": spark, "job_id": job_id}

    # SparkMeasure: end collection
    if "sparkmeasure" in collectors and hasattr(collectors["sparkmeasure"], "end_collection"):
        try:
            sm_metrics = collectors["sparkmeasure"].end_collection()
            collected_metrics_data["sparkmeasure"] = sm_metrics
        except Exception as e:
            logger.error(f"Error finalizing SparkMeasureCollector: {e}")
            collected_metrics_data["sparkmeasure_error"] = str(e)

    # Other collectors: call collect
    for name, collector_instance in collectors.items():
        if name == "sparkmeasure": # Already handled
            continue
        if hasattr(collector_instance, "collect"):
            try:
                metrics = collector_instance.collect(base_context)
                collected_metrics_data[name] = metrics
            except Exception as e:
                logger.error(f"Error collecting from {name}: {e}")
                collected_metrics_data[f"{name}_error"] = str(e)

    # Ensure metrics output directory exists
    metrics_output_dir = os.path.join(metrics_output_dir_parent, "spark_skew_demo_metrics")
    os.makedirs(metrics_output_dir, exist_ok=True)

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    metrics_filename = f"metrics_{job_name}_{job_id}_{timestamp_str}.json"
    metrics_filepath = os.path.join(metrics_output_dir, metrics_filename)

    try:
        with open(metrics_filepath, 'w') as f:
            json.dump(collected_metrics_data, f, indent=4, default=str) # default=str for datetime
        logger.info(f"Aggregated metrics saved to: {metrics_filepath}")
        return metrics_filepath
    except Exception as e:
        logger.error(f"Failed to save metrics to {metrics_filepath}: {e}")
        return None

# --- Main section for standalone testing ---
if __name__ == "__main__":
    logger.info("Running run_collectors.py standalone for testing setup.")

    spark_session = None
    try:
        spark_session = SparkSession.builder \
            .appName("RunCollectorsTest") \
            .master("local[*]") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", tempfile.mkdtemp(prefix="spark-events-collector-test-")) \
            .getOrCreate()

        logger.info(f"Test SparkSession created with App ID: {spark_session.sparkContext.applicationId}")
        logger.info(f"Spark UI: {spark_session.sparkContext.uiWebUrl}")

        test_job_name = "collector_test_job"
        test_job_id = f"{test_job_name}_{int(time.time())}"

        # Test setup_collectors
        initialized_collectors, job_log_dir = setup_collectors(spark_session, test_job_name)
        assert "sparkmeasure" in initialized_collectors
        assert "http" in initialized_collectors
        assert "logging" in initialized_collectors
        assert "os_metrics" in initialized_collectors
        logger.info(f"Log directory for this test run: {job_log_dir}")

        # Test start_job_collection
        start_job_collection(initialized_collectors, test_job_id)
        logger.info("Simulating some Spark work...")
        data = spark_session.range(1000).toDF("id")
        data.show()
        data.groupBy((col("id") % 10).alias("group")).count().show()
        time.sleep(2) # Simulate work

        # Test end_job_collection
        sample_results = {"rows_processed": 1000, "output_path": "/tmp/test_output"}
        metrics_file = end_job_collection(
            initialized_collectors,
            spark_session,
            test_job_id,
            test_job_name,
            job_success=True,
            result_stats=sample_results
        )

        if metrics_file and os.path.exists(metrics_file):
            logger.info(f"Standalone test successful. Metrics saved to: {metrics_file}")
            with open(metrics_file, 'r') as f:
                data = json.load(f)
                assert data["job_id"] == test_job_id
                assert "sparkmeasure" in data
                assert "http" in data
                assert "logging" in data
                assert "os_metrics" in data
        else:
            logger.error("Standalone test failed to produce metrics file.")

    except Exception as e:
        logger.error(f"Error during standalone test: {e}", exc_info=True)
    finally:
        if spark_session:
            spark_session.stop()
            logger.info("Test SparkSession stopped.")

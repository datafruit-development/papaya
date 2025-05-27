import os
import sys
import glob
import warnings
from pyspark import __version__ as spark_version

def _detect_scala_version(spark_ver: str) -> str:
    major, minor = map(int, spark_ver.split(".")[:2])
    # You can customize this mapping based on what versions you build for
    if major == 3 and minor >= 4:
        return "2.13"
    return "2.12"

def _already_started():
    from pyspark.sql import SparkSession
    return SparkSession._instantiatedSession is not None

def load_plugin():
    if _already_started():
        raise RuntimeError("papaya.load_plugin() must be called before SparkSession is created. If you are using pyspark.shell, papaya.load_plugin() must be called before pyspark.shell is imported.")

    spark_ver = spark_version
    scala_ver = _detect_scala_version(spark_ver)

    base_dir = os.path.dirname(__file__)
    jar_dir = os.path.join(base_dir, "plugin_jars")
    jar_pattern = f"spark-{spark_ver[:3]}-scala-{scala_ver}*.jar"

    matching_jars = glob.glob(os.path.join(jar_dir, jar_pattern))
    if not matching_jars:
        raise FileNotFoundError(f"[papaya] No plugin JAR found for Spark {spark_ver[:3]}, Scala {scala_ver} in {jar_dir}")

    jar_path = matching_jars[0]  # First match
    plugin_class = "datafruit.PapayaObservabilityPlugin"

    # Compose Spark submit args
    user_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    inject_args = f"--jars {jar_path} --conf spark.plugins={plugin_class}"

    # Set PYSPARK_SUBMIT_ARGS
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"{inject_args} {user_args} pyspark-shell".strip()

    print(f"[papaya] Plugin loaded: {os.path.basename(jar_path)}")
    print(f"[papaya] Plugin class: {plugin_class}")

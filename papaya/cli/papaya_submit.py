import os
import sys
import glob
import subprocess
from pyspark import __version__ as spark_version


def detect_scala_version(spark_ver: str) -> str:
    major, minor = map(int, spark_ver.split(".")[:2])
    return "2.13" if (major == 3 and minor >= 4) else "2.12"


def main():
    scala_ver = detect_scala_version(spark_version)
    jar_dir = os.path.join(os.path.dirname(__file__), "..", "plugin_jars")
    jar_dir = os.path.abspath(jar_dir)
    jar_pattern = f"spark-{spark_version[:3]}-scala-{scala_ver}*.jar"
    matching_jars = glob.glob(os.path.join(jar_dir, jar_pattern))

    if not matching_jars:
        print(f"[papaya-submit] No JAR found in {jar_dir} for Spark {spark_version[:3]} Scala {scala_ver}")
        sys.exit(1)

    jar_path = matching_jars[0]
    plugin_class = "datafruit.PapayaObservabilityPlugin"

    submit_args = [
        "spark-submit",
        "--jars", jar_path,
        "--conf", f"spark.plugins={plugin_class}",
    ] + sys.argv[1:]

    print(f"[papaya-submit] Running: {' '.join(submit_args)}")
    subprocess.run(submit_args)

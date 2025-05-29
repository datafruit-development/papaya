import papaya

papaya.load_plugin()

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("PapayaPluginTest") \
        .getOrCreate()

    print("\n=== Spark Session Started ===")
    print(f"Spark Version: {spark.version}")
    print("=============================\n")

    # Do something trivial
    df = spark.createDataFrame([(1, "one"), (2, "two")], ["id", "label"])
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()

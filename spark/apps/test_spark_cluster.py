"""
Test Spark Cluster Connectivity & HDFS Access
Computes sum(1..5) = 15 on the Spark cluster and tests HDFS I/O.
"""
import sys
from pyspark.sql import SparkSession

def main():
    print(">>> Initializing Spark Session on cluster...")
    spark = (
        SparkSession.builder
        .appName("InfraTest-SparkCluster")
        .config("spark.driver.memory", "384m")
        .config("spark.executor.memory", "512m")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # 1. Cluster Compute Test: 1 + 2 + 3 + 4 + 5 = 15
    print(">>> Running cluster compute test: 1 + 2 + 3 + 4 + 5")
    numbers = [1, 2, 3, 4, 5]
    rdd = sc.parallelize(numbers, 2)
    total = rdd.reduce(lambda a, b: a + b)
    print(f">>> Result of sum: {total}")

    if total != 15:
        print(f"FAILED: Expected 15, got {total}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    # 2. HDFS Interaction Test
    hdfs_test_path = "hdfs://namenode:9000/wildfire/test/spark_verify.txt"
    print(f">>> Writing test dataframe to HDFS: {hdfs_test_path}")
    df = spark.createDataFrame([("wildfire_infra_verified",)], ["value"])
    df.write.mode("overwrite").text(hdfs_test_path)

    print(">>> Reading back test dataframe from HDFS...")
    read_df = spark.read.text(hdfs_test_path)
    count = read_df.count()
    print(f">>> HDFS records read: {count}")

    if count < 1:
        print("FAILED: HDFS read returned 0 rows", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    print("SUCCESS: Spark cluster compute and HDFS I/O verified successfully!")
    spark.stop()
    sys.exit(0)

if __name__ == "__main__":
    main()

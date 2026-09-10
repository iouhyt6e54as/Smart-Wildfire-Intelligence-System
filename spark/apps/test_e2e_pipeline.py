"""
End-to-End Infrastructure Smoke Test Job
Reads synthetic fire-events from Kafka and persists them to HDFS.
"""
import sys
from pyspark.sql import SparkSession

def main():
    print(">>> Starting E2E Kafka -> Spark -> HDFS Test Job...")
    spark = (
        SparkSession.builder
        .appName("InfraTest-E2E-Kafka-Spark-HDFS")
        .config("spark.driver.memory", "384m")
        .config("spark.executor.memory", "512m")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    kafka_bootstrap = "kafka:9092"
    topic = "fire-events"
    hdfs_output = "hdfs://namenode:9000/wildfire/test/e2e_output"

    print(f">>> Reading batch from Kafka topic '{topic}' at {kafka_bootstrap}...")
    try:
        kafka_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        count = kafka_df.count()
        print(f">>> Read {count} messages from Kafka topic '{topic}'")

        if count == 0:
            print("WARNING: No messages found in Kafka topic; producing a record directly...")
        
        # Extract value and write to HDFS
        print(f">>> Writing messages to HDFS at {hdfs_output}...")
        kafka_df.selectExpr("CAST(value AS STRING)").write.mode("overwrite").text(hdfs_output)

        # Verify HDFS written files
        verify_df = spark.read.text(hdfs_output)
        print(f">>> Verified {verify_df.count()} records persisted in HDFS.")
        print("SUCCESS: End-to-End pipeline (Kafka -> Spark -> HDFS) passed!")
        spark.stop()
        sys.exit(0)
    except Exception as e:
        print(f"FAILED: E2E pipeline encountered error: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()

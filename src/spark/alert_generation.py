from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_json,
    struct,
    when,
    lit,
    concat
)
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType
)


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("WildfireAlertGeneration")
    .config("spark.driver.memory", "512m")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.cores", "1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Kafka Configuration
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"

RISK_TOPIC = "risk_predictions"
ANOMALY_TOPIC = "anomaly_alerts"
OUTPUT_TOPIC = "wildfire_alerts"


print(">>> Starting Wildfire Alert Generation...")
print(f">>> Reading risk predictions from: {RISK_TOPIC}")
print(f">>> Reading anomaly alerts from: {ANOMALY_TOPIC}")


# ============================================================
# Risk Prediction Stream
# ============================================================

risk_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", RISK_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 5)
    .load()
)


risk_schema = StructType([
    StructField("event_timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("brightness", DoubleType(), True),
    StructField("bright_t31", DoubleType(), True),
    StructField("frp", DoubleType(), True),
    StructField("confidence", StringType(), True),
    StructField("daynight", StringType(), True),
    StructField("prediction", DoubleType(), True),
    StructField("risk_level", StringType(), True)
])


risk_data = (
    risk_stream
    .select(
        from_json(
            col("value").cast("string"),
            risk_schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# Generate Alerts from Risk Predictions
# ============================================================

risk_alerts = (
    risk_data
    .filter(
        col("risk_level").isin("High", "Medium")
    )
    .select(
        col("event_timestamp"),
        col("latitude"),
        col("longitude"),
        col("frp"),
        lit(None).cast("double").alias("anomaly_score"),
        col("risk_level"),
        lit(None).cast("string").alias("anomaly_alert_level"),
        when(
            col("risk_level") == "High",
            lit("HIGH")
        )
        .otherwise(
            lit("MEDIUM")
        )
        .alias("alert_level"),
        when(
            col("risk_level") == "High",
            lit("High wildfire risk predicted by ML model")
        )
        .otherwise(
            lit("Medium wildfire risk predicted by ML model")
        )
        .alias("alert_reason"),
        lit("risk_prediction").alias("alert_source")
    )
)


# ============================================================
# Anomaly Stream
# ============================================================

anomaly_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", ANOMALY_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 5)
    .load()
)


anomaly_schema = StructType([
    StructField("event_timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("frp", DoubleType(), True),
    StructField("brightness", DoubleType(), True),
    StructField("bright_t31", DoubleType(), True),
    StructField("scan", DoubleType(), True),
    StructField("track", DoubleType(), True),
    StructField("confidence", StringType(), True),
    StructField("daynight", StringType(), True),
    StructField("anomaly_score", DoubleType(), True),
    StructField("is_anomaly", IntegerType(), True),
    StructField("alert_level", StringType(), True),
    StructField("alert_reason", StringType(), True)
])


anomaly_data = (
    anomaly_stream
    .select(
        from_json(
            col("value").cast("string"),
            anomaly_schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# Generate Alerts from Anomalies
# ============================================================

anomaly_alerts = (
    anomaly_data
    .filter(
        col("is_anomaly") == 1
    )
    .select(
        col("event_timestamp"),
        col("latitude"),
        col("longitude"),
        col("frp"),
        col("anomaly_score"),
        lit(None).cast("string").alias("risk_level"),
        col("alert_level").alias("anomaly_alert_level"),
        col("alert_level").alias("alert_level"),
        col("alert_reason"),
        lit("anomaly_detection").alias("alert_source")
    )
)


# ============================================================
# Combine Risk + Anomaly Alerts
# ============================================================

final_alerts = risk_alerts.unionByName(anomaly_alerts)


# ============================================================
# Add Alert ID / Message
# ============================================================

final_alerts = (
    final_alerts
    .withColumn(
        "alert_message",
        concat(
            lit("WILDFIRE ALERT: "),
            col("alert_reason")
        )
    )
)


# ============================================================
# Convert to Kafka JSON
# ============================================================

kafka_output = (
    final_alerts
    .select(
        to_json(
            struct(
                "event_timestamp",
                "latitude",
                "longitude",
                "frp",
                "anomaly_score",
                "risk_level",
                "anomaly_alert_level",
                "alert_level",
                "alert_source",
                "alert_reason",
                "alert_message"
            )
        ).alias("value")
    )
)


# ============================================================
# Write Alerts to Kafka
# ============================================================

print(
    f">>> Writing generated alerts to Kafka topic: "
    f"{OUTPUT_TOPIC}"
)


query = (
    kafka_output
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_TOPIC)
    .option(
        "checkpointLocation",
        "/tmp/spark-wildfire-alerts-checkpoint"
    )
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)


print(">>> Wildfire Alert Generation is ACTIVE.")
print(">>> Monitoring risk predictions and anomaly alerts...")
print(">>> Alerts are being sent to Kafka.")
print(">>> Press Ctrl+C to stop the streaming job.")


query.awaitTermination()
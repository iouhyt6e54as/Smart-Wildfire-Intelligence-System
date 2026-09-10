#!/usr/bin/env python3

"""
Lab 3 — Real-Time Anomaly Detection Streaming Pipeline

Flow:
Kafka (firms_fire_events)
        ↓
Spark Structured Streaming
        ↓
Load anomaly model configuration from HDFS
        ↓
Calculate Z-score anomaly score
        ↓
Detect anomalies
        ↓
Generate alerts
        ↓
Kafka (anomaly_alerts)
"""

import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
)


# ============================================================
# 1. Spark Session
# ============================================================

def get_spark_session():

    return (
        SparkSession.builder
        .appName("Wildfire-Anomaly-Streaming")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ============================================================
# 2. Kafka Fire Event Schema
# ============================================================

fire_schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("brightness", DoubleType(), True),
    StructField("scan", DoubleType(), True),
    StructField("track", DoubleType(), True),

    StructField("acq_date", StringType(), True),
    StructField("acq_time", StringType(), True),

    StructField("satellite", StringType(), True),
    StructField("instrument", StringType(), True),

    StructField("confidence", StringType(), True),
    StructField("version", StringType(), True),

    StructField("bright_t31", DoubleType(), True),
    StructField("frp", DoubleType(), True),

    StructField("daynight", StringType(), True),
    StructField("type", IntegerType(), True),
])


# ============================================================
# 3. Load Anomaly Model Configuration
# ============================================================

def load_anomaly_model(spark, model_path):

    print(">>> Loading anomaly model configuration...")

    # Read the JSON file from HDFS
    model_df = spark.read.text(model_path)

    # Collect all lines
    lines = [
        row["value"]
        for row in model_df.collect()
    ]

    # Reconstruct JSON
    model_json = "\n".join(lines)

    model_config = json.loads(model_json)

    print(">>> Anomaly model loaded successfully")

    print(
        f">>> Model type: "
        f"{model_config.get('model_type')}"
    )

    print(
        f">>> Threshold: "
        f"{model_config.get('threshold')}"
    )

    print(
        f">>> Training rows: "
        f"{model_config.get('training_rows')}"
    )

    return model_config


# ============================================================
# 4. Apply Anomaly Detection
# ============================================================

def calculate_anomaly_score(df, model_config):

    features = model_config["features"]

    threshold = float(
        model_config["threshold"]
    )

    statistics = model_config["statistics"]

    scored_df = df

    z_columns = []

    # --------------------------------------------------------
    # Calculate absolute Z-score for every feature
    # --------------------------------------------------------

    for feature in features:

        mean_value = float(
            statistics[feature]["mean"]
        )

        std_value = float(
            statistics[feature]["std"]
        )

        # Prevent division by zero
        if std_value == 0:
            std_value = 1.0

        z_column = f"{feature}_z"

        scored_df = scored_df.withColumn(
            z_column,
            F.abs(
                (
                    F.col(feature)
                    - F.lit(mean_value)
                )
                / F.lit(std_value)
            )
        )

        z_columns.append(z_column)

    # --------------------------------------------------------
    # Anomaly score = maximum Z-score
    # --------------------------------------------------------

    scored_df = scored_df.withColumn(
        "anomaly_score",
        F.greatest(
            *[
                F.col(column)
                for column in z_columns
            ]
        )
    )

    # --------------------------------------------------------
    # Anomaly classification
    # --------------------------------------------------------

    scored_df = scored_df.withColumn(
        "is_anomaly",
        F.when(
            F.col("anomaly_score") >= threshold,
            1
        )
        .otherwise(0)
    )

    return scored_df


# ============================================================
# 5. Generate Alert Information
# ============================================================

def generate_alerts(df):

    alerts = (
        df
        .filter(
            F.col("is_anomaly") == 1
        )
        .withColumn(
            "alert_level",
            F.when(
                F.col("anomaly_score") >= 5,
                "CRITICAL"
            )
            .when(
                F.col("anomaly_score") >= 4,
                "HIGH"
            )
            .otherwise(
                "MEDIUM"
            )
        )
        .withColumn(
            "alert_reason",
            F.concat(
                F.lit("Unusual wildfire observation detected. "),
                F.lit("Anomaly score = "),
                F.round(
                    F.col("anomaly_score"),
                    2
                ).cast(StringType())
            )
        )
    )

    return alerts


# ============================================================
# 6. Main Streaming Pipeline
# ============================================================

def main():

    spark = get_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print(
        ">>> Starting Wildfire Anomaly Streaming Pipeline..."
    )

    try:

        # ----------------------------------------------------
        # Load trained anomaly model
        # ----------------------------------------------------

        model_path = (
            "hdfs://namenode:9000/"
            "wildfire/models/"
            "anomaly_detector/"
            "anomaly_model.json"
        )

        model_config = load_anomaly_model(
            spark,
            model_path
        )

        # ----------------------------------------------------
        # Read Kafka stream
        # ----------------------------------------------------

        print(
            ">>> Connecting to Kafka topic "
            "'firms_fire_events'..."
        )

        kafka_df = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                "kafka:9092"
            )
            .option(
                "subscribe",
                "firms_fire_events"
            )
            .option(
                "startingOffsets",
                "earliest"
            )
            .option(
                "failOnDataLoss",
                "false"
            )
            .load()
        )

        # ----------------------------------------------------
        # Convert Kafka value from binary to string
        # ----------------------------------------------------

        json_df = (
            kafka_df
            .select(
                F.col("value")
                .cast("string")
                .alias("json_value")
            )
        )

        # ----------------------------------------------------
        # Parse JSON
        # ----------------------------------------------------

        events_df = (
            json_df
            .select(
                F.from_json(
                    F.col("json_value"),
                    fire_schema
                ).alias("data")
            )
            .select("data.*")
        )

        # ----------------------------------------------------
        # Create event timestamp
        # ----------------------------------------------------

        events_df = events_df.withColumn(
            "event_timestamp",
            F.to_timestamp(
                F.concat(
                    F.col("acq_date"),
                    F.lit(" "),
                    F.col("acq_time")
                ),
                "yyyy-MM-dd HH:mm"
            )
        )

        # ----------------------------------------------------
        # Calculate anomaly score
        # ----------------------------------------------------

        scored_df = calculate_anomaly_score(
            events_df,
            model_config
        )

        # ----------------------------------------------------
        # Generate anomaly alerts
        # ----------------------------------------------------

        alerts_df = generate_alerts(
            scored_df
        )

        # ----------------------------------------------------
        # Select final alert fields
        # ----------------------------------------------------

        final_alerts = (
            alerts_df
            .select(
                "event_timestamp",
                "latitude",
                "longitude",
                "frp",
                "brightness",
                "bright_t31",
                "scan",
                "track",
                "confidence",
                "daynight",
                "anomaly_score",
                "is_anomaly",
                "alert_level",
                "alert_reason"
            )
        )

        # ----------------------------------------------------
        # Convert alert to JSON
        # ----------------------------------------------------

        kafka_alerts = (
            final_alerts
            .select(
                F.to_json(
                    F.struct(
                        "*"
                    )
                ).alias("value")
            )
        )

        # ----------------------------------------------------
        # Write alerts to Kafka
        # ----------------------------------------------------

        print(
            ">>> Writing anomaly alerts to Kafka topic "
            "'anomaly_alerts'..."
        )

        query = (
            kafka_alerts
            .writeStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                "kafka:9092"
            )
            .option(
                "topic",
                "anomaly_alerts"
            )
            .option(
                "checkpointLocation",
                "/tmp/spark-anomaly-checkpoint"
            )
            .outputMode("append")
            .trigger(
                processingTime="5 seconds"
            )
            .start()
        )

        print(
            ">>> Anomaly streaming pipeline is ACTIVE."
        )

        print(
            ">>> Listening for wildfire anomalies..."
        )

        query.awaitTermination()

    except KeyboardInterrupt:

        print(
            ">>> Streaming stopped by user."
        )

    finally:

        spark.stop()

        print(
            ">>> Spark session stopped."
        )


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":

    main()
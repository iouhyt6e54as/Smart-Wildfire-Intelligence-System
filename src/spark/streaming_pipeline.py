#!/usr/bin/env python3
"""
Spark Structured Streaming Ingestion Pipeline for Smart Wildfire Intelligence System.
Consumes real-time events from Kafka topics:
  - firms_fire_events -> Parses with Canonical Fire Schema -> Enriches -> Writes to HDFS
  - environmental_data -> Parses with Environmental Schema -> Writes to HDFS
Uses explicit schemas, dedicated streaming storage, and checkpointing.
"""

import argparse
import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType, IntegerType, TimestampType
)

# Explicit JSON Schemas for Streaming Ingestion
FIRE_EVENT_STREAM_SCHEMA = StructType([
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

ENVIRONMENTAL_STREAM_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("rainfall", DoubleType(), True),
    StructField("smoke", DoubleType(), True),
    StructField("air_quality_index", IntegerType(), True),
    StructField("source", StringType(), True),
])

def get_spark_session(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "384m")
        .config("spark.executor.memory", "768m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def start_fire_streaming(spark, args):
    print(f">>> [STREAM 1] Initializing Fire Events Stream from topic '{args.fire_topic}'...")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.fire_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON value
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), FIRE_EVENT_STREAM_SCHEMA).alias("data"))
        .select("data.*")
    )

    # Validate coordinates & derive event_timestamp
    valid_fire = (
        parsed
        .filter(
            (F.col("latitude").between(-90.0, 90.0)) &
            (F.col("longitude").between(-180.0, 180.0))
        )
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.concat(F.col("acq_date"), F.lit(" "), F.col("acq_time")), "yyyy-MM-dd HH:mm")
        )
        .withColumn("year", F.year(F.col("event_timestamp")))
        .withColumn("month", F.month(F.col("event_timestamp")))
        .withColumn("day", F.dayofmonth(F.col("event_timestamp")))
        .withColumn("hour", F.hour(F.col("event_timestamp")))
        .withColumn("is_night", F.when(F.col("daynight") == "N", 1).otherwise(0))
        .withColumn("latitude_bucket", F.round(F.col("latitude"), 1))
        .withColumn("longitude_bucket", F.round(F.col("longitude"), 1))
    )

    # Write to HDFS Parquet
    query = (
        valid_fire.writeStream
        .format("parquet")
        .option("path", args.output_fire)
        .option("checkpointLocation", args.checkpoint_fire)
        .partitionBy("year", "month")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )
    return query

def start_environmental_streaming(spark, args):
    print(f">>> [STREAM 2] Initializing Environmental Telemetry Stream from topic '{args.env_topic}'...")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.env_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), ENVIRONMENTAL_STREAM_SCHEMA).alias("data"))
        .select("data.*")
    )

    valid_env = (
        parsed
        .filter(
            (F.col("latitude").between(-90.0, 90.0)) &
            (F.col("longitude").between(-180.0, 180.0))
        )
        .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("year", F.year(F.col("event_timestamp")))
        .withColumn("month", F.month(F.col("event_timestamp")))
        .withColumn("day", F.dayofmonth(F.col("event_timestamp")))
    )

    query = (
        valid_env.writeStream
        .format("parquet")
        .option("path", args.output_env)
        .option("checkpointLocation", args.checkpoint_env)
        .partitionBy("year", "month")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )
    return query

def main():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Pipeline")
    parser.add_argument("--mode", choices=["fire", "environmental", "both"], default="both", help="Streams to run")
    parser.add_argument("--kafka-bootstrap", default="kafka:9092", help="Kafka bootstrap server")
    parser.add_argument("--fire-topic", default="firms_fire_events", help="Fire topic name")
    parser.add_argument("--env-topic", default="environmental_data", help="Environmental topic name")
    parser.add_argument("--output-fire", default="hdfs://namenode:9000/wildfire/curated/streaming_fire", help="HDFS path for streaming fire")
    parser.add_argument("--output-env", default="hdfs://namenode:9000/wildfire/curated/streaming_environmental", help="HDFS path for streaming environmental")
    parser.add_argument("--checkpoint-fire", default="hdfs://namenode:9000/wildfire/checkpoints/firms_fire_events", help="Fire stream checkpoint")
    parser.add_argument("--checkpoint-env", default="hdfs://namenode:9000/wildfire/checkpoints/environmental_data", help="Env stream checkpoint")
    parser.add_argument("--duration", type=int, default=0, help="Run duration in seconds (0 for indefinite)")
    args = parser.parse_args()

    spark = get_spark_session(f"Wildfire-Streaming-{args.mode}")
    spark.sparkContext.setLogLevel("WARN")

    queries = []
    if args.mode in ["fire", "both"]:
        q_fire = start_fire_streaming(spark, args)
        queries.append(q_fire)

    if args.mode in ["environmental", "both"]:
        q_env = start_environmental_streaming(spark, args)
        queries.append(q_env)

    print(f">>> Streaming pipeline active. Running mode: '{args.mode}', duration: {args.duration}s...")

    try:
        if args.duration > 0:
            time.sleep(args.duration)
            print(f">>> Duration timeout reached ({args.duration}s). Stopping streaming queries...")
            for q in queries:
                q.stop()
        else:
            for q in queries:
                q.awaitTermination()
    except KeyboardInterrupt:
        print("\n>>> Streaming stopped by user.")
        for q in queries:
            q.stop()
    finally:
        spark.stop()
        print(">>> Spark streaming session terminated cleanly.")

if __name__ == "__main__":
    main()

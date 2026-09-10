#!/usr/bin/env python3
"""
Lab 4 — Batch load: HDFS curated Parquet -> PostgreSQL wildfire.fire_events

Reads the curated FIRMS Parquet dataset written by Lab 1's Spark cleaning
pipeline and loads it into Postgres.
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/src/spark")
from postgres_sink import write_df_to_postgres  # noqa: E402


HDFS_CURATED_DIR = "hdfs://namenode:9000/wildfire/curated"


FIRE_EVENTS_COLUMNS = [
    "event_timestamp",
    "latitude",
    "longitude",
    "brightness",
    "bright_t31",
    "scan",
    "track",
    "frp",
    "confidence",
    "daynight",
    "satellite",
    "instrument",
    "version",
    "type",
    "year",
]


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        required=True,
        help="Years to load, e.g. --years 2024 2025 2026",
    )

    parser.add_argument(
        "--mode",
        choices=["append", "overwrite"],
        default="overwrite",
        help=(
            "overwrite (default): TRUNCATE + reload. "
            "append: insert only."
        ),
    )

    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("LoadCuratedToPostgres")
        .master("spark://spark-master:7077")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "768m")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("Lab 4 - HDFS Curated Parquet -> PostgreSQL")
    print("=" * 70)
    print(f">>> Years requested: {args.years}")
    print(f">>> Mode: {args.mode}")
    print(f">>> Reading: {HDFS_CURATED_DIR}")

    # Read only the requested partition directories.
    year_paths = [
        f"{HDFS_CURATED_DIR}/year={year}"
        for year in args.years
    ]

    existing_paths = []

    for path in year_paths:
        print(f">>> Checking: {path}")

        try:
            test_df = spark.read.parquet(path)
            count = test_df.count()

            print(f">>> Found {count} rows")
            existing_paths.append(path)

        except Exception as e:
            print(f">>> WARNING: Could not read {path}")
            print(f">>> Reason: {str(e).splitlines()[0]}")

    if not existing_paths:
        print(">>> No data found for the requested years.")
        spark.stop()
        return

    print(">>> Reading existing year partitions...")

    df = spark.read.parquet(*existing_paths)

    # Make sure year exists even if it was not stored as a physical column.
    if "year" not in df.columns:
        df = df.withColumn(
            "year",
            F.year(F.col("event_timestamp"))
        )

    row_count = df.count()

    print(f">>> Loaded {row_count} rows from HDFS")

    if row_count == 0:
        print(">>> Nothing to load, exiting.")
        spark.stop()
        return

    # Derive season and is_night.
    out_df = (
        df
        .withColumn(
            "season",
            F.when(
                F.month("event_timestamp").isin(12, 1, 2),
                "Winter",
            )
            .when(
                F.month("event_timestamp").isin(3, 4, 5),
                "Spring",
            )
            .when(
                F.month("event_timestamp").isin(6, 7, 8),
                "Summer",
            )
            .otherwise("Fall"),
        )
        .withColumn(
            "is_night",
            F.when(F.col("daynight") == "N", 1).otherwise(0),
        )
        .select(
            *FIRE_EVENTS_COLUMNS,
            "season",
            "is_night",
        )
        .dropDuplicates(
            [
                "event_timestamp",
                "latitude",
                "longitude",
            ]
        )
    )

    final_count = out_df.count()

    print(
        f">>> Writing {final_count} deduplicated rows "
        f"-> wildfire.fire_events "
        f"(mode={args.mode})"
    )

    write_df_to_postgres(
        out_df,
        "fire_events",
        mode=args.mode,
    )

    print(">>> Load complete.")
    spark.stop()


if __name__ == "__main__":
    main()
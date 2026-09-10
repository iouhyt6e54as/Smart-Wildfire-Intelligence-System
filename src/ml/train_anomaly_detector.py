#!/usr/bin/env python3
"""
Lab 3 — Anomaly Detection

Detect unusual wildfire observations using statistical anomaly scores.

The anomaly score is based on how unusual the fire observation is
compared with historical wildfire observations.

Features:
- FRP
- brightness
- bright_t31
- scan
- track

The model learns mean and standard deviation from historical data.
"""

import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark_session():
    return (
        SparkSession.builder
        .appName("Wildfire-Anomaly-Detection")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_features(spark, features_dir, years):
    df = spark.read.parquet(features_dir)

    return df.filter(
        F.col("year").isin(years)
    )


def main():

    parser = argparse.ArgumentParser(
        description="Train wildfire anomaly detection model"
    )

    parser.add_argument(
        "--features-dir",
        default="hdfs://namenode:9000/wildfire/features"
    )

    parser.add_argument(
        "--models-dir",
        default="hdfs://namenode:9000/wildfire/models"
    )

    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=[2020, 2021, 2022, 2023, 2024, 2025]
    )

    args = parser.parse_args()

    start_time = time.time()

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:

        # --------------------------------------------------
        # 1. Read historical wildfire features
        # --------------------------------------------------

        print(">>> Reading wildfire features...")

        df = read_features(
            spark,
            args.features_dir,
            args.years
        )

        print(">>> Features loaded successfully")

        row_count = df.count()

        print(f">>> Number of rows: {row_count}")

        if row_count == 0:
            raise RuntimeError(
                "No wildfire feature rows found."
            )

        # --------------------------------------------------
        # 2. Features used for anomaly detection
        # --------------------------------------------------

        anomaly_features = [
            "frp",
            "brightness",
            "bright_t31",
            "scan",
            "track"
        ]

        print(">>> Anomaly features:")

        for feature in anomaly_features:
            print(f"    - {feature}")

        # --------------------------------------------------
        # 3. Calculate historical statistics
        # --------------------------------------------------

        print(">>> Calculating historical statistics...")

        stats = (
            df
            .select(
                [
                    F.mean(F.col(feature)).alias(f"{feature}_mean")
                    for feature in anomaly_features
                ]
                +
                [
                    F.stddev(F.col(feature)).alias(f"{feature}_std")
                    for feature in anomaly_features
                ]
            )
            .collect()[0]
        )

        statistics = {}

        for feature in anomaly_features:

            mean_value = stats[f"{feature}_mean"]
            std_value = stats[f"{feature}_std"]

            if mean_value is None:
                mean_value = 0.0

            if std_value is None or std_value == 0:
                std_value = 1.0

            statistics[feature] = {
                "mean": float(mean_value),
                "std": float(std_value)
            }

            print(
                f">>> {feature}: "
                f"mean={mean_value:.4f}, "
                f"std={std_value:.4f}"
            )

        # --------------------------------------------------
        # 4. Calculate anomaly score
        # --------------------------------------------------

        print(">>> Calculating anomaly scores...")

        scored_df = df

        z_score_columns = []

        for feature in anomaly_features:

            mean_value = statistics[feature]["mean"]
            std_value = statistics[feature]["std"]

            z_column = f"{feature}_z"

            scored_df = scored_df.withColumn(
                z_column,
                F.abs(
                    (
                        F.col(feature) - F.lit(mean_value)
                    ) / F.lit(std_value)
                )
            )

            z_score_columns.append(z_column)

        scored_df = scored_df.withColumn(
            "anomaly_score",
            F.greatest(*[
                F.col(column)
                for column in z_score_columns
            ])
        )

        # --------------------------------------------------
        # 5. Define anomaly threshold
        # --------------------------------------------------

        anomaly_threshold = 3.0

        scored_df = scored_df.withColumn(
            "is_anomaly",
            F.when(
                F.col("anomaly_score") >= anomaly_threshold,
                1
            )
            .otherwise(0)
        )

        # --------------------------------------------------
        # 6. Check anomaly distribution
        # --------------------------------------------------

        print(">>> Checking anomaly distribution...")

        distribution = (
            scored_df
            .groupBy("is_anomaly")
            .count()
            .collect()
        )

        for row in distribution:
            print(
                f">>> is_anomaly={row['is_anomaly']} "
                f"count={row['count']}"
            )

        # --------------------------------------------------
        # 7. Save anomaly configuration
        # --------------------------------------------------

        model_config = {
            "model_type": "statistical_z_score",
            "features": anomaly_features,
            "threshold": anomaly_threshold,
            "statistics": statistics,
            "training_years": args.years,
            "training_rows": row_count,
            "generated_at": time.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "duration_seconds": round(
                time.time() - start_time,
                2
            )
        }

        local_path = "/opt/reports_anomaly_model.json"

        with open(local_path, "w") as f:
            json.dump(
                model_config,
                f,
                indent=2
            )

        print(
            f">>> Anomaly model configuration saved to "
            f"{local_path}"
        )

        print(
            f">>> Training completed in "
            f"{model_config['duration_seconds']} seconds"
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
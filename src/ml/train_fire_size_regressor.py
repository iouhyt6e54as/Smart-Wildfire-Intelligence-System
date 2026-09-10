#!/usr/bin/env python3
"""
Lab 2 — Fire Size Regression (Random Forest vs GBT)

NASA FIRMS has no direct "fire size / burned area" field, so this model
predicts Fire Radiative Power (FRP, in Megawatts) as a continuous proxy
for fire intensity/size, from spatial/temporal/satellite-observation
features only (`frp` itself is never used as a feature).

Pipeline:
  1. Read curated feature Parquet from HDFS /wildfire/features (partitioned by year)
  2. Time-based split: train on past years, validate/test on future years
  3. Build the SAME shared preprocessing Pipeline used by the risk classifier
  4. Train RandomForestRegressor and GBTRegressor
  5. Evaluate both (RMSE, MAE, R^2) on validation + test
  6. Save: preprocessing pipeline model, both trained models, comparison report

Run inside the Spark container:

  docker exec spark-master /spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      /opt/src/ml/train_fire_size_regressor.py \
      --years-train 2020 2021 2022 2023 2024 \
      --years-val 2025 \
      --years-test 2026
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor

try:
    from src.ml.ml_config import (
        FRP_COLUMN, REGRESSION_LABEL_COL,
        DEFAULT_TRAIN_YEARS, DEFAULT_VAL_YEARS, DEFAULT_TEST_YEARS,
        DEFAULT_FEATURES_DIR, DEFAULT_MODELS_DIR, DEFAULT_REPORTS_DIR,
        FIRE_SIZE_PIPELINE_MODEL_NAME, FIRE_SIZE_RF_MODEL_NAME, FIRE_SIZE_GBT_MODEL_NAME,
    )
    from src.ml.preprocessing import clean_for_ml, build_feature_pipeline_stages
    from src.ml.metrics_utils import evaluate_regressor, save_json_report
    from src.utils.logging import setup_logger
except ImportError:
    from ml.ml_config import (
        FRP_COLUMN, REGRESSION_LABEL_COL,
        DEFAULT_TRAIN_YEARS, DEFAULT_VAL_YEARS, DEFAULT_TEST_YEARS,
        DEFAULT_FEATURES_DIR, DEFAULT_MODELS_DIR, DEFAULT_REPORTS_DIR,
        FIRE_SIZE_PIPELINE_MODEL_NAME, FIRE_SIZE_RF_MODEL_NAME, FIRE_SIZE_GBT_MODEL_NAME,
    )
    from ml.preprocessing import clean_for_ml, build_feature_pipeline_stages
    from ml.metrics_utils import evaluate_regressor, save_json_report
    from utils.logging import setup_logger

logger = setup_logger("FireSizeRegressorTraining")


def get_spark_session():
    return (
        SparkSession.builder
        .appName("Wildfire-FireSize-Regressor-Training")
        .config("spark.driver.memory", "384m")
        .config("spark.executor.memory", "768m")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_features(spark, features_dir, years):
    df = spark.read.parquet(features_dir)
    return df.filter(df["year"].isin(years))


def main():
    parser = argparse.ArgumentParser(description="Train Fire Size (FRP) Regressor (RF vs GBT)")
    parser.add_argument("--features-dir", default=DEFAULT_FEATURES_DIR)
    parser.add_argument("--models-dir", default=DEFAULT_MODELS_DIR)
    parser.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--years-train", type=int, nargs="+", default=DEFAULT_TRAIN_YEARS)
    parser.add_argument("--years-val", type=int, nargs="+", default=DEFAULT_VAL_YEARS)
    parser.add_argument("--years-test", type=int, nargs="+", default=DEFAULT_TEST_YEARS)
    parser.add_argument("--rf-num-trees", type=int, default=100)
    parser.add_argument("--rf-max-depth", type=int, default=8)
    parser.add_argument("--gbt-max-iter", type=int, default=50)
    parser.add_argument("--gbt-max-depth", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    start_time = time.time()
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info(f"Reading features from {args.features_dir}")
        train_df = read_features(spark, args.features_dir, args.years_train)
        val_df = read_features(spark, args.features_dir, args.years_val)
        test_df = read_features(spark, args.features_dir, args.years_test)

        train_df = clean_for_ml(train_df).filter(train_df[FRP_COLUMN].isNotNull())
        val_df = clean_for_ml(val_df).filter(val_df[FRP_COLUMN].isNotNull())
        test_df = clean_for_ml(test_df).filter(test_df[FRP_COLUMN].isNotNull())

        train_count, val_count, test_count = train_df.count(), val_df.count(), test_df.count()
        logger.info(f"Rows -> train={train_count}, val={val_count}, test={test_count}")
        if train_count == 0:
            raise RuntimeError(
                "No training rows found. Did you run Lab 1's batch_pipeline.py for the "
                f"train years {args.years_train} yet? Check {args.features_dir}."
            )

        # Shared feature pipeline — same features/encoding as the risk classifier,
        # frp is the target here so it is naturally excluded (not in NUMERIC_FEATURES).
        feature_stages, assembler_inputs = build_feature_pipeline_stages()
        pipeline = Pipeline(stages=feature_stages)
        pipeline_model = pipeline.fit(train_df)

        train_feat = pipeline_model.transform(train_df).cache()
        val_feat = pipeline_model.transform(val_df).cache()
        test_feat = pipeline_model.transform(test_df).cache()
        train_feat.count()

        logger.info("Training RandomForestRegressor...")
        rf = RandomForestRegressor(
            labelCol=REGRESSION_LABEL_COL, featuresCol="features",
            numTrees=args.rf_num_trees, maxDepth=args.rf_max_depth, seed=args.seed,
        )
        rf_model = rf.fit(train_feat)

        logger.info("Training GBTRegressor...")
        gbt = GBTRegressor(
            labelCol=REGRESSION_LABEL_COL, featuresCol="features",
            maxIter=args.gbt_max_iter, maxDepth=args.gbt_max_depth, seed=args.seed,
        )
        gbt_model = gbt.fit(train_feat)

        report = {
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": None,
            "target": f"{REGRESSION_LABEL_COL} (Fire Radiative Power, MW — proxy for fire size/intensity)",
            "train_years": args.years_train,
            "val_years": args.years_val,
            "test_years": args.years_test,
            "row_counts": {"train": train_count, "val": val_count, "test": test_count},
            "models": {},
        }

        for model_name, model in [("random_forest", rf_model), ("gbt", gbt_model)]:
            val_pred = model.transform(val_feat)
            test_pred = model.transform(test_feat)
            report["models"][model_name] = {
                "validation": evaluate_regressor(val_pred, label_col=REGRESSION_LABEL_COL),
                "test": evaluate_regressor(test_pred, label_col=REGRESSION_LABEL_COL),
            }
            logger.info(
                f"{model_name} -> test RMSE={report['models'][model_name]['test']['rmse']}, "
                f"R2={report['models'][model_name]['test']['r2']}"
            )

        best_model_name = min(
            report["models"], key=lambda m: report["models"][m]["test"]["rmse"]
        )
        report["best_model_by_test_rmse"] = best_model_name
        report["duration_seconds"] = round(time.time() - start_time, 2)

        pipeline_path = f"{args.models_dir}/{FIRE_SIZE_PIPELINE_MODEL_NAME}"
        rf_path = f"{args.models_dir}/{FIRE_SIZE_RF_MODEL_NAME}"
        gbt_path = f"{args.models_dir}/{FIRE_SIZE_GBT_MODEL_NAME}"

        logger.info(f"Saving preprocessing pipeline to {pipeline_path}")
        pipeline_model.write().overwrite().save(pipeline_path)
        logger.info(f"Saving Random Forest model to {rf_path}")
        rf_model.write().overwrite().save(rf_path)
        logger.info(f"Saving GBT model to {gbt_path}")
        gbt_model.write().overwrite().save(gbt_path)

        report_path = save_json_report(report, args.reports_dir, "fire_size_regressor_report.json")
        logger.info(f"Saved comparison report to {report_path}")
        logger.info(f"Best model by test RMSE: {best_model_name}")
        logger.info(f"Lab 2 fire size regressor training complete in {report['duration_seconds']}s")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

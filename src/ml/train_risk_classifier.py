#!/usr/bin/env python3
"""
Lab 2 — Wildfire Risk Classification (Random Forest vs GBT)

Pipeline:
  1. Read curated feature Parquet from HDFS /wildfire/features (partitioned by year)
  2. Time-based split: train on past years, validate/test on future years
  3. Derive `risk_level` (Low/Medium/High) from FRP quantiles computed on TRAIN ONLY
  4. Build a shared preprocessing Pipeline (StringIndexer/OneHotEncoder/VectorAssembler)
  5. Train RandomForestClassifier and GBTClassifier (wrapped in OneVsRest for multiclass,
     since Spark MLlib's GBTClassifier only supports binary classification natively)
  6. Evaluate both on validation + test splits (accuracy, F1, precision/recall, confusion matrix)
  7. Save: preprocessing pipeline model, both trained models, label thresholds,
     and a JSON comparison report — all fixed artifacts for Lab 3 to reuse.

Run inside the Spark container (same pattern as Lab 1's batch_pipeline.py):

  docker exec spark-master /spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      /opt/src/ml/train_risk_classifier.py \
      --years-train 2020 2021 2022 2023 2024 \
      --years-val 2025 \
      --years-test 2026
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier, OneVsRest

try:
    from src.ml.ml_config import (
        RISK_LABEL_COL, RISK_LABEL_INDEX_COL, FRP_COLUMN,
        DEFAULT_TRAIN_YEARS, DEFAULT_VAL_YEARS, DEFAULT_TEST_YEARS,
        DEFAULT_FEATURES_DIR, DEFAULT_MODELS_DIR, DEFAULT_REPORTS_DIR,
        RISK_PIPELINE_MODEL_NAME, RISK_RF_MODEL_NAME, RISK_GBT_MODEL_NAME,
        DEFAULT_LOW_QUANTILE, DEFAULT_HIGH_QUANTILE,
    )
    from src.ml.label_engineering import (
        compute_frp_quantile_thresholds, apply_risk_label, label_distribution,
    )
    from src.ml.preprocessing import clean_for_ml, build_feature_pipeline_stages
    from src.ml.metrics_utils import evaluate_classifier, save_json_report
    from src.utils.logging import setup_logger
except ImportError:
    from ml.ml_config import (
        RISK_LABEL_COL, RISK_LABEL_INDEX_COL, FRP_COLUMN,
        DEFAULT_TRAIN_YEARS, DEFAULT_VAL_YEARS, DEFAULT_TEST_YEARS,
        DEFAULT_FEATURES_DIR, DEFAULT_MODELS_DIR, DEFAULT_REPORTS_DIR,
        RISK_PIPELINE_MODEL_NAME, RISK_RF_MODEL_NAME, RISK_GBT_MODEL_NAME,
        DEFAULT_LOW_QUANTILE, DEFAULT_HIGH_QUANTILE,
    )
    from ml.label_engineering import (
        compute_frp_quantile_thresholds, apply_risk_label, label_distribution,
    )
    from ml.preprocessing import clean_for_ml, build_feature_pipeline_stages
    from ml.metrics_utils import evaluate_classifier, save_json_report
    from utils.logging import setup_logger

logger = setup_logger("RiskClassifierTraining")


def get_spark_session():
    return (
        SparkSession.builder
        .appName("Wildfire-Risk-Classifier-Training")
        .config("spark.driver.memory", "384m")
        .config("spark.executor.memory", "768m")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_features(spark, features_dir, years):
    df = spark.read.parquet(features_dir)
    return df.filter(df["year"].isin(years))


def main():
    parser = argparse.ArgumentParser(description="Train Wildfire Risk Classifier (RF vs GBT)")
    parser.add_argument("--features-dir", default=DEFAULT_FEATURES_DIR)
    parser.add_argument("--models-dir", default=DEFAULT_MODELS_DIR)
    parser.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--years-train", type=int, nargs="+", default=DEFAULT_TRAIN_YEARS)
    parser.add_argument("--years-val", type=int, nargs="+", default=DEFAULT_VAL_YEARS)
    parser.add_argument("--years-test", type=int, nargs="+", default=DEFAULT_TEST_YEARS)
    parser.add_argument("--low-quantile", type=float, default=DEFAULT_LOW_QUANTILE)
    parser.add_argument("--high-quantile", type=float, default=DEFAULT_HIGH_QUANTILE)
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
        train_raw = read_features(spark, args.features_dir, args.years_train)
        val_raw = read_features(spark, args.features_dir, args.years_val)
        test_raw = read_features(spark, args.features_dir, args.years_test)

        train_raw = clean_for_ml(train_raw).filter(train_raw[FRP_COLUMN].isNotNull())
        val_raw = clean_for_ml(val_raw).filter(val_raw[FRP_COLUMN].isNotNull())
        test_raw = clean_for_ml(test_raw).filter(test_raw[FRP_COLUMN].isNotNull())

        train_count, val_count, test_count = train_raw.count(), val_raw.count(), test_raw.count()
        logger.info(f"Rows -> train={train_count}, val={val_count}, test={test_count}")
        if train_count == 0:
            raise RuntimeError(
                "No training rows found. Did you run Lab 1's batch_pipeline.py for the "
                f"train years {args.years_train} yet? Check {args.features_dir}."
            )

        # 1. Label engineering — thresholds computed on TRAIN ONLY, then reused everywhere
        q_low, q_high = compute_frp_quantile_thresholds(
            train_raw, args.low_quantile, args.high_quantile
        )
        logger.info(f"FRP quantile thresholds (train-derived): low={q_low:.3f}, high={q_high:.3f}")

        train_df = apply_risk_label(train_raw, q_low, q_high)
        val_df = apply_risk_label(val_raw, q_low, q_high)
        test_df = apply_risk_label(test_raw, q_low, q_high)

        logger.info(f"Train class distribution: {label_distribution(train_df)}")
        logger.info(f"Val class distribution:   {label_distribution(val_df)}")
        logger.info(f"Test class distribution:  {label_distribution(test_df)}")

        # 2. Label indexing (fit on train only, so class->index mapping is fixed)
        label_indexer = StringIndexer(
            inputCol=RISK_LABEL_COL, outputCol=RISK_LABEL_INDEX_COL, handleInvalid="keep"
        ).fit(train_df)
        label_converter = IndexToString(
            inputCol="prediction", outputCol="predicted_risk_level",
            labels=label_indexer.labels,
        )
        logger.info(f"Label index mapping: {list(enumerate(label_indexer.labels))}")

        # 3. Shared feature pipeline (categorical encoding + assembler)
        feature_stages, assembler_inputs = build_feature_pipeline_stages()
        full_pipeline = Pipeline(stages=[label_indexer] + feature_stages)
        pipeline_model = full_pipeline.fit(train_df)

        train_feat = pipeline_model.transform(train_df).cache()
        val_feat = pipeline_model.transform(val_df).cache()
        test_feat = pipeline_model.transform(test_df).cache()
        train_feat.count()  # materialize cache

        # 4. Train Random Forest (native multiclass support)
        logger.info("Training RandomForestClassifier...")
        rf = RandomForestClassifier(
            labelCol=RISK_LABEL_INDEX_COL, featuresCol="features",
            numTrees=args.rf_num_trees, maxDepth=args.rf_max_depth, seed=args.seed,
        )
        rf_model = rf.fit(train_feat)

        # 5. Train GBT wrapped in OneVsRest (GBTClassifier itself is binary-only in Spark MLlib)
        logger.info("Training GBTClassifier via OneVsRest (multiclass wrapper)...")
        gbt = GBTClassifier(
            labelCol=RISK_LABEL_INDEX_COL, featuresCol="features",
            maxIter=args.gbt_max_iter, maxDepth=args.gbt_max_depth, seed=args.seed,
        )
        ovr_gbt = OneVsRest(classifier=gbt, labelCol=RISK_LABEL_INDEX_COL, featuresCol="features")
        gbt_model = ovr_gbt.fit(train_feat)

        # 6. Evaluate both models on val + test
        report = {
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": None,
            "train_years": args.years_train,
            "val_years": args.years_val,
            "test_years": args.years_test,
            "row_counts": {"train": train_count, "val": val_count, "test": test_count},
            "label_thresholds": {"frp_low_quantile": q_low, "frp_high_quantile": q_high},
            "label_index_mapping": {str(i): lbl for i, lbl in enumerate(label_indexer.labels)},
            "class_distribution": {
                "train": label_distribution(train_df),
                "val": label_distribution(val_df),
                "test": label_distribution(test_df),
            },
            "models": {},
        }

        for model_name, model in [("random_forest", rf_model), ("gbt_one_vs_rest", gbt_model)]:
            val_pred = model.transform(val_feat)
            test_pred = model.transform(test_feat)
            report["models"][model_name] = {
                "validation": evaluate_classifier(val_pred, label_col=RISK_LABEL_INDEX_COL),
                "test": evaluate_classifier(test_pred, label_col=RISK_LABEL_INDEX_COL),
            }
            logger.info(
                f"{model_name} -> test accuracy="
                f"{report['models'][model_name]['test']['accuracy']}, "
                f"test f1={report['models'][model_name]['test']['f1']}"
            )

        best_model_name = max(
            report["models"], key=lambda m: report["models"][m]["test"]["f1"]
        )
        report["best_model_by_test_f1"] = best_model_name
        report["duration_seconds"] = round(time.time() - start_time, 2)

        # 7. Persist artifacts: preprocessing pipeline + both models + report
        pipeline_path = f"{args.models_dir}/{RISK_PIPELINE_MODEL_NAME}"
        rf_path = f"{args.models_dir}/{RISK_RF_MODEL_NAME}"
        gbt_path = f"{args.models_dir}/{RISK_GBT_MODEL_NAME}"

        logger.info(f"Saving preprocessing pipeline to {pipeline_path}")
        pipeline_model.write().overwrite().save(pipeline_path)
        logger.info(f"Saving Random Forest model to {rf_path}")
        rf_model.write().overwrite().save(rf_path)
        logger.info(f"Saving GBT (OneVsRest) model to {gbt_path}")
        gbt_model.write().overwrite().save(gbt_path)

        report_path = save_json_report(report, args.reports_dir, "risk_classifier_report.json")
        logger.info(f"Saved comparison report to {report_path}")
        logger.info(f"Best model by test F1: {best_model_name}")
        logger.info(f"Lab 2 risk classifier training complete in {report['duration_seconds']}s")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

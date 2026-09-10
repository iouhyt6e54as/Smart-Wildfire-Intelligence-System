"""
Evaluation helpers shared by the risk classifier and fire-size regressor
training scripts. Keeps metric definitions and report JSON shape consistent
across both models, so Lab 5 (dashboard) can consume them uniformly.
"""

import json
import os

from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator


def evaluate_classifier(predictions_df, label_col="label", prediction_col="prediction"):
    """
    Computes accuracy, weighted F1, weighted precision/recall and a
    confusion matrix (as a nested dict) for a multiclass classifier's
    predictions DataFrame.
    """
    metrics = {}
    for metric_name in ["accuracy", "f1", "weightedPrecision", "weightedRecall"]:
        evaluator = MulticlassClassificationEvaluator(
            labelCol=label_col, predictionCol=prediction_col, metricName=metric_name
        )
        metrics[metric_name] = round(evaluator.evaluate(predictions_df), 4)

    confusion_rows = (
        predictions_df.groupBy(label_col, prediction_col)
        .count()
        .orderBy(label_col, prediction_col)
        .collect()
    )
    confusion_matrix = {}
    for row in confusion_rows:
        actual = str(row[label_col])
        predicted = str(row[prediction_col])
        confusion_matrix.setdefault(actual, {})[predicted] = row["count"]

    metrics["confusion_matrix"] = confusion_matrix
    return metrics


def evaluate_regressor(predictions_df, label_col="frp", prediction_col="prediction"):
    """Computes RMSE, MAE and R^2 for a regressor's predictions DataFrame."""
    metrics = {}
    for metric_name in ["rmse", "mae", "r2"]:
        evaluator = RegressionEvaluator(
            labelCol=label_col, predictionCol=prediction_col, metricName=metric_name
        )
        metrics[metric_name] = round(evaluator.evaluate(predictions_df), 4)
    return metrics


def save_json_report(report_dict, reports_dir, filename):
    os.makedirs(reports_dir, exist_ok=True)
    path = os.path.join(reports_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report_dict, f, indent=2, ensure_ascii=False)
    return path

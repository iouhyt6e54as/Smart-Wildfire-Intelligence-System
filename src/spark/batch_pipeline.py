#!/usr/bin/env python3
"""
Spark Historical Batch Ingestion, Cleaning, Validation, Feature Engineering & Parquet Writer.
Ingests NASA FIRMS CSV data year-by-year, cleans and validates records,
derives features, and writes Snappy Parquet to HDFS /wildfire/curated and /wildfire/features.
Generates comprehensive Data Quality Reports in JSON format.
"""

import argparse
import glob
import json
import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Local imports
try:
    from src.schemas.fire_schema import RAW_ARCHIVE_SCHEMA, RAW_NRT_SCHEMA
    from src.spark.clean import clean_firms_dataframe
    from src.spark.validate import validate_firms_dataframe, deduplicate_firms_dataframe
    from src.spark.feature_engineering import enrich_firms_features
    from src.utils.logging import setup_logger
except ImportError:
    from schemas.fire_schema import RAW_ARCHIVE_SCHEMA, RAW_NRT_SCHEMA
    from spark.clean import clean_firms_dataframe
    from spark.validate import validate_firms_dataframe, deduplicate_firms_dataframe
    from spark.feature_engineering import enrich_firms_features
    from utils.logging import setup_logger

logger = setup_logger("FirmsBatchPipeline")

def get_spark_session():
    return (
        SparkSession.builder
        .appName("Wildfire-Historical-Batch-Pipeline")
        .config("spark.driver.memory", "384m")
        .config("spark.executor.memory", "768m")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def read_raw_year_data(spark, data_dir, year, sample_limit=None):
    year_dir = os.path.join(data_dir, str(year))
    if not os.path.exists(year_dir):
        raise FileNotFoundError(f"Year directory does not exist: {year_dir}")

    csv_files = sorted(glob.glob(os.path.join(year_dir, "*.csv")))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {year_dir}")

    dfs = []
    file_info = []

    for fpath in csv_files:
        fname = os.path.basename(fpath)
        fsize = os.path.getsize(fpath)
        file_info.append({"file_name": fname, "size_mb": round(fsize / (1024*1024), 2)})

        is_nrt = "nrt" in fname.lower()
        schema = RAW_NRT_SCHEMA if is_nrt else RAW_ARCHIVE_SCHEMA

        df = spark.read.option("header", "true").schema(schema).csv(fpath)

        if is_nrt and "type" not in df.columns:
            df = df.withColumn("type", F.lit(None).cast(IntegerType()))

        if sample_limit:
            df = df.limit(sample_limit)

        dfs.append(df)

    combined = dfs[0]
    for other in dfs[1:]:
        combined = combined.unionByName(other)

    return combined, file_info

def process_year(spark, year, args):
    start_time = time.time()
    logger.info(f"=== Starting Batch Processing for Year {year} (sample={args.sample}) ===")

    # 1. Ingestion
    raw_df, file_info = read_raw_year_data(spark, args.data_dir, year, sample_limit=args.sample)
    input_rows = raw_df.count()
    logger.info(f"Year {year}: Ingested {input_rows} raw records from {len(file_info)} file(s).")

    if input_rows == 0:
        logger.warning(f"Year {year}: No records found.")
        return None

    # Optional: Save a small representative raw sample to HDFS /wildfire/raw/sample/
    if args.save_raw_sample:
        raw_sample_path = f"{args.output_raw}/year={year}"
        logger.info(f"Writing raw demonstration sample to {raw_sample_path}...")
        raw_df.limit(1000).coalesce(1).write.mode("overwrite").option("header", "true").csv(raw_sample_path)

    # 2. Cleaning & Standardization
    is_nrt_year = (year == 2026)
    cleaned_df = clean_firms_dataframe(raw_df, is_nrt=is_nrt_year)

    # 3. Validation
    valid_df, invalid_df = validate_firms_dataframe(cleaned_df)
    valid_count = valid_df.count()
    invalid_count = input_rows - valid_count
    logger.info(f"Year {year}: Valid rows = {valid_count}, Invalid/Quarantined rows = {invalid_count}")

    # 4. Deduplication
    deduped_df = deduplicate_firms_dataframe(valid_df)
    final_valid_count = deduped_df.count()
    duplicate_count = valid_count - final_valid_count
    logger.info(f"Year {year}: Unique valid rows = {final_valid_count}, Duplicates removed = {duplicate_count}")

    # 5. Feature Engineering
    features_df = enrich_firms_features(deduped_df)

    # 6. Quality Metrics Aggregation
    metrics_row = features_df.agg(
        F.min("latitude").alias("min_lat"),
        F.max("latitude").alias("max_lat"),
        F.min("longitude").alias("min_lon"),
        F.max("longitude").alias("max_lon"),
        F.min("acq_date").alias("min_date"),
        F.max("acq_date").alias("max_date"),
        F.min("brightness").alias("min_brightness"),
        F.max("brightness").alias("max_brightness"),
        F.avg("brightness").alias("avg_brightness"),
        F.min("frp").alias("min_frp"),
        F.max("frp").alias("max_frp"),
        F.avg("frp").alias("avg_frp")
    ).first().asDict()

    # Determine optimal partition count for Snappy Parquet write
    num_output_partitions = max(1, min(4, final_valid_count // 50000))

    # 7. Write Curated Parquet to HDFS (Partitioned by year)
    curated_year_path = f"{args.output_curated}/year={year}"
    logger.info(f"Writing curated dataset to {curated_year_path} with {num_output_partitions} partition(s)...")
    deduped_df.coalesce(num_output_partitions).write.mode("overwrite").parquet(curated_year_path)

    # 8. Write Features Parquet to HDFS (Partitioned by year)
    features_year_path = f"{args.output_features}/year={year}"
    logger.info(f"Writing features dataset to {features_year_path} with {num_output_partitions} partition(s)...")
    features_df.coalesce(num_output_partitions).write.mode("overwrite").parquet(features_year_path)

    duration = round(time.time() - start_time, 2)
    logger.info(f"Year {year} successfully processed in {duration}s. Output rows = {final_valid_count}")

    # 9. Build Data Quality Report
    quality_report = {
        "year": year,
        "processing_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": duration,
        "input_files": file_info,
        "input_rows": input_rows,
        "invalid_rows": invalid_count,
        "duplicate_rows": duplicate_count,
        "output_curated_rows": final_valid_count,
        "output_features_rows": final_valid_count,
        "hdfs_curated_path": curated_year_path,
        "hdfs_features_path": features_year_path,
        "spatial_bounds": {
            "min_latitude": metrics_row["min_lat"],
            "max_latitude": metrics_row["max_lat"],
            "min_longitude": metrics_row["min_lon"],
            "max_longitude": metrics_row["max_lon"]
        },
        "temporal_bounds": {
            "min_acq_date": metrics_row["min_date"],
            "max_acq_date": metrics_row["max_date"]
        },
        "value_statistics": {
            "brightness_min": metrics_row["min_brightness"],
            "brightness_max": metrics_row["max_brightness"],
            "brightness_avg": round(metrics_row["avg_brightness"], 2) if metrics_row["avg_brightness"] else None,
            "frp_min": metrics_row["min_frp"],
            "frp_max": metrics_row["max_frp"],
            "frp_avg": round(metrics_row["avg_frp"], 2) if metrics_row["avg_frp"] else None
        },
        "quality_status": "PASSED" if invalid_count == 0 else "PASSED_WITH_WARNINGS"
    }

    # Save year quality report JSON
    year_quality_file = os.path.join(args.reports_dir, f"{year}_quality.json")
    with open(year_quality_file, "w", encoding="utf-8") as f:
        json.dump(quality_report, f, indent=2)
    logger.info(f"Data quality report saved: {year_quality_file}")

    return quality_report

def main():
    parser = argparse.ArgumentParser(description="NASA FIRMS Batch Processing Pipeline")
    parser.add_argument("--year", type=int, help="Single year to process (e.g. 2026)")
    parser.add_argument("--years", type=int, nargs="+", help="Multiple years to process (e.g. 2025 2026)")
    parser.add_argument("--all-years", action="store_true", help="Process all years 2020-2026")
    parser.add_argument("--sample", type=int, default=None, help="Process first N rows per file for safe testing")
    parser.add_argument("--data-dir", default="/data/nasa-wildfire-data", help="Path to raw NASA data")
    parser.add_argument("--output-curated", default="hdfs://namenode:9000/wildfire/curated", help="HDFS curated path")
    parser.add_argument("--output-features", default="hdfs://namenode:9000/wildfire/features", help="HDFS features path")
    parser.add_argument("--output-raw", default="hdfs://namenode:9000/wildfire/raw/sample", help="HDFS raw sample path")
    parser.add_argument("--reports-dir", default="/opt/reports/data_quality", help="Data quality reports directory")
    parser.add_argument("--save-raw-sample", action="store_true", default=True, help="Save representative raw sample")
    args = parser.parse_args()

    os.makedirs(args.reports_dir, exist_ok=True)
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    if args.year:
        years = [args.year]
    elif args.years:
        years = sorted(args.years)
    elif args.all_years:
        years = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
    else:
        years = [2026]

    logger.info(f"Batch pipeline started. Processing years: {years} with sample limit: {args.sample}")

    overall_quality = {
        "execution_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "years_processed": years,
        "sample_limit": args.sample,
        "total_input_rows": 0,
        "total_invalid_rows": 0,
        "total_duplicate_rows": 0,
        "total_output_rows": 0,
        "yearly_results": {}
    }

    try:
        for y in years:
            rep = process_year(spark, y, args)
            if rep:
                overall_quality["yearly_results"][y] = rep
                overall_quality["total_input_rows"] += rep["input_rows"]
                overall_quality["total_invalid_rows"] += rep["invalid_rows"]
                overall_quality["total_duplicate_rows"] += rep["duplicate_rows"]
                overall_quality["total_output_rows"] += rep["output_curated_rows"]

        overall_file = os.path.join(args.reports_dir, "overall_quality.json")
        with open(overall_file, "w", encoding="utf-8") as f:
            json.dump(overall_quality, f, indent=2)
        logger.info(f"Overall data quality report saved: {overall_file}")
        logger.info(f"Pipeline finished! Total rows written to HDFS: {overall_quality['total_output_rows']}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

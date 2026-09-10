"""
Lab 4 — Airflow DAG: load curated FIRMS data (HDFS Parquet) into PostgreSQL

Follows the same pattern as pipeline_hdfs_kafka_spark.py: the Airflow
container has pyspark pip-installed and talks to spark-master directly over
the docker network (no SparkSubmitOperator / docker socket needed).

Tasks:
  1. load_to_postgres   -> Spark job reads /wildfire/curated from HDFS,
                            writes wildfire.fire_events in Postgres (TRUNCATE+reload)
  2. verify_row_count   -> sanity check: query Postgres, fail the DAG if empty

Trigger manually for now (schedule=None). Once you're happy with it, give it
a daily schedule, e.g. schedule="0 3 * * *" (03:00 UTC every day).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# All years currently in the curated dataset. Keep this in sync with
# schemas/data_contract.md section 2 - the load is a full TRUNCATE+reload
# of exactly these years every run, so an omitted year disappears from
# Postgres until it's added back here.
YEARS = [2020, 2021, 2022, 2023, 2024, 2025, 2026]


def load_to_postgres():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder
        .appName("Airflow-LoadCuratedToPostgres")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "768m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.parquet("hdfs://namenode:9000/wildfire/curated/year=*")
        .withColumn("year", F.year("event_timestamp"))
        .filter(F.col("year").isin(YEARS))
    )

    fire_events_columns = [
        "event_timestamp", "latitude", "longitude", "brightness", "bright_t31",
        "scan", "track", "frp", "confidence", "daynight", "satellite",
        "instrument", "version", "type", "year",
    ]

    out_df = (
        df
        .withColumn(
            "season",
            F.when(F.month("event_timestamp").isin(12, 1, 2), "Winter")
             .when(F.month("event_timestamp").isin(3, 4, 5), "Spring")
             .when(F.month("event_timestamp").isin(6, 7, 8), "Summer")
             .otherwise("Fall"),
        )
        .withColumn("is_night", F.when(F.col("daynight") == "N", 1).otherwise(0))
        .select(*fire_events_columns, "season", "is_night")
        .dropDuplicates(["event_timestamp", "latitude", "longitude"])
    )

    row_count = out_df.count()
    print(f">>> Writing {row_count} rows -> wildfire.fire_events")

    (
        out_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/wildfire")
        .option("dbtable", "wildfire.fire_events")
        .option("user", "wildfire")
        .option("password", "wildfire_secret")
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

    spark.stop()
    print(">>> Load complete.")


def verify_row_count():
    import psycopg2

    conn = psycopg2.connect(
        host="postgres", port=5432, dbname="wildfire",
        user="wildfire", password="wildfire_secret",
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM wildfire.fire_events;")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f">>> wildfire.fire_events row count: {count}")
    if count == 0:
        raise ValueError("wildfire.fire_events is empty after load - check the Spark job logs")


with DAG(
    dag_id="load_curated_to_postgres",
    description="Lab 4: load curated FIRMS Parquet from HDFS into PostgreSQL wildfire.fire_events",
    default_args=default_args,
    schedule=None,  # trigger manually; switch to a cron string once validated
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab4", "postgres", "batch"],
) as dag:

    t1 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t2 = PythonOperator(task_id="verify_row_count", python_callable=verify_row_count)

    t1 >> t2

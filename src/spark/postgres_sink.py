"""
Lab 4 — Shared PostgreSQL JDBC helper for Spark batch & streaming jobs.

Used by:
  - src/spark/load_curated_to_postgres.py   (batch)
  - src/spark/real_time_risk_inference.py   (streaming, via foreachBatch)
  - src/spark/anomaly_streaming.py          (streaming, via foreachBatch)
  - src/spark/alert_generation.py           (streaming, via foreachBatch)

Requires the Postgres JDBC driver on the Spark classpath. Easiest way:
add `--packages org.postgresql:postgresql:42.7.3` to every spark-submit
call that imports this module (see docs/lab4_postgres_airflow.md).
"""

import os

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_WILD_DB", "wildfire")
PG_USER = os.environ.get("POSTGRES_WILD_USER", "wildfire")
PG_PASSWORD = os.environ.get("POSTGRES_WILD_PASSWORD", "wildfire_secret")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

JDBC_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def write_df_to_postgres(df, table_name, mode="append"):
    """
    Write a static (non-streaming) Spark DataFrame to a wildfire.<table_name> table.

    IMPORTANT: mode="overwrite" here always sets `truncate=true`, which makes
    Spark run TRUNCATE TABLE instead of DROP+CREATE. Without it, Spark would
    drop our hand-written table (and its indexes/constraints/views that
    depend on it) and recreate a bare one from the DataFrame's inferred
    schema - never use plain overwrite against a table created by
    postgres/schema_wildfire.sql.
    """
    writer = (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"wildfire.{table_name}")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
    )
    if mode == "overwrite":
        writer = writer.option("truncate", "true")
    writer.save()


def make_postgres_foreach_batch(table_name):
    """
    Return a function suitable for `.writeStream.foreachBatch(...)` that
    appends every micro-batch of a streaming DataFrame into the given
    wildfire.<table_name> table. Duplicate-safe tables (fire_events) rely
    on a UNIQUE constraint + ON CONFLICT handled at the DB level is not
    supported by plain JDBC append, so for high-frequency streams we simply
    append (dedup happens in the SQL views / Lab 5 queries if needed).
    """

    def _write(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        write_df_to_postgres(batch_df, table_name, mode="append")
        print(f">>> [postgres_sink] batch {batch_id}: wrote "
              f"{batch_df.count()} rows -> wildfire.{table_name}")

    return _write

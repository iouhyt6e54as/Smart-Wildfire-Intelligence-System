from pyspark.sql import functions as F

def build_validity_condition():
    """
    Returns a PySpark Column boolean expression for record validity.
    """
    coord_valid = (
        (F.col("latitude").isNotNull()) &
        (F.col("latitude") >= -90.0) &
        (F.col("latitude") <= 90.0) &
        (F.col("longitude").isNotNull()) &
        (F.col("longitude") >= -180.0) &
        (F.col("longitude") <= 180.0)
    )

    temporal_valid = (
        (F.col("event_timestamp").isNotNull()) &
        (F.to_date(F.col("acq_date"), "yyyy-MM-dd").isNotNull())
    )

    numeric_valid = (
        (F.col("brightness").isNull() | (F.col("brightness") > 0.0)) &
        (F.col("bright_t31").isNull() | (F.col("bright_t31") > 0.0)) &
        (F.col("frp").isNull() | (F.col("frp") >= 0.0)) &
        (F.col("scan").isNull() | (F.col("scan") >= 0.0)) &
        (F.col("track").isNull() | (F.col("track") >= 0.0))
    )

    return coord_valid & temporal_valid & numeric_valid

def validate_firms_dataframe(df):
    """
    Separates DataFrame into valid and quarantined/invalid records with metrics.
    """
    is_valid_expr = build_validity_condition()

    # Add boolean flag
    tagged_df = df.withColumn("is_valid", is_valid_expr)

    valid_df = tagged_df.filter(F.col("is_valid")).drop("is_valid")
    invalid_df = tagged_df.filter(~F.col("is_valid")).drop("is_valid")

    return valid_df, invalid_df

def deduplicate_firms_dataframe(df, key_cols=None):
    """
    Deduplicates DataFrame based on spatial-temporal observation keys.
    Default key: latitude, longitude, acq_date, acq_time.
    """
    if key_cols is None:
        key_cols = ["latitude", "longitude", "acq_date", "acq_time"]

    # Keep first occurrence
    deduped_df = df.dropDuplicates(key_cols)
    return deduped_df

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DoubleType

def normalize_time_expression(col_name="acq_time"):
    """
    Normalizes NASA FIRMS acq_time from strings like '0', '30', '0030', '1235'
    to standard 'HH:MM' 24-hour UTC format.
    """
    # 1. Trim whitespace
    trimmed = F.trim(F.col(col_name))
    # 2. Pad to 4 digits with leading zeros: '30' -> '0030', '125' -> '0125'
    padded = F.lpad(trimmed, 4, "0")
    # 3. Format as HH:MM
    return F.concat(F.substring(padded, 1, 2), F.lit(":"), F.substring(padded, 3, 2))

def clean_firms_dataframe(df, is_nrt=False):
    """
    Cleans, standardizes and harmonizes raw NASA FIRMS DataFrames.
    """
    # 1. Ensure 'type' exists and has proper type
    if "type" not in df.columns or is_nrt:
        df = df.withColumn("type", F.lit(None).cast(IntegerType()))
    else:
        df = df.withColumn("type", F.col("type").cast(IntegerType()))

    # 2. Trim string columns
    string_cols = ["satellite", "instrument", "confidence", "daynight", "version", "acq_date"]
    for c in string_cols:
        if c in df.columns:
            # Empty strings become null
            df = df.withColumn(c, F.when(F.length(F.trim(F.col(c))) == 0, None).otherwise(F.trim(F.col(c))))

    # 3. Cast numeric columns safely
    numeric_cols = ["latitude", "longitude", "brightness", "scan", "track", "bright_t31", "frp"]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(DoubleType()))

    # 4. Normalize acq_time and create event_timestamp
    time_normalized = normalize_time_expression("acq_time")
    df = df.withColumn("acq_time_norm", time_normalized)

    # event_timestamp = to_timestamp(YYYY-MM-DD HH:MM)
    timestamp_expr = F.to_timestamp(
        F.concat(F.col("acq_date"), F.lit(" "), F.col("acq_time_norm")),
        "yyyy-MM-dd HH:mm"
    )
    df = df.withColumn("event_timestamp", timestamp_expr)

    # Standardize acq_time to normalized format and drop temporary col
    df = df.withColumn("acq_time", F.col("acq_time_norm")).drop("acq_time_norm")

    # Reorder columns to canonical schema order
    canonical_order = [
        "latitude", "longitude", "brightness", "scan", "track",
        "acq_date", "acq_time", "satellite", "instrument", "confidence",
        "version", "bright_t31", "frp", "daynight", "type", "event_timestamp"
    ]
    return df.select(canonical_order)

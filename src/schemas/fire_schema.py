from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType
)

# Raw Archive Schema (15 columns)
RAW_ARCHIVE_SCHEMA = StructType([
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("brightness", DoubleType(), nullable=True),
    StructField("scan", DoubleType(), nullable=True),
    StructField("track", DoubleType(), nullable=True),
    StructField("acq_date", StringType(), nullable=False),
    StructField("acq_time", StringType(), nullable=False),
    StructField("satellite", StringType(), nullable=True),
    StructField("instrument", StringType(), nullable=True),
    StructField("confidence", StringType(), nullable=True),
    StructField("version", StringType(), nullable=True),
    StructField("bright_t31", DoubleType(), nullable=True),
    StructField("frp", DoubleType(), nullable=True),
    StructField("daynight", StringType(), nullable=True),
    StructField("type", IntegerType(), nullable=True),
])

# Raw NRT Schema (14 columns, missing 'type')
RAW_NRT_SCHEMA = StructType([
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("brightness", DoubleType(), nullable=True),
    StructField("scan", DoubleType(), nullable=True),
    StructField("track", DoubleType(), nullable=True),
    StructField("acq_date", StringType(), nullable=False),
    StructField("acq_time", StringType(), nullable=False),
    StructField("satellite", StringType(), nullable=True),
    StructField("instrument", StringType(), nullable=True),
    StructField("confidence", StringType(), nullable=True),
    StructField("version", StringType(), nullable=True),
    StructField("bright_t31", DoubleType(), nullable=True),
    StructField("frp", DoubleType(), nullable=True),
    StructField("daynight", StringType(), nullable=True),
])

# Canonical FIRMS Schema (harmonized, nullable 'type', normalized timestamps)
CANONICAL_FIRMS_SCHEMA = StructType([
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("brightness", DoubleType(), nullable=True),
    StructField("scan", DoubleType(), nullable=True),
    StructField("track", DoubleType(), nullable=True),
    StructField("acq_date", StringType(), nullable=False),
    StructField("acq_time", StringType(), nullable=False),
    StructField("satellite", StringType(), nullable=True),
    StructField("instrument", StringType(), nullable=True),
    StructField("confidence", StringType(), nullable=True),
    StructField("version", StringType(), nullable=True),
    StructField("bright_t31", DoubleType(), nullable=True),
    StructField("frp", DoubleType(), nullable=True),
    StructField("daynight", StringType(), nullable=True),
    StructField("type", IntegerType(), nullable=True),
    StructField("event_timestamp", TimestampType(), nullable=True),
])

# Features Schema (Canonical + Derived Spatial/Temporal Features)
FEATURE_FIRMS_SCHEMA = StructType([
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("brightness", DoubleType(), nullable=True),
    StructField("scan", DoubleType(), nullable=True),
    StructField("track", DoubleType(), nullable=True),
    StructField("acq_date", StringType(), nullable=False),
    StructField("acq_time", StringType(), nullable=False),
    StructField("satellite", StringType(), nullable=True),
    StructField("instrument", StringType(), nullable=True),
    StructField("confidence", StringType(), nullable=True),
    StructField("version", StringType(), nullable=True),
    StructField("bright_t31", DoubleType(), nullable=True),
    StructField("frp", DoubleType(), nullable=True),
    StructField("daynight", StringType(), nullable=True),
    StructField("type", IntegerType(), nullable=True),
    StructField("event_timestamp", TimestampType(), nullable=True),
    StructField("year", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
    StructField("day", IntegerType(), nullable=False),
    StructField("hour", IntegerType(), nullable=False),
    StructField("day_of_week", IntegerType(), nullable=False),
    StructField("day_of_year", IntegerType(), nullable=False),
    StructField("is_weekend", IntegerType(), nullable=False),
    StructField("season", StringType(), nullable=False),
    StructField("is_night", IntegerType(), nullable=False),
    StructField("latitude_bucket", DoubleType(), nullable=False),
    StructField("longitude_bucket", DoubleType(), nullable=False),
])

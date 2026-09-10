from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType, IntegerType, TimestampType
)

ENVIRONMENTAL_SCHEMA = StructType([
    StructField("timestamp", StringType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("humidity", DoubleType(), nullable=True),
    StructField("wind_speed", DoubleType(), nullable=True),
    StructField("wind_direction", DoubleType(), nullable=True),
    StructField("rainfall", DoubleType(), nullable=True),
    StructField("smoke", DoubleType(), nullable=True),
    StructField("air_quality_index", IntegerType(), nullable=True),
    StructField("source", StringType(), nullable=True),
])

STREAMING_ENVIRONMENTAL_TRANSFORMED_SCHEMA = StructType([
    StructField("event_timestamp", TimestampType(), nullable=True),
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("humidity", DoubleType(), nullable=True),
    StructField("wind_speed", DoubleType(), nullable=True),
    StructField("wind_direction", DoubleType(), nullable=True),
    StructField("rainfall", DoubleType(), nullable=True),
    StructField("smoke", DoubleType(), nullable=True),
    StructField("air_quality_index", IntegerType(), nullable=True),
    StructField("source", StringType(), nullable=True),
    StructField("year", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
    StructField("day", IntegerType(), nullable=False),
])

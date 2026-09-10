from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType

def enrich_firms_features(df):
    """
    Derives spatial and temporal features exclusively from NASA FIRMS observations.
    No external or fabricated environmental features are created.
    """
    ts = F.col("event_timestamp")

    enriched = (
        df
        .withColumn("year", F.year(ts).cast(IntegerType()))
        .withColumn("month", F.month(ts).cast(IntegerType()))
        .withColumn("day", F.dayofmonth(ts).cast(IntegerType()))
        .withColumn("hour", F.hour(ts).cast(IntegerType()))
        .withColumn("day_of_week", F.dayofweek(ts).cast(IntegerType()))
        .withColumn("day_of_year", F.dayofyear(ts).cast(IntegerType()))
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0).cast(IntegerType()))
        .withColumn(
            "season",
            F.when(F.col("month").isin(12, 1, 2), F.lit("Winter"))
            .when(F.col("month").isin(3, 4, 5), F.lit("Spring"))
            .when(F.col("month").isin(6, 7, 8), F.lit("Summer"))
            .otherwise(F.lit("Fall"))
            .cast(StringType())
        )
        .withColumn("is_night", F.when(F.col("daynight") == "N", 1).otherwise(0).cast(IntegerType()))
        .withColumn("latitude_bucket", F.round(F.col("latitude"), 1).cast(DoubleType()))
        .withColumn("longitude_bucket", F.round(F.col("longitude"), 1).cast(DoubleType()))
    )
    return enriched

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    concat,
    lit,
    year,
    month,
    dayofmonth,
    hour,
    dayofweek,
    dayofyear,
    when,
    floor,
    to_json,
    struct
)

from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType
)

from pyspark.ml import PipelineModel
from pyspark.ml.classification import OneVsRestModel


# ============================================================
# 1. Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("RealTimeRiskInference")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "768m")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Kafka Configuration
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"

INPUT_TOPIC = "firms_fire_events"

OUTPUT_TOPIC = "risk_predictions"


# ============================================================
# 3. Read Fire Events from Kafka
# ============================================================

print(">>> Starting Real-Time Risk Inference...")
print(f">>> Reading from Kafka topic: {INPUT_TOPIC}")

fire_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP
    )
    .option(
        "subscribe",
        INPUT_TOPIC
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .option(
        "failOnDataLoss",
        "false"
    )
    .option(
        "maxOffsetsPerTrigger",
        5
    )
    .load()
)


# Kafka value is binary → convert to String

fire_json_stream = (
    fire_stream
    .select(
        col("value")
        .cast("string")
        .alias("json")
    )
)

print(">>> Kafka fire stream initialized successfully.")


# ============================================================
# 4. Fire Event Schema
# ============================================================

fire_schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("brightness", DoubleType(), True),
    StructField("scan", DoubleType(), True),
    StructField("track", DoubleType(), True),

    StructField("acq_date", StringType(), True),
    StructField("acq_time", StringType(), True),

    StructField("satellite", StringType(), True),
    StructField("instrument", StringType(), True),

    StructField("confidence", StringType(), True),
    StructField("version", StringType(), True),

    StructField("bright_t31", DoubleType(), True),
    StructField("frp", DoubleType(), True),

    StructField("daynight", StringType(), True),
    StructField("type", IntegerType(), True)
])


# ============================================================
# 5. Parse JSON
# ============================================================

fire_parsed = (
    fire_json_stream
    .select(
        from_json(
            col("json"),
            fire_schema
        ).alias("data")
    )
    .select("data.*")
)

print(">>> Fire JSON schema configured successfully.")


# ============================================================
# 6. Feature Engineering
# ============================================================

fire_features = (
    fire_parsed

    # --------------------------------------------------------
    # Event timestamp
    # --------------------------------------------------------

    .withColumn(
        "event_timestamp",
        to_timestamp(
            concat(
                col("acq_date"),
                lit(" "),
                col("acq_time")
            ),
            "yyyy-MM-dd HH:mm"
        )
    )

    # --------------------------------------------------------
    # Time features
    # --------------------------------------------------------

    .withColumn(
        "year",
        year(col("event_timestamp"))
    )

    .withColumn(
        "month",
        month(col("event_timestamp"))
    )

    .withColumn(
        "day",
        dayofmonth(col("event_timestamp"))
    )

    .withColumn(
        "hour",
        hour(col("event_timestamp"))
    )

    .withColumn(
        "day_of_week",
        dayofweek(col("event_timestamp"))
    )

    .withColumn(
        "day_of_year",
        dayofyear(col("event_timestamp"))
    )

    # --------------------------------------------------------
    # Weekend
    # Saturday = 7
    # Sunday = 1
    # --------------------------------------------------------

    .withColumn(
        "is_weekend",
        when(
            col("event_timestamp").isNotNull()
            & col("day_of_week").isin(1, 7),
            1
        )
        .otherwise(0)
    )

    # --------------------------------------------------------
    # Night indicator
    # --------------------------------------------------------

    .withColumn(
        "is_night",
        when(
            col("daynight").isin("N", "n"),
            1
        )
        .otherwise(0)
    )

    # --------------------------------------------------------
    # Season
    # --------------------------------------------------------

    .withColumn(
        "season",
        when(
            col("month").isin(12, 1, 2),
            "winter"
        )
        .when(
            col("month").isin(3, 4, 5),
            "spring"
        )
        .when(
            col("month").isin(6, 7, 8),
            "summer"
        )
        .when(
            col("month").isin(9, 10, 11),
            "autumn"
        )
        .otherwise("unknown")
    )

    # --------------------------------------------------------
    # Geographic buckets
    # --------------------------------------------------------

    .withColumn(
        "latitude_bucket",
        floor(col("latitude") * 10) / 10
    )

    .withColumn(
        "longitude_bucket",
        floor(col("longitude") * 10) / 10
    )
)

print(">>> Feature engineering configured successfully.")


# ============================================================
# 7. Load Saved Risk Feature Pipeline
# ============================================================

RISK_PIPELINE_PATH = (
    "hdfs://namenode:9000/"
    "wildfire/models/"
    "risk_feature_pipeline"
)

print(">>> Loading saved risk feature pipeline...")

risk_pipeline = PipelineModel.load(
    RISK_PIPELINE_PATH
)

print(
    ">>> Risk feature pipeline loaded successfully."
)


# ============================================================
# 8. Load Saved GBT Risk Model
# ============================================================

RISK_MODEL_PATH = (
    "hdfs://namenode:9000/"
    "wildfire/models/"
    "risk_classifier_gbt"
)

print(">>> Loading saved GBT risk model...")

risk_model = OneVsRestModel.load(
    RISK_MODEL_PATH
)

print(
    ">>> GBT risk model loaded successfully."
)


# ============================================================
# 9. Apply Feature Pipeline
# ============================================================

print(">>> Applying saved feature pipeline...")

# Stage 0 was used only for training:
# risk_level -> label

transformed_features = fire_features

for stage in risk_pipeline.stages[1:]:
    transformed_features = stage.transform(
        transformed_features
    )

print(
    ">>> Feature transformation configured successfully."
)


# ============================================================
# 10. Generate Risk Predictions
# ============================================================

print(">>> Applying GBT risk model...")

predictions = risk_model.transform(
    transformed_features
)

print(
    ">>> Risk prediction configured successfully."
)


# ============================================================
# 11. Convert Prediction Index to Risk Level
# ============================================================

predictions_with_risk = (
    predictions
    .withColumn(
        "risk_level",
        when(
            col("prediction") == 0.0,
            "High"
        )
        .when(
            col("prediction") == 1.0,
            "Low"
        )
        .when(
            col("prediction") == 2.0,
            "Medium"
        )
        .otherwise("Unknown")
    )
)

print(
    ">>> Risk levels generated successfully."
)


# ============================================================
# 12. Prepare Final Prediction Output
# ============================================================

prediction_output = (
    predictions_with_risk
    .select(
        "event_timestamp",
        "latitude",
        "longitude",
        "brightness",
        "bright_t31",
        "frp",
        "confidence",
        "daynight",
        "prediction",
        "risk_level"
    )
)


# ============================================================
# 13. Convert Predictions to Kafka JSON
# ============================================================

kafka_predictions = (
    prediction_output
    .select(
        to_json(
            struct(
                "*"
            )
        ).alias("value")
    )
)


# ============================================================
# 14. Start Kafka Streaming Query
# ============================================================

print(
    f">>> Writing risk predictions to Kafka topic: "
    f"{OUTPUT_TOPIC}"
)

query = (
    kafka_predictions
    .writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP
    )
    .option(
        "topic",
        OUTPUT_TOPIC
    )
    .option(
        "checkpointLocation",
        "/tmp/spark-risk-prediction-checkpoint"
    )
    .outputMode("append")
    .trigger(
        processingTime="5 seconds"
    )
    .start()
)


print(
    ">>> Real-time risk prediction is ACTIVE."
)

print(
    ">>> Predictions are being sent to Kafka."
)

print(
    ">>> Press Ctrl+C to stop the streaming job."
)


# ============================================================
# 15. Keep Streaming Application Running
# ============================================================

query.awaitTermination()
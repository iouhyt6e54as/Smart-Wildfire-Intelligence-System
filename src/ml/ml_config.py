"""
Lab 2 — Machine Learning Configuration
Central, fixed definition of feature lists, label thresholds contract,
and HDFS/local paths used by every Lab 2 training script.

This file is the single source of truth for Lab 3 (Real-Time Streaming ML):
Lab 3 MUST use the exact same NUMERIC_FEATURES / CATEGORICAL_FEATURES lists
and the same saved PipelineModel to featurize streaming records identically.
"""

# ---------------------------------------------------------------------------
# Feature contract (matches schemas/data_contract.md canonical + engineered schema)
# ---------------------------------------------------------------------------

# Numeric features fed directly into VectorAssembler
NUMERIC_FEATURES = [
    "latitude",
    "longitude",
    "brightness",
    "bright_t31",
    "scan",
    "track",
    "month",
    "day",
    "hour",
    "day_of_week",
    "day_of_year",
    "is_weekend",
    "is_night",
    "latitude_bucket",
    "longitude_bucket",
]

# Categorical features -> StringIndexer -> OneHotEncoder
CATEGORICAL_FEATURES = [
    "confidence",   # l / n / h / unknown
    "daynight",     # D / N / unknown
    "season",       # Winter / Spring / Summer / Fall
    "type",         # 0/1/2/3/unknown fire-pixel type (cast to string)
]

# Column that is the source of both ML targets. Excluded from features
# in BOTH models to avoid label leakage.
FRP_COLUMN = "frp"

# Risk classification label
RISK_LABEL_COL = "risk_level"          # string: Low / Medium / High
RISK_LABEL_INDEX_COL = "label"         # numeric index used by Spark ML

# Fire-size regression label
REGRESSION_LABEL_COL = FRP_COLUMN

# Default FRP quantile split points (recomputed from TRAIN data at run time;
# these are only fallback defaults if quantile computation ever fails)
DEFAULT_LOW_QUANTILE = 0.33
DEFAULT_HIGH_QUANTILE = 0.66

# ---------------------------------------------------------------------------
# Time-based split contract (train on the past, validate/test on the future)
# ---------------------------------------------------------------------------
DEFAULT_TRAIN_YEARS = [2020, 2021, 2022, 2023, 2024]
DEFAULT_VAL_YEARS = [2025]
DEFAULT_TEST_YEARS = [2026]

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DEFAULT_FEATURES_DIR = "hdfs://namenode:9000/wildfire/features"
DEFAULT_MODELS_DIR = "hdfs://namenode:9000/wildfire/models"
DEFAULT_REPORTS_DIR = "/opt/reports/ml"

RISK_PIPELINE_MODEL_NAME = "risk_feature_pipeline"
RISK_RF_MODEL_NAME = "risk_classifier_rf"
RISK_GBT_MODEL_NAME = "risk_classifier_gbt"

FIRE_SIZE_PIPELINE_MODEL_NAME = "fire_size_feature_pipeline"
FIRE_SIZE_RF_MODEL_NAME = "fire_size_regressor_rf"
FIRE_SIZE_GBT_MODEL_NAME = "fire_size_regressor_gbt"

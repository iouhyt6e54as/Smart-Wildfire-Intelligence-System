"""
Preprocessing — Lab 2

Shared null-handling and Spark ML feature pipeline (StringIndexer +
OneHotEncoder for categoricals, VectorAssembler for the final feature
vector) used identically by the risk classifier and the fire-size
regressor, so both models are trained on features that are engineered
the exact same way.
"""

from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

try:
    from src.ml.ml_config import NUMERIC_FEATURES, CATEGORICAL_FEATURES
except ImportError:
    from ml.ml_config import NUMERIC_FEATURES, CATEGORICAL_FEATURES


def clean_for_ml(df):
    """
    Fills nulls that can legitimately still exist after Lab 1 validation
    (e.g. confidence, type, bright_t31 on some NRT/edge records) so that
    no row is silently dropped by VectorAssembler(handleInvalid='skip')
    unless it is truly unusable (missing coordinates/timestamp, already
    filtered out upstream in Lab 1).
    """
    out = df

    # Categorical: cast 'type' (int) to string first, then fill unknowns
    out = out.withColumn("type", F.col("type").cast("string"))
    for c in ["confidence", "daynight", "season", "type"]:
        if c in out.columns:
            out = out.withColumn(c, F.when(F.col(c).isNull(), F.lit("unknown")).otherwise(F.col(c)))

    # Numeric: fill with 0.0 where genuinely missing (rare after Lab 1 validation)
    numeric_fillable = ["brightness", "bright_t31", "scan", "track"]
    fill_map = {c: 0.0 for c in numeric_fillable if c in out.columns}
    if fill_map:
        out = out.fillna(fill_map)

    return out


def build_feature_pipeline_stages(numeric_features=None, categorical_features=None,
                                   output_col="features"):
    """
    Builds the reusable list of Spark ML Pipeline stages:
    [StringIndexer, OneHotEncoder] x categorical_features + [VectorAssembler]

    Returns (stages, assembler_input_cols) so callers can inspect the
    final feature-vector composition if needed (e.g. for feature importance
    mapping back to human-readable names).
    """
    numeric_features = numeric_features or NUMERIC_FEATURES
    categorical_features = categorical_features or CATEGORICAL_FEATURES

    stages = []
    assembler_inputs = list(numeric_features)

    for c in categorical_features:
        idx_col = f"{c}_idx"
        ohe_col = f"{c}_ohe"
        indexer = StringIndexer(inputCol=c, outputCol=idx_col, handleInvalid="keep")
        encoder = OneHotEncoder(inputCol=idx_col, outputCol=ohe_col, handleInvalid="keep")
        stages += [indexer, encoder]
        assembler_inputs.append(ohe_col)

    assembler = VectorAssembler(
        inputCols=assembler_inputs,
        outputCol=output_col,
        handleInvalid="skip",
    )
    stages.append(assembler)

    return stages, assembler_inputs


def build_feature_pipeline(numeric_features=None, categorical_features=None,
                            output_col="features"):
    """Convenience wrapper returning a ready-to-fit Spark ML Pipeline."""
    stages, assembler_inputs = build_feature_pipeline_stages(
        numeric_features, categorical_features, output_col
    )
    return Pipeline(stages=stages), assembler_inputs

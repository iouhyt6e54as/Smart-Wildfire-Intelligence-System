"""
Label Engineering — Lab 2

NASA FIRMS does NOT ship a ready-made "risk level" or "fire size" label.
Both ML targets are derived, honestly and traceably, from the Fire Radiative
Power (FRP) observation that NASA already provides:

  - Risk Classification target (`risk_level`): FRP bucketed into
    Low / Medium / High using quantile thresholds computed ONLY on the
    training split (2020-2024), then re-applied unchanged to validation
    and test splits. This avoids leaking future-data statistics into the
    label definition.

  - Fire Size Regression target: FRP itself, used as a continuous proxy
    for fire intensity/size (FIRMS has no fire-area/fire-size field).

`frp` is therefore ALWAYS excluded from the input features of both models.
"""

from pyspark.sql import functions as F

try:
    from src.ml.ml_config import (
        FRP_COLUMN, RISK_LABEL_COL, DEFAULT_LOW_QUANTILE, DEFAULT_HIGH_QUANTILE
    )
except ImportError:
    from ml.ml_config import (
        FRP_COLUMN, RISK_LABEL_COL, DEFAULT_LOW_QUANTILE, DEFAULT_HIGH_QUANTILE
    )


def compute_frp_quantile_thresholds(train_df, low_q=DEFAULT_LOW_QUANTILE,
                                     high_q=DEFAULT_HIGH_QUANTILE,
                                     relative_error=0.001):
    """
    Computes FRP quantile thresholds from the TRAINING split only.
    Returns (q_low, q_high) as floats.
    """
    non_null_train = train_df.filter(F.col(FRP_COLUMN).isNotNull())
    q_low, q_high = non_null_train.approxQuantile(FRP_COLUMN, [low_q, high_q], relative_error)
    if q_low is None or q_high is None:
        raise ValueError("Could not compute FRP quantiles - check that 'frp' has non-null values.")
    if q_low >= q_high:
        # Extremely skewed data fallback: nudge apart so 3 buckets stay possible
        q_high = q_low + 1e-6
    return float(q_low), float(q_high)


def apply_risk_label(df, q_low, q_high, frp_col=FRP_COLUMN, label_col=RISK_LABEL_COL):
    """
    Applies FIXED thresholds (computed once on train) to any split
    (train, val, test, or live streaming data in Lab 3).
    """
    return df.withColumn(
        label_col,
        F.when(F.col(frp_col).isNull(), F.lit(None))
         .when(F.col(frp_col) <= q_low, F.lit("Low"))
         .when(F.col(frp_col) <= q_high, F.lit("Medium"))
         .otherwise(F.lit("High"))
    )


def label_distribution(df, label_col=RISK_LABEL_COL):
    """Returns a dict of {label: count} for quick sanity-checking class balance."""
    rows = (
        df.groupBy(label_col)
          .count()
          .orderBy(label_col)
          .collect()
    )
    return {row[label_col]: row["count"] for row in rows}

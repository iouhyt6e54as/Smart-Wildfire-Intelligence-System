# Lab 2 — Model Input/Output Contract (Fixed Handoff to Lab 3)

This is the **fixed, must-not-change-without-team-agreement** contract for
both Lab 2 models. Lab 3 (Real-Time Streaming + Inference) MUST featurize
incoming streaming records using exactly this list, in this order, then
load the saved `PipelineModel` objects below rather than re-implementing
feature engineering — otherwise streaming predictions will not match what
the models were trained on.

---

## 1. Input Features (identical for both models)

### Numeric (fed straight into `VectorAssembler`)
`latitude`, `longitude`, `brightness`, `bright_t31`, `scan`, `track`,
`month`, `day`, `hour`, `day_of_week`, `day_of_year`, `is_weekend`,
`is_night`, `latitude_bucket`, `longitude_bucket`

### Categorical (→ `StringIndexer` → `OneHotEncoder`)
`confidence` (l/n/h/unknown), `daynight` (D/N/unknown), `season`
(Winter/Spring/Summer/Fall), `type` (0/1/2/3/unknown — cast to string)

### Explicitly EXCLUDED from features
`frp` — this is the source of both labels, so it is never a model input
(would be label leakage). `acq_date`, `acq_time`, `satellite`,
`instrument`, `version`, `event_timestamp` are also excluded (either
redundant with the temporal features above, or non-predictive identifiers).

> Source of truth in code: `src/ml/ml_config.py` →
> `NUMERIC_FEATURES` / `CATEGORICAL_FEATURES`

---

## 2. Model 1 — Wildfire Risk Classification

| | |
|---|---|
| **Target** | `risk_level` ∈ {Low, Medium, High} |
| **Derivation** | Quantile-bucketed from `frp` (33rd / 66th percentiles), thresholds computed **once on the 2020–2024 training split** and re-applied unchanged to validation/test/streaming data |
| **Algorithms** | `RandomForestClassifier` (native multiclass) and `GBTClassifier` wrapped in `OneVsRest` (Spark MLlib's GBT is binary-only) |
| **Saved artifacts (HDFS)** | `/wildfire/models/risk_feature_pipeline`, `/wildfire/models/risk_classifier_rf`, `/wildfire/models/risk_classifier_gbt` |
| **Label index mapping** | Saved per-run inside `reports/ml/risk_classifier_report.json` → `label_index_mapping` (e.g. `{"0": "High", "1": "Low", "2": "Medium"}`) — Lab 3 MUST read this mapping back rather than assuming a fixed order, since `StringIndexer` orders labels by descending frequency. |
| **FRP thresholds used** | Saved per-run inside the same report → `label_thresholds` (`frp_low_quantile`, `frp_high_quantile`) |

## 3. Model 2 — Fire Size Regression

| | |
|---|---|
| **Target** | `frp` (Fire Radiative Power, MW) — continuous proxy for fire intensity/size, since FIRMS has no direct fire-size/area field |
| **Algorithms** | `RandomForestRegressor`, `GBTRegressor` |
| **Saved artifacts (HDFS)** | `/wildfire/models/fire_size_feature_pipeline`, `/wildfire/models/fire_size_regressor_rf`, `/wildfire/models/fire_size_regressor_gbt` |

---

## 4. How Lab 3 Should Load and Use These Models

```python
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel, OneVsRestModel
from pyspark.ml.regression import RandomForestRegressionModel

risk_pipeline = PipelineModel.load("hdfs://namenode:9000/wildfire/models/risk_feature_pipeline")
risk_rf_model = RandomForestClassificationModel.load("hdfs://namenode:9000/wildfire/models/risk_classifier_rf")

featurized = risk_pipeline.transform(incoming_streaming_df)
predictions = risk_rf_model.transform(featurized)
# predictions.prediction is a label INDEX -> map back using label_index_mapping
# from reports/ml/risk_classifier_report.json for the run that trained this model.
```

## 5. Train / Validation / Test Split (time-based, not random)

| Split | Years | Purpose |
|---|---|---|
| Train | 2020–2024 | Model fitting + FRP quantile thresholds |
| Validation | 2025 | Hyperparameter / model selection |
| Test | 2026 | Final, untouched evaluation |

## 6. Evaluation Reports (JSON, regenerated on every training run)

- `reports/ml/risk_classifier_report.json` — accuracy, F1, precision/recall,
  confusion matrix, RF vs GBT comparison, chosen best model.
- `reports/ml/fire_size_regressor_report.json` — RMSE, MAE, R², RF vs GBT
  comparison, chosen best model.

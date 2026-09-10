#!/usr/bin/env bash
# ============================================================
# Lab 2 — Smart Wildfire Intelligence System
# Machine Learning: Risk Classification + Fire Size Regression
# ============================================================
set -e

step() { echo ""; echo "============================================================"; echo "  STEP $1 — $2"; echo "============================================================"; }
ok()   { echo "  [OK] $1"; }
info() { echo "  [..] $1"; }

step 1 "Checking containers"
docker ps --format "table {{.Names}}\t{{.Status}}"
ok "containers checked"

step 2 "Ensuring numpy is installed in Spark containers"
info "The bde2020/spark images do not ship numpy by default; pyspark.ml needs it."
docker exec spark-master pip3 install numpy --quiet
docker exec spark-worker pip3 install numpy --quiet
ok "numpy installed on spark-master and spark-worker"


step 3 "Training Wildfire Risk Classifier (Random Forest vs GBT)"
info "Reading /wildfire/features, training on 2020-2024, validating on 2025, testing on 2026..."
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/src/ml/train_risk_classifier.py \
    --years-train 2020 2021 2022 2023 2024 \
    --years-val 2025 \
    --years-test 2026
ok "Risk classifier training complete"

step 4 "Training Fire Size Regressor (Random Forest vs GBT)"
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/src/ml/train_fire_size_regressor.py \
    --years-train 2020 2021 2022 2023 2024 \
    --years-val 2025 \
    --years-test 2026
ok "Fire size regressor training complete"

step 5 "Verifying saved models in HDFS"
docker exec namenode hdfs dfs -ls /wildfire/models/
ok "Models present in HDFS"

step 6 "Evaluation reports"
info "Risk classifier report:"
cat reports/ml/risk_classifier_report.json 2>/dev/null | head -40 || echo "  (not found locally - check container volume mapping)"
info "Fire size regressor report:"
cat reports/ml/fire_size_regressor_report.json 2>/dev/null | head -40 || echo "  (not found locally - check container volume mapping)"

echo ""
echo "Lab 2 training pipeline finished."

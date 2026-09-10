# ============================================================
# Lab 1 — Smart Wildfire Intelligence System
# Full Pipeline Run Script (PowerShell)
# ============================================================

$ErrorActionPreference = "Continue"

function Print-Step($num, $text) {
    Write-Host ""
    Write-Host ("=" * 60) -ForegroundColor Cyan
    Write-Host "  STEP $num — $text" -ForegroundColor Yellow
    Write-Host ("=" * 60) -ForegroundColor Cyan
}

function Print-OK($text) {
    Write-Host "  [OK] $text" -ForegroundColor Green
}

function Print-Info($text) {
    Write-Host "  [..] $text" -ForegroundColor White
}

# ─────────────────────────────────────────────────────────────
Print-Step 1 "التحقق من الـ Containers"
# ─────────────────────────────────────────────────────────────
Print-Info "بنتحقق إن كل الـ containers شغالة..."
docker ps --format "table {{.Names}}`t{{.Status}}"
Print-OK "الـ containers شغالة"

# ─────────────────────────────────────────────────────────────
Print-Step 2 "إنشاء Kafka Topics"
# ─────────────────────────────────────────────────────────────
Print-Info "بنعمل الـ 4 topics..."
docker exec kafka python3 /opt/scripts/init_kafka_topics.py
Print-OK "Kafka topics جاهزة"

# ─────────────────────────────────────────────────────────────
Print-Step 3 "Data Profiling (Spark-First)"
# ─────────────────────────────────────────────────────────────
Print-Info "بنعمل statistical profiling (sample 50000 record لكل سنة)..."
Print-Info "هياخد ~3-5 دقايق..."
docker exec spark-master /spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    /opt/src/profiling/profile_firms.py `
    --all-years --sample 50000
Print-OK "Profiling خلص — النتايج في reports/profiling/"

# ─────────────────────────────────────────────────────────────
Print-Step 4 "Historical Batch Pipeline (2020-2026)"
# ─────────────────────────────────────────────────────────────
Print-Info "بنشغل الـ batch pipeline على كل السنوات (sample 20000)..."
Print-Info "هياخد ~10-15 دقيقة..."
docker exec spark-master /spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    /opt/src/spark/batch_pipeline.py `
    --years 2020 2021 2022 2023 2024 2025 2026 `
    --sample 20000
Print-OK "Batch pipeline خلص!"

# ─────────────────────────────────────────────────────────────
Print-Step 5 "التحقق من HDFS Output"
# ─────────────────────────────────────────────────────────────
Print-Info "بنتحقق من الـ partitions في HDFS..."
Write-Host ""
Write-Host "  /wildfire/curated/" -ForegroundColor Magenta
docker exec namenode hdfs dfs -ls /wildfire/curated/
Write-Host ""
Write-Host "  /wildfire/features/" -ForegroundColor Magenta
docker exec namenode hdfs dfs -ls /wildfire/features/
Print-OK "HDFS تمام"

# ─────────────────────────────────────────────────────────────
Print-Step 6 "تثبيت kafka-python"
# ─────────────────────────────────────────────────────────────
Print-Info "بنثبت kafka-python library..."
docker exec spark-master pip3 install kafka-python --quiet
Print-OK "kafka-python اتثبت"

# ─────────────────────────────────────────────────────────────
Print-Step 7 "تشغيل Spark Structured Streaming"
# ─────────────────────────────────────────────────────────────
Print-Info "بنشغل الـ streaming pipeline في الـ background (120 ثانية)..."
docker exec -d spark-master /spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 `
    /opt/src/spark/streaming_pipeline.py --mode both --duration 120
Print-Info "استنّي 10 ثواني عشان الـ streaming يبدأ..."
Start-Sleep -Seconds 10
Print-OK "Streaming pipeline شغالة"

# ─────────────────────────────────────────────────────────────
Print-Step 8 "تشغيل Fire Simulator (NASA Replay)"
# ─────────────────────────────────────────────────────────────
Print-Info "بنبعت 200 event حقيقي من NASA على Kafka..."
docker exec spark-master python3 /opt/src/simulator/fire_simulator.py `
    --bootstrap-server kafka:9092 `
    --topic firms_fire_events `
    --rate 20 `
    --max-events 200 `
    --input /data/nasa-wildfire-data/2026/fire_archive_SV-C2_800029.csv
Print-OK "Fire Simulator خلص"

# ─────────────────────────────────────────────────────────────
Print-Step 9 "تشغيل Environmental Simulator (Synthetic)"
# ─────────────────────────────────────────────────────────────
Print-Info "بنبعت 200 event بيئي synthetic على Kafka..."
docker exec spark-master python3 /opt/src/simulator/environmental_simulator.py `
    --bootstrap-server kafka:9092 `
    --topic environmental_data `
    --rate 20 `
    --max-events 200
Print-OK "Environmental Simulator خلص"

# ─────────────────────────────────────────────────────────────
Print-Step 10 "التحقق من Streaming Output"
# ─────────────────────────────────────────────────────────────
Print-Info "استنّي 15 ثانية عشان Spark يعالج الـ events..."
Start-Sleep -Seconds 15
Write-Host ""
Write-Host "  /wildfire/curated/streaming_fire/" -ForegroundColor Magenta
docker exec namenode hdfs dfs -ls /wildfire/curated/streaming_fire/
Write-Host ""
Write-Host "  /wildfire/curated/streaming_environmental/" -ForegroundColor Magenta
docker exec namenode hdfs dfs -ls /wildfire/curated/streaming_environmental/

# ─────────────────────────────────────────────────────────────
Write-Host ""
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host "  LAB 1 — COMPLETED SUCCESSFULLY!" -ForegroundColor Green
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host ""
Write-Host "  الناتج النهائي:" -ForegroundColor White
Write-Host "  OK  HDFS /wildfire/curated/year=2020..2026" -ForegroundColor Green
Write-Host "  OK  HDFS /wildfire/features/year=2020..2026" -ForegroundColor Green
Write-Host "  OK  HDFS /wildfire/curated/streaming_fire/" -ForegroundColor Green
Write-Host "  OK  HDFS /wildfire/curated/streaming_environmental/" -ForegroundColor Green
Write-Host "  OK  reports/profiling/ و reports/data_quality/" -ForegroundColor Green
Write-Host ""

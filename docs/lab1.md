# Lab 1: Big Data Infrastructure & Data Engineering Implementation
## Smart Wildfire Intelligence System

---

## 1. Executive Summary & Objective

**Lab 1** establishes the foundational Big Data engineering infrastructure for the **Smart Wildfire Intelligence System**, a graduation project combining Big Data distributed processing and Artificial Intelligence. 

The primary objectives achieved in Lab 1 include:
- Ingesting, discovering, and profiling 10.6 GB of NASA FIRMS active fire satellite observations (2020–2026).
- Resolving discovered schema drift between standard archive and near real-time (NRT) products.
- Enforcing a formal **Data Contract** and canonical schema for downstream consumption.
- Implementing an incremental, memory-safe, and distributed Apache Spark historical batch pipeline.
- Applying data cleaning, coordinate/temporal validation, deduplication, and derived feature engineering.
- Writing partitioned, Snappy-compressed Parquet datasets to HDFS (`/wildfire/curated` and `/wildfire/features`).
- Configuring Apache Kafka (KRaft mode) topics for fire telemetry, environmental telemetry, anomaly alerts, and risk predictions.
- Building a real-world **Fire Event Simulator** replaying real NASA observations and an **Environmental Telemetry Simulator** generating synthetic weather/smoke data.
- Establishing the foundation for **Spark Structured Streaming** to continuously ingest real-time streams into dedicated HDFS Parquet partitions without premature ML interference.

---

## 2. System Architecture

The overall Lab 1 data engineering architecture is organized as follows:

```text
                        SMART WILDFIRE INTELLIGENCE SYSTEM
                               LAB 1 ARCHITECTURE

     NASA FIRMS Source (F:)
     [Read-Only Host Mount]
               │
               ▼
   ┌───────────────────────┐
   │ Spark Batch Pipeline  │
   │ 1. Schema Harmonizer  │
   │ 2. Cleaning & Norm    │
   │ 3. Validation Rules   │
   │ 4. Deduplication      │
   │ 5. Feature Derivation │
   └───────────┬───────────┘
               │
               ▼
      [Snappy Parquet]
               │
               ├────────────────────────────────────────┐
               ▼                                        ▼
    HDFS: /wildfire/curated                  HDFS: /wildfire/features
    (year=2020 ... year=2026)                (year=2020 ... year=2026)
               │                                        │
               ▼                                        ▼
    Historical Ground Truth                  Ready for Lab 2 ML Models
                                             (Risk & Fire Size)


REAL-TIME STREAMING FOUNDATION:

 NASA FIRMS Sample                     Synthetic Weather Telemetry
        │                                          │
        ▼                                          ▼
 Python Fire Simulator                    Python Environmental Simulator
        │                                          │
        ▼                                          ▼
 Kafka Topic:                             Kafka Topic:
 firms_fire_events                        environmental_data
        │                                          │
        └───────────────────┬──────────────────────┘
                            ▼
               Spark Structured Streaming
               ├── Schema Validation & Parsing
               ├── Spatiotemporal Derivations
               └── Checkpointed Parquet Sink
                            │
                            ▼
                   HDFS Streaming Sinks:
                   ├── /wildfire/curated/streaming_fire
                   └── /wildfire/curated/streaming_environmental
                            │
                            ▼
               Ready for Lab 3 Real-Time ML
               (Inference, Anomalies, Alerts)
```

---

## 3. Dataset Discovery & Profiling Findings

### 3.1 Raw Dataset Inventory
- **Location:** `nasa-wildfire-data/`
- **Total Raw Size:** ~10,174.15 MB (~10.6 GB) across 8 CSV files.
- **Coverage:** 2020 through 2026.

| Year | Files | Raw Size (MB) | Instrument / Satellite | Schema |
|------|-------|---------------|------------------------|--------|
| 2020 | 1 | 1,596.2 MB | SNPP / VIIRS | 15 columns (Archive) |
| 2021 | 1 | 1,567.7 MB | SNPP / VIIRS | 15 columns (Archive) |
| 2022 | 1 | 1,328.6 MB | SNPP / VIIRS | 15 columns (Archive) |
| 2023 | 1 | 1,685.9 MB | SNPP / VIIRS | 15 columns (Archive) |
| 2024 | 1 | 1,624.0 MB | SNPP / VIIRS | 15 columns (Archive) |
| 2025 | 1 | 1,407.1 MB | SNPP / VIIRS | 15 columns (Archive) |
| 2026 | 2 | 964.6 MB | SNPP / VIIRS | 15 cols (Archive) + 14 cols (NRT) |

### 3.2 Key Architectural Findings & Schema Drift
1. **Schema Drift in 2026 Near Real-Time Data:**
   - Archive files (`2020`–`2025` and `2026/fire_archive_SV-C2_800029.csv`) contain 15 columns, including `type` (0=vegetation fire, 1=active volcano, 2=static land source, 3=offshore).
   - NRT file (`2026/fire_nrt_SV-C2_800029.csv`) contains 14 columns; the `type` column is completely omitted.
   - In Archive data, `version = "2"`; in NRT data, `version = "2.0NRT"`.
2. **Harmonization Strategy:**
   - The Canonical Schema defines `type` as nullable `IntegerType` (defaulting to `null` for NRT observations) and `version` as `StringType`.
3. **Data Quality Profile:**
   - Coordinates strictly fall within physical bounds: Latitude ∈ `[-90.0, 90.0]`, Longitude ∈ `[-180.0, 180.0]`.
   - Fire Radiative Power (FRP) has 0 negative entries.
   - Acquisition dates strictly follow ISO `YYYY-MM-DD`.

---

## 4. Formal Data Contract & Schema Specification

The formal data contract is published in [`schemas/data_contract.md`](file:///f:/Downloads/bigdata-lab/bigdata-lab/schemas/data_contract.md).

### 4.1 Canonical Schema Summary
- `latitude` (Double, non-null)
- `longitude` (Double, non-null)
- `brightness` (Double, Kelvin)
- `scan` (Double, km)
- `track` (Double, km)
- `acq_date` (String, `YYYY-MM-DD`)
- `acq_time` (String, normalized `HH:MM`)
- `satellite` (String)
- `instrument` (String)
- `confidence` (String: `l`, `n`, `h`)
- `version` (String: `"2"`, `"2.0NRT"`)
- `bright_t31` (Double, Kelvin)
- `frp` (Double, MW)
- `daynight` (String: `D`, `N`)
- `type` (Integer, nullable)
- `event_timestamp` (Timestamp, UTC)

### 4.2 Feature Engineering (Purely Derived from NASA Observations)
- `year`, `month`, `day`, `hour`
- `day_of_week` (1=Sun ... 7=Sat), `day_of_year` (1–366), `is_weekend` (0 or 1)
- `season` (`Winter`, `Spring`, `Summer`, `Fall`)
- `is_night` (1 if nocturnal observation `daynight == 'N'`, else 0)
- `latitude_bucket`, `longitude_bucket` (spatial grid rounded to 0.1 deg ~11 km)

> [!CAUTION]
> **No Fabricated Weather Data**: Historical NASA records do NOT contain fabricated weather attributes (humidity, wind, rainfall). Those attributes are handled strictly in the separate environmental telemetry stream.

---

## 5. Storage Architecture & Disk Resource Management

### 5.1 Storage Calculation & Constraints
- **Host C: Drive:** Free space ~14.32 GB (Docker WSL2 virtual disk storage).
- **Host F: Drive:** Free space ~125.5 GB (where raw 10.6 GB NASA CSV resides).
- **Architectural Decision:** Direct raw copying of 10.6 GB CSV into HDFS was strictly avoided to prevent Docker disk exhaustion. Instead, Spark mounts `./nasa-wildfire-data` as read-only (`/data/nasa-wildfire-data:ro`), processes data in-memory/on-the-fly, and persists compressed Snappy Parquet to HDFS.

### 5.2 Compression & Efficiency Results
- A sample of 40,000 records occupies only **1,014 KB** in HDFS Snappy Parquet format (~25 bytes/row).
- The entire ~100M row historical dataset compresses to ~2.5 GB in HDFS, fully preserving disk safety on host drive C:.

### 5.3 HDFS Directory Layout
```text
/wildfire
│
├── raw
│   └── sample/
│       └── year=2026/part-00000.csv
│
├── processed/                      (reserved for future intermediate representations)
│
├── curated/
│   ├── year=2020/part-*.snappy.parquet
│   ├── ...
│   ├── year=2026/part-*.snappy.parquet
│   ├── streaming_fire/
│   └── streaming_environmental/
│
├── features/
│   ├── year=2020/part-*.snappy.parquet
│   ├── ...
│   └── year=2026/part-*.snappy.parquet
│
├── models/                         (prepared for Lab 2 ML models)
├── predictions/                    (prepared for Lab 2/3 outputs)
│
└── checkpoints/
    ├── firms_fire_events/
    └── environmental_data/
```

---

## 6. Kafka Infrastructure & Simulators

### 6.1 Topics Created (KRaft Mode)
- `firms_fire_events`: 2 partitions, replication 1 (active fire telemetry replay).
- `environmental_data`: 2 partitions, replication 1 (active synthetic weather telemetry).
- `anomaly_alerts`: 1 partition, replication 1 (prepared for Lab 3).
- `risk_predictions`: 1 partition, replication 1 (prepared for Lab 2/3).

### 6.2 Fire Event Simulator
- File: [`src/simulator/fire_simulator.py`](file:///f:/Downloads/bigdata-lab/bigdata-lab/src/simulator/fire_simulator.py)
- Replays real historical NASA CSV rows as canonical JSON payloads.
- Configurable `--input`, `--bootstrap-server`, `--topic`, `--rate`, `--max-events`, `--loop`.

### 6.3 Environmental Telemetry Simulator
- File: [`src/simulator/environmental_simulator.py`](file:///f:/Downloads/bigdata-lab/bigdata-lab/src/simulator/environmental_simulator.py)
- Generates physically correlated synthetic environmental observations:
  `timestamp, latitude, longitude, temperature, humidity, wind_speed, wind_direction, rainfall, smoke, air_quality_index, source="SIMULATED_ENVIRONMENTAL_TELEMETRY"`.
- Explicitly documented as synthetic data.

---

## 7. Spark Structured Streaming Foundation

- File: [`src/spark/streaming_pipeline.py`](file:///f:/Downloads/bigdata-lab/bigdata-lab/src/spark/streaming_pipeline.py)
- Ingests streaming events from Kafka without inferring schemas dynamically.
- Enforces explicit `FIRE_EVENT_STREAM_SCHEMA` and `ENVIRONMENTAL_STREAM_SCHEMA`.
- Filters invalid coordinate bounds and normalizes timestamps.
- Writes to dedicated streaming paths in HDFS (`/wildfire/curated/streaming_fire` and `/wildfire/curated/streaming_environmental`) partitioned by `year` and `month`.
- Guarantees exactly-once fault tolerance via HDFS checkpointing.

---

## 8. Reproducibility & Pipeline Execution Guide

### 8.1 Dataset Profiling (Spark-First)
```bash
# Profile single year (or test sample)
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/src/profiling/profile_firms.py --year 2026 --sample 50000

# Profile all years 2020-2026
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/src/profiling/profile_firms.py --all-years --sample 50000
```

### 8.2 Historical Batch Processing
```bash
# Run batch pipeline for year 2026
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/src/spark/batch_pipeline.py --year 2026 --sample 20000 --save-raw-sample

# Run batch pipeline across multiple years
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/src/spark/batch_pipeline.py --years 2020 2021 2022 2023 2024 2025 2026
```

### 8.3 Simulators & Kafka Replay
```bash
# Initialize Kafka topics
python scripts/init_kafka_topics.py

# Replay 100 NASA fire events at 10 events/sec
python src/simulator/fire_simulator.py --bootstrap-server localhost:9094 --rate 10 --max-events 100

# Generate 100 synthetic environmental telemetry events at 10 events/sec
python src/simulator/environmental_simulator.py --bootstrap-server localhost:9094 --rate 10 --max-events 100
```

### 8.4 Running Structured Streaming
```bash
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    /opt/src/spark/streaming_pipeline.py --mode both --duration 30
```

### 8.5 Automated Test Verification
```bash
# Unit Tests
python -m unittest tests/test_unit.py -v

# Integration Tests
python tests/test_integration.py

# Complete Infrastructure Smoke Test
python scripts/test_infra.py
```

---

## 9. Handoff to Subsequent Labs

### 9.1 Handoff to Lab 2 (Batch Machine Learning)
- **Ready Data Layer:** `/wildfire/features/year=YYYY` in HDFS.
- **Contract:** Snappy-compressed Parquet with canonical types and pre-calculated spatiotemporal features (`year, month, day, hour, day_of_week, day_of_year, is_weekend, season, is_night, latitude_bucket, longitude_bucket`).
- **Target Tasks in Lab 2:** Wildfire Risk Classification (Random Forest / GBT / XGBoost) and Fire Size Regression.

### 9.2 Handoff to Lab 3 (Real-Time ML & Streaming Intelligence)
- **Ready Ingestion Layer:** Kafka topics `firms_fire_events` and `environmental_data`.
- **Ready Stream Architecture:** Spark Structured Streaming parsing with canonical schemas, validated coordinates, and checkpointed sinks.
- **Target Tasks in Lab 3:** Real-time ML model scoring, Isolation Forest streaming anomaly detection, and alert generation to topic `anomaly_alerts`.

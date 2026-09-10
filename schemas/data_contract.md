# Smart Wildfire Intelligence System — Data Contract (Lab 1)

## 1. Specification Metadata
- **System:** Smart Wildfire Intelligence System
- **Layer:** Data Engineering & Infrastructure (Lab 1)
- **Primary Data Source:** NASA FIRMS (Fire Information for Resource Management System)
- **Sensor / Collection:** Suomi NPP (SNPP) VIIRS 375m Active Fire Product (Collection 2 - SV-C2)
- **Temporal Coverage:** January 1, 2020 – Current (2026 partial / Near Real-Time)
- **Consumer Labs:** Lab 2 (Batch ML Risk & Fire Size), Lab 3 (Real-Time Streaming ML & Anomaly Detection)

---

## 2. Raw Ingestion Schemas & Schema Drift Analysis

The raw dataset consists of 8 CSV files across 7 year partitions (`2020` through `2026`), totaling approximately 10.6 GB. Profiling revealed an architectural schema drift between **Standard Archive** and **Near Real-Time (NRT)** files.

### 2.1 Raw Archive Schema (15 Columns — 2020 to 2025 + 2026 Archive)
Applies to:
- `2020/fire_archive_SV-C2_800032.csv`
- `2021/fire_archive_SV-C2_800031.csv`
- `2022/fire_archive_SV-C2_800030.csv`
- `2023/fire_archive_SV-C2_799968.csv`
- `2024/fire_archive_SV-C2_800027.csv`
- `2025/fire_archive_SV-C2_800028.csv`
- `2026/fire_archive_SV-C2_800029.csv`

| Field Name | Raw Type | Description | Nullable | Example |
|------------|----------|-------------|----------|---------|
| `latitude` | Double | Center latitude of active fire pixel (-90.0 to 90.0) | NO | `69.36538` |
| `longitude` | Double | Center longitude of active fire pixel (-180.0 to 180.0) | NO | `88.11484` |
| `brightness` | Double | VIIRS I-4 channel brightness temperature in Kelvin | YES | `334.44` |
| `scan` | Double | Spatial resolution in the scan direction (km) | YES | `0.53` |
| `track` | Double | Spatial resolution in the track direction (km) | YES | `0.67` |
| `acq_date` | String | Acquisition date of satellite observation (`YYYY-MM-DD`) | NO | `"2020-01-01"` |
| `acq_time` | String | Acquisition time in UTC (`HHMM`, e.g. "0030", "26", "0") | NO | `"0030"` |
| `satellite` | String | Satellite platform name | YES | `"SNPP"` |
| `instrument` | String | Instrument name | YES | `"VIIRS"` / `"SNPP"` |
| `confidence` | String | Detection confidence level (`l` = low, `n` = nominal, `h` = high) | YES | `"n"` |
| `version` | String | Algorithm/product version identifier | YES | `"2"` |
| `bright_t31` | Double | VIIRS I-5 channel brightness temperature in Kelvin | YES | `259.08` |
| `frp` | Double | Fire Radiative Power in Megawatts (MW) | YES | `4.17` |
| `daynight` | String | Day or Night flag (`D` = daytime, `N` = nighttime) | YES | `"N"` |
| `type` | Integer | Fire pixel detection type (0=vegetation, 1=volcano, 2=static land, 3=offshore) | YES | `2` |

### 2.2 Raw Near Real-Time Schema (14 Columns — 2026 NRT)
Applies to:
- `2026/fire_nrt_SV-C2_800029.csv`

**Discovered Differences:**
1. **Missing Column:** The `type` column is **not present** in the NRT file because real-time processing does not perform post-hoc land cover / static source classification.
2. **Version Format:** `version` is formatted as `"2.0NRT"` instead of `"2"`.

---

## 3. Canonical Schema Specification (Harmonized Contract)

Downstream consumers (Lab 2 ML pipelines and Lab 3 Streaming engines) MUST rely exclusively on the canonical schema.

```text
NASA Archive (15 cols) ──────┐
                             ├──► Schema Harmonization ──► Canonical FIRMS Parquet
NASA NRT (14 cols) ──────────┘     (type=null, version=String)
```

### Canonical Field Definitions

| Column | Physical Type | Logical Type | Nullable | Validation Constraints | Default for NRT |
|--------|---------------|--------------|----------|------------------------|-----------------|
| `latitude` | DoubleType | Coordinate | NO | `[-90.0, 90.0]` | — |
| `longitude` | DoubleType | Coordinate | NO | `[-180.0, 180.0]` | — |
| `brightness` | DoubleType | Kelvin | YES | `brightness > 0.0` | — |
| `scan` | DoubleType | Kilometers | YES | `scan >= 0.0` | — |
| `track` | DoubleType | Kilometers | YES | `track >= 0.0` | — |
| `acq_date` | StringType | ISO Date | NO | Regex `^\d{4}-\d{2}-\d{2}$` | — |
| `acq_time` | StringType | Time HH:MM | NO | Normalized to 4 digits `HH:MM` | — |
| `event_timestamp`| TimestampType| UTC Timestamp | YES | Derived from `acq_date` + `acq_time` | — |
| `satellite` | StringType | Categorical | YES | Preserved as String | — |
| `instrument` | StringType | Categorical | YES | Preserved as String | — |
| `confidence` | StringType | Categorical | YES | Allowed: `{'l', 'n', 'h', null}` | — |
| `version` | StringType | String | YES | Preserved as String (e.g. `"2"`, `"2.0NRT"`) | — |
| `bright_t31` | DoubleType | Kelvin | YES | `bright_t31 > 0.0` | — |
| `frp` | DoubleType | MW | YES | `frp >= 0.0` | — |
| `daynight` | StringType | Flag | YES | Allowed: `{'D', 'N', null}` | — |
| `type` | IntegerType | Categorical | YES | Allowed: `{0, 1, 2, 3, null}` | `null` |

---

## 4. Feature Engineered Schema (Downstream ML Handoff)

For Lab 2 (Wildfire Risk Classification & Fire Size Regression), features are strictly derived from NASA observations:

| Feature Name | Type | Derivation Logic | Description |
|--------------|------|------------------|-------------|
| `year` | IntegerType | `year(event_timestamp)` | Partition key and temporal feature |
| `month` | IntegerType | `month(event_timestamp)` | 1 to 12 |
| `day` | IntegerType | `dayofmonth(event_timestamp)` | 1 to 31 |
| `hour` | IntegerType | `hour(event_timestamp)` | 0 to 23 |
| `day_of_week` | IntegerType | `dayofweek(event_timestamp)` | 1 (Sun) to 7 (Sat) |
| `day_of_year` | IntegerType | `dayofyear(event_timestamp)` | 1 to 366 |
| `is_weekend` | IntegerType | `when(day_of_week in (1, 7), 1).otherwise(0)` | Binary indicator |
| `season` | StringType | Month based: Winter(12,1,2), Spring(3,4,5), Summer(6,7,8), Fall(9,10,11) | Astronomical season |
| `is_night` | IntegerType | `when(daynight == 'N', 1).otherwise(0)` | Nocturnal fire detection |
| `latitude_bucket`| DoubleType | `round(latitude, 1)` | Spatial aggregation grid (~11km) |
| `longitude_bucket`| DoubleType | `round(longitude, 1)` | Spatial aggregation grid (~11km) |

> [!CAUTION]
> **No Fabricated NASA Weather Features**: Temperature, humidity, wind, rainfall, and NDVI are NOT part of the NASA FIRMS contract. They belong exclusively to external environmental datasets or simulation streams.

---

## 5. Environmental Telemetry Schema (Separate Stream Contract)

For the real-time environmental simulation and streaming ingestion pipeline:

| Column | Physical Type | Description | Source |
|--------|---------------|-------------|--------|
| `timestamp` | StringType (ISO-8601) | Telemetry observation time | Simulated Telemetry |
| `latitude` | DoubleType | Station / sensor latitude | Simulated Telemetry |
| `longitude` | DoubleType | Station / sensor longitude | Simulated Telemetry |
| `temperature` | DoubleType | Ambient temperature in Celsius | Simulated Telemetry |
| `humidity` | DoubleType | Relative humidity in percentage (0–100) | Simulated Telemetry |
| `wind_speed` | DoubleType | Wind speed in km/h | Simulated Telemetry |
| `wind_direction`| DoubleType | Wind direction in degrees (0–360) | Simulated Telemetry |
| `rainfall` | DoubleType | Precipitation in mm | Simulated Telemetry |
| `smoke` | DoubleType | Particulate matter PM2.5 / smoke index (ug/m3) | Simulated Telemetry |
| `air_quality_index`| IntegerType| Air Quality Index (0–500) | Simulated Telemetry |
| `source` | StringType | Fixed: `"SIMULATED_ENVIRONMENTAL_TELEMETRY"` | Simulated Telemetry |

---

## 6. HDFS Storage & Partitioning Contract

| Stage | HDFS Target Path | Format | Partitioning | Compression |
|-------|------------------|--------|--------------|-------------|
| Raw Demonstration Sample | `/wildfire/raw/sample` | CSV / Parquet | None | Uncompressed |
| Curated Historical | `/wildfire/curated` | Parquet | `year=YYYY` | Snappy |
| Feature-Ready Dataset | `/wildfire/features` | Parquet | `year=YYYY` | Snappy |
| Real-Time Streaming Fire | `/wildfire/curated/streaming_fire` | Parquet | `year=YYYY/month=MM` | Snappy |
| Real-Time Streaming Environmental | `/wildfire/curated/streaming_environmental` | Parquet | `year=YYYY/month=MM` | Snappy |

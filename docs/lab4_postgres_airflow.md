# Lab 4 — PostgreSQL & Airflow

## 1. What was added

```
postgres/schema_wildfire.sql               <- NEW: tables + indexes + dashboard views
src/spark/postgres_sink.py                  <- NEW: shared JDBC helper
src/spark/load_curated_to_postgres.py       <- NEW: batch loader (HDFS -> Postgres)
src/spark/real_time_risk_inference.py       <- MODIFIED: now also writes to Postgres
src/spark/anomaly_streaming.py              <- MODIFIED: now also writes to Postgres
src/spark/alert_generation.py               <- MODIFIED: now also writes to Postgres
airflow/dags/load_curated_to_postgres_dag.py <- NEW: daily batch-load DAG
airflow/dags/model_retraining_dag.py        <- NEW: retraining DAG (reuses Lab 2 scripts)
scripts/start_streaming_pipeline.sh         <- NEW: launches the 3 streaming jobs
scripts/stop_streaming_pipeline.sh          <- NEW: stops them
docker-compose.yml                          <- MODIFIED: airflow env vars + volumes
airflow/docker/requirements.txt             <- MODIFIED: +psycopg2-binary, +numpy
```

## 2. Why streaming jobs are NOT Airflow tasks

Airflow schedules things that **start and finish**. `real_time_risk_inference.py`,
`anomaly_streaming.py` and `alert_generation.py` run **forever** (Spark
Structured Streaming). Wrapping an infinite job in an Airflow task blocks a
worker slot forever and isn't how streaming apps are normally run — real
systems start them once (via `spark-submit`, a supervisor, or a
`start.sh`-style script) and let the cluster manager keep them alive.

So the split is:
- **Airflow** → the two *finite* jobs: batch-loading curated data into
  Postgres, and periodically retraining the ML models.
- **`scripts/start_streaming_pipeline.sh`** → the 3 *infinite* streaming
  jobs, same idea as your existing `scripts/start.sh` / `run_lab1_full.ps1`.

## 3. ERD (text form)

```
fire_events            risk_predictions         fire_size_predictions
------------           ------------------        ----------------------
event_id PK             prediction_id PK          prediction_id PK
event_timestamp         event_timestamp           event_timestamp
latitude / longitude    latitude / longitude      latitude / longitude
brightness, frp, ...    prediction, risk_level    predicted_frp
                         model_version             model_version

anomalies                          alerts
------------                       ------------------
anomaly_id PK                      alert_id PK
event_timestamp                    event_timestamp
latitude / longitude               latitude / longitude
anomaly_score, is_anomaly          alert_source (risk_prediction | anomaly_detection)
alert_level, alert_reason          risk_level, anomaly_score, alert_level, alert_message

All 5 tables -> joined only inside VIEWS (v_active_alerts, v_daily_fire_kpis,
v_risk_distribution, v_anomaly_summary), which Lab 5 queries directly.
```

There's no foreign key between the tables on purpose: `fire_events` is
historical/curated, while `risk_predictions` / `anomalies` / `alerts` are
independent live streams keyed loosely by `(event_timestamp, latitude,
longitude)`. Joining them precisely is optional future work, not required
for the dashboard.

## 4. How to run everything (order matters)

### Step 0 — one-time: add the Postgres JDBC driver dependency
Nothing to install manually — `--packages org.postgresql:postgresql:42.7.3`
in every spark-submit / SparkSession config below downloads it automatically
from Maven Central the first time (needs internet access from the
`spark-master` / `airflow-scheduler` containers). If your machine has no
internet access from inside Docker, download
`postgresql-42.7.3.jar` once and mount it into `/spark/jars/` on
spark-master/worker instead — ask me and I'll adjust the scripts.

### Step 1 — apply the Postgres schema
The `wildfire` database + role already exist (from Lab 1's
`postgres/init-wildfire-db.sql`), so just run the new schema file against it:

```bash
docker cp postgres/schema_wildfire.sql airflow-postgres:/tmp/schema_wildfire.sql
docker exec -it airflow-postgres psql -U wildfire -d wildfire -f /tmp/schema_wildfire.sql
```

Verify:
```bash
docker exec -it airflow-postgres psql -U wildfire -d wildfire -c "\dt wildfire.*"
```
You should see `fire_events`, `risk_predictions`, `fire_size_predictions`,
`anomalies`, `alerts`.

### Step 2 — rebuild the Airflow image (requirements.txt changed)
```bash
docker compose build airflow-scheduler airflow-webserver airflow-init
docker compose --profile orchestration up -d
```

### Step 3 — batch-load historical curated data into Postgres
Either from Airflow (recommended — it's the deliverable):
```bash
docker exec airflow-scheduler airflow dags trigger load_curated_to_postgres
```
or directly via spark-submit, same result:
```bash
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.7.3 \
    /opt/src/spark/load_curated_to_postgres.py --years 2020 2021 2022 2023 2024 2025 2026
```
Check it worked:
```bash
docker exec -it airflow-postgres psql -U wildfire -d wildfire -c "SELECT COUNT(*) FROM wildfire.fire_events;"
```

### Step 4 — start the real-time streaming pipeline (writes to Kafka *and* Postgres)
```bash
chmod +x scripts/start_streaming_pipeline.sh scripts/stop_streaming_pipeline.sh
./scripts/start_streaming_pipeline.sh
```
Then feed it events with your Lab 1 simulator (same as before), and confirm
rows are landing in Postgres:
```bash
docker exec -it airflow-postgres psql -U wildfire -d wildfire -c \
  "SELECT * FROM wildfire.v_active_alerts LIMIT 10;"
```
Stop with `./scripts/stop_streaming_pipeline.sh` when done.

### Step 5 — (optional) run the retraining DAG
```bash
docker exec airflow-scheduler airflow dags trigger model_retraining
```

### Step 6 — hand off to Lab 5
Lab 5's Streamlit app should connect to Postgres with:
```
host=postgres (or localhost if running Streamlit outside Docker on the same
machine — use POSTGRES_PORT from .env), port=5432, dbname=wildfire,
user=wildfire, password=wildfire_secret
```
and read straight from `wildfire.v_active_alerts`, `wildfire.v_daily_fire_kpis`,
`wildfire.v_risk_distribution`, `wildfire.v_anomaly_summary`, and
`wildfire.fire_events` for the map.

## 5. Gotchas worth knowing

- **`load_curated_to_postgres` truncates the whole table every run** and
  reloads exactly the years listed in `YEARS` inside the DAG / passed via
  `--years`. If you add a new year of data, update both
  `airflow/dags/load_curated_to_postgres_dag.py` (`YEARS` list) and the
  `--years` args in Step 3, or you'll silently drop older years.
- **Never write to a table created by `schema_wildfire.sql` with plain
  Spark `mode("overwrite")`** without `.option("truncate", "true")` —
  Spark's default overwrite does `DROP TABLE` + recreate, which deletes your
  indexes/constraints and breaks the views. `postgres_sink.py` already
  handles this correctly; just don't bypass it.
- The 3 streaming scripts now run **two** `writeStream` queries each (Kafka +
  Postgres) and end with `spark.streams.awaitAnyTermination()` instead of
  `query.awaitTermination()` — if one sink dies the whole job will exit, which
  is what you want during grading/demo (fails loudly instead of silently
  losing a sink).

-- ============================================================================
-- Smart Wildfire Intelligence System — Lab 4 PostgreSQL Schema
-- ============================================================================
-- Runs against the `wildfire` database (created already by
-- postgres/init-wildfire-db.sql, owner role = wildfire).
--
-- How to apply (see docs/lab4_postgres_airflow.md for the full walkthrough):
--   docker exec -i airflow-postgres psql -U wildfire -d wildfire \
--       < postgres/schema_wildfire.sql
--
-- Design notes:
--   * Column names mirror schemas/data_contract.md and the Kafka message
--     schemas used in src/spark/*.py (Lab 1/2/3) 1:1, so Spark -> Postgres
--     writes need zero remapping.
--   * All tables use `created_at` (load time) separately from
--     `event_timestamp` (satellite/observation time) so the dashboard can
--     tell "when it happened" apart from "when we ingested it".
--   * Idempotent: safe to re-run (CREATE ... IF NOT EXISTS everywhere).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS wildfire AUTHORIZATION wildfire;
SET search_path TO wildfire, public;

-- ----------------------------------------------------------------------------
-- 1. fire_events — curated historical + near-real-time fire detections
--    Source: HDFS /wildfire/curated (Parquet), loaded by
--    src/spark/load_curated_to_postgres.py (Airflow DAG: load_curated_to_postgres)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wildfire.fire_events (
    event_id         BIGSERIAL PRIMARY KEY,
    event_timestamp  TIMESTAMP        NOT NULL,
    latitude         DOUBLE PRECISION NOT NULL,
    longitude        DOUBLE PRECISION NOT NULL,
    brightness       DOUBLE PRECISION,
    bright_t31       DOUBLE PRECISION,
    scan             DOUBLE PRECISION,
    track             DOUBLE PRECISION,
    frp              DOUBLE PRECISION,
    confidence       VARCHAR(10),
    daynight         VARCHAR(1),
    satellite        VARCHAR(20),
    instrument       VARCHAR(20),
    version          VARCHAR(20),
    type             SMALLINT,
    year             SMALLINT,
    season           VARCHAR(10),
    is_night         SMALLINT,
    created_at       TIMESTAMP DEFAULT now(),
    CONSTRAINT uq_fire_event UNIQUE (event_timestamp, latitude, longitude)
);

CREATE INDEX IF NOT EXISTS idx_fire_events_ts    ON wildfire.fire_events (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fire_events_geo    ON wildfire.fire_events (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_fire_events_year   ON wildfire.fire_events (year);

-- ----------------------------------------------------------------------------
-- 2. risk_predictions — output of the Lab 2 risk classifier, written live
--    by src/spark/real_time_risk_inference.py (Kafka topic: risk_predictions)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wildfire.risk_predictions (
    prediction_id    BIGSERIAL PRIMARY KEY,
    event_timestamp  TIMESTAMP        NOT NULL,
    latitude         DOUBLE PRECISION NOT NULL,
    longitude        DOUBLE PRECISION NOT NULL,
    brightness       DOUBLE PRECISION,
    bright_t31       DOUBLE PRECISION,
    frp              DOUBLE PRECISION,
    confidence       VARCHAR(10),
    daynight         VARCHAR(1),
    prediction       DOUBLE PRECISION,
    risk_level       VARCHAR(10),          -- High / Medium / Low
    model_version    VARCHAR(50) DEFAULT 'risk_classifier_v1',
    created_at       TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_risk_pred_ts    ON wildfire.risk_predictions (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_risk_pred_level ON wildfire.risk_predictions (risk_level);
CREATE INDEX IF NOT EXISTS idx_risk_pred_geo   ON wildfire.risk_predictions (latitude, longitude);

-- ----------------------------------------------------------------------------
-- 3. fire_size_predictions — output of the Lab 2 FRP regressor
--    Source: batch scoring or a streaming job analogous to real_time_risk_inference.py
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wildfire.fire_size_predictions (
    prediction_id    BIGSERIAL PRIMARY KEY,
    event_timestamp  TIMESTAMP        NOT NULL,
    latitude         DOUBLE PRECISION NOT NULL,
    longitude        DOUBLE PRECISION NOT NULL,
    predicted_frp    DOUBLE PRECISION,     -- MW, proxy for fire size/intensity
    model_version    VARCHAR(50) DEFAULT 'fire_size_regressor_v1',
    created_at       TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fire_size_pred_ts  ON wildfire.fire_size_predictions (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fire_size_pred_geo ON wildfire.fire_size_predictions (latitude, longitude);

-- ----------------------------------------------------------------------------
-- 4. anomalies — output of Lab 3 anomaly detection
--    Source: src/spark/anomaly_streaming.py (Kafka topic: anomaly_alerts)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wildfire.anomalies (
    anomaly_id       BIGSERIAL PRIMARY KEY,
    event_timestamp  TIMESTAMP        NOT NULL,
    latitude         DOUBLE PRECISION NOT NULL,
    longitude        DOUBLE PRECISION NOT NULL,
    frp              DOUBLE PRECISION,
    brightness       DOUBLE PRECISION,
    bright_t31       DOUBLE PRECISION,
    scan             DOUBLE PRECISION,
    track            DOUBLE PRECISION,
    confidence       VARCHAR(10),
    daynight         VARCHAR(1),
    anomaly_score    DOUBLE PRECISION,
    is_anomaly       SMALLINT,
    alert_level      VARCHAR(10),
    alert_reason     TEXT,
    created_at       TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_ts       ON wildfire.anomalies (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_anomalies_is_anom  ON wildfire.anomalies (is_anomaly);

-- ----------------------------------------------------------------------------
-- 5. alerts — final merged alert stream
--    Source: src/spark/alert_generation.py (Kafka topic: wildfire_alerts)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wildfire.alerts (
    alert_id             BIGSERIAL PRIMARY KEY,
    event_timestamp      TIMESTAMP        NOT NULL,
    latitude             DOUBLE PRECISION NOT NULL,
    longitude            DOUBLE PRECISION NOT NULL,
    frp                  DOUBLE PRECISION,
    anomaly_score        DOUBLE PRECISION,
    risk_level           VARCHAR(10),
    anomaly_alert_level  VARCHAR(10),
    alert_level          VARCHAR(10),      -- HIGH / MEDIUM
    alert_source         VARCHAR(30),      -- risk_prediction / anomaly_detection
    alert_reason         TEXT,
    alert_message        TEXT,
    created_at           TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_ts     ON wildfire.alerts (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_level  ON wildfire.alerts (alert_level);
CREATE INDEX IF NOT EXISTS idx_alerts_source ON wildfire.alerts (alert_source);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON wildfire.alerts (created_at DESC);

-- ============================================================================
-- Dashboard views (Lab 5 reads from these directly — no joins needed there)
-- ============================================================================

-- Most recent alerts first, ready for a live "Alerts" panel
CREATE OR REPLACE VIEW wildfire.v_active_alerts AS
SELECT alert_id, event_timestamp, latitude, longitude, frp, anomaly_score,
       risk_level, alert_level, alert_source, alert_reason, alert_message, created_at
FROM wildfire.alerts
ORDER BY created_at DESC
LIMIT 500;

-- Daily KPIs: fire count, avg/max FRP, per day
CREATE OR REPLACE VIEW wildfire.v_daily_fire_kpis AS
SELECT date_trunc('day', event_timestamp)::date AS event_date,
       COUNT(*)                                  AS fire_count,
       ROUND(AVG(frp)::numeric, 2)                AS avg_frp,
       ROUND(MAX(frp)::numeric, 2)                AS max_frp,
       COUNT(*) FILTER (WHERE daynight = 'N')     AS night_fires
FROM wildfire.fire_events
GROUP BY 1
ORDER BY 1 DESC;

-- Risk level distribution (for a pie/bar chart)
CREATE OR REPLACE VIEW wildfire.v_risk_distribution AS
SELECT risk_level, COUNT(*) AS count
FROM wildfire.risk_predictions
GROUP BY risk_level;

-- Anomaly summary per day
CREATE OR REPLACE VIEW wildfire.v_anomaly_summary AS
SELECT date_trunc('day', event_timestamp)::date AS event_date,
       COUNT(*) FILTER (WHERE is_anomaly = 1)    AS anomaly_count,
       ROUND(AVG(anomaly_score)::numeric, 3)     AS avg_anomaly_score
FROM wildfire.anomalies
GROUP BY 1
ORDER BY 1 DESC;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA wildfire TO wildfire;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA wildfire TO wildfire;

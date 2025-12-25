-- ===========================================
-- KPI TABLES (Serving layer for BI dashboards)
-- ===========================================

-- 1) Minute machine KPIs (core for Operations dashboard)
CREATE TABLE IF NOT EXISTS kpi_minute_machine (
  minute_ts              TIMESTAMP NOT NULL,

  factory_id             INT,
  factory_name           TEXT,
  city                   TEXT,
  capacity_class         TEXT,

  machine_id             INT,
  machine_name           TEXT,
  machine_type           TEXT,
  vendor                 TEXT,
  criticality            TEXT,

  product_id             INT,
  product_name           TEXT,

  operator_id            INT,
  operator_name          TEXT,

  events_count           BIGINT NOT NULL,
  avg_reading            DOUBLE PRECISION,
  max_reading            DOUBLE PRECISION,
  min_reading            DOUBLE PRECISION,

  downtime_events        BIGINT NOT NULL,
  downtime_rate_pct      DOUBLE PRECISION,
  planned_downtime_events BIGINT NOT NULL,
  unplanned_downtime_events BIGINT NOT NULL,

  availability_pct_proxy DOUBLE PRECISION,

  run_ts                 TIMESTAMP NOT NULL,

  PRIMARY KEY (minute_ts, factory_id, machine_id, sensorless_pk_guard)
);

-- NOTE: PostgreSQL needs concrete PK columns.
-- Since we don't include sensor_type_id here (machine-level KPI),
-- we use a small trick: add a constant guard column so PK is valid.
-- If you dislike this, we can instead include sensor_type_id or remove PK and use indexes.
ALTER TABLE kpi_minute_machine
  ADD COLUMN IF NOT EXISTS sensorless_pk_guard SMALLINT DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_kpi_minute_machine_minute_ts
ON kpi_minute_machine (minute_ts);

CREATE INDEX IF NOT EXISTS idx_kpi_minute_machine_factory_machine
ON kpi_minute_machine (factory_id, machine_id);

CREATE INDEX IF NOT EXISTS idx_kpi_minute_machine_criticality
ON kpi_minute_machine (criticality);


-- 2) Hourly factory KPIs (trend)
CREATE TABLE IF NOT EXISTS kpi_hourly_factory (
  hour_ts            TIMESTAMP NOT NULL,
  factory_id         INT,
  factory_name       TEXT,
  city               TEXT,
  capacity_class     TEXT,

  events_count       BIGINT NOT NULL,
  avg_reading        DOUBLE PRECISION,
  max_reading        DOUBLE PRECISION,

  downtime_events    BIGINT NOT NULL,
  downtime_rate_pct  DOUBLE PRECISION,
  planned_downtime_events BIGINT NOT NULL,

  availability_pct_proxy DOUBLE PRECISION,
  run_ts             TIMESTAMP NOT NULL,

  PRIMARY KEY (hour_ts, factory_id)
);

CREATE INDEX IF NOT EXISTS idx_kpi_hourly_factory_hour_ts
ON kpi_hourly_factory (hour_ts);


-- 3) Daily machine KPIs (reliability ranking)
CREATE TABLE IF NOT EXISTS kpi_daily_machine (
  day_dt                 DATE NOT NULL,

  factory_id             INT,
  factory_name           TEXT,
  city                   TEXT,

  machine_id             INT,
  machine_name           TEXT,
  machine_type           TEXT,
  criticality            TEXT,

  events_count           BIGINT NOT NULL,
  downtime_events        BIGINT NOT NULL,
  unplanned_downtime_events BIGINT NOT NULL,
  downtime_rate_pct      DOUBLE PRECISION,
  availability_pct_proxy DOUBLE PRECISION,

  avg_reading            DOUBLE PRECISION,
  max_reading            DOUBLE PRECISION,

  run_ts                 TIMESTAMP NOT NULL,

  PRIMARY KEY (day_dt, machine_id)
);

CREATE INDEX IF NOT EXISTS idx_kpi_daily_machine_day
ON kpi_daily_machine (day_dt);

CREATE INDEX IF NOT EXISTS idx_kpi_daily_machine_factory
ON kpi_daily_machine (factory_id);


-- 4) Daily product KPIs (product impact story)
CREATE TABLE IF NOT EXISTS kpi_daily_product (
  day_dt            DATE NOT NULL,

  factory_id        INT,
  factory_name      TEXT,

  product_id        INT,
  product_name      TEXT,
  category          TEXT,

  events_count      BIGINT NOT NULL,
  avg_reading       DOUBLE PRECISION,
  max_reading       DOUBLE PRECISION,

  downtime_events   BIGINT NOT NULL,
  downtime_rate_pct DOUBLE PRECISION,

  run_ts            TIMESTAMP NOT NULL,

  PRIMARY KEY (day_dt, factory_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_kpi_daily_product_day
ON kpi_daily_product (day_dt);


-- 5) Alert feed (Quality dashboard “live alerts”)
CREATE TABLE IF NOT EXISTS kpi_alert_events (
  event_timestamp_utc  TIMESTAMP NOT NULL,

  factory_id           INT,
  factory_name         TEXT,
  city                 TEXT,

  machine_id           INT,
  machine_name         TEXT,
  machine_type         TEXT,
  criticality          TEXT,

  sensor_type_id       INT,
  sensor_type_code     TEXT,
  sensor_type_name     TEXT,
  unit                 TEXT,

  product_id           INT,
  product_name         TEXT,
  category             TEXT,

  operator_id          INT,
  operator_name        TEXT,

  actual_value         DOUBLE PRECISION,
  threshold_value      DOUBLE PRECISION,
  threshold_condition  TEXT,
  severity             TEXT,
  alert_type           TEXT,
  alert_message        TEXT,

  run_ts               TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kpi_alert_events_ts
ON kpi_alert_events (event_timestamp_utc);

CREATE INDEX IF NOT EXISTS idx_kpi_alert_events_severity
ON kpi_alert_events (severity);

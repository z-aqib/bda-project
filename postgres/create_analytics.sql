-- Create schema (optional)
-- CREATE SCHEMA IF NOT EXISTS analytics;

-- 1) Overall KPIs per minute
CREATE TABLE IF NOT EXISTS agg_overall_minute (
  minute_ts              TIMESTAMP PRIMARY KEY,
  total_events           BIGINT NOT NULL,
  avg_reading            DOUBLE PRECISION,
  max_reading            DOUBLE PRECISION,
  min_reading            DOUBLE PRECISION,
  downtime_events        BIGINT NOT NULL,
  downtime_rate_pct      DOUBLE PRECISION,
  planned_downtime_events BIGINT NOT NULL,
  unplanned_downtime_events BIGINT NOT NULL,
  updated_at             TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agg_overall_minute_minute_ts
ON agg_overall_minute (minute_ts);


-- 2) Factory KPIs per minute
CREATE TABLE IF NOT EXISTS agg_factory_minute (
  minute_ts         TIMESTAMP NOT NULL,
  factory_id        INT NOT NULL,
  factory_name      TEXT,
  factory_location  TEXT,
  total_events      BIGINT NOT NULL,
  avg_reading       DOUBLE PRECISION,
  max_reading       DOUBLE PRECISION,
  downtime_events   BIGINT NOT NULL,
  downtime_rate_pct DOUBLE PRECISION,
  updated_at        TIMESTAMP NOT NULL,
  PRIMARY KEY (minute_ts, factory_id)
);

CREATE INDEX IF NOT EXISTS idx_agg_factory_minute_minute_ts
ON agg_factory_minute (minute_ts);

CREATE INDEX IF NOT EXISTS idx_agg_factory_minute_factory_id
ON agg_factory_minute (factory_id);


-- 3) Sensor KPIs per minute (by sensor type)
CREATE TABLE IF NOT EXISTS agg_sensor_minute (
  minute_ts         TIMESTAMP NOT NULL,
  sensor_type_id    INT NOT NULL,
  sensor_type_name  TEXT,
  unit              TEXT,
  total_events      BIGINT NOT NULL,
  avg_reading       DOUBLE PRECISION,
  max_reading       DOUBLE PRECISION,
  min_reading       DOUBLE PRECISION,
  updated_at        TIMESTAMP NOT NULL,
  PRIMARY KEY (minute_ts, sensor_type_id)
);

CREATE INDEX IF NOT EXISTS idx_agg_sensor_minute_minute_ts
ON agg_sensor_minute (minute_ts);

CREATE INDEX IF NOT EXISTS idx_agg_sensor_minute_sensor_type_id
ON agg_sensor_minute (sensor_type_id);


-- 4) Top machines snapshot (keeps history each run)
CREATE TABLE IF NOT EXISTS agg_top_machines_last60 (
  run_ts                     TIMESTAMP NOT NULL,
  machine_id                 INT NOT NULL,
  machine_name               TEXT,
  factory_id                 INT,
  factory_name               TEXT,
  criticality                TEXT,
  total_events               BIGINT NOT NULL,
  unplanned_downtime_events  BIGINT NOT NULL,
  unplanned_downtime_rate_pct DOUBLE PRECISION,
  avg_reading                DOUBLE PRECISION,
  max_reading                DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_agg_top_machines_run_ts
ON agg_top_machines_last60 (run_ts);

CREATE INDEX IF NOT EXISTS idx_agg_top_machines_machine_id
ON agg_top_machines_last60 (machine_id);

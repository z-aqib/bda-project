CREATE TABLE IF NOT EXISTS fact_machine_alert_events (
    alert_id               BIGSERIAL PRIMARY KEY,

    alert_time_utc          TIMESTAMP NOT NULL DEFAULT NOW(),

    machine_id              INT NOT NULL,
    sensor_type_id          INT NOT NULL,
    product_id              INT,
    operator_id             INT,

    actual_value            NUMERIC(12,4) NOT NULL,
    threshold_value         NUMERIC(12,4) NOT NULL,
    threshold_condition     VARCHAR(5) NOT NULL,   -- '>', '<', '>=', '<='

    severity                VARCHAR(20) NOT NULL,  -- 'WARNING', 'CRITICAL'
    alert_type              VARCHAR(100) NOT NULL, -- 'High Temperature'
    alert_message           VARCHAR(255) NOT NULL,

    resolved_flag           BOOLEAN DEFAULT FALSE,
    resolved_at             TIMESTAMP NULL,

    CONSTRAINT fk_alert_machine
        FOREIGN KEY (machine_id) REFERENCES dim_machine(machine_id),
    CONSTRAINT fk_alert_sensor
        FOREIGN KEY (sensor_type_id) REFERENCES dim_sensor_type(sensor_type_id),
    CONSTRAINT fk_alert_product
        FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    CONSTRAINT fk_alert_operator
        FOREIGN KEY (operator_id) REFERENCES dim_operator(operator_id)
);

CREATE TABLE IF NOT EXISTS agg_overall_minute (
  minute_ts TIMESTAMP PRIMARY KEY,
  event_count BIGINT,
  avg_value DOUBLE PRECISION,
  min_value DOUBLE PRECISION,
  max_value DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS agg_factory_minute (
  minute_ts TIMESTAMP,
  factory_id INT,
  event_count BIGINT,
  avg_value DOUBLE PRECISION,
  min_value DOUBLE PRECISION,
  max_value DOUBLE PRECISION,
  PRIMARY KEY (minute_ts, factory_id)
);

CREATE TABLE IF NOT EXISTS agg_machine_minute (
  minute_ts TIMESTAMP,
  factory_id INT,
  machine_id INT,
  event_count BIGINT,
  avg_value DOUBLE PRECISION,
  min_value DOUBLE PRECISION,
  max_value DOUBLE PRECISION,
  PRIMARY KEY (minute_ts, factory_id, machine_id)
);

CREATE TABLE IF NOT EXISTS agg_product_minute (
  minute_ts TIMESTAMP,
  factory_id INT,
  product_id INT,
  event_count BIGINT,
  avg_value DOUBLE PRECISION,
  min_value DOUBLE PRECISION,
  max_value DOUBLE PRECISION,
  PRIMARY KEY (minute_ts, factory_id, product_id)
);

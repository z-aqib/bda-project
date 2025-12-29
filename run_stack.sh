#!/usr/bin/env bash
set -euo pipefail

# Run from repo root
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# --- Compose file paths ---
KAFKA_YML="kafka/kafka-docker-compose.yml"
GEN_YML="data_generator/generator-docker-compose.yml"
MONGO_YML="mongo/mongo-docker-compose.yml"
CONSUMER_YML="data_consumer/consumer-docker-compose.yml"
POSTGRES_YML="postgres/postgres-docker-compose.yml"
SPARK_YML="spark/spark-docker-compose.yml"
HDFS_YML="hdfs/hdfs-docker-compose.yml"
SUPERSET_YML="superset_bi/superset-docker-compose.yml"
AIRFLOW_YML="airflow/airflow-docker-compose.yml"

dc() {
  docker compose "$@"
}

echo "==> Ensuring shared Docker network exists..."
docker network inspect bda-network >/dev/null 2>&1 || docker network create bda-network

echo "==> 1) Starting Kafka + Zookeeper..."
dc -f "$KAFKA_YML" up -d --build

echo "==> 2) Starting MongoDB..."
dc -f "$MONGO_YML" up -d --build

echo "==> 3) Starting Data Generator..."
dc -f "$GEN_YML" up -d --build

echo "==> 4) Starting Kafka → Mongo Consumer..."
dc -f "$CONSUMER_YML" up -d --build

echo "==> 5) Starting Postgres..."
dc -f "$POSTGRES_YML" up -d --build

echo
echo "==> Waiting for Postgres..."
for i in {1..30}; do
  if docker exec postgres pg_isready -U analytics_user -d analytics_db >/dev/null 2>&1; then
    echo "      Postgres is ready."
    break
  fi
  sleep 2
done

# ------------------------------------------------------------------
# POSTGRES INIT
# ------------------------------------------------------------------

echo
echo "==> Copying CSVs into Postgres container..."

CSV_SRC="$ROOT_DIR/data_generator/static_data"

docker cp "$CSV_SRC/dim_factory.csv" postgres:/tmp/dim_factory.csv
docker cp "$CSV_SRC/dim_machine.csv" postgres:/tmp/dim_machine.csv
docker cp "$CSV_SRC/dim_operator.csv" postgres:/tmp/dim_operator.csv
docker cp "$CSV_SRC/dim_product.csv" postgres:/tmp/dim_product.csv
docker cp "$CSV_SRC/dim_sensor_type.csv" postgres:/tmp/dim_sensor_type.csv

echo "==> Creating tables & loading data..."

docker exec -i postgres psql -U analytics_user -d analytics_db <<'EOF'

-- DIMENSIONS
CREATE TABLE IF NOT EXISTS public.dim_factory (
    factory_id VARCHAR(100) PRIMARY KEY,
    factory_code VARCHAR(100),
    factory_name VARCHAR(100),
    city VARCHAR(100),
    capacity_class VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS public.dim_machine (
    machine_id VARCHAR(20) PRIMARY KEY,
    machine_name VARCHAR(100),
    line_code VARCHAR(50),
    line_name VARCHAR(100),
    product_family VARCHAR(50),
    factory_id VARCHAR(20),
    machine_type VARCHAR(50),
    vendor VARCHAR(50),
    install_date DATE,
    criticality VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS public.dim_operator (
    operator_id VARCHAR(20) PRIMARY KEY,
    operator_name VARCHAR(100),
    experience_level VARCHAR(20),
    shift_pattern VARCHAR(50),
    factory_id VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS public.dim_product (
    product_id VARCHAR(20) PRIMARY KEY,
    sku VARCHAR(50),
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50),
    pack_size_g INTEGER
);

CREATE TABLE IF NOT EXISTS public.dim_sensor_type (
    sensor_type_id INTEGER PRIMARY KEY,
    sensor_type_code VARCHAR(20),
    sensor_type_name VARCHAR(50),
    unit VARCHAR(20),
    typical_min NUMERIC(10,2),
    typical_max NUMERIC(10,2)
);

TRUNCATE dim_factory, dim_machine, dim_operator, dim_product, dim_sensor_type;

COPY dim_factory FROM '/tmp/dim_factory.csv' CSV HEADER;
COPY dim_machine FROM '/tmp/dim_machine.csv' CSV HEADER;
COPY dim_operator FROM '/tmp/dim_operator.csv' CSV HEADER;
COPY dim_product FROM '/tmp/dim_product.csv' CSV HEADER;
COPY dim_sensor_type FROM '/tmp/dim_sensor_type.csv' CSV HEADER;

-- FACT TABLE
CREATE TABLE IF NOT EXISTS public.minute_kpi (
    kpi_minute_start_utc timestamp,
    factory_id text,
    machine_id text,
    sensor_type_id integer,
    product_id integer,
    operator_id integer,
    avg_reading double precision,
    max_reading double precision,
    min_reading double precision,
    num_events bigint NOT NULL,
    planned_downtime_sec integer NOT NULL,
    unplanned_downtime_sec integer NOT NULL,
    num_alerts bigint
);

-- ALERT EVENTS
CREATE TABLE IF NOT EXISTS public.alert_events (
    alert_id bigint NOT NULL,
    alert_time_utc timestamp NOT NULL,
    machine_id varchar(50),
    factory_id varchar(50),
    sensor_type_id integer,
    product_id varchar(50),
    operator_id varchar(50),
    actual_value numeric(12,4),
    threshold_value numeric(12,4),
    threshold_condition varchar(5),
    severity varchar(20),
    alert_type varchar(100),
    alert_message varchar(255),
    resolved_flag boolean DEFAULT false,
    resolved_at timestamp,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);

CREATE SEQUENCE IF NOT EXISTS alert_events_alert_id_seq;
ALTER TABLE alert_events
ALTER COLUMN alert_id SET DEFAULT nextval('alert_events_alert_id_seq');
ALTER SEQUENCE alert_events_alert_id_seq
OWNED BY alert_events.alert_id;

EOF

echo "      Postgres initialized."

echo "==> 6) Starting HDFS..."
dc -f "$HDFS_YML" up -d --build

echo "==> 7) Starting Spark..."
dc -f "$SPARK_YML" up -d --build

echo "==> 8) Starting Superset..."
dc -f "$SUPERSET_YML" up -d --build

# ------------------------------------------------------------------
# SUPERSET INIT
# ------------------------------------------------------------------

SUPERSET_USER="admin"
SUPERSET_PASS="admin123"

echo
echo "==> Initializing Superset..."

docker exec superset superset db upgrade
docker exec superset superset init

docker exec superset superset fab create-admin \
  --username "$SUPERSET_USER" \
  --firstname Admin \
  --lastname User \
  --email admin@superset.com \
  --password "$SUPERSET_PASS" || true

echo
echo "Superset credentials:"
echo "  URL:      http://localhost:8088"
echo "  Username: $SUPERSET_USER"
echo "  Password: $SUPERSET_PASS"

echo "==> 9) Starting Airflow..."
dc -f "$AIRFLOW_YML" up -d --build

# ------------------------------------------------------------------
# AIRFLOW INIT
# ------------------------------------------------------------------

AIRFLOW_USER="admin"
AIRFLOW_PASS="admin123"

echo
echo "==> Initializing Airflow..."

docker exec airflow-webserver airflow db upgrade

docker exec airflow-webserver airflow users create \
  --username "$AIRFLOW_USER" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@airflow.com \
  --password "$AIRFLOW_PASS" || true

echo
echo "Airflow credentials:"
echo "  URL:      http://localhost:8080"
echo "  Username: $AIRFLOW_USER"
echo "  Password: $AIRFLOW_PASS"

echo
echo "==> STACK READY 🚀"
echo "--------------------------------------"
echo " Kafka:        kafka:9092"
echo " MongoDB:      localhost:27017"
echo " Postgres:     localhost:5432"
echo " Superset:     http://localhost:8088"
echo " Airflow:      http://localhost:8080"
echo " Spark UI:     http://localhost:4040"
echo "--------------------------------------"

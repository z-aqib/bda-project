#!/usr/bin/env bash
set -euo pipefail

# Run from repo root:  ~/bda-project
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# --- Compose file paths (repo structure) ---
KAFKA_YML="kafka/kafka-docker-compose.yml"
GEN_YML="data_generator/generator-docker-compose.yml"
MONGO_YML="mongo/mongo-docker-compose.yml"
CONSUMER_YML="data_consumer/consumer-docker-compose.yml"
POSTGRES_YML="postgres/postgres-docker-compose.yml"
SPARK_YML="spark/spark-docker-compose.yml"
HDFS_YML="hdfs/hdfs-docker-compose.yml"
SUPERSET_YML="superset_bi/superset-docker-compose.yml"
AIRFLOW_YML="airflow/airflow-docker-compose.yml"

# --- Helper: use docker compose v2 if available ---
dc() {
  # If your machine uses legacy docker-compose, replace `docker compose` with `docker-compose`
  docker compose "$@"
}

echo "==> Ensuring shared Docker network exists..."
docker network inspect bda-network >/dev/null 2>&1 || docker network create bda-network

echo "==> 1) Starting Kafka + Zookeeper..."
dc -f "$KAFKA_YML" up -d --build

echo "==> 2) Starting MongoDB (fresh storage)..."
dc -f "$MONGO_YML" up -d --build

echo "==> 3) Starting Data Generator (Kafka producer)..."
dc -f "$GEN_YML" up -d --build

echo "==> 4) Starting Kafka → Mongo Consumer..."
dc -f "$CONSUMER_YML" up -d --build

echo "==> 5) Starting Postgres (analytics DB for Spark outputs)..."
dc -f "$POSTGRES_YML" up -d --build

echo "==> 6) Starting HDFS (archive + metadata storage)..."
dc -f "$HDFS_YML" up -d --build

echo "==> 7) Starting Spark (processing + KPI computation)..."
dc -f "$SPARK_YML" up -d --build

echo "==> 8) Starting Superset (BI dashboard)..."
dc -f "$SUPERSET_YML" up -d --build

echo "==> 9) Starting Airflow (orchestration)..."
dc -f "$AIRFLOW_YML" up -d --build

echo
echo "==> Basic readiness checks (best-effort)..."

echo "   -> Waiting for Postgres to accept connections..."
for i in {1..30}; do
  if docker exec postgres pg_isready -U analytics_user -d analytics_db >/dev/null 2>&1; then
    echo "      Postgres is ready."
    break
  fi
  sleep 2
done

echo "   -> Checking Mongo responds..."
if docker exec mongo mongo --quiet --eval "db.runCommand({ ping: 1 })" >/dev/null 2>&1; then
  echo "      Mongo is reachable."
else
  echo "      WARNING: Mongo ping failed (container may still be starting)."
fi

echo
echo "==> Stack is up."
echo "   Airflow:   http://localhost:8080"
echo "   Superset:  http://localhost:8088"
echo "   Spark UI:  http://localhost:4040 (only while a job is running)"
echo
echo "Tip: Use 'docker ps' to confirm containers are Up/healthy."

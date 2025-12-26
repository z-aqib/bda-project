#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

dc() { docker compose "$@"; }

AIRFLOW_YML="airflow/airflow-docker-compose.yml"
SUPERSET_YML="superset_bi/superset-docker-compose.yml"
SPARK_YML="spark/spark-docker-compose.yml"
HDFS_YML="hdfs/hdfs-docker-compose.yml"
POSTGRES_YML="postgres/postgres-docker-compose.yml"
CONSUMER_YML="data_consumer/consumer-docker-compose.yml"
MONGO_YML="mongo/mongo-docker-compose.yml"
GEN_YML="data_generator/generator-docker-compose.yml"
KAFKA_YML="kafka/kafka-docker-compose.yml"

echo "==> Stopping stack (reverse order)..."
dc -f "$AIRFLOW_YML" down
dc -f "$SUPERSET_YML" down
dc -f "$SPARK_YML" down
dc -f "$HDFS_YML" down
dc -f "$POSTGRES_YML" down
dc -f "$CONSUMER_YML" down
dc -f "$GEN_YML" down
dc -f "$MONGO_YML" down
dc -f "$KAFKA_YML" down
echo "==> Done."

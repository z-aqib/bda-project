# Real-Time BDA Pipeline (Bisconni IoT Manufacturing) — Zuha Aqib (26106) & Mahnoor Adeel (26913)

This repository implements a fully dockerized real-time Big Data Analytics (BDA) pipeline for an IoT manufacturing scenario (Bisconni-style FMCG factory lines). The system generates statistically controlled sensor streams, ingests them via Kafka, stores fresh data in MongoDB, archives data to HDFS with metadata, computes minute-level KPIs using Spark, orchestrates workflows through Airflow, and exposes analytics tables for BI dashboards in Superset.

---

## 1) Business Domain & Problem

Modern FMCG manufacturing lines run continuously at high volume. Even short-lived faults (temperature spikes, abnormal vibration, downtime) can produce large batches of defective product, waste energy, and reduce machine life. Traditional reporting is often delayed (end-of-shift or batch analytics), which leads to reactive decisions.

This project enables **real-time analytics** so managers can monitor:
- minute-level production behavior,
- sensor drift and anomalies,
- downtime (planned vs unplanned),
- and operational alerts,

allowing faster intervention and improved factory performance.

---

## 2) Architecture (High-Level)

1. **Data Generator (Producer)** generates statistically bounded IoT readings and publishes to Kafka.
2. **Kafka** handles ingestion via a streaming topic.
3. **Kafka → Mongo Consumer** stores fresh streaming data in MongoDB (`iot_database.sensor_readings`).
4. **Airflow** orchestrates periodic pipeline tasks:
   - raw data export to HDFS,
   - Spark analytics execution,
   - archiving + metadata management (threshold-based policy).
5. **HDFS** stores archived data and metadata.
6. **Spark** computes analytics KPIs per minute and writes results to PostgreSQL.
7. **PostgreSQL** stores analytics tables (`minute_kpi`, `alert_events`, plus dims).
8. **Superset** queries Postgres to build BI dashboards.

---

## 3) Repository Structure

```text
project/
├── airflow/                # Airflow orchestration (DAGs + Dockerfile)
├── data_generator/         # Kafka producer (statistical generator)
├── data_consumer/          # Kafka->Mongo consumer
├── kafka/                  # Kafka + Zookeeper cluster
├── mongo/                  # MongoDB fresh storage
├── hdfs/                   # Hadoop HDFS (archive + metadata)
├── spark/                  # Spark analytics jobs
├── postgres/               # Analytics warehouse (Postgres)
├── superset_bi/            # BI layer (Superset)
````

---

## 4) Prerequisites

* Docker + Docker Compose v2
* Linux / WSL recommended for running scripts

---

## 5) Quickstart (Run All Services)

Run the startup script using: 

```bash
chmod +x run_stack.sh
./run_stack.sh
```

---

## 6) Key Ports (Common)

* Airflow: `http://localhost:8080`
* Superset: `http://localhost:8088`
* Spark UI: `http://localhost:4040`
* HDFS UI (NameNode): `http://localhost:9870`
* MongoDB: `mongodb://localhost:27017`
* PostgreSQL: `localhost:5432`
* Kafka Brokers: `9092`, `9093`, `9094` (as configured)

---

## 7) Data Storage

### MongoDB (Fresh Streaming Storage)

Database: `iot_database`
Collection: `sensor_readings`

Example document:

```js
{
  "factory_id": "FCT005",
  "machine_id": "MAC0295",
  "sensor_type_id": 2,
  "operator_id": "OPR0002",
  "product_id": 38,
  "event_timestamp": "2025-12-25 20:59:57",
  "reading_value": 4.12,
  "is_downtime": false,
  "planned_downtime": false
}
```

### PostgreSQL (Analytics Warehouse)

Analytics tables produced by Spark:

* `minute_kpi` (minute-level KPIs)
* `alert_events` (alerts)

Dimensions:

* `dim_factory`, `dim_machine`, `dim_operator`, `dim_product`, `dim_sensor_type`

---

## 8) Verify the Pipeline is Updating (Per Minute)

### MongoDB has fresh events

```bash
docker exec -it mongo mongosh --eval 'db.getSiblingDB("iot_database").sensor_readings.findOne()'
```

### Postgres minute_kpi is updating

```bash
docker exec -it postgres psql -U analytics_user -d analytics_db -c "
SELECT MAX(kpi_minute_start_utc) AS latest_minute, COUNT(*) AS rows
FROM minute_kpi;
"
```

Run twice with a one-minute gap; `latest_minute` should advance.

---

## 9) Superset (BI)

Superset connects to PostgreSQL and uses `minute_kpi` and `alert_events` for dashboard charts. Dashboards are designed to refresh frequently.

---

## 10) Stop the Stack

Run the stop script:

```bash
chmod +x stop_stack.sh
./stop_stack.sh
```

---

## Authors

* Zuha Aqib (26106)
* Mahnoor Adeel (26913)
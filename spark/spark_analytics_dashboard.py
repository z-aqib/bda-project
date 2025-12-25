"""
spark_analytics_dashboard.py
MongoDB (hot sensor events) -> Spark (minute KPIs + threshold alerts) -> PostgreSQL

Outputs (Postgres):
  - agg_overall_minute
  - agg_factory_minute
  - agg_machine_minute
  - agg_product_minute
  - fact_machine_alert_events

Assumptions:
  Mongo documents contain at least:
    event_timestamp_utc (timestamp/string)
    machine_id (int)
    sensor_type_id (int)
    product_id (int, nullable)
    operator_id (int, nullable)
    reading_value (numeric/double)

  Dimensions exist in Postgres:
    dim_machine(machine_id, factory_id, machine_name, ...)
    dim_sensor_type(sensor_type_id, sensor_type_code, sensor_type_name, typical_min, typical_max, unit, ...)

NOTE:
  - Dashboards will JOIN KPI/fact tables with dim tables in Postgres (BI layer).
  - This script intentionally writes IDs in KPI/fact tables.
"""

import time
from datetime import datetime, timezone

import psycopg2
from pyspark.sql import SparkSession, functions as F


# =========================
# CONFIG
# =========================

# How frequently this job repeats (seconds)
BATCH_INTERVAL_SECONDS = 60

# Re-read last N minutes every cycle (handles late events)
LOOKBACK_MINUTES = 60

# Mongo
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "iot_db"
MONGO_COLLECTION = "fact_machine_sensor_events"   # <-- set your actual collection name

# Postgres
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "analytics_db"
PG_USER = "analytics_user"
PG_PASSWORD = "analytics_pass"

# Target tables
T_OVERALL = "agg_overall_minute"
T_FACTORY  = "agg_factory_minute"
T_MACHINE  = "agg_machine_minute"
T_PRODUCT  = "agg_product_minute"
T_ALERTS   = "fact_machine_alert_events"

# Spark packages (adjust if needed for your Spark version)
SPARK_PACKAGES = ",".join([
    "org.mongodb.spark:mongo-spark-connector_2.12:10.2.2",
    "org.postgresql:postgresql:42.7.3",
])


# =========================
# POSTGRES HELPERS
# =========================

def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )

def delete_range(conn, table, ts_col, start_ts, end_ts):
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {table} WHERE {ts_col} >= %s AND {ts_col} <= %s",
            (start_ts, end_ts),
        )
    conn.commit()

def spark_write_jdbc(df, table, mode="append"):
    url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    props = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}
    (df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )

def spark_read_jdbc(table):
    url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    return (spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


# =========================
# SPARK SESSION
# =========================

spark = (SparkSession.builder
    .appName("RealTime_KPIs_And_Alerts")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# =========================
# LOAD DIMENSIONS (POSTGRES)
# =========================

dim_machine = (spark_read_jdbc("dim_machine")
    .select(
        F.col("machine_id").cast("int").alias("machine_id"),
        F.col("factory_id").cast("int").alias("factory_id"),
    )
)

dim_sensor = (spark_read_jdbc("dim_sensor_type")
    .select(
        F.col("sensor_type_id").cast("int").alias("sensor_type_id"),
        F.col("sensor_type_code").cast("string").alias("sensor_type_code"),
        F.col("sensor_type_name").cast("string").alias("sensor_type_name"),
        F.col("typical_min").cast("double").alias("typical_min"),
        F.col("typical_max").cast("double").alias("typical_max"),
        F.col("unit").cast("string").alias("unit"),
    )
)

dim_machine.cache()
dim_sensor.cache()


# =========================
# READ MONGO HOT EVENTS
# =========================

def read_mongo_hot_events(lookback_minutes: int):
    raw = (spark.read.format("mongodb")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION)
        .load()
    )

    # --- Map fields (adjust if your Mongo field names differ) ---
    df = (raw
        .withColumn("event_timestamp_utc", F.to_timestamp(F.col("event_timestamp_utc")))
        .withColumn("machine_id", F.col("machine_id").cast("int"))
        .withColumn("sensor_type_id", F.col("sensor_type_id").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("operator_id", F.col("operator_id").cast("int"))
        .withColumn("reading_value", F.col("reading_value").cast("double"))
    )

    cutoff = F.expr(f"timestampadd(MINUTE, -{lookback_minutes}, current_timestamp())")

    df = df.filter(
        (F.col("event_timestamp_utc").isNotNull()) &
        (F.col("event_timestamp_utc") >= cutoff) &
        (F.col("machine_id").isNotNull()) &
        (F.col("sensor_type_id").isNotNull()) &
        (F.col("reading_value").isNotNull())
    )

    return df


# =========================
# BUILD KPIs
# =========================

def build_kpis(events_df):
    df = events_df.withColumn("minute_ts", F.date_trunc("minute", F.col("event_timestamp_utc")))

    # add factory_id (required for factory/machine/product KPIs)
    df = (df.join(F.broadcast(dim_machine), on="machine_id", how="left")
            .filter(F.col("factory_id").isNotNull())
    )

    overall = (df.groupBy("minute_ts")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("reading_value").alias("avg_value"),
            F.min("reading_value").alias("min_value"),
            F.max("reading_value").alias("max_value"),
        )
    )

    factory = (df.groupBy("minute_ts", "factory_id")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("reading_value").alias("avg_value"),
            F.min("reading_value").alias("min_value"),
            F.max("reading_value").alias("max_value"),
        )
    )

    machine = (df.groupBy("minute_ts", "factory_id", "machine_id")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("reading_value").alias("avg_value"),
            F.min("reading_value").alias("min_value"),
            F.max("reading_value").alias("max_value"),
        )
    )

    product = (df.filter(F.col("product_id").isNotNull())
        .groupBy("minute_ts", "factory_id", "product_id")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("reading_value").alias("avg_value"),
            F.min("reading_value").alias("min_value"),
            F.max("reading_value").alias("max_value"),
        )
    )

    return overall, factory, machine, product


# =========================
# GENERATE ALERTS (THRESHOLD-BASED)
# =========================

def generate_alerts(events_df):
    df = (events_df
        .join(F.broadcast(dim_sensor), on="sensor_type_id", how="left")
        .filter(F.col("typical_min").isNotNull() & F.col("typical_max").isNotNull())
    )

    breach_high = F.col("reading_value") > F.col("typical_max")
    breach_low  = F.col("reading_value") < F.col("typical_min")

    alerts = (df.filter(breach_high | breach_low)
        .withColumn("alert_time_utc", F.col("event_timestamp_utc"))
        .withColumn("threshold_value", F.when(breach_high, F.col("typical_max")).otherwise(F.col("typical_min")))
        .withColumn("threshold_condition", F.when(breach_high, F.lit(">")).otherwise(F.lit("<")))
        .withColumn("severity", F.when(breach_high, F.lit("CRITICAL")).otherwise(F.lit("WARNING")))
        .withColumn(
            "alert_type",
            F.when(breach_high, F.concat(F.lit("High "), F.col("sensor_type_name")))
             .otherwise(F.concat(F.lit("Low "), F.col("sensor_type_name")))
        )
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Machine "), F.col("machine_id").cast("string"),
                F.lit(" | "), F.col("sensor_type_name"),
                F.lit(" breached. Value="), F.round(F.col("reading_value"), 2).cast("string"),
                F.lit(" Threshold="), F.round(F.col("threshold_value"), 2).cast("string"),
                F.lit(" "), F.coalesce(F.col("unit"), F.lit(""))
            )
        )
        .select(
            "alert_time_utc",
            "machine_id",
            "sensor_type_id",
            F.col("product_id"),
            F.col("operator_id"),
            F.col("reading_value").alias("actual_value"),
            "threshold_value",
            "threshold_condition",
            "severity",
            "alert_type",
            "alert_message",
            F.lit(False).alias("resolved_flag"),
            F.lit(None).cast("timestamp").alias("resolved_at"),
        )
    )

    return alerts


def get_min_max(df, col_name):
    r = df.agg(F.min(col_name).alias("mn"), F.max(col_name).alias("mx")).collect()[0]
    return r["mn"], r["mx"]


# =========================
# MAIN LOOP
# =========================

while True:
    now = datetime.now(timezone.utc)
    print(f"\n[{now.isoformat()}] Running real-time KPI + alerts batch...")

    events = read_mongo_hot_events(LOOKBACK_MINUTES)
    if events.rdd.isEmpty():
        print("No events in lookback window. Sleeping...")
        time.sleep(BATCH_INTERVAL_SECONDS)
        continue

    overall_df, factory_df, machine_df, product_df = build_kpis(events)
    alerts_df = generate_alerts(events)

    # figure out minute range for idempotent refresh
    min_ts, max_ts = get_min_max(overall_df, "minute_ts")
    print(f"Minute window to refresh: {min_ts} -> {max_ts}")

    conn = pg_connect()
    try:
        # KPI tables: delete time range then insert
        delete_range(conn, T_OVERALL, "minute_ts", min_ts, max_ts)
        delete_range(conn, T_FACTORY,  "minute_ts", min_ts, max_ts)
        delete_range(conn, T_MACHINE,  "minute_ts", min_ts, max_ts)
        delete_range(conn, T_PRODUCT,  "minute_ts", min_ts, max_ts)

        spark_write_jdbc(overall_df, T_OVERALL, mode="append")
        spark_write_jdbc(factory_df,  T_FACTORY,  mode="append")
        spark_write_jdbc(machine_df,  T_MACHINE,  mode="append")
        spark_write_jdbc(product_df,  T_PRODUCT,  mode="append")

        # Alerts: delete alerts in same time window to avoid duplicates
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {T_ALERTS} WHERE alert_time_utc >= %s AND alert_time_utc <= %s",
                (min_ts, max_ts),
            )
        conn.commit()

        spark_write_jdbc(alerts_df, T_ALERTS, mode="append")

        print("Batch complete ✅")

    finally:
        conn.close()

    time.sleep(BATCH_INTERVAL_SECONDS)

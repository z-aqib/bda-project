"""
spark_analytics_dashboard.py
MongoDB (hot data) -> Spark batch micro-loop -> PostgreSQL (KPIs + Alerts)

Tables written:
- agg_overall_minute
- agg_factory_minute
- agg_machine_minute
- agg_product_minute
- fact_machine_alert_events

Optional:
- fact_machine_sensor_events (raw facts appended from Mongo)
"""

import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# Config
# ============================================================

# How often to run the batch loop
BATCH_INTERVAL_SECONDS = 60

# How much hot data to re-read each loop (for correction / late events)
LOOKBACK_MINUTES = 60

# Toggle: write raw events into Postgres fact table as well
WRITE_RAW_EVENTS_TO_POSTGRES = False

# Mongo (hot data)
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "iot_db"
MONGO_COLLECTION = "sensor_events"  # adjust to your collection name

# Postgres (analytics layer)
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "analytics_db"
PG_USER = "postgres"
PG_PASSWORD = "postgres"

# Target tables
T_OVERALL = "agg_overall_minute"
T_FACTORY = "agg_factory_minute"
T_MACHINE = "agg_machine_minute"
T_PRODUCT = "agg_product_minute"
T_ALERTS = "fact_machine_alert_events"
T_RAW = "fact_machine_sensor_events"

# Spark packages (adjust versions to match your Spark)
# Mongo Spark Connector and Postgres JDBC driver
SPARK_PACKAGES = ",".join([
    "org.mongodb.spark:mongo-spark-connector_2.12:10.2.2",
    "org.postgresql:postgresql:42.7.3",
])


# ============================================================
# Helpers: Postgres connection + idempotent minute upsert pattern
# ============================================================

def pg_connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def delete_minute_range(conn, table_name: str, minute_col: str, start_ts, end_ts):
    """
    Deletes rows in [start_ts, end_ts] for idempotent re-inserts.
    """
    sql = f"""
        DELETE FROM {table_name}
        WHERE {minute_col} >= %s AND {minute_col} <= %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start_ts, end_ts))
    conn.commit()


def write_df_jdbc(df, table_name: str, mode="append"):
    """
    Spark JDBC write helper.
    """
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    props = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

    (df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )


# ============================================================
# Spark: build session
# ============================================================

spark = (
    SparkSession.builder
    .appName("Mongo_to_Postgres_RealTime_KPIs_And_Alerts")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Load dimension tables from Postgres (small tables -> broadcast join)
# ============================================================

def load_dim_sensor_type():
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_sensor_type")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select(
            F.col("sensor_type_id").cast("int"),
            F.col("sensor_type_code"),
            F.col("typical_min").cast("double"),
            F.col("typical_max").cast("double"),
            F.col("unit"),
        )
    )


dim_sensor_type_df = load_dim_sensor_type()
dim_sensor_type_df.cache()


# ============================================================
# Mongo Read
# ============================================================

def read_mongo_hot_events(lookback_minutes: int):
    """
    Reads only recent events from Mongo to keep workload bounded.
    Expects fields aligned to your fact schema:
      - event_timestamp_utc
      - machine_id
      - sensor_type_id
      - product_id (nullable)
      - operator_id (nullable)
      - reading_value
      - reading_status
      - is_planned_downtime
    """
    cutoff_expr = F.expr(f"timestampadd(MINUTE, -{lookback_minutes}, current_timestamp())")

    raw = (
        spark.read.format("mongodb")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION)
        .load()
    )

    # Adjust these mappings if your Mongo field names differ
    df = (raw
          .withColumn("event_timestamp_utc", F.to_timestamp(F.col("event_timestamp_utc")))
          .withColumn("machine_id", F.col("machine_id").cast("int"))
          .withColumn("sensor_type_id", F.col("sensor_type_id").cast("int"))
          .withColumn("product_id", F.col("product_id").cast("int"))
          .withColumn("operator_id", F.col("operator_id").cast("int"))
          .withColumn("reading_value", F.col("reading_value").cast("double"))
          .withColumn("reading_status", F.col("reading_status").cast("string"))
          .withColumn("is_planned_downtime", F.col("is_planned_downtime").cast("boolean"))
          .filter(F.col("event_timestamp_utc") >= cutoff_expr)
    )

    # Drop rows that cannot be processed
    df = df.filter(
        F.col("event_timestamp_utc").isNotNull()
        & F.col("machine_id").isNotNull()
        & F.col("sensor_type_id").isNotNull()
        & F.col("reading_value").isNotNull()
    )

    return df


# ============================================================
# KPI Aggregations
# ============================================================

def build_kpis(events_df):
    """
    Returns 4 aggregate DataFrames:
    - overall_minute
    - factory_minute
    - machine_minute
    - product_minute
    """
    # minute bucket
    df = events_df.withColumn("minute_ts", F.date_trunc("minute", F.col("event_timestamp_utc")))

    # Need factory_id for grouping -> join via dim_machine
    dim_machine = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", "dim_machine")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select(F.col("machine_id").cast("int"), F.col("factory_id").cast("int"))
    )

    df = df.join(F.broadcast(dim_machine), on="machine_id", how="left").filter(F.col("factory_id").isNotNull())

    # Overall
    overall = (
        df.groupBy("minute_ts")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("reading_value").alias("avg_value"),
            F.min("reading_value").alias("min_value"),
            F.max("reading_value").alias("max_value"),
        )
    )

    # Factory
    factory = (
        df.groupBy("minute_ts", "factory_id")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("reading_value").alias("avg_value"),
            F.min("reading_value").alias("min_value"),
            F.max("reading_value").alias("max_value"),
        )
    )

    # Machine
    machine = (
        df.groupBy("minute_ts", "factory_id", "machine_id")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("reading_value").alias("avg_value"),
            F.min("reading_value").alias("min_value"),
            F.max("reading_value").alias("max_value"),
        )
    )

    # Product (nullable product_id -> keep only known)
    product = (
        df.filter(F.col("product_id").isNotNull())
        .groupBy("minute_ts", "factory_id", "product_id")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("reading_value").alias("avg_value"),
            F.min("reading_value").alias("min_value"),
            F.max("reading_value").alias("max_value"),
        )
    )

    return overall, factory, machine, product


# ============================================================
# Alert Generation
# ============================================================

def generate_alerts(events_df):
    """
    Generates alerts using dim_sensor_type typical_min / typical_max as thresholds.

    Severity rule (simple and explainable):
    - CRITICAL: reading_value < typical_min OR reading_value > typical_max
    - WARNING: reading_value within 5% band near edges (optional)
    Here we implement only CRITICAL for clarity, and you can extend later.
    """
    df = (
        events_df
        .join(F.broadcast(dim_sensor_type_df), on="sensor_type_id", how="left")
        .filter(F.col("typical_min").isNotNull() & F.col("typical_max").isNotNull())
    )

    # Determine breach
    breach_high = F.col("reading_value") > F.col("typical_max")
    breach_low = F.col("reading_value") < F.col("typical_min")

    alerts = (
        df.filter(breach_high | breach_low)
        .withColumn("alert_time_utc", F.col("event_timestamp_utc"))
        .withColumn("threshold_value", F.when(breach_high, F.col("typical_max")).otherwise(F.col("typical_min")))
        .withColumn("threshold_condition", F.when(breach_high, F.lit(">")).otherwise(F.lit("<")))
        .withColumn("severity", F.lit("CRITICAL"))
        .withColumn(
            "alert_type",
            F.when(breach_high, F.concat(F.lit("High "), F.col("sensor_type_code"))).otherwise(
                F.concat(F.lit("Low "), F.col("sensor_type_code"))
            )
        )
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Machine "), F.col("machine_id").cast("string"),
                F.lit(" violated "), F.col("sensor_type_code"),
                F.lit(" threshold. Value="), F.round(F.col("reading_value"), 2).cast("string"),
                F.lit(" Threshold="), F.round(F.col("threshold_value"), 2).cast("string"),
                F.lit(" "), F.coalesce(F.col("unit"), F.lit(""))
            )
        )
        .select(
            "alert_time_utc",
            "machine_id",
            "sensor_type_id",
            "product_id",
            "operator_id",
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


# ============================================================
# Main loop
# ============================================================

def get_min_max_minute(df, minute_col="minute_ts"):
    row = df.agg(F.min(minute_col).alias("min_ts"), F.max(minute_col).alias("max_ts")).collect()[0]
    return row["min_ts"], row["max_ts"]


while True:
    loop_start = datetime.now(timezone.utc)
    print(f"[{loop_start.isoformat()}] Running KPI + Alert batch...")

    events = read_mongo_hot_events(LOOKBACK_MINUTES)

    if events.rdd.isEmpty():
        print("No events found in lookback window. Sleeping...")
        time.sleep(BATCH_INTERVAL_SECONDS)
        continue

    # Optionally persist raw facts into Postgres
    if WRITE_RAW_EVENTS_TO_POSTGRES:
        write_df_jdbc(
            events.select(
                "event_timestamp_utc",
                "machine_id",
                "sensor_type_id",
                "product_id",
                "operator_id",
                "reading_value",
                "reading_status",
                "is_planned_downtime",
            ),
            T_RAW,
            mode="append",
        )

    # Build KPIs
    overall_df, factory_df, machine_df, product_df = build_kpis(events)

    # Generate alerts
    alerts_df = generate_alerts(events)

    # Determine minute range for idempotent refresh
    min_ts, max_ts = get_min_max_minute(overall_df, "minute_ts")

    conn = pg_connect()
    try:
        # Clear minute ranges before inserting (idempotent)
        delete_minute_range(conn, T_OVERALL, "minute_ts", min_ts, max_ts)
        delete_minute_range(conn, T_FACTORY, "minute_ts", min_ts, max_ts)
        delete_minute_range(conn, T_MACHINE, "minute_ts", min_ts, max_ts)
        delete_minute_range(conn, T_PRODUCT, "minute_ts", min_ts, max_ts)

        # Write KPI tables
        write_df_jdbc(overall_df, T_OVERALL, mode="append")
        write_df_jdbc(factory_df, T_FACTORY, mode="append")
        write_df_jdbc(machine_df, T_MACHINE, mode="append")
        write_df_jdbc(product_df, T_PRODUCT, mode="append")

        # Alerts: to avoid duplicates, delete alerts in the same time window (optional)
        # Here we delete alerts that match the time window; you can refine if needed.
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {T_ALERTS} WHERE alert_time_utc >= %s AND alert_time_utc <= %s",
                (min_ts, max_ts),
            )
        conn.commit()

        write_df_jdbc(alerts_df, T_ALERTS, mode="append")

        print(f"Batch complete. Minute range: {min_ts} -> {max_ts}")

    finally:
        conn.close()

    time.sleep(BATCH_INTERVAL_SECONDS)

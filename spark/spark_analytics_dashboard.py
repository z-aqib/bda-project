"""
Mongo (raw + dims)  --> Spark SQL aggregates --> Postgres (analytics tables)

Creates 5 KPI outputs (tables) in ONE Postgres DB:
1) kpi_minute_machine        (minute-level machine KPIs, joined labels)
2) kpi_hourly_factory        (hour-level factory KPIs, trend)
3) kpi_daily_machine         (daily machine performance, reliability)
4) kpi_daily_product         (daily product impact KPIs)
5) kpi_alert_events          (derived alert feed from sensor thresholds)

Run (example):
spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2,org.postgresql:postgresql:42.7.3 \
  spark_analytics_postgres_kpis.py

Schedule every 1 minute using Airflow later.
"""

from __future__ import annotations
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


# -------------------------
# CONFIG (edit here)
# -------------------------
MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB   = os.getenv("MONGO_DB", "iot_database")

MONGO_URI = os.getenv("MONGO_URI", f"mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB   = os.getenv("POSTGRES_DB", "analytics_db")
PG_USER = os.getenv("POSTGRES_USER", "analytics_user")
PG_PASS = os.getenv("POSTGRES_PASS", "analytics_pass")
PG_URL  = os.getenv("POSTGRES_JDBC_URL", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")

# Windows for “live” analytics
LAST_N_MINUTES = int(os.getenv("LAST_N_MINUTES", "180"))  # recompute last 3 hours minute KPIs
LAST_N_DAYS    = int(os.getenv("LAST_N_DAYS", "14"))      # daily aggregates last 2 weeks

# Alert rules (from dim_sensor_type typical_min/max)
# WARN: outside [min, max]
# CRITICAL: outside [min - k*(max-min), max + k*(max-min)]
CRITICAL_RANGE_MULT = float(os.getenv("CRITICAL_RANGE_MULT", "0.50"))

# Snapshot alert feed size
ALERT_FEED_LIMIT = int(os.getenv("ALERT_FEED_LIMIT", "200"))


# -------------------------
# Spark + IO helpers
# -------------------------
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("BDA_Mongo_To_Postgres_KPIs")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_mongo(spark: SparkSession, collection: str) -> DataFrame:
    return (
        spark.read.format("mongo")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", collection)
        .load()
    )


def write_pg(df: DataFrame, table: str, mode: str = "overwrite", truncate: bool = True) -> None:
    w = (
        df.write.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
    )
    if truncate and mode.lower() == "overwrite":
        w = w.option("truncate", "true")
    w.save()


def normalize_fact(fact_raw: DataFrame) -> DataFrame:
    """
    Ensures consistent types + derives time buckets.
    """
    ts = F.col("event_timestamp")
    fact = (
        fact_raw
        .withColumn("event_ts", F.when(ts.cast("timestamp").isNotNull(), ts.cast("timestamp")).otherwise(F.to_timestamp(ts)))
        .withColumn("factory_id", F.col("factory_id").cast("int"))
        .withColumn("machine_id", F.col("machine_id").cast("int"))
        .withColumn("sensor_type_id", F.col("sensor_type_id").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("operator_id", F.col("operator_id").cast("int"))
        .withColumn("reading_value_d", F.col("reading_value").cast("double"))
        .withColumn("is_downtime_b", F.col("is_downtime").cast("boolean"))
        .withColumn("planned_downtime_b", F.col("planned_downtime").cast("boolean"))
        .withColumn("minute_ts", F.date_trunc("minute", F.col("event_ts")))
        .withColumn("hour_ts", F.date_trunc("hour", F.col("event_ts")))
        .withColumn("day_dt", F.to_date(F.col("event_ts")))
    )
    return fact


# -------------------------
# MAIN
# -------------------------
def main() -> None:
    spark = build_spark()

    # ----- read fact -----
    fact_raw = read_mongo(spark, "sensor_readings")
    fact = normalize_fact(
        fact_raw.select(
            "factory_id","machine_id","sensor_type_id","product_id","operator_id",
            "event_timestamp","reading_value","is_downtime","planned_downtime"
        )
    )

    # ----- read dims from Mongo -----
    dim_factory = read_mongo(spark, "dim_factory").select(
        F.col("factory_id").cast("int").alias("factory_id"),
        F.col("factory_code").cast("string").alias("factory_code"),
        F.col("factory_name").cast("string").alias("factory_name"),
        F.col("city").cast("string").alias("city"),
        F.col("capacity_class").cast("string").alias("capacity_class"),
    )

    dim_machine = read_mongo(spark, "dim_machine").select(
        F.col("machine_id").cast("int").alias("machine_id"),
        F.col("factory_id").cast("int").alias("factory_id"),
        F.col("machine_name").cast("string").alias("machine_name"),
        F.col("machine_type").cast("string").alias("machine_type"),
        F.col("vendor").cast("string").alias("vendor"),
        F.col("criticality").cast("string").alias("criticality"),
    )

    dim_sensor = read_mongo(spark, "dim_sensor_type").select(
        F.col("sensor_type_id").cast("int").alias("sensor_type_id"),
        F.col("sensor_type_code").cast("string").alias("sensor_type_code"),
        F.col("unit").cast("string").alias("unit"),
        F.col("typical_min").cast("double").alias("typical_min"),
        F.col("typical_max").cast("double").alias("typical_max"),
    )

    dim_product = read_mongo(spark, "dim_product").select(
        F.col("product_id").cast("int").alias("product_id"),
        F.col("sku").cast("string").alias("sku"),
        F.col("product_name").cast("string").alias("product_name"),
        F.col("category").cast("string").alias("category"),
        F.col("pack_size_g").cast("int").alias("pack_size_g"),
    )

    dim_operator = read_mongo(spark, "dim_operator").select(
        F.col("operator_id").cast("int").alias("operator_id"),
        F.col("operator_name").cast("string").alias("operator_name"),
        F.col("experience_level").cast("string").alias("experience_level"),
        F.col("shift_pattern").cast("string").alias("shift_pattern"),
    )

    # ----- filters -----
    cutoff_live = F.current_timestamp() - F.expr(f"INTERVAL {LAST_N_MINUTES} MINUTES")
    cutoff_days = F.current_timestamp() - F.expr(f"INTERVAL {LAST_N_DAYS} DAYS")

    fact_live = fact.filter(F.col("event_ts") >= cutoff_live)
    fact_days = fact.filter(F.col("event_ts") >= cutoff_days)

    # ----- enrich with dims for readable dashboards -----
    def enrich(df: DataFrame) -> DataFrame:
        return (
            df.join(dim_factory, on="factory_id", how="left")
              .join(dim_machine, on=["machine_id","factory_id"], how="left")
              .join(dim_sensor, on="sensor_type_id", how="left")
              .join(dim_product, on="product_id", how="left")
              .join(dim_operator, on="operator_id", how="left")
        )

    live_enriched = enrich(fact_live)
    days_enriched = enrich(fact_days)

    # =========================================================
    # KPI TABLE 5: Derived alert feed (WARN/CRITICAL)
    # =========================================================
    # WARN: reading < typical_min OR reading > typical_max
    # CRITICAL: outside expanded range by CRITICAL_RANGE_MULT*(typical_max-typical_min)
    span = (F.col("typical_max") - F.col("typical_min"))
    crit_low = (F.col("typical_min") - F.lit(CRITICAL_RANGE_MULT) * span)
    crit_high = (F.col("typical_max") + F.lit(CRITICAL_RANGE_MULT) * span)

    alerts = (
        live_enriched
        .withColumn(
            "severity",
            F.when((F.col("reading_value_d") < crit_low) | (F.col("reading_value_d") > crit_high), F.lit("CRITICAL"))
             .when((F.col("reading_value_d") < F.col("typical_min")) | (F.col("reading_value_d") > F.col("typical_max")), F.lit("WARNING"))
             .otherwise(F.lit(None))
        )
        .filter(F.col("severity").isNotNull())
        .withColumn(
            "threshold_value",
            F.when(F.col("reading_value_d") < F.col("typical_min"), F.col("typical_min"))
             .when(F.col("reading_value_d") > F.col("typical_max"), F.col("typical_max"))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "threshold_condition",
            F.when(F.col("reading_value_d") < F.col("typical_min"), F.lit("<"))
             .when(F.col("reading_value_d") > F.col("typical_max"), F.lit(">"))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "alert_type",
            F.concat(F.lit("Sensor "), F.col("sensor_type_code"), F.lit(" Out of Typical Range"))
        )
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Reading "), F.col("reading_value_d").cast("string"),
                F.lit(" "), F.coalesce(F.col("unit"), F.lit("")),
                F.lit(" violated typical range ["),
                F.col("typical_min").cast("string"), F.lit(", "),
                F.col("typical_max").cast("string"), F.lit("].")
            )
        )
        .select(
            F.col("event_ts").alias("event_timestamp_utc"),
            "factory_id","factory_name","city",
            "machine_id","machine_name","machine_type","criticality",
            "sensor_type_id","sensor_type_code","sensor_type_name","unit",
            "product_id","product_name","category",
            "operator_id","operator_name",
            F.col("reading_value_d").alias("actual_value"),
            "threshold_value","threshold_condition",
            "severity","alert_type","alert_message",
            F.current_timestamp().alias("run_ts")
        )
        .orderBy(F.col("event_timestamp_utc").desc(), F.col("severity").desc())
        .limit(ALERT_FEED_LIMIT)
    )

    # =========================================================
    # KPI TABLE 1: Minute machine KPIs (Operations core)
    # =========================================================
    # Availability proxy = 100*(1-downtime_rate)
    minute_machine = (
        live_enriched
        .groupBy(
            "minute_ts",
            "factory_id","factory_name","city","capacity_class",
            "machine_id","machine_name","machine_type","vendor","criticality",
            "product_id","product_name",
            "operator_id","operator_name"
        )
        .agg(
            F.count("*").alias("events_count"),
            F.avg("reading_value_d").alias("avg_reading"),
            F.max("reading_value_d").alias("max_reading"),
            F.min("reading_value_d").alias("min_reading"),
            F.sum(F.when(F.col("is_downtime_b") == True, 1).otherwise(0)).alias("downtime_events"),
            F.sum(F.when(F.col("planned_downtime_b") == True, 1).otherwise(0)).alias("planned_downtime_events"),
            F.sum(F.when((F.col("is_downtime_b") == True) & (F.col("planned_downtime_b") == False), 1).otherwise(0)).alias("unplanned_downtime_events"),
        )
        .withColumn(
            "downtime_rate_pct",
            F.when(F.col("events_count") > 0, (F.col("downtime_events") / F.col("events_count")) * 100.0).otherwise(F.lit(0.0))
        )
        .withColumn(
            "availability_pct_proxy",
            F.round(F.lit(100.0) - F.col("downtime_rate_pct"), 4)
        )
        .withColumn("run_ts", F.current_timestamp())
        .select(
            "minute_ts",
            "factory_id","factory_name","city","capacity_class",
            "machine_id","machine_name","machine_type","vendor","criticality",
            "product_id","product_name",
            "operator_id","operator_name",
            "events_count",
            F.round("avg_reading",4).alias("avg_reading"),
            "max_reading","min_reading",
            "downtime_events",
            F.round("downtime_rate_pct",4).alias("downtime_rate_pct"),
            "planned_downtime_events","unplanned_downtime_events",
            "availability_pct_proxy",
            "run_ts"
        )
    )

    # =========================================================
    # KPI TABLE 2: Hourly factory KPIs (Factory trend story)
    # =========================================================
    hourly_factory = (
        live_enriched
        .groupBy("hour_ts","factory_id","factory_name","city","capacity_class")
        .agg(
            F.count("*").alias("events_count"),
            F.avg("reading_value_d").alias("avg_reading"),
            F.max("reading_value_d").alias("max_reading"),
            F.sum(F.when(F.col("is_downtime_b") == True, 1).otherwise(0)).alias("downtime_events"),
            F.sum(F.when(F.col("planned_downtime_b") == True, 1).otherwise(0)).alias("planned_downtime_events"),
        )
        .withColumn(
            "downtime_rate_pct",
            F.when(F.col("events_count") > 0, (F.col("downtime_events") / F.col("events_count")) * 100.0).otherwise(F.lit(0.0))
        )
        .withColumn("availability_pct_proxy", F.round(F.lit(100.0) - F.col("downtime_rate_pct"), 4))
        .withColumn("run_ts", F.current_timestamp())
        .select(
            "hour_ts",
            "factory_id","factory_name","city","capacity_class",
            "events_count",
            F.round("avg_reading",4).alias("avg_reading"),
            "max_reading",
            "downtime_events",
            F.round("downtime_rate_pct",4).alias("downtime_rate_pct"),
            "planned_downtime_events",
            "availability_pct_proxy",
            "run_ts"
        )
    )

    # =========================================================
    # KPI TABLE 3: Daily machine performance (Reliability / ranking story)
    # =========================================================
    daily_machine = (
        days_enriched
        .groupBy(
            "day_dt",
            "factory_id","factory_name","city",
            "machine_id","machine_name","machine_type","criticality"
        )
        .agg(
            F.count("*").alias("events_count"),
            F.sum(F.when(F.col("is_downtime_b") == True, 1).otherwise(0)).alias("downtime_events"),
            F.sum(F.when((F.col("is_downtime_b") == True) & (F.col("planned_downtime_b") == False), 1).otherwise(0)).alias("unplanned_downtime_events"),
            F.avg("reading_value_d").alias("avg_reading"),
            F.max("reading_value_d").alias("max_reading"),
        )
        .withColumn(
            "downtime_rate_pct",
            F.when(F.col("events_count") > 0, (F.col("downtime_events") / F.col("events_count")) * 100.0).otherwise(F.lit(0.0))
        )
        .withColumn("availability_pct_proxy", F.round(F.lit(100.0) - F.col("downtime_rate_pct"), 4))
        .withColumn("run_ts", F.current_timestamp())
        .select(
            "day_dt",
            "factory_id","factory_name","city",
            "machine_id","machine_name","machine_type","criticality",
            "events_count",
            "downtime_events","unplanned_downtime_events",
            F.round("downtime_rate_pct",4).alias("downtime_rate_pct"),
            "availability_pct_proxy",
            F.round("avg_reading",4).alias("avg_reading"),
            "max_reading",
            "run_ts"
        )
    )

    # =========================================================
    # KPI TABLE 4: Daily product impact (Product story dashboard)
    # =========================================================
    daily_product = (
        days_enriched
        .groupBy("day_dt", "factory_id","factory_name", "product_id","product_name","category")
        .agg(
            F.count("*").alias("events_count"),
            F.avg("reading_value_d").alias("avg_reading"),
            F.max("reading_value_d").alias("max_reading"),
            F.sum(F.when(F.col("is_downtime_b") == True, 1).otherwise(0)).alias("downtime_events"),
        )
        .withColumn(
            "downtime_rate_pct",
            F.when(F.col("events_count") > 0, (F.col("downtime_events") / F.col("events_count")) * 100.0).otherwise(F.lit(0.0))
        )
        .withColumn("run_ts", F.current_timestamp())
        .select(
            "day_dt",
            "factory_id","factory_name",
            "product_id","product_name","category",
            "events_count",
            F.round("avg_reading",4).alias("avg_reading"),
            "max_reading",
            "downtime_events",
            F.round("downtime_rate_pct",4).alias("downtime_rate_pct"),
            "run_ts"
        )
    )

    # =========================================================
    # Write to Postgres (5 KPI tables)
    # =========================================================
    # minute/hour/daily tables recomputed each run => overwrite + truncate
    write_pg(minute_machine, "kpi_minute_machine", mode="overwrite", truncate=True)
    write_pg(hourly_factory, "kpi_hourly_factory", mode="overwrite", truncate=True)
    write_pg(daily_machine, "kpi_daily_machine", mode="overwrite", truncate=True)
    write_pg(daily_product, "kpi_daily_product", mode="overwrite", truncate=True)

    # alert feed: overwrite so it always shows latest alerts cleanly
    write_pg(alerts, "kpi_alert_events", mode="overwrite", truncate=True)

    print("✅ KPI tables refreshed in Postgres successfully.")


if __name__ == "__main__":
    main()

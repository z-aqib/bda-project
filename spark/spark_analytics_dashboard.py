"""
Spark Analytics Job: Mongo (raw + dims) -> Postgres (aggregates)

How to run (example):
spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2,org.postgresql:postgresql:42.7.3 \
  spark_analytics_postgres.py

Notes:
- This is written as a batch job meant to be scheduled every minute (Airflow later).
- It recomputes aggregates for the last N minutes and truncates target Postgres tables.
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


# -----------------------------
# Config (edit here)
# -----------------------------
MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB = os.getenv("MONGO_DB", "iot_database")

MONGO_URI = os.getenv(
    "MONGO_URI",
    f"mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}"
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASS", "analytics_pass")

POSTGRES_JDBC_URL = os.getenv(
    "POSTGRES_JDBC_URL",
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# time windows
LAST_N_MINUTES = int(os.getenv("LAST_N_MINUTES", "60"))     # used for per-minute charts
TOP_MACHINES_WINDOW_MIN = int(os.getenv("TOP_MACHINES_WINDOW_MIN", "60"))  # snapshot window

# write behavior
WRITE_MODE_MINUTE_TABLES = os.getenv("WRITE_MODE_MINUTE_TABLES", "overwrite")  # overwrite + truncate=true
WRITE_MODE_SNAPSHOT_TABLES = os.getenv("WRITE_MODE_SNAPSHOT_TABLES", "append")  # append snapshots (recommended)
TOP_MACHINES_LIMIT = int(os.getenv("TOP_MACHINES_LIMIT", "10"))


# -----------------------------
# Helpers
# -----------------------------
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("MongoToPostgresAnalytics")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_mongo_collection(spark: SparkSession, coll: str) -> DataFrame:
    # Works with mongo-spark-connector v3.x format
    return (
        spark.read.format("mongo")
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", coll)
        .load()
    )


def write_postgres(df: DataFrame, table: str, mode: str, truncate: bool = True) -> None:
    """
    Writes DataFrame to Postgres via JDBC.
    - For minute tables: use mode=overwrite + option("truncate","true") to keep schema.
    - For snapshot tables: use mode=append.
    """
    writer = (
        df.write.format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
    )

    # truncate only meaningful with overwrite
    if truncate and mode.lower() == "overwrite":
        writer = writer.option("truncate", "true")

    writer.save()


def normalize_fact(df: DataFrame) -> DataFrame:
    """
    Ensures types are consistent:
    - event_timestamp -> timestamp
    - is_downtime/planned_downtime -> boolean
    - reading_value -> double
    """
    # event_timestamp might already be a timestamp in Mongo.
    # If it's string, parse; if it's date, cast to timestamp.
    ts_col = F.col("event_timestamp")
    df = df.withColumn(
        "event_ts",
        F.when(ts_col.cast("timestamp").isNotNull(), ts_col.cast("timestamp"))
         .otherwise(F.to_timestamp(ts_col))
    )

    # booleans (might be True/False already, or 0/1, or "true"/"false")
    def to_bool(cname: str) -> F.Column:
        c = F.col(cname)
        return (
            F.when(c.cast("boolean").isNotNull(), c.cast("boolean"))
             .when(F.lower(c.cast("string")) == F.lit("true"), F.lit(True))
             .when(F.lower(c.cast("string")) == F.lit("false"), F.lit(False))
             .when(c.cast("int") == F.lit(1), F.lit(True))
             .when(c.cast("int") == F.lit(0), F.lit(False))
             .otherwise(F.lit(False))
        )

    df = (
        df.withColumn("is_downtime_b", to_bool("is_downtime"))
          .withColumn("planned_downtime_b", to_bool("planned_downtime"))
          .withColumn("reading_value_d", F.col("reading_value").cast("double"))
    )

    # minute bucket
    df = df.withColumn("minute_ts", F.date_trunc("minute", F.col("event_ts")))
    return df


def main() -> None:
    spark = build_spark()

    # -----------------------------
    # Read fact + dims
    # -----------------------------
    fact_raw = read_mongo_collection(spark, "sensor_readings")
    dim_factory = read_mongo_collection(spark, "dim_factory")
    dim_machine = read_mongo_collection(spark, "dim_machine")
    dim_sensor = read_mongo_collection(spark, "dim_sensor_type")

    # Normalize / select only needed fields for performance
    fact = normalize_fact(
        fact_raw.select(
            "factory_id",
            "machine_id",
            "sensor_type_id",
            "product_id",
            "operator_id",
            "event_timestamp",
            "reading_value",
            "is_downtime",
            "planned_downtime"
        )
    )

    # Filter last N minutes (this is your “live analytics window”)
    cutoff = F.current_timestamp() - F.expr(f"INTERVAL {LAST_N_MINUTES} MINUTES")
    fact_live = fact.filter(F.col("event_ts") >= cutoff)

    # -----------------------------
    # Join dims (labels for BI)
    # -----------------------------
    # Keep dim columns flat and clean for BI tools
    dim_factory_s = dim_factory.select(
        F.col("factory_id").cast("int").alias("factory_id"),
        F.col("factory_name").cast("string").alias("factory_name"),
        F.col("location").cast("string").alias("factory_location")
    )

    dim_machine_s = dim_machine.select(
        F.col("machine_id").cast("int").alias("machine_id"),
        F.col("factory_id").cast("int").alias("factory_id"),
        F.col("machine_name").cast("string").alias("machine_name"),
        F.col("machine_type").cast("string").alias("machine_type"),
        F.col("criticality").cast("string").alias("criticality")
    )

    dim_sensor_s = dim_sensor.select(
        F.col("sensor_type_id").cast("int").alias("sensor_type_id"),
        F.col("sensor_type_name").cast("string").alias("sensor_type_name"),
        F.col("unit").cast("string").alias("unit")
    )

    live_enriched = (
        fact_live
        .withColumn("factory_id", F.col("factory_id").cast("int"))
        .withColumn("machine_id", F.col("machine_id").cast("int"))
        .withColumn("sensor_type_id", F.col("sensor_type_id").cast("int"))
        .join(dim_factory_s, on="factory_id", how="left")
        .join(dim_machine_s, on=["machine_id", "factory_id"], how="left")
        .join(dim_sensor_s, on="sensor_type_id", how="left")
    )

    # -----------------------------
    # AGG TABLE 1: Overall KPIs per minute
    # -----------------------------
    agg_overall_minute = (
        live_enriched
        .groupBy("minute_ts")
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.avg("reading_value_d").alias("avg_reading"),
            F.max("reading_value_d").alias("max_reading"),
            F.min("reading_value_d").alias("min_reading"),
            F.sum(F.when(F.col("is_downtime_b") == True, 1).otherwise(0)).alias("downtime_events"),
            F.sum(F.when(F.col("planned_downtime_b") == True, 1).otherwise(0)).alias("planned_downtime_events"),
            F.sum(F.when((F.col("is_downtime_b") == True) & (F.col("planned_downtime_b") == False), 1).otherwise(0)).alias("unplanned_downtime_events"),
        )
        .withColumn(
            "downtime_rate_pct",
            F.when(F.col("total_events") > 0, (F.col("downtime_events") / F.col("total_events")) * 100.0).otherwise(F.lit(0.0))
        )
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "minute_ts",
            "total_events",
            F.round("avg_reading", 4).alias("avg_reading"),
            "max_reading",
            "min_reading",
            "downtime_events",
            F.round("downtime_rate_pct", 4).alias("downtime_rate_pct"),
            "planned_downtime_events",
            "unplanned_downtime_events",
            "updated_at"
        )
        .orderBy("minute_ts")
    )

    # -----------------------------
    # AGG TABLE 2: Factory KPIs per minute
    # -----------------------------
    agg_factory_minute = (
        live_enriched
        .groupBy("minute_ts", "factory_id", "factory_name", "factory_location")
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.avg("reading_value_d").alias("avg_reading"),
            F.max("reading_value_d").alias("max_reading"),
            F.sum(F.when(F.col("is_downtime_b") == True, 1).otherwise(0)).alias("downtime_events"),
        )
        .withColumn(
            "downtime_rate_pct",
            F.when(F.col("total_events") > 0, (F.col("downtime_events") / F.col("total_events")) * 100.0).otherwise(F.lit(0.0))
        )
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "minute_ts",
            "factory_id",
            "factory_name",
            "factory_location",
            "total_events",
            F.round("avg_reading", 4).alias("avg_reading"),
            "max_reading",
            "downtime_events",
            F.round("downtime_rate_pct", 4).alias("downtime_rate_pct"),
            "updated_at"
        )
        .orderBy("minute_ts", "factory_id")
    )

    # -----------------------------
    # AGG TABLE 3: Sensor type KPIs per minute (across all factories)
    # -----------------------------
    agg_sensor_minute = (
        live_enriched
        .groupBy("minute_ts", "sensor_type_id", "sensor_type_name", "unit")
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.avg("reading_value_d").alias("avg_reading"),
            F.max("reading_value_d").alias("max_reading"),
            F.min("reading_value_d").alias("min_reading"),
        )
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "minute_ts",
            "sensor_type_id",
            "sensor_type_name",
            "unit",
            "total_events",
            F.round("avg_reading", 4).alias("avg_reading"),
            "max_reading",
            "min_reading",
            "updated_at"
        )
        .orderBy("minute_ts", "sensor_type_id")
    )

    # -----------------------------
    # AGG TABLE 4: Top machines snapshot (last 60 minutes, each run)
    # This is a "snapshot table" -> append rows with run_ts
    # -----------------------------
    cutoff_top = F.current_timestamp() - F.expr(f"INTERVAL {TOP_MACHINES_WINDOW_MIN} MINUTES")

    top_window = fact.filter(F.col("event_ts") >= cutoff_top)
    top_enriched = (
        top_window
        .withColumn("factory_id", F.col("factory_id").cast("int"))
        .withColumn("machine_id", F.col("machine_id").cast("int"))
        .join(dim_factory_s, on="factory_id", how="left")
        .join(dim_machine_s, on=["machine_id", "factory_id"], how="left")
    )

    agg_top_machines_last60 = (
        top_enriched
        .groupBy("machine_id", "machine_name", "factory_id", "factory_name", "criticality")
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.sum(F.when((F.col("is_downtime_b") == True) & (F.col("planned_downtime_b") == False), 1).otherwise(0)).alias("unplanned_downtime_events"),
            F.avg("reading_value_d").alias("avg_reading"),
            F.max("reading_value_d").alias("max_reading"),
        )
        .withColumn(
            "unplanned_downtime_rate_pct",
            F.when(F.col("total_events") > 0, (F.col("unplanned_downtime_events") / F.col("total_events")) * 100.0).otherwise(F.lit(0.0))
        )
        .withColumn("run_ts", F.current_timestamp())
        .select(
            "run_ts",
            "machine_id",
            "machine_name",
            "factory_id",
            "factory_name",
            "criticality",
            "total_events",
            "unplanned_downtime_events",
            F.round("unplanned_downtime_rate_pct", 4).alias("unplanned_downtime_rate_pct"),
            F.round("avg_reading", 4).alias("avg_reading"),
            "max_reading"
        )
        .orderBy(F.col("unplanned_downtime_events").desc(), F.col("max_reading").desc())
        .limit(TOP_MACHINES_LIMIT)
    )

    # -----------------------------
    # Write to Postgres
    # -----------------------------
    write_postgres(agg_overall_minute, "agg_overall_minute", mode=WRITE_MODE_MINUTE_TABLES, truncate=True)
    write_postgres(agg_factory_minute, "agg_factory_minute", mode=WRITE_MODE_MINUTE_TABLES, truncate=True)
    write_postgres(agg_sensor_minute, "agg_sensor_minute", mode=WRITE_MODE_MINUTE_TABLES, truncate=True)

    # Snapshot table should be append (keeps history of rankings over time)
    write_postgres(agg_top_machines_last60, "agg_top_machines_last60", mode=WRITE_MODE_SNAPSHOT_TABLES, truncate=False)

    print("✅ Spark analytics completed: aggregates written to Postgres.")


if __name__ == "__main__":
    main()

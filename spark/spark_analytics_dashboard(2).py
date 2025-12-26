"""
spark_analytics_dashboard.py
MongoDB -> Spark -> PostgreSQL

OUTPUT TABLES (OVERWRITE BOTH):
1) fact_machine_minute_kpi
2) fact_machine_alert_events
"""

from pyspark.sql import SparkSession, functions as F

# =========================
# CONFIG
# =========================
LOOKBACK_MINUTES = 120

MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "iot_database"
MONGO_COLLECTION = "sensor_readings"

PG_URL = "jdbc:postgresql://postgres:5432/analytics_db"
PG_USER = "analytics_user"
PG_PASSWORD = "analytics_pass"

PG_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

SPARK_PACKAGES = ",".join([
    "org.mongodb.spark:mongo-spark-connector_2.12:10.2.2",
    "org.postgresql:postgresql:42.7.3"
])

# =========================
# SPARK SESSION
# =========================
spark = (
    SparkSession.builder
    .appName("Final_KPI_And_Alerts")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================
# READ DIMENSIONS (POSTGRES)
# =========================
dim_machine = (
    spark.read.jdbc(PG_URL, "dim_machine", properties=PG_PROPS)
    .select("machine_id", "factory_id")
)

dim_sensor = (
    spark.read.jdbc(PG_URL, "dim_sensor_type", properties=PG_PROPS)
    .select("sensor_type_id", "typical_min", "typical_max")
)

# =========================
# READ HOT EVENTS (MONGO)
# =========================
events = (
    spark.read.format("mongodb")
    .option("uri", MONGO_URI)
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION)
    .load()
    .withColumn("event_timestamp_utc", F.to_timestamp("event_timestamp"))
    .withColumn("minute_ts", F.date_trunc("minute", "event_timestamp_utc"))
    .withColumn("reading_value", F.col("reading_value").cast("double"))
    .withColumn("sensor_type_id", F.col("sensor_type_id").cast("int"))
    .filter(
        F.col("event_timestamp_utc") >=
        F.expr(f"current_timestamp() - INTERVAL {LOOKBACK_MINUTES} MINUTES")
    )
)

# =========================
# ENRICH EVENTS
# =========================
events = (
    events
    .join(dim_machine, "machine_id", "left")
    .join(dim_sensor, "sensor_type_id", "left")
    .filter(F.col("factory_id").isNotNull())
)

# =========================
# ALERTS (OVERWRITE)
# =========================
alerts = (
    events
    .filter(
        (F.col("reading_value") > F.col("typical_max")) |
        (F.col("reading_value") < F.col("typical_min"))
    )
    .withColumn("alert_time_utc", F.col("event_timestamp_utc"))
    .withColumn(
        "severity",
        F.when(F.col("reading_value") > F.col("typical_max"), "CRITICAL")
         .otherwise("WARNING")
    )
    .withColumn(
        "threshold_value",
        F.when(F.col("reading_value") > F.col("typical_max"), F.col("typical_max"))
         .otherwise(F.col("typical_min"))
    )
    .withColumn(
        "threshold_condition",
        F.when(F.col("reading_value") > F.col("typical_max"), ">").otherwise("<")
    )
    .withColumn("alert_type", F.lit("Sensor Threshold Breach"))
    .withColumn("resolved_flag", F.lit(False))
    .withColumn("resolved_at", F.lit(None).cast("timestamp"))
    .select(
        "alert_time_utc",
        "machine_id",
        "sensor_type_id",
        "product_id",
        F.col("reading_value").alias("actual_value"),
        "threshold_value",
        "threshold_condition",
        "severity",
        "alert_type",
        "resolved_flag",
        "resolved_at"
    )
)

# =========================
# KPI TABLE (1‑MIN GRAIN)
# =========================
kpi = (
    events.groupBy(
        "minute_ts",
        "factory_id",
        "machine_id",
        "sensor_type_id",
        "product_id",
        "operator_id"
    )
    .agg(
        F.avg("reading_value").alias("avg_reading"),
        F.min("reading_value").alias("min_reading"),
        F.max("reading_value").alias("max_reading"),
        F.sum(F.when(F.col("is_planned_downtime"), 60).otherwise(0))
            .alias("planned_downtime_sec"),
        F.sum(F.when(~F.col("is_planned_downtime"), 60).otherwise(0))
            .alias("unplanned_downtime_sec"),
        F.count(
            F.when(
                (F.col("reading_value") > F.col("typical_max")) |
                (F.col("reading_value") < F.col("typical_min")),
                True
            )
        ).alias("num_alerts")
    )
    .withColumnRenamed("minute_ts", "kpi_minute_start_utc")
)

# =========================
# WRITE TO POSTGRES (OVERWRITE BOTH)
# =========================
kpi.write.jdbc(
    PG_URL,
    "fact_machine_minute_kpi",
    mode="overwrite",
    properties=PG_PROPS
)

alerts.write.jdbc(
    PG_URL,
    "fact_machine_alert_events",
    mode="overwrite",
    properties=PG_PROPS
)

spark.stop()

"""
spark_analytics_dashboard_2.py

MongoDB (sensor events)
-> Spark (1-minute KPIs + alerts)
-> PostgreSQL (analytics_db)

POSTGRES TABLES:
- minute_kpi      (OVERWRITE every run)
- alert_events    (APPEND only)

Design:
- One KPI table (machine + sensor + minute)
- Alerts stored as fact table
- Superset-friendly schema
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast

# =====================================================
# CONFIG
# =====================================================
LOOKBACK_MINUTES = 120   # recompute last N minutes

# Mongo
MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "iot_database"
MONGO_COLLECTION = "sensor_readings"

# Postgres
PG_URL = "jdbc:postgresql://postgres:5432/analytics_db"
PG_USER = "analytics_user"
PG_PASSWORD = "analytics_pass"
PG_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": "5000"
}

SPARK_PACKAGES = ",".join([
    "org.mongodb.spark:mongo-spark-connector_2.12:10.2.2",
    "org.postgresql:postgresql:42.7.3"
])

# =====================================================
# SPARK SESSION
# =====================================================
spark = (
    SparkSession.builder
    .appName("Minute_KPI_And_Alerts")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.mongodb.read.connection.uri", MONGO_URI)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# READ DIMENSIONS (POSTGRES)
# =====================================================
dim_machine = (
    spark.read.jdbc(PG_URL, "dim_machine", properties=PG_PROPS)
    .select(
        F.col("machine_id").cast("string"),
        F.col("factory_id").cast("string")
    )
)
dim_machine.cache()

dim_sensor = (
    spark.read.jdbc(PG_URL, "dim_sensor_type", properties=PG_PROPS)
    .select(
        F.col("sensor_type_id").cast("int"),
        F.col("sensor_type_name"),
        F.col("typical_min").cast("double"),
        F.col("typical_max").cast("double"),
        F.col("unit")
    )
)
dim_sensor.cache()

# =====================================================
# READ EVENTS (MONGO)
# =====================================================
events = (
    spark.read.format("mongodb")
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION)
    .load()
    .withColumn(
        "event_timestamp_utc",
        F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn("machine_id", F.col("machine_id").cast("string"))
    .withColumn("sensor_type_id", F.col("sensor_type_id").cast("int"))
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("operator_id", F.col("operator_id").cast("int"))
    .withColumn("reading_value", F.col("reading_value").cast("double"))
    .filter(
        F.col("event_timestamp_utc") >=
        F.expr(f"current_timestamp() - INTERVAL {LOOKBACK_MINUTES} MINUTES")
    )
)

# =====================================================
# ENRICH EVENTS
# =====================================================
events = (
    events
    .join(broadcast(dim_machine), "machine_id", "left")
    .join(broadcast(dim_sensor), "sensor_type_id", "left")
    .filter(F.col("factory_id").isNotNull())
    .withColumn("kpi_minute_start_utc", F.date_trunc("minute", "event_timestamp_utc"))
)

# =====================================================
# ALERT GENERATION (APPEND ONLY)
# =====================================================
alert_events = (
    events
    .filter(
        (F.col("reading_value") > F.col("typical_max")) |
        (F.col("reading_value") < F.col("typical_min"))
    )
    .withColumn("alert_time_utc", F.col("event_timestamp_utc"))
    .withColumn(
        "severity",
        F.when(F.col("reading_value") > F.col("typical_max"), F.lit("CRITICAL"))
         .otherwise(F.lit("WARNING"))
    )
    .withColumn(
        "threshold_value",
        F.when(F.col("reading_value") > F.col("typical_max"), F.col("typical_max"))
         .otherwise(F.col("typical_min"))
    )
    .withColumn(
        "threshold_condition",
        F.when(F.col("reading_value") > F.col("typical_max"), F.lit(">"))
         .otherwise(F.lit("<"))
    )
    .withColumn(
        "alert_message",
        F.concat(
            F.lit("Sensor "),
            F.col("sensor_type_name"),
            F.lit(" breached: "),
            F.round(F.col("reading_value"), 2),
            F.lit(" "),
            F.col("unit")
        )
    )
    .select(
        "alert_time_utc",
        "factory_id",
        "machine_id",
        "sensor_type_id",
        "product_id",
        "operator_id",
        F.col("reading_value").alias("actual_value"),
        "threshold_value",
        "threshold_condition",
        "severity",
        F.lit("Sensor Threshold Breach").alias("alert_type"),
        "alert_message",
        F.lit(False).alias("resolved_flag"),
        F.lit(None).cast("timestamp").alias("resolved_at")
    )
)

# =====================================================
# KPI AGGREGATION (SINGLE TABLE)
# =====================================================
minute_kpi = (
    events.groupBy(
        "kpi_minute_start_utc",
        "factory_id",
        "machine_id",
        "product_id",
        "operator_id",
        "sensor_type_id"
    )
    .agg(
        F.avg("reading_value").alias("avg_reading"),
        F.max("reading_value").alias("max_reading"),
        F.min("reading_value").alias("min_reading"),
        F.count("*").alias("num_events")
    )
    .withColumn("planned_downtime_sec", F.lit(0))
    .withColumn("unplanned_downtime_sec", F.lit(0))
)

# Add num_alerts per minute
alerts_per_minute = (
    alert_events.groupBy(
        F.col("factory_id"),
        F.col("machine_id"),
        F.col("sensor_type_id"),
        F.col("alert_time_utc").alias("kpi_minute_start_utc")
    )
    .agg(F.count("*").alias("num_alerts"))
)

minute_kpi = (
    minute_kpi.join(
        alerts_per_minute,
        ["kpi_minute_start_utc", "factory_id", "machine_id", "sensor_type_id"],
        "left"
    )
    .fillna(0, subset=["num_alerts"])
)

# =====================================================
# WRITE TO POSTGRES
# =====================================================
minute_kpi.write.jdbc(
    PG_URL, "minute_kpi", mode="overwrite", properties=PG_PROPS
)

alert_events.write.jdbc(
    PG_URL, "alert_events", mode="append", properties=PG_PROPS
)

spark.stop()

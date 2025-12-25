"""
spark_analytics_dashboard.py

MongoDB (hot sensor events)
   -> Spark (minute KPIs + threshold alerts)
   -> PostgreSQL (analytics_db)

OUTPUT TABLES (Postgres):
  - agg_overall_minute
  - agg_factory_minute
  - agg_machine_minute
  - agg_product_minute
  - fact_machine_alert_events

NOTE:
- Append-only design (BI filters by time)
- Dimension joins happen in BI layer
"""

from pyspark.sql import SparkSession, functions as F

# =========================
# CONFIG
# =========================

LOOKBACK_MINUTES = 60

# Mongo (Docker service name)
MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "iot_database"
MONGO_COLLECTION = "sensor_readings"

# Postgres (Docker service name)
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
    .appName("RealTime_KPI_And_Alerts")
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
    .select(
        F.col("machine_id").cast("int"),
        F.col("factory_id").cast("int")
    )
)

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

# =========================
# READ HOT EVENTS (MONGO)
# =========================

events = (
    spark.read.format("mongo")
    .option("uri", MONGO_URI)
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION)
    .load()
    .withColumn("event_timestamp_utc", F.to_timestamp("event_timestamp_utc"))
    .withColumn("machine_id", F.col("machine_id").cast("int"))
    .withColumn("sensor_type_id", F.col("sensor_type_id").cast("int"))
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("reading_value", F.col("reading_value").cast("double"))
    .filter(
        F.col("event_timestamp_utc")
        >= F.expr(f"timestampadd(MINUTE, -{LOOKBACK_MINUTES}, current_timestamp())")
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
    .withColumn("minute_ts", F.date_trunc("minute", "event_timestamp_utc"))
)

# =========================
# KPI AGGREGATES
# =========================

agg_overall_minute = (
    events.groupBy("minute_ts")
    .agg(
        F.count("*").alias("event_count"),
        F.avg("reading_value").alias("avg_value"),
        F.min("reading_value").alias("min_value"),
        F.max("reading_value").alias("max_value")
    )
)

agg_factory_minute = (
    events.groupBy("minute_ts", "factory_id")
    .agg(
        F.count("*").alias("event_count"),
        F.avg("reading_value").alias("avg_value"),
        F.min("reading_value").alias("min_value"),
        F.max("reading_value").alias("max_value")
    )
)

agg_machine_minute = (
    events.groupBy("minute_ts", "factory_id", "machine_id")
    .agg(
        F.count("*").alias("event_count"),
        F.avg("reading_value").alias("avg_value"),
        F.min("reading_value").alias("min_value"),
        F.max("reading_value").alias("max_value")
    )
)

agg_product_minute = (
    events.filter(F.col("product_id").isNotNull())
    .groupBy("minute_ts", "factory_id", "product_id")
    .agg(
        F.count("*").alias("event_count"),
        F.avg("reading_value").alias("avg_value"),
        F.min("reading_value").alias("min_value"),
        F.max("reading_value").alias("max_value")
    )
)

# =========================
# ALERT GENERATION
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
            F.lit("Sensor "), F.col("sensor_type_name"),
            F.lit(" breached. Value="),
            F.round(F.col("reading_value"), 2),
            F.lit(" Threshold="),
            F.round(F.col("threshold_value"), 2),
            F.lit(" "), F.col("unit")
        )
    )
    .select(
        "alert_time_utc",
        "machine_id",
        "sensor_type_id",
        "product_id",
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

# =========================
# WRITE TO POSTGRES
# =========================

agg_overall_minute.write.jdbc(PG_URL, "agg_overall_minute", "append", PG_PROPS)
agg_factory_minute.write.jdbc(PG_URL, "agg_factory_minute", "append", PG_PROPS)
agg_machine_minute.write.jdbc(PG_URL, "agg_machine_minute", "append", PG_PROPS)
agg_product_minute.write.jdbc(PG_URL, "agg_product_minute", "append", PG_PROPS)
alerts.write.jdbc(PG_URL, "fact_machine_alert_events", "append", PG_PROPS)

spark.stop()

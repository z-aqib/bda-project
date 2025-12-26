from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType
)
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, hour

RAW_PATH = "hdfs://namenode:8020/bda/raw/mongo/raw.json"
PARQUET_PATH = "hdfs://namenode:8020/bda/archive/data/"

spark = (
    SparkSession.builder
    .appName("mongo-raw-to-parquet")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# -----------------------------
# Explicit schema (IMPORTANT)
# -----------------------------
schema = StructType([
    StructField("_id", StringType(), True),
    StructField("factory_id", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("sensor_type_id", IntegerType(), True),
    StructField("operator_id", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("reading_value", DoubleType(), True),
    StructField("is_downtime", BooleanType(), True),
    StructField("planned_downtime", BooleanType(), True),
])

# -----------------------------
# Read JSON from HDFS
# -----------------------------
df = (
    spark.read
    .schema(schema)
    .option("multiLine", "false")   # newline JSON
    .json(RAW_PATH)
)

if df.rdd.isEmpty():
    print("No data found in raw JSON. Exiting safely.")
    spark.stop()
    exit(0)

# -----------------------------
# Transform
# -----------------------------
df = df.withColumn(
    "event_ts",
    to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss")
)

df = (
    df.withColumn("year", year("event_ts"))
      .withColumn("month", month("event_ts"))
      .withColumn("day", dayofmonth("event_ts"))
      .withColumn("hour", hour("event_ts"))
)

# -----------------------------
# Write Parquet
# -----------------------------
(
    df.repartition("factory_id")
      .write
      .mode("append")
      .partitionBy("factory_id", "year", "month", "day", "hour")
      .parquet(PARQUET_PATH)
)

spark.stop()

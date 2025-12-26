from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, hour

spark = (
    SparkSession.builder
    .appName("mongo-raw-archive")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .getOrCreate()
)

RAW_PATH = "hdfs://namenode:8020/bda/raw/mongo/"
ARCHIVE_PATH = "hdfs://namenode:8020/bda/archive/data/"

df = spark.read.json(RAW_PATH)

df = df.withColumn(
    "event_ts",
    to_timestamp("event_timestamp")
)

(df
 .withColumn("year", year("event_ts"))
 .withColumn("month", month("event_ts"))
 .withColumn("day", dayofmonth("event_ts"))
 .withColumn("hour", hour("event_ts"))
 .repartition("factory_id")
 .write
 .mode("append")
 .partitionBy("factory_id", "year", "month", "day", "hour")
 .parquet(ARCHIVE_PATH)
)

spark.stop()

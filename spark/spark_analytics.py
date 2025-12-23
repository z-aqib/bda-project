from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count

spark = SparkSession.builder \
    .appName("IoT Analytics") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongo:27017/iot_database.sensor_readings") \
    .getOrCreate()

# Read from Mongo
df = spark.read \
    .format("mongodb") \
    .load()

df.printSchema()

# Example KPIs
kpi_df = df.groupBy("device_id").agg(
    avg("temperature").alias("avg_temperature"),
    max("temperature").alias("max_temperature"),
    min("temperature").alias("min_temperature"),
    count("*").alias("total_readings")
)

kpi_df.show()

spark.stop()

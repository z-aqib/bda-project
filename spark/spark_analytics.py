from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, sum, when, to_date, to_timestamp

# Initialize Spark session with MongoDB
spark = SparkSession.builder \
    .appName("IoT Analytics") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongo:27017/iot_database.sensor_readings") \
    .getOrCreate()

# Read from Mongo
df = spark.read.format("mongodb").load()

# Convert event_timestamp to timestamp type
df = df.withColumn("event_timestamp", to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))


# Optional: average reading per machine per day
daily_df = df.withColumn("event_date", to_date("event_timestamp")) \
    .groupBy("machine_id", "event_date") \
    .agg(
        avg("reading_value").alias("avg_reading"),
        max("reading_value").alias("max_reading"),
        min("reading_value").alias("min_reading"),
        count("*").alias("total_readings")
    )

daily_df.show()

spark.stop()

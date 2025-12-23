from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# 1. Kafka Consumer
consumer = KafkaConsumer(
    "iot_sensor_stream",
    bootstrap_servers=[
        "kafka1:9092",
        "kafka2:9092",
        "kafka3:9092"
    ],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mongo-consumer-group"
)

# 2. MongoDB Connection
mongo_client = MongoClient("mongodb://mongo:27017/")
db = mongo_client["iot_database"]
collection = db["sensor_readings"]

print("Kafka → MongoDB consumer started...")

# 3. Consume & insert continuously
for message in consumer:
    collection.insert_one(message.value)
    print("Inserted:", message.value)

from kafka import KafkaProducer
import pandas as pd
import random
from datetime import datetime, timedelta
import time
import json

producer = KafkaProducer(
    bootstrap_servers=[
        "kafka1:9092",
        "kafka2:9092",
        "kafka3:9092"
    ],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "iot_sensor_stream"

# --- Load dimension IDs from CSVs ---
factories = pd.read_csv(r".\static_data\dim_factory.csv")['factory_id'].tolist()
machines = pd.read_csv(r".\static_data\dim_machine.csv")['machine_id'].tolist()
sensor_types = pd.read_csv(r".\static_data\dim_sensor_type.csv")['sensor_type_id'].tolist()
operators = pd.read_csv(r".\static_data\dim_operator.csv")['operator_id'].tolist()
products = pd.read_csv(r".\static_data\dim_product.csv")['product_id'].tolist()

# --- Settings ---
missing_probability = 0.05       # 5% chance to skip a reading
downtime_probability = 0.01
planned_downtime_probability = 0.02
publish_interval_seconds = 1      # interval between timestamps
min_product_duration = 300        # minimum 5 minutes
max_product_duration = 600        # optional maximum duration

# --- Function to generate a single IoT record ---
def generate_iot_record(current_time, machine_id, sensor_id, product_id):

    planned_downtime = random.random() < planned_downtime_probability
    is_downtime = planned_downtime or (random.random() < downtime_probability)
    
    return {
        "factory_id": random.choice(factories),
        "machine_id": machine_id,
        "sensor_type_id": sensor_id,
        "operator_id": random.choice(operators),
        "product_id": product_id,
        "event_timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        "reading_value": round(random.uniform(0, 100), 2),
        "is_downtime": is_downtime,
        "planned_downtime": planned_downtime
    }

# --- Continuous IoT data stream ---
def iot_data_stream():
    current_time = datetime.now()
    # Choose initial product and its duration
    current_product = random.choice(products)
    product_duration = random.randint(min_product_duration, max_product_duration)
    product_switch_time = current_time + timedelta(seconds=product_duration)

    while True:
        # Switch product after the duration
        if current_time >= product_switch_time:
            current_product = random.choice(products)
            product_duration = random.randint(min_product_duration, max_product_duration)
            product_switch_time = current_time + timedelta(seconds=product_duration)

        for machine_id in machines:
            for sensor_id in sensor_types:
                if random.random() < missing_probability:
                    continue  # skip some readings
                event = generate_iot_record(current_time, machine_id, sensor_id, current_product)
                producer.send(TOPIC, event)
                print("Sent:", event)
 

        # Increment time by 1 second
        current_time += timedelta(seconds=publish_interval_seconds)
        time.sleep(publish_interval_seconds)
    
# --- Start streaming ---
iot_data_stream()

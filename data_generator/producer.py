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

# --- Load dimension data ---
factories = pd.read_csv("static_data/dim_factory.csv")['factory_id'].tolist()
machines = pd.read_csv("static_data/dim_machine.csv")['machine_id'].tolist()
operators = pd.read_csv("static_data/dim_operator.csv")['operator_id'].tolist()
products = pd.read_csv("static_data/dim_product.csv")['product_id'].tolist()

# Sensor types WITH ranges
sensor_df = pd.read_csv("static_data/dim_sensor_type.csv")

sensor_ranges = {
    row["sensor_type_id"]: {
        "min": row["typical_min"],
        "max": row["typical_max"]
    }
    for _, row in sensor_df.iterrows()
}

sensor_types = list(sensor_ranges.keys())

# --- Settings ---
missing_probability = 0.05          # 5% missing readings
downtime_probability = 0.01
planned_downtime_probability = 0.02
out_of_range_probability = 0.05     # 5% abnormal readings
publish_interval_seconds = 1

min_product_duration = 300
max_product_duration = 600

# --- Generate sensor reading ---
def generate_sensor_value(sensor_id):
    r = sensor_ranges[sensor_id]
    typical_min = r["min"]
    typical_max = r["max"]

    # Decide if out-of-range
    if random.random() < out_of_range_probability:
        # Generate anomaly
        if random.random() < 0.5:
            return round(random.uniform(typical_min * 0.5, typical_min * 0.9), 2)
        else:
            return round(random.uniform(typical_max * 1.1, typical_max * 1.5), 2)
    else:
        # Normal reading
        return round(random.uniform(typical_min, typical_max), 2)

# --- Generate one IoT event ---
def generate_iot_record(current_time, machine_id, sensor_id, product_id):

    planned_downtime = random.random() < planned_downtime_probability
    is_downtime = planned_downtime or (random.random() < downtime_probability)

    reading_value = generate_sensor_value(sensor_id)

    return {
        "factory_id": random.choice(factories),
        "machine_id": machine_id,
        "sensor_type_id": sensor_id,
        "operator_id": random.choice(operators),
        "product_id": product_id,
        "event_timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        "reading_value": reading_value,
        "is_downtime": is_downtime,
        "planned_downtime": planned_downtime
    }

# --- Continuous IoT stream ---
def iot_data_stream():
    current_time = datetime.now()

    current_product = random.choice(products)
    product_duration = random.randint(min_product_duration, max_product_duration)
    product_switch_time = current_time + timedelta(seconds=product_duration)

    while True:
        if current_time >= product_switch_time:
            current_product = random.choice(products)
            product_duration = random.randint(min_product_duration, max_product_duration)
            product_switch_time = current_time + timedelta(seconds=product_duration)

        for machine_id in machines:
            for sensor_id in sensor_types:
                if random.random() < missing_probability:
                    continue

                event = generate_iot_record(
                    current_time,
                    machine_id,
                    sensor_id,
                    current_product
                )

                producer.send(TOPIC, event)
                print("Sent:", event)

        current_time += timedelta(seconds=publish_interval_seconds)
        time.sleep(publish_interval_seconds)

# --- Start ---
iot_data_stream()

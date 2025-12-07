"""
generate_static_data_faker.py

Generates larger static tables using Faker:
- dim_factory
- dim_machine
- dim_sensor_type
- dim_product
- dim_operator

Each table -> its own CSV (and optional JSON) in the output folder.

NOTE:
- factory_id, machine_id, operator_id are now STRING codes:
  e.g. 'FCT001', 'MAC0001', 'OPR0001'
- No separate *_code columns.
- dim_operator includes factory_id (FK to dim_factory.factory_id).
"""

from pathlib import Path
import random
from faker import Faker
import pandas as pd

# ==========================
# CONFIG
# ==========================

OUTPUT_DIR = Path("generated_data")
ALSO_WRITE_JSON = True

# You can increase these to get more static data:
N_FACTORIES = 10                       # was 5
MIN_MACHINES_PER_FACTORY = 20          # was 10
MAX_MACHINES_PER_FACTORY = 40          # was 25
N_PRODUCTS = 40
N_OPERATORS = 200                      # was 50

PAK_CITIES = [
    "Karachi", "Lahore", "Islamabad", "Faisalabad", "Rawalpindi",
    "Multan", "Hyderabad", "Peshawar", "Quetta", "Sialkot"
]

MACHINE_TYPES = ["Oven", "Mixer", "Packer", "Conveyor", "Cooling Tunnel"]
VENDORS = ["Siemens", "ABB", "Bosch", "Mitsubishi", "GE"]
PRODUCT_FAMILIES = ["Biscuits", "Wafers", "Snacks"]

faker = Faker()
# For reproducible results:
Faker.seed(42)
random.seed(42)


def ensure_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def write_table(df: pd.DataFrame, name: str):
    csv_path = OUTPUT_DIR / f"{name}.csv"
    df.to_csv(csv_path, index=False)
    print(f"Wrote CSV: {csv_path}")

    if ALSO_WRITE_JSON:
        json_path = OUTPUT_DIR / f"{name}.json"
        df.to_json(json_path, orient="records", indent=2)
        print(f"Wrote JSON: {json_path}")


# ==========================
# TABLE GENERATORS
# ==========================

def generate_dim_factory() -> pd.DataFrame:
    """
    factory_id is now a STRING like 'FCT001'.
    """
    rows = []
    for idx in range(1, N_FACTORIES + 1):
        factory_id = f"FCT{idx:03d}"
        city = random.choice(PAK_CITIES)
        rows.append({
            "factory_id": factory_id,                  # string PK
            "factory_name": f"Bisconni {city} Plant",
            "city": city,
            "country": "Pakistan",
            "timezone": "Asia/Karachi",
            "capacity_class": random.choice(["Small", "Medium", "Large"]),
        })
    return pd.DataFrame(rows)


def generate_dim_product() -> pd.DataFrame:
    """
    Generate a mix of real-ish Bisconni-style products + random ones.
    product_id stays numeric (int), that's fine.
    """
    base_products = [
        ("CHOCOLATTO", "Chocolatto Chocochip"),
        ("RITE", "Rite Filled Biscuit"),
        ("COCOMO", "Cocomo Chocolate Filled"),
        ("NOVITA", "Novita Vanilla Cream"),
        ("CHOCO_CHIP", "Choco Chip Biscuit"),
        ("CREAM_CRACKER", "Cream Cracker"),
    ]

    rows = []
    product_id = 1

    # First some hand-crafted Bisconni-ish SKUs with sizes
    for code, name in base_products:
        for size in [45, 90, 100, 150]:
            rows.append({
                "product_id": product_id,
                "sku": f"{code}_{size}G",
                "product_name": f"{name} {size}g",
                "category": "Biscuit",
                "brand": "Bisconni",
                "pack_size_g": size,
            })
            product_id += 1
            if product_id > N_PRODUCTS:
                return pd.DataFrame(rows)

    # If we still need more products, generate random snacks
    while product_id <= N_PRODUCTS:
        size = random.choice([30, 45, 60, 75, 90, 100, 120, 150, 200])
        code = faker.bothify(text="SNACK_##").upper()
        name = faker.word().capitalize() + " Biscuit"
        rows.append({
            "product_id": product_id,
            "sku": f"{code}_{size}G",
            "product_name": f"{name} {size}g",
            "category": random.choice(["Biscuit", "Wafer", "Snack"]),
            "brand": "Bisconni",
            "pack_size_g": size,
        })
        product_id += 1

    return pd.DataFrame(rows)


def generate_dim_sensor_type() -> pd.DataFrame:
    """
    sensor_type_id stays numeric, with explicit codes like TEMP, VIB, etc.
    """
    data = [
        {
            "sensor_type_id": 1,
            "sensor_type_code": "TEMP",
            "sensor_type_name": "Temperature",
            "unit": "C",
            "typical_min": 180.0,
            "typical_max": 230.0,
        },
        {
            "sensor_type_id": 2,
            "sensor_type_code": "VIB",
            "sensor_type_name": "Vibration",
            "unit": "mm/s",
            "typical_min": 2.0,
            "typical_max": 6.0,
        },
        {
            "sensor_type_id": 3,
            "sensor_type_code": "CURR",
            "sensor_type_name": "Current",
            "unit": "A",
            "typical_min": 10.0,
            "typical_max": 30.0,
        },
        {
            "sensor_type_id": 4,
            "sensor_type_code": "HUM",
            "sensor_type_name": "Humidity",
            "unit": "%",
            "typical_min": 30.0,
            "typical_max": 60.0,
        },
    ]
    return pd.DataFrame(data)


def generate_dim_machine(factories_df: pd.DataFrame) -> pd.DataFrame:
    """
    machine_id is now STRING like 'MAC0001'.
    factory_id is a STRING FK to dim_factory.factory_id.
    """
    rows = []
    machine_counter = 1

    for _, factory in factories_df.iterrows():
        factory_id = factory["factory_id"]  # string, e.g. 'FCT001'

        n_machines = random.randint(MIN_MACHINES_PER_FACTORY, MAX_MACHINES_PER_FACTORY)

        for _ in range(n_machines):
            product_family = random.choice(PRODUCT_FAMILIES)
            line_index = random.randint(1, 4)
            line_code = f"{product_family.upper().replace(' ', '_')}_LINE_{line_index}"
            line_name = f"{product_family} Line {line_index}"

            machine_type = random.choice(MACHINE_TYPES)

            # String machine ID like MAC0001
            machine_id = f"MAC{machine_counter:04d}"
            machine_counter += 1

            rows.append({
                "machine_id": machine_id,  # string PK
                "machine_name": f"{machine_type}-{machine_id}",
                "line_code": line_code,
                "line_name": line_name,
                "product_family": product_family,
                "factory_id": factory_id,  # string FK -> dim_factory.factory_id
                "machine_type": machine_type,
                "vendor": random.choice(VENDORS),
                "install_date": faker.date_between(start_date="-10y", end_date="-1y"),
                "criticality": random.choice(["Low", "Medium", "High"]),
            })

    return pd.DataFrame(rows)


def generate_dim_operator(factories_df: pd.DataFrame) -> pd.DataFrame:
    """
    operator_id is STRING like 'OPR0001'.
    Each operator is assigned to a factory via factory_id (string FK).
    """
    levels = ["Junior", "Mid", "Senior"]
    shifts = ["Morning only", "Evening only", "Night only", "Rotational"]

    factory_ids = factories_df["factory_id"].tolist()

    rows = []
    for idx in range(1, N_OPERATORS + 1):
        operator_id = f"OPR{idx:04d}"  # string PK
        rows.append({
            "operator_id": operator_id,
            "operator_name": faker.name(),
            "experience_level": random.choice(levels),
            "shift_pattern": random.choice(shifts),
            "factory_id": random.choice(factory_ids),  # string FK
        })
    return pd.DataFrame(rows)


# ==========================
# MAIN
# ==========================

def main():
    ensure_output_dir()

    dim_factory = generate_dim_factory()
    dim_product = generate_dim_product()
    dim_sensor_type = generate_dim_sensor_type()
    dim_machine = generate_dim_machine(dim_factory)
    dim_operator = generate_dim_operator(dim_factory)

    write_table(dim_factory, "dim_factory")
    write_table(dim_machine, "dim_machine")
    write_table(dim_sensor_type, "dim_sensor_type")
    write_table(dim_product, "dim_product")
    write_table(dim_operator, "dim_operator")

    print("All static dimension tables generated with Faker.")


if __name__ == "__main__":
    main()
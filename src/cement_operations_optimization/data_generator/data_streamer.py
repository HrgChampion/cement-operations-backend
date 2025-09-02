import psycopg2
from psycopg2.extras import execute_values
import random
import time
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# --- PostgreSQL Connection ---
DB_CONFIG = {
    "dbname": "cement_db",
    "user": "postgres",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "localhost",
    "port": "5432"
}

def create_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cement_plant_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            equipment TEXT NOT NULL,
            temperature FLOAT,
            pressure FLOAT,
            vibration FLOAT,
            power FLOAT,
            emission FLOAT,
            anomaly BOOLEAN,
            anomaly_type TEXT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# --- Synthetic Data Generator ---
def generate_data():
    equipment_list = ["Kiln", "Cement Mill", "Preheater", "Cooler", "Raw Mill", "Baghouse"]
    anomaly_events = [
        ("Kiln temperature spike", {"temperature": (1500, 1600)}),
        ("Emission surge", {"emission": (200, 300)}),
        ("Power dip", {"power": (200, 300)}),
        ("Pressure drop", {"pressure": (20, 40)}),
        ("High vibration", {"vibration": (10, 15)})
    ]

    equipment = random.choice(equipment_list)
    data = {
        "timestamp": datetime.now(),
        "equipment": equipment,
        "temperature": random.uniform(1200, 1450),
        "pressure": random.uniform(50, 100),
        "vibration": random.uniform(0.1, 5.0),
        "power": random.uniform(400, 600),
        "emission": random.uniform(50, 150),
        "anomaly": False,
        "anomaly_type": None
    }

    # Randomly inject anomaly
    if random.random() < 0.1:  # 10% chance
        anomaly = random.choice(anomaly_events)
        data["anomaly"] = True
        data["anomaly_type"] = anomaly[0]
        for key, (low, high) in anomaly[1].items():
            data[key] = random.uniform(low, high)

    return data

# --- Insert Data into PostgreSQL ---
def insert_data(batch):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    query = """
        INSERT INTO cement_plant_data
        (timestamp, equipment, temperature, pressure, vibration, power, emission, anomaly, anomaly_type)
        VALUES %s
    """
    values = [(d["timestamp"], d["equipment"], d["temperature"], d["pressure"],
               d["vibration"], d["power"], d["emission"], d["anomaly"], d["anomaly_type"])
              for d in batch]
    execute_values(cur, query, values)
    conn.commit()
    cur.close()
    conn.close()

# --- Main Loop ---
def run():
    create_table()
    print("Streaming synthetic data to PostgreSQL...")
    while True:
        batch = [generate_data() for _ in range(5)]  # send 5 rows per second
        insert_data(batch)
        print(f"Inserted {len(batch)} records at {datetime.now()}")
        time.sleep(1)

if __name__ == "__main__":
    run()

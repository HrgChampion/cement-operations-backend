import random
import time
import json
import pandas as pd
from datetime import datetime
from typing import Dict
import threading


# CONFIGURATION
PROCESS_NAMES = ["Raw_Materials", "Grinding", "Kiln", "Emissions", "Utilities"]
BATCH_SIZE = 10
DATA_INTERVAL = 1  # seconds between records per process
OUTPUT_FILE = "synthetic_cement_data.csv"


# PROCESS SIMULATION LOGIC

def simulate_raw_materials() -> Dict:
    return {
        "limestone_feed": round(random.uniform(60, 80), 2),
        "clay_feed": round(random.uniform(10, 20), 2),
        "iron_ore_feed": round(random.uniform(1, 5), 2),
        "moisture_content": round(random.uniform(5, 12), 2)
    }

def simulate_grinding() -> Dict:
    return {
        "mill_power_kw": round(random.uniform(2000, 3500), 2),
        "fineness_blaine": round(random.uniform(280, 350), 2),
        "grinding_temp": round(random.uniform(80, 120), 2)
    }

def simulate_kiln() -> Dict:
    return {
        "kiln_speed_rpm": round(random.uniform(3, 5), 2),
        "kiln_feed_rate_tph": round(random.uniform(100, 150), 2),
        "kiln_temp_c": round(random.uniform(1350, 1450), 2),
        "fuel_rate_kg_hr": round(random.uniform(1000, 1400), 2)
    }

def simulate_emissions() -> Dict:
    return {
        "co2_emission": round(random.uniform(700, 900), 2),
        "nox_ppm": round(random.uniform(500, 700), 2),
        "sox_ppm": round(random.uniform(50, 150), 2),
        "dust_mg_m3": round(random.uniform(20, 50), 2)
    }

def simulate_utilities() -> Dict:
    return {
        "power_consumption_kw": round(random.uniform(3000, 5000), 2),
        "water_usage_m3": round(random.uniform(50, 100), 2),
        "compressed_air_bar": round(random.uniform(5, 8), 2)
    }

PROCESS_SIMULATORS = {
    "Raw_Materials": simulate_raw_materials,
    "Grinding": simulate_grinding,
    "Kiln": simulate_kiln,
    "Emissions": simulate_emissions,
    "Utilities": simulate_utilities
}

# DATA GENERATION

def generate_data(process: str, stop_event: threading.Event):
    records = []
    while not stop_event.is_set():
        timestamp = datetime.now().isoformat()
        data = PROCESS_SIMULATORS[process]()
        record = {"timestamp": timestamp, "process": process, **data}
        records.append(record)

        # Print JSON to simulate streaming (Kafka/PubSub)
        print(json.dumps(record))

        # Write in batches
        if len(records) >= BATCH_SIZE:
            df = pd.DataFrame(records)
            df.to_csv(OUTPUT_FILE, mode='a', header=not pd.io.common.file_exists(OUTPUT_FILE), index=False)
            records.clear()

        time.sleep(DATA_INTERVAL)


# MAIN FUNCTION

def start_simulation():
    stop_event = threading.Event()
    threads = []

    for process in PROCESS_NAMES:
        thread = threading.Thread(target=generate_data, args=(process, stop_event), daemon=True)
        threads.append(thread)
        thread.start()

    try:
        print("Synthetic data generation started. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping data simulation...")
        stop_event.set()
        for t in threads:
            t.join()
        print("Simulation stopped.")

if __name__ == "__main__":
    start_simulation()

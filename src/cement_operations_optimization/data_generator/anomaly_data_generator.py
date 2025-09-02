import random
import time
import json
import datetime

# Define equipment and sensors
EQUIPMENT = ["Raw Mill", "Kiln", "Clinker Cooler", "Cement Mill", "Packing Plant"]

# Normal ranges for metrics
NORMAL_RANGES = {
    "temperature": (100, 1400),      # °C
    "pressure": (1, 5),              # bar
    "vibration": (0.1, 3.0),         # mm/s
    "power": (100, 5000),            # kW
    "emissions": (200, 800),         # mg/Nm³
    "fineness": (280, 400),          # Blaine (m²/kg)
    "residue": (0.5, 5.0)            # % residue
}

# Predefined anomaly types
ANOMALIES = [
    "Kiln temperature spike",
    "Clinker cooler fan failure",
    "Grinding motor overload",
    "Power supply dip",
    "Emission spike",
    "Low cement fineness",
    "High residue"
]

def generate_normal_readings():
    """Generate readings in normal ranges."""
    return {
        "temperature": round(random.uniform(*NORMAL_RANGES["temperature"]), 2),
        "pressure": round(random.uniform(*NORMAL_RANGES["pressure"]), 2),
        "vibration": round(random.uniform(*NORMAL_RANGES["vibration"]), 2),
        "power": round(random.uniform(*NORMAL_RANGES["power"]), 2),
        "emissions": round(random.uniform(*NORMAL_RANGES["emissions"]), 2),
        "fineness": round(random.uniform(*NORMAL_RANGES["fineness"]), 2),
        "residue": round(random.uniform(*NORMAL_RANGES["residue"]), 2),
    }

def inject_anomaly(readings):
    """Randomly modify readings to simulate a fault."""
    anomaly = random.choice(ANOMALIES)
    
    if anomaly == "Kiln temperature spike":
        readings["temperature"] = round(random.uniform(1500, 1700), 2)
    elif anomaly == "Clinker cooler fan failure":
        readings["temperature"] += random.uniform(100, 200)
    elif anomaly == "Grinding motor overload":
        readings["power"] = round(random.uniform(6000, 8000), 2)
    elif anomaly == "Power supply dip":
        readings["power"] = round(random.uniform(50, 200), 2)
    elif anomaly == "Emission spike":
        readings["emissions"] = round(random.uniform(1000, 2000), 2)
    elif anomaly == "Low cement fineness":
        readings["fineness"] = round(random.uniform(200, 250), 2)
    elif anomaly == "High residue":
        readings["residue"] = round(random.uniform(6.0, 10.0), 2)

    return anomaly, readings


def generate_data():
    """Generate synthetic data with occasional anomalies."""
    equipment = random.choice(EQUIPMENT)
    readings = generate_normal_readings()
    anomaly_flag = False
    anomaly_type = None

    # Inject anomaly with 10% probability
    if random.random() < 0.1:
        anomaly_flag = True
        anomaly_type, readings = inject_anomaly(readings)

    data = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "equipment": equipment,
        "metrics": readings,
        "anomaly": anomaly_flag,
        "anomaly_type": anomaly_type
    }

    return data


if __name__ == "__main__":
    while True:
        datapoint = generate_data()
        print(json.dumps(datapoint, indent=2))
        time.sleep(1)

import os
import json
import random
import asyncio
from datetime import datetime, timezone
from typing import Dict

from dotenv import load_dotenv
from fastapi import APIRouter, WebSocket, Query
from starlette.websockets import WebSocketDisconnect
from cement_operations_optimization.data_generator.pubsub_push import connected_websockets
from google.cloud import pubsub_v1

# ----------------------------
# Router
# ----------------------------
router = APIRouter(tags=["Cement Data"])

# ----------------------------
# Config
# ----------------------------
load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "cement-operations-optimization")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "cement-raw")
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def publish_to_pubsub(record: Dict):
    """Publish record to Pub/Sub."""
    data = json.dumps(record).encode("utf-8")
    future = publisher.publish(topic_path, data)
    return future.result()

# ----------------------------
# Synthetic data generator
# ----------------------------
EQUIPMENT = ["Raw Mill", "Kiln", "Clinker Cooler", "Cement Mill", "Packing Plant"]
NORMAL_RANGES = {
    "temperature": (100, 1400),
    "pressure": (1, 5),
    "vibration": (0.1, 3.0),
    "power": (100, 5000),
    "emissions": (200, 800),
    "fineness": (280, 400),
    "residue": (0.5, 5.0),
}
ANOMALIES = [
    "Kiln temperature spike",
    "Clinker cooler fan failure",
    "Grinding motor overload",
    "Power supply dip",
    "Emission spike",
    "Low cement fineness",
    "High residue",
]

def generate_normal_readings():
    return {k: round(random.uniform(*rng), 2) for k, rng in NORMAL_RANGES.items()}

def inject_anomaly(readings: Dict):
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

def generate_record():
    equipment = random.choice(EQUIPMENT)
    readings = generate_normal_readings()
    anomaly_flag, anomaly_type = False, None
    if random.random() < 0.1:  # 10% anomaly chance
        anomaly_flag = True
        anomaly_type, readings = inject_anomaly(readings)
    ts = datetime.now(timezone.utc).isoformat()
    return {
        "timestamp": ts,
        "equipment": equipment,
        "metrics": readings,
        "anomaly": anomaly_flag,
        "anomaly_type": anomaly_type,
    }

# ----------------------------
# Router endpoints
# ----------------------------
@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "cement-data"}

@router.get("/latest")
async def get_latest():
    record = generate_record()
    publish_to_pubsub(record)
    return record

@router.get("/batch")
async def get_batch(size: int = Query(10, gt=0, le=1000)):
    records = [generate_record() for _ in range(size)]
    for r in records:
        publish_to_pubsub(r)
    return records

@router.websocket("/ws/data")
async def websocket_data(ws: WebSocket):
    await ws.accept()
    connected_websockets.add(ws)
    try:
        while True:
            await asyncio.sleep(60)
    except Exception:
        pass
    finally:
        try:
            connected_websockets.remove(ws)
        except Exception:
            pass

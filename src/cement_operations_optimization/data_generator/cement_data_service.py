# cement_data_service.py
import os
import json
import random
import asyncio
from datetime import datetime, timezone
from typing import Dict
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from fastapi import APIRouter, WebSocket, Query
from starlette.websockets import WebSocketDisconnect

# ----------------------------
# Router
# ----------------------------
router = APIRouter(tags=["Cement Data"])

# ----------------------------
# Config
# ----------------------------
load_dotenv()
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "cement_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
}
DB_EXECUTOR = ThreadPoolExecutor(max_workers=4)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

def create_table_sync():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cement_plant_data (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL,
        equipment TEXT NOT NULL,
        temperature DOUBLE PRECISION,
        pressure DOUBLE PRECISION,
        vibration DOUBLE PRECISION,
        power DOUBLE PRECISION,
        emissions DOUBLE PRECISION,
        fineness DOUBLE PRECISION,
        residue DOUBLE PRECISION,
        anomaly BOOLEAN,
        anomaly_type TEXT
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

def insert_record_sync(record: Dict):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cement_plant_data
        (timestamp, equipment, temperature, pressure, vibration, power,
         emissions, fineness, residue, anomaly, anomaly_type)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        record["timestamp"],
        record["equipment"],
        record["metrics"].get("temperature"),
        record["metrics"].get("pressure"),
        record["metrics"].get("vibration"),
        record["metrics"].get("power"),
        record["metrics"].get("emissions"),
        record["metrics"].get("fineness"),
        record["metrics"].get("residue"),
        record.get("anomaly", False),
        record.get("anomaly_type"),
    ))
    conn.commit()
    cur.close()
    conn.close()

async def run_db(fn, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(DB_EXECUTOR, lambda: fn(*args, **kwargs))

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
    if random.random() < 0.1:
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
@router.on_event("startup")
async def startup_event():
    await run_db(create_table_sync)

@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "cement-data"}

@router.get("/latest")
async def get_latest():
    record = generate_record()
    await run_db(insert_record_sync, record)
    return record

@router.get("/batch")
async def get_batch(size: int = Query(10, gt=0, le=1000)):
    records = [generate_record() for _ in range(size)]
    for r in records:
        await run_db(insert_record_sync, r)
    return records

@router.websocket("/ws/data")
async def websocket_data(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            record = generate_record()
            asyncio.create_task(run_db(insert_record_sync, record))
            await ws.send_text(json.dumps(record))
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        # Client disconnected, just exit the function
        pass
    except Exception:
        # Handle other exceptions if needed
        pass

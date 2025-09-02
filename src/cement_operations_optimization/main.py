from fastapi import FastAPI
from .db import get_db_connection
from .models.plantData import PlantData
from typing import List

app = FastAPI(title="Cement Plant AI API")

@app.get("/")
def home():
    return {"message": "Cement Plant API is running"}

@app.get("/data", response_model=List[PlantData])
def get_latest_data(limit: int = 10):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT timestamp, equipment, temperature, pressure, vibration, power, emission, anomaly, anomaly_type
        FROM cement_plant_data
        ORDER BY timestamp DESC
        LIMIT %s
    """, (limit,))
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

@app.get("/anomalies", response_model=List[PlantData])
def get_anomalies(limit: int = 10):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT timestamp, equipment, temperature, pressure, vibration, power, emission, anomaly, anomaly_type
        FROM cement_plant_data
        WHERE anomaly = TRUE
        ORDER BY timestamp DESC
        LIMIT %s
    """, (limit,))
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

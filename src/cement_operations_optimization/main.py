from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from .db import get_db_connection
from .models.plantData import PlantData
from typing import List
from .auth.main import router as auth_router
from .utils.auth import verify_token

app = FastAPI(title="Cement Plant AI API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can specify your frontend's origin instead of '*'
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/auth", tags=["Auth"])

@app.get("/")
def home():
    return {"message": "Cement Plant API is running"}

@app.get("/analytics")
def get_analytics(user: str = Depends(verify_token)):
    return {"user": user, "analytics": "Secure data only for logged-in users"}

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

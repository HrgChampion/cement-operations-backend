from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from .db import get_db_connection
from .models.plantData import PlantData
from typing import List
from .auth.main import router as auth_router
from .data_generator.cement_data_service import router as data_router
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

app.include_router(data_router)

@app.get("/")
def home():
    return {"message": "Cement Plant API is running"}



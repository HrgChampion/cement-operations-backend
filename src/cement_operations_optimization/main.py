from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from cement_operations_optimization.auth.main import router as auth_router
from cement_operations_optimization.data_generator.cement_data_service import router as data_router
import os
import uvicorn
from cement_operations_optimization.data_generator.main import router as predictions_router
from cement_operations_optimization.utils.alerts_service_async import router as alerts_router
from cement_operations_optimization.trends.trends import router as trends_router
from cement_operations_optimization.kpis.kpis import router as kpis_router

app = FastAPI(title="Cement Plant AI API")

origins = [
    "https://cement-operations-frontend.vercel.app",
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/auth", tags=["Auth"])

app.include_router(predictions_router, prefix="/ml", tags=["ML"])

app.include_router(data_router)
app.include_router(alerts_router)
app.include_router(trends_router)
app.include_router(kpis_router) 

@app.get("/")
def home():
    return {"message": "Cement Plant API is running"}



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)

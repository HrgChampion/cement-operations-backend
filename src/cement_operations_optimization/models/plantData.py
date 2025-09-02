from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class PlantData(BaseModel):
    timestamp: datetime
    equipment: str
    temperature: float
    pressure: float
    vibration: float
    power: float
    emission: float
    anomaly: bool
    anomaly_type: Optional[str]

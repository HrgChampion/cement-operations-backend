# routes/trends.py
import os
from fastapi import APIRouter, Query
from google.cloud import bigquery

router = APIRouter(tags=["Trends"])
bq_client = bigquery.Client()

PROJECT_ID = os.getenv("GCP_PROJECT", "cement-operations-optimization")
BQ_DATASET = "plant"
PREDICTIONS_TABLE = "cement_predictions"

@router.get("/trends")
def get_trends(
    equipment: str = Query(..., description="Equipment name"),
    hours: int = Query(2, description="Past hours to fetch")
):
    query = f"""
        SELECT
          seq_id,
          prediction_time,
          avg_temperature,
          avg_emissions,
          is_anomaly,
          anomaly_prob
        FROM `{PROJECT_ID}.{BQ_DATASET}.{PREDICTIONS_TABLE}`
        WHERE equipment = @equipment
          AND prediction_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
        ORDER BY prediction_time ASC
    """

    job = bq_client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("equipment", "STRING", equipment),
            bigquery.ScalarQueryParameter("hours", "INT64", hours),
        ]
    ))

    rows = list(job.result())
    return {"equipment": equipment, "data": [dict(r) for r in rows]}

import os
from fastapi import APIRouter
from google.cloud import bigquery

router = APIRouter(tags=["KPIs"])

PROJECT_ID = os.getenv("GCP_PROJECT", "cement-operations-optimization")
BQ_DATASET = "plant"
PREDICTIONS_TABLE = "cement_predictions"

bq_client = bigquery.Client()

@router.get("/kpis")
def get_kpis():
    """
    Aggregate key KPIs from BigQuery and return JSON for dashboard
    """

    query = f"""
    WITH base AS (
        SELECT
            TIMESTAMP_TRUNC(prediction_time, HOUR) AS hour_bucket,
            AVG(avg_power) AS avg_power,
            AVG(avg_temperature) AS avg_temp,
            AVG(avg_emissions) AS avg_emissions,
            AVG(avg_fineness) AS avg_fineness,
            AVG(avg_residue) AS avg_residue,
            AVG(is_anomaly) AS anomaly_rate
        FROM `{PROJECT_ID}.{BQ_DATASET}.{PREDICTIONS_TABLE}`
        WHERE prediction_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        GROUP BY hour_bucket
        ORDER BY hour_bucket DESC
        LIMIT 168
    )
    SELECT
        AVG(avg_power) AS see,
        AVG(avg_temp) AS ste,
        AVG(avg_emissions) AS co2_per_ton,
        AVG(avg_fineness) AS blaine,
        AVG(avg_residue) AS residue,
        AVG(anomaly_rate) * 100 AS out_of_spec_pct
    FROM base
    """

    job = bq_client.query(query)
    row = list(job.result())[0]

    return {
        "energy": {
            "see": row["see"],
            "ste": row["ste"]
        },
        "quality": {
            "blaine": row["blaine"],
            "residue": row["residue"],
            "out_of_spec_pct": row["out_of_spec_pct"]
        },
        "sustainability": {
            "co2_per_ton": row["co2_per_ton"]
        },
        "stability": {
            "kiln_stops": 0,   
            "alarm_minutes": 0 
        }
    }

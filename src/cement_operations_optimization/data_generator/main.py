import base64
import json
import os
from google.cloud import bigquery
from fastapi import APIRouter

BQ_PROJECT = "cement-operations-optimization"
BQ_DATASET = os.getenv("BQ_DATASET", "plant")
BQ_TABLE = os.getenv("BQ_TABLE", "cement_raw")

client = bigquery.Client(project=BQ_PROJECT)
table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)
router = APIRouter()

def pubsub_to_bq(event, context):
    """Triggered by Pub/Sub message → Insert into BigQuery."""
    payload = base64.b64decode(event["data"]).decode("utf-8")
    row = json.loads(payload)

    row_to_insert = {
        "timestamp": row["timestamp"],
        "equipment": row["equipment"],
        "temperature": row["metrics"]["temperature"],
        "pressure": row["metrics"]["pressure"],
        "vibration": row["metrics"]["vibration"],
        "power": row["metrics"]["power"],
        "emissions": row["metrics"]["emissions"],
        "fineness": row["metrics"]["fineness"],
        "residue": row["metrics"]["residue"],
        "anomaly": row.get("anomaly", False),
        "anomaly_type": row.get("anomaly_type", None),
    }

    errors = client.insert_rows_json(table_ref, [row_to_insert])
    if errors:
        print("BigQuery insert errors:", errors)


@router.get("/predictions")
def get_predictions(limit: int = 50):
    """Fetch latest anomaly predictions from BigQuery."""
    query = f"""
        SELECT *
        FROM `cement-operations-optimization.plant.cement_predictions`
        ORDER BY prediction_time DESC
        LIMIT {limit}
    """
    query_job = client.query(query)
    rows = [dict(row) for row in query_job]
    return {"predictions": rows}

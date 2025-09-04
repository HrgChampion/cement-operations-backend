import base64
import json
from google.cloud import bigquery

BQ_TABLE = "cement-operations-optimization.plant.cement_plant_data"
bq = bigquery.Client()

def pubsub_to_bq(event, context):
    """Triggered by Pub/Sub; writes row to BigQuery"""
    if 'data' not in event:
        return

    payload = base64.b64decode(event['data']).decode('utf-8')
    record = json.loads(payload)

    row = {
        "timestamp": record.get("timestamp"),
        "equipment": record.get("equipment"),
        "temperature": record.get("temperature"),
        "pressure": record.get("pressure"),
        "vibration": record.get("vibration"),
        "power": record.get("power"),
        "emission": record.get("emission"),
        "anomaly": record.get("anomaly"),
        "anomaly_type": record.get("anomaly_type")
    }

    errors = bq.insert_rows_json(BQ_TABLE, [row])
    if errors:
        print("BQ insert errors:", errors)

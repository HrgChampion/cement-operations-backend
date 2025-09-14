import os
import json
from google.cloud import aiplatform
from google.cloud import bigquery, pubsub_v1

# Config
PROJECT_ID = os.getenv("GCP_PROJECT")
LOCATION = "us-central1"  # change if needed
ENDPOINT_ID = os.getenv("VERTEX_ENDPOINT_ID")
BQ_DATASET = "cement_ai"
PREDICTIONS_TABLE = "cement_predictions"
ALERTS_TOPIC = "cement-alerts"

bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()
endpoint = aiplatform.Endpoint(endpoint_name=f"projects/{PROJECT_ID}/locations/{LOCATION}/endpoints/{ENDPOINT_ID}")

def predict_and_store(event, context):
    """Triggered by Pub/Sub event with enriched features"""
    payload = json.loads(base64.b64decode(event["data"]).decode("utf-8"))

    # Prepare features for model
    instance = {
        "avg_temperature": payload["avg_temperature"],
        "avg_pressure": payload["avg_pressure"],
        "avg_vibration": payload["avg_vibration"],
        "avg_power": payload["avg_power"],
        "avg_emissions": payload["avg_emissions"],
        "avg_fineness": payload["avg_fineness"],
        "avg_residue": payload["avg_residue"],
        "temp_lag_1h": payload.get("temp_lag_1h", 0),
        "emissions_lag_1h": payload.get("emissions_lag_1h", 0),
        "temp_roll_3h": payload.get("temp_roll_3h", 0),
        "emissions_trend_3h": payload.get("emissions_trend_3h", 0),
    }

    # Call Vertex AI endpoint
    prediction = endpoint.predict(instances=[instance])
    is_anomaly = int(prediction.predictions[0]["is_anomaly"])
    anomaly_prob = prediction.predictions[0]["anomaly_prob"]

    # Save to BigQuery
    row = {
        "seq_id": payload["seq_id"],
        "equipment": payload["equipment"],
        **instance,
        "is_anomaly": is_anomaly,
        "anomaly_prob": anomaly_prob,
        "prediction_time": payload["hour_bucket"]
    }

    errors = bq_client.insert_rows_json(f"{PROJECT_ID}.{BQ_DATASET}.{PREDICTIONS_TABLE}", [row])
    if errors:
        print(f"BigQuery errors: {errors}")

    # If anomaly, publish alert
    if is_anomaly:
        alert_msg = json.dumps({"equipment": payload["equipment"], "prob": anomaly_prob})
        publisher.publish(f"projects/{PROJECT_ID}/topics/{ALERTS_TOPIC}", alert_msg.encode("utf-8"))

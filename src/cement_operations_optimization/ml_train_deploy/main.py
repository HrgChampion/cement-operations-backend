import os
import json
import base64
import joblib
import numpy as np
from google.cloud import aiplatform
from google.cloud import bigquery, pubsub_v1

# Config
PROJECT_ID = os.getenv("GCP_PROJECT","cement-operations-optimization")
LOCATION = os.getenv("VERTEX_REGION", "us-central1")  # default region
ENDPOINT_ID = os.getenv("2269422786055241728")
BQ_DATASET = "plant"
PREDICTIONS_TABLE = "cement_predictions"
ALERTS_TOPIC = "cement-alerts"

# Clients
bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()

# Try Vertex endpoint if configured
endpoint = None
if ENDPOINT_ID:
    try:
        endpoint = aiplatform.Endpoint(
            endpoint_name=f"projects/{PROJECT_ID}/locations/{LOCATION}/endpoints/{ENDPOINT_ID}"
        )
        print(f"Using Vertex endpoint: {ENDPOINT_ID}")
    except Exception as e:
        print(f"Could not init Vertex endpoint: {e}")

# Local fallback model
LOCAL_MODEL_PATH = "cement_xgb_model.pkl"
local_model = None
if os.path.exists(LOCAL_MODEL_PATH):
    try:
        local_model = joblib.load(LOCAL_MODEL_PATH)
        print(f"Loaded local model from {LOCAL_MODEL_PATH}")
    except Exception as e:
        print(f"Error loading local model: {e}")


def run_prediction(instance: dict):
    """Run prediction via Vertex AI if available, else local model."""
    # Try Vertex AI first
    if endpoint:
        try:
            prediction = endpoint.predict(instances=[instance])
            is_anomaly = int(prediction.predictions[0]["is_anomaly"])
            anomaly_prob = float(prediction.predictions[0]["anomaly_prob"])
            return is_anomaly, anomaly_prob
        except Exception as e:
            print(f"Vertex AI failed: {e}")

    # Fallback to local model
    if local_model:
        X = np.array(list(instance.values())).reshape(1, -1)
        pred = local_model.predict(X)
        prob = (
            local_model.predict_proba(X)[:, 1]
            if hasattr(local_model, "predict_proba")
            else [None]
        )
        return int(pred[0]), float(prob[0]) if prob[0] is not None else None

    raise RuntimeError("No model available (Vertex or local).")


def predict_and_store(event, context):
    """Triggered by Pub/Sub event with enriched features."""
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

    # Run prediction (Vertex AI or local fallback)
    is_anomaly, anomaly_prob = run_prediction(instance)

    # Save to BigQuery
    row = {
        "seq_id": payload["seq_id"],
        "equipment": payload["equipment"],
        **instance,
        "is_anomaly": is_anomaly,
        "anomaly_prob": anomaly_prob,
        "prediction_time": payload["hour_bucket"],
    }

    errors = bq_client.insert_rows_json(
        f"{PROJECT_ID}.{BQ_DATASET}.{PREDICTIONS_TABLE}", [row]
    )
    if errors:
        print(f"BigQuery insert errors: {errors}")

    # If anomaly, publish alert
    if is_anomaly:
        alert_msg = json.dumps(
            {"equipment": payload["equipment"], "prob": anomaly_prob}
        )
        topic_path = f"projects/{PROJECT_ID}/topics/{ALERTS_TOPIC}"
        publisher.publish(topic_path, alert_msg.encode("utf-8"))
        print(f"Published anomaly alert: {alert_msg}")

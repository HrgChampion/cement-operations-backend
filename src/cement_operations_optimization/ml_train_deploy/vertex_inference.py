import os
import json
import base64
from google.cloud import aiplatform
from google.cloud import bigquery, pubsub_v1

# Config
PROJECT_ID = os.getenv("GCP_PROJECT")
LOCATION = os.getenv("VERTEX_LOCATION", "asia-south1")
ENDPOINT_ID = os.getenv("VERTEX_ENDPOINT_ID","1436749436200943616")
BQ_DATASET = os.getenv("BQ_DATASET_PRED", "cement_ai")
PREDICTIONS_TABLE = os.getenv("PREDICTIONS_TABLE", "cement_predictions")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "cement-alerts")
ANOMALY_THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "0.5"))

bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()


FEATURE_KEYS = [
    "avg_temperature", "avg_pressure", "avg_vibration", "avg_power", "avg_emissions",
    "avg_fineness", "avg_residue",
    "temp_lag_1h", "temp_lag_2h", "emissions_lag_1h", "emissions_lag_2h",
    "temp_roll_3h", "temp_roll_6h", "emissions_roll_3h",
    "temp_trend_3h", "emissions_trend_3h",
]


def _get_endpoint():
    if not (PROJECT_ID and ENDPOINT_ID):
        raise RuntimeError("GCP_PROJECT and VERTEX_ENDPOINT_ID must be set")
    return aiplatform.Endpoint(endpoint_name=f"projects/{PROJECT_ID}/locations/{LOCATION}/endpoints/{ENDPOINT_ID}")


def predict_and_store(event, context):
    """Triggered by Pub/Sub event with enriched features"""
    payload = json.loads(base64.b64decode(event["data"]).decode("utf-8"))

    # Prepare features for model (fill missing as 0.0)
    instance = {k: float(payload.get(k, 0.0)) for k in FEATURE_KEYS}

    # Call Vertex AI endpoint
    endpoint = _get_endpoint()
    prediction = endpoint.predict(instances=[instance])

    # Interpret prediction output from sklearn prebuilt container
    # Typically returns a list of arrays or floats; we take the first value as anomaly probability
    pred0 = prediction.predictions[0]
    if isinstance(pred0, list):
        anomaly_prob = float(pred0[0]) if pred0 else 0.0
    else:
        try:
            anomaly_prob = float(pred0)
        except Exception:
            anomaly_prob = 0.0

    is_anomaly = int(anomaly_prob >= ANOMALY_THRESHOLD)

    # Save to BigQuery
    row = {
        "seq_id": payload.get("seq_id"),
        "equipment": payload.get("equipment"),
        **instance,
        "is_anomaly": is_anomaly,
        "anomaly_prob": anomaly_prob,
        "prediction_time": payload.get("hour_bucket"),
    }

    errors = bq_client.insert_rows_json(f"{PROJECT_ID}.{BQ_DATASET}.{PREDICTIONS_TABLE}", [row])
    if errors:
        print(f"BigQuery errors: {errors}")

    # If anomaly, publish alert
    if is_anomaly:
        alert_msg = json.dumps({"equipment": payload.get("equipment"), "prob": anomaly_prob})
        publisher.publish(f"projects/{PROJECT_ID}/topics/{ALERTS_TOPIC}", alert_msg.encode("utf-8"))

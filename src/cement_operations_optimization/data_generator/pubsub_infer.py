# pubsub_infer.py
import base64
import json
import os
from google.cloud import pubsub_v1, bigquery, aiplatform
from google.cloud.aiplatform.gapic import PredictionServiceClient
from google.cloud.aiplatform.gapic.types import PredictRequest, Value
from google.protobuf import struct_pb2
from datetime import datetime, timezone

# ENV (set these in Cloud Function)
PROJECT = os.getenv("GCP_PROJECT", "cement-operations-optimization")
REGION = os.getenv("GCP_LOCATION", "asia-south1")
BQ_DATASET = os.getenv("BQ_DATASET", "plant")
BQ_PREDICTIONS_TABLE = os.getenv("BQ_PREDICTIONS_TABLE", "cement_predictions")
VERTEX_ENDPOINT = os.getenv("VERTEX_ENDPOINT")  # e.g. "projects/123/locations/asia-south1/endpoints/456"
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "cement-alerts")
ANOMALY_THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "0.8"))

# Clients
bq = bigquery.Client(project=PROJECT)
publisher = pubsub_v1.PublisherClient()
alert_topic_path = publisher.topic_path(PROJECT, ALERT_TOPIC)

# Vertex client (PredictionServiceClient)
client = PredictionServiceClient(client_options={"api_endpoint": f"{REGION}-aiplatform.googleapis.com"})

def parse_pubsub_event(event):
    data_b64 = event.get("data", "")
    if not data_b64:
        return None
    payload = base64.b64decode(data_b64).decode("utf-8")
    return json.loads(payload)

def build_instance_from_record(record: dict):
    """
    Convert your feature dict to the prediction instance format.
    Vertex models may require a list order or dict depending on how trained/served.
    Adjust fields to match your model's expected input.
    """
    # If your model expects a dict of named features:
    instance = struct_pb2.Value()
    instance_struct = instance.struct_value
    # flatten metrics into top-level keys
    metrics = record.get("metrics", {})
    for k,v in metrics.items():
        # ensure types are numeric if required
        try:
            instance_struct[k] = float(v) if v is not None else None
        except Exception:
            instance_struct[k] = v
    # optionally add equipment as feature (one-hot or embedding handled in model)
    instance_struct["equipment"] = record.get("equipment")
    return instance

def call_vertex_predict(instance):
    """
    Call Vertex AI endpoint. Returns raw prediction response.
    """
    request = {
        "endpoint": VERTEX_ENDPOINT,
        "instances": [instance],
        "parameters": {},  # optional
    }
    response = client.predict(request=request)
    return response

def write_prediction_to_bq(record, prediction_result, anomaly_prob, is_anomaly):
    table_id = f"{PROJECT}.{BQ_DATASET}.{BQ_PREDICTIONS_TABLE}"
    row = {
        "timestamp": record.get("timestamp", datetime.now(timezone.utc).isoformat()),
        "equipment": record.get("equipment"),
        # include feature aggregates if available in record
        "avg_temperature": record.get("metrics", {}).get("temperature"),
        "avg_pressure": record.get("metrics", {}).get("pressure"),
        "avg_vibration": record.get("metrics", {}).get("vibration"),
        "avg_power": record.get("metrics", {}).get("power"),
        "avg_emissions": record.get("metrics", {}).get("emissions"),
        "avg_fineness": record.get("metrics", {}).get("fineness"),
        "avg_residue": record.get("metrics", {}).get("residue"),
        "is_anomaly": int(is_anomaly),
        "anomaly_prob": float(anomaly_prob),
        "prediction_raw": json.dumps(json.loads(prediction_result._pb.SerializeToString().hex()) if hasattr(prediction_result, "_pb") else str(prediction_result)),
        "ingest_time": datetime.now(timezone.utc).isoformat()
    }
    errors = bq.insert_rows_json(table_id, [row])
    if errors:
        print("BQ insert errors:", errors)

def publish_alert(record, anomaly_prob, prediction_payload):
    alert = {
        "timestamp": record.get("timestamp"),
        "equipment": record.get("equipment"),
        "anomaly_prob": anomaly_prob,
        "prediction": prediction_payload,
        "anomaly": anomaly_prob >= ANOMALY_THRESHOLD
    }
    publisher.publish(alert_topic_path, json.dumps(alert).encode("utf-8"))

def pubsub_infer(event, context):
    """
    Cloud Function entry point triggered by Pub/Sub (cement-raw).
    """
    try:
        record = parse_pubsub_event(event)
        if not record:
            print("No data in event")
            return

        # Build instance for Vertex
        instance = build_instance_from_record(record)

        # Call Vertex (instances should be JSON-serializable Python dicts)
        vertex_response = call_vertex_predict(instance)

        # Interpret vertex_response. The exact parsing depends on your model server.
        # For a typical classification returning "predictions": [[prob0, prob1]], inspect:
        predictions = vertex_response.predictions  # list
        # attempt to extract anomaly prob (assume prob-of-1 at index 1)
        anomaly_prob = None
        try:
            # if predictions[0] is a list of probs
            p0 = predictions[0]
            if isinstance(p0, list) or isinstance(p0, tuple):
                anomaly_prob = float(p0[-1])  # assume last is positive class
            elif isinstance(p0, dict) and "probabilities" in p0:
                anomaly_prob = float(p0["probabilities"][-1])
            elif isinstance(p0, dict) and "scores" in p0:
                anomaly_prob = float(p0["scores"][-1])
            else:
                anomaly_prob = float(p0)  # fallback
        except Exception as e:
            print("Failed to parse prediction", e, predictions)
            anomaly_prob = 0.0

        is_anomaly = anomaly_prob >= ANOMALY_THRESHOLD

        # Write to BigQuery
        write_prediction_to_bq(record, vertex_response, anomaly_prob, is_anomaly)

        # Publish alert if anomalous (or publish every prediction if you prefer)
        publish_alert(record, anomaly_prob, {"predictions": predictions})

        print("Processed record", record.get("timestamp"), "anomaly_prob", anomaly_prob)

    except Exception as e:
        print("Error in pubsub_infer:", str(e))
        raise

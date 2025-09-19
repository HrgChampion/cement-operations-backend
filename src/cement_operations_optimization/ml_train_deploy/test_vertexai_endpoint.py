import argparse
import json
import google.auth
from google.cloud import aiplatform

LOCATION = "asia-south1"
TEST_JSON_PATH = "test.json"


def _get_project_id() -> str:
    creds, project_id = google.auth.default()
    if not project_id:
        raise SystemExit(
            "No GCP project found from default credentials. Run 'gcloud auth application-default login' and 'gcloud config set project <PROJECT_ID>'."
        )
    return project_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint",default="1436749436200943616", required=True, help="Vertex AI Endpoint ID")
    parser.add_argument("--location", default=LOCATION, help="Vertex AI region")
    parser.add_argument("--instances", default=TEST_JSON_PATH, help="Path to JSON file with 'instances'")
    args = parser.parse_args()

    project_id = _get_project_id()

    with open(args.instances, "r") as f:
        payload = json.load(f)
    instances = payload.get("instances") or payload

    aiplatform.init(project=project_id, location=args.location)
    endpoint = aiplatform.Endpoint(endpoint_name=f"projects/{project_id}/locations/{args.location}/endpoints/{args.endpoint}")

    print("Sending prediction request…")
    prediction = endpoint.predict(instances=instances)

    print("Raw predictions:")
    print(prediction.predictions)
    print("Deployed model ID(s):", prediction.deployed_model_id)


if __name__ == "__main__":
    main()

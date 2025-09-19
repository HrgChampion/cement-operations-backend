from google.cloud import aiplatform
import google.auth
from google.api_core.exceptions import GoogleAPICallError

LOCATION = "asia-south1"
GCS_BUCKET = "cement-ops-models"
MODEL_ARTIFACT_URI = f"gs://{GCS_BUCKET}/models"
MODEL_DISPLAY_NAME = "cement-sklearn-lr"
ENDPOINT_DISPLAY_NAME = "cement-anomaly-endpoint"
MACHINE_TYPE = "n1-standard-2"
MIN_REPLICAS = 1
MAX_REPLICAS = 1
DEPLOY_TIMEOUT_SECONDS = 1800
FORCE_UNDEPLOY_EXISTING = True


def _pick_sklearn_image() -> str:
    try:
        import sklearn  # type: ignore
        ver = sklearn.__version__.split(".")
        major, minor = int(ver[0]), int(ver[1])
    except Exception:
        return "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-4:latest"

    if major == 1 and minor >= 5:
        track = "1-5"
    elif major == 1 and minor == 4:
        track = "1-4"
    elif major == 1 and minor == 3:
        track = "1-3"
    elif major == 1 and minor == 2:
        track = "1-2"
    else:
        track = "1-4"

    image = f"us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.{track}:latest"
    print(f"Using sklearn serving image: {image}")
    return image


def _get_project_id() -> str:
    creds, project_id = google.auth.default()
    if not project_id:
        raise SystemExit(
            "No GCP project found from default credentials. Run 'gcloud auth application-default login' and 'gcloud config set project <PROJECT_ID>'."
        )
    return project_id


def get_or_create_endpoint(display_name: str) -> aiplatform.Endpoint:
    endpoints = aiplatform.Endpoint.list(filter=f'display_name="{display_name}"')
    if endpoints:
        print(f"Using existing endpoint: {endpoints[0].resource_name}")
        return endpoints[0]
    print(f"Creating endpoint: {display_name}")
    return aiplatform.Endpoint.create(display_name=display_name)


def _undeploy_all(endpoint: aiplatform.Endpoint):
    deployed = endpoint.list_models()
    if not deployed:
        return
    for dm in deployed:
        try:
            print(f"Undeploying model id={dm.id} display_name={dm.display_name}…")
            endpoint.undeploy(deployed_model_id=dm.id)
        except GoogleAPICallError as e:
            print(f"Warning: undeploy failed for {dm.id}: {e}")


def main():
    project_id = _get_project_id()

    print("Initializing Vertex AI SDK…")
    aiplatform.init(project=project_id, location=LOCATION, staging_bucket=f"gs://{GCS_BUCKET}")

    serving_image = _pick_sklearn_image()

    print("Uploading model from:", MODEL_ARTIFACT_URI)
    model = aiplatform.Model.upload(
        display_name=MODEL_DISPLAY_NAME,
        artifact_uri=MODEL_ARTIFACT_URI,
        serving_container_image_uri=serving_image,
    )
    model.wait()
    print("Model uploaded:", model.resource_name)

    endpoint = get_or_create_endpoint(ENDPOINT_DISPLAY_NAME)

    if FORCE_UNDEPLOY_EXISTING:
        print("Cleaning endpoint: undeploying existing models (if any)…")
        _undeploy_all(endpoint)

    print("Deploying model to endpoint…")
    try:
        model.deploy(
            endpoint=endpoint,
            machine_type=MACHINE_TYPE,
            min_replica_count=MIN_REPLICAS,
            max_replica_count=MAX_REPLICAS,
            traffic_split={"0": 100},
        )
    except GoogleAPICallError as e:
        print("Deploy call failed:", e)
        raise

    endpoint = aiplatform.Endpoint(endpoint.resource_name)
    endpoint_id = endpoint.resource_name.split("/")[-1]
    print("✅ Deployed to endpoint:", endpoint.resource_name)
    print("ENDPOINT_ID:", endpoint_id)


if __name__ == "__main__":
    main()

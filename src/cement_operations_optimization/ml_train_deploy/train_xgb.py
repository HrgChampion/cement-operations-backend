import os
from google.cloud import bigquery, storage
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction import DictVectorizer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
import joblib

# CONFIG
PROJECT = os.getenv("GCP_PROJECT", "cement-operations-optimization")
BQ_DATASET = os.getenv("BQ_DATASET", "plant")
BQ_TABLE = os.getenv("BQ_FEATURES_TABLE", "cement_features_enriched")
GCS_BUCKET = os.getenv("GCS_BUCKET", "cement-ops-models")
LOCATION = os.getenv("VERTEX_LOCATION", "asia-south1")

MODEL_JOBLIB_PATH = "models/model.joblib"

FEATURE_COLS = [
    "avg_temperature", "avg_pressure", "avg_vibration", "avg_power", "avg_emissions",
    "avg_fineness", "avg_residue",
    "temp_lag_1h", "temp_lag_2h", "emissions_lag_1h", "emissions_lag_2h",
    "temp_roll_3h", "temp_roll_6h", "emissions_roll_3h",
    "temp_trend_3h", "emissions_trend_3h"
]
LABEL_COL = "anomaly_label"

def load_from_bigquery():
    client = bigquery.Client(project=PROJECT)
    sql = f"""
    SELECT {', '.join(FEATURE_COLS + [LABEL_COL])}
    FROM `{PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
    WHERE {LABEL_COL} IS NOT NULL
    """
    return client.query(sql).to_dataframe()

def upload_to_gcs(local_path, bucket_name, dest_path):
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name, location=LOCATION)
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded to gs://{bucket_name}/{dest_path}")


def build_pipeline():
    clf = Pipeline(steps=[
        ("dictvec", DictVectorizer(sparse=False)),
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("lr", LogisticRegression(max_iter=500, class_weight="balanced", solver="lbfgs")),
    ])
    return clf


def main():
    print("📥 Loading data from BigQuery…")
    df = load_from_bigquery()
    print("Rows:", len(df))

    if df.empty:
        raise ValueError("❌ No data returned from BigQuery")

    X_records = df[FEATURE_COLS].to_dict(orient="records")
    y = df[LABEL_COL].astype(int)

    if len(df) < 20 or len(y.unique()) < 2:
        X_train, X_test, y_train, y_test = X_records, X_records, y, y
    else:
        X_train, X_test, y_train, y_test = train_test_split(
            X_records, y, test_size=0.2, random_state=42, stratify=y
        )

    print("🛠️ Training scikit-learn LogisticRegression pipeline…")
    model = build_pipeline()
    model.fit(X_train, y_train)

    try:
        import numpy as np
        preds_proba = model.predict_proba(X_test)[:, 1]
        preds_binary = (preds_proba >= 0.5).astype(int)
        auc = roc_auc_score(y_test, preds_proba)
        print("ROC AUC:", auc)
        print(classification_report(y_test, preds_binary))
    except Exception as e:
        print("⚠️ Metrics skipped:", e)

    os.makedirs("tmp", exist_ok=True)
    local_joblib = "tmp/model.joblib"
    joblib.dump(model, local_joblib)
    joblib.dump(model, "cement_sklearn_model.pkl")

    upload_to_gcs(local_joblib, GCS_BUCKET.replace("gs://", ""), MODEL_JOBLIB_PATH)

    print("✅ Model saved (sklearn pipeline):")
    print(f"   - Joblib: gs://{GCS_BUCKET}/{MODEL_JOBLIB_PATH}")

    print("\n📊 Model Information:")
    print(f"   Model type: {type(model)}")
    try:
        feature_names = model.named_steps["dictvec"].feature_names_
    except Exception:
        feature_names = None
    print(f"   Feature names: {feature_names if feature_names else 'Not available'}")


if __name__ == "__main__":
    main()
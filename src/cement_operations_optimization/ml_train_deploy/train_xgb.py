# ml/train_xgb.py
import os
import pandas as pd
from google.cloud import bigquery, storage
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
import xgboost as xgb
import joblib

# CONFIG
PROJECT = os.getenv("GCP_PROJECT", "cement-operations-optimization")
BQ_DATASET = os.getenv("BQ_DATASET", "plant")
BQ_TABLE = os.getenv("BQ_FEATURES_TABLE", "cement_features_enriched")
GCS_BUCKET = os.getenv("GCS_BUCKET", "cement-ops-models")

MODEL_JOBLIB_PATH = "models/model.joblib"
MODEL_JSON_PATH = "models/model.json"

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
        bucket = storage_client.create_bucket(bucket_name, location="asia-south1")
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded to gs://{bucket_name}/{dest_path}")

def main():
    print("📥 Loading data from BigQuery...")
    df = load_from_bigquery()
    print("Rows:", len(df))

    if df.empty:
        raise ValueError("❌ No data returned from BigQuery")

    X = df[FEATURE_COLS]
    y = df[LABEL_COL].astype(int)

    if len(df) < 20 or len(y.unique()) < 2:
        X_train, X_test, y_train, y_test = X, X, y, y
    else:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

    dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=FEATURE_COLS)
    dtest = xgb.DMatrix(X_test, label=y_test, feature_names=FEATURE_COLS)

    params = {
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 6,
        "eta": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "seed": 42
    }

    print("🛠️ Training XGBoost model...")
    bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=200, evals=[(dtrain, "train"), (dtest, "eval")], verbose_eval=True)

    preds = bst.predict(dtest)
    try:
        auc = roc_auc_score(y_test, preds)
        print("ROC AUC:", auc)
        print(classification_report(y_test, (preds > 0.5).astype(int)))
    except Exception as e:
        print("⚠️ Metrics skipped:", e)

    os.makedirs("tmp", exist_ok=True)

    # Save JSON for Vertex AI
    local_json = "tmp/model.json"
    bst.save_model(local_json)
    upload_to_gcs(local_json, GCS_BUCKET.replace("gs://",""), MODEL_JSON_PATH)

    # Save joblib for local testing
    local_joblib = "tmp/model.joblib"
    joblib.dump(bst, local_joblib)
    joblib.dump(model, "cement_xgb_model.pkl")
    upload_to_gcs(local_joblib, GCS_BUCKET.replace("gs://",""), MODEL_JOBLIB_PATH)

    print("✅ Models saved:")
    print(f"   - Joblib: gs://{GCS_BUCKET}/{MODEL_JOBLIB_PATH}")
    print(f"   - JSON:   gs://{GCS_BUCKET}/{MODEL_JSON_PATH}")

if __name__ == "__main__":
    main()

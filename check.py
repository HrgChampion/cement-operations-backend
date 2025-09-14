import joblib
import json
import pandas as pd
import xgboost as xgb
import os

# ----------------------
# CONFIG
# ----------------------
MODEL_PATH = "tmp/model.joblib"  # path to local trained model
TEST_JSON_PATH = "/home/himanshu/Desktop/cement-operations-backend/test.json"  # path to test JSON

FEATURE_COLS = [
    "avg_temperature", "avg_pressure", "avg_vibration", "avg_power", "avg_emissions",
    "avg_fineness", "avg_residue",
    "temp_lag_1h", "temp_lag_2h", "emissions_lag_1h", "emissions_lag_2h",
    "temp_roll_3h", "temp_roll_6h", "emissions_roll_3h",
    "temp_trend_3h", "emissions_trend_3h"
]

# ----------------------
# LOAD MODEL
# ----------------------
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"Model not found at {MODEL_PATH}")

model = joblib.load(MODEL_PATH)
print("✅ Model loaded successfully.")

# ----------------------
# LOAD TEST JSON
# ----------------------
if not os.path.exists(TEST_JSON_PATH):
    raise FileNotFoundError(f"Test JSON not found at {TEST_JSON_PATH}")

with open(TEST_JSON_PATH, "r") as f:
    test_data = json.load(f)

if "instances" not in test_data:
    raise ValueError("test.json must contain 'instances' key.")

instances = test_data["instances"]

# Convert to DataFrame
df = pd.DataFrame(instances)

# Ensure all FEATURE_COLS are present and in correct order
missing_cols = [col for col in FEATURE_COLS if col not in df.columns]
if missing_cols:
    raise ValueError(f"Missing required columns in test JSON: {missing_cols}")

df = df[FEATURE_COLS]

print(f"✅ Test data loaded. Shape: {df.shape}")
print(df.head())

# ----------------------
# PREDICT
# ----------------------
try:
    # Convert to DMatrix for Booster
    dmatrix = xgb.DMatrix(df, feature_names=FEATURE_COLS)
    predictions = model.predict(dmatrix)

    print("\n--- Predictions ---")
    print(predictions)

except Exception as e:
    print("❌ Error during prediction:", e)

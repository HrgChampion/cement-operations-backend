import json
import os

STANDARDS_PATH = "./cement_standards.json"

def load_standards():
    with open(STANDARDS_PATH, "r") as f:
        return json.load(f)

# Cache at import
STANDARDS = load_standards()

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    if os.getenv("K_SERVICE"):  
        # Running in Cloud Run
        DB_CONFIG = {
            'dbname': os.getenv("POSTGRES_DB", "cement-postgres"),
            'user': os.getenv("POSTGRES_USER", "cementuser"),
            'password': os.getenv("POSTGRES_PASSWORD"),
            'host': f"/cloudsql/{os.getenv('INSTANCE_CONNECTION_NAME')}"
        }
    else:
        # Running locally (fall back to TCP host)
        DB_CONFIG = {
            'dbname': os.getenv("POSTGRES_DB", "cement-postgres"),
            'user': os.getenv("POSTGRES_USER", "cementuser"),
            'password': os.getenv("POSTGRES_PASSWORD"),
            'host': os.getenv("POSTGRES_HOST", "localhost"),
            'port': int(os.getenv("POSTGRES_PORT", 5432))
        }

    conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    return conn

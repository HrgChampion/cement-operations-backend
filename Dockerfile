FROM python:3.11-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libpq-dev \
    curl \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Upgrade pip first
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install each package separately to avoid resolver backtracking
RUN pip install --no-cache-dir flask-socketio>=5.5.1
RUN pip install --no-cache-dir grpcio==1.64.1
RUN pip install --no-cache-dir grpcio-status==1.64.1
RUN pip install --no-cache-dir flask>=3.1.2
RUN pip install --no-cache-dir psycopg2-binary>=2.9.10
RUN pip install --no-cache-dir google-genai>=1.32.0
RUN pip install --no-cache-dir google-cloud-bigquery>=3.36.0
RUN pip install --no-cache-dir firebase-admin>=7.1.0
RUN pip install --no-cache-dir google-cloud-storage>=3.3.1
RUN pip install --no-cache-dir google-cloud-vision>=3.10.2
RUN pip install --no-cache-dir fastapi>=0.110.0
RUN pip install --no-cache-dir uvicorn>=0.29.0
RUN pip install --no-cache-dir pandas>=2.3.2
RUN pip install --no-cache-dir python-dotenv>=1.0.0
RUN pip install --no-cache-dir dotenv>=0.9.9
RUN pip install --no-cache-dir google-cloud-pubsub>=2.31.1
RUN pip install --no-cache-dir bcrypt>=4.3.0
RUN pip install --no-cache-dir passlib>=1.7.4
RUN pip install --no-cache-dir python-jose>=3.5.0
RUN pip install --no-cache-dir 'pydantic[email]>=1.10.0'
RUN pip install --no-cache-dir scikit-learn>=1.7.2
RUN pip install --no-cache-dir xgboost==1.6.0
RUN pip install --no-cache-dir joblib>=1.5.2
RUN pip install --no-cache-dir pyarrow>=21.0.0
RUN pip install --no-cache-dir google-cloud-aiplatform
RUN pip install --no-cache-dir google-auth
RUN pip install --no-cache-dir 'google-cloud-pubsub[asyncio]'

# Copy application
COPY src/ /app

ENV PYTHONPATH=/app

EXPOSE 8080
CMD ["uvicorn", "cement_operations_optimization.main:app", "--host", "0.0.0.0", "--port", "8080"]

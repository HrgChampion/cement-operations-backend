# Use Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/cement_operations_optimization/ /app

# Expose Cloud Run port
EXPOSE 8080

# Run FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

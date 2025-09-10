gcloud builds submit --tag gcr.io/cement-operations-optimization/cement-operations-backend .


gcloud run deploy cement-operations-backend \
  --image gcr.io/cement-operations-optimization/cement-operations-backend \
  --platform managed \
  --region asia-south1 \
  --allow-unauthenticated

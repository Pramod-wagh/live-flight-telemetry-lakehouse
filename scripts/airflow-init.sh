#!/usr/bin/env bash
set -e

echo "=== Initializing Apache Airflow ==="

# 1. Run database migrations
echo "Migrating Airflow DB..."
airflow db migrate

# 2. Create admin user
echo "Creating Airflow admin user..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" || true

# 3. Initialize MinIO buckets
echo "Initializing MinIO buckets..."
python - << 'EOF'
import time, os
from minio import Minio

endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
user = os.getenv("MINIO_ACCESS_KEY", "admin")
password = os.getenv("MINIO_SECRET_KEY", "adminpassword123")

for attempt in range(1, 31):
    try:
        client = Minio(endpoint, access_key=user, secret_key=password, secure=False)
        raw_b = os.getenv("MINIO_RAW_BUCKET", "flight-raw")
        iceberg_b = os.getenv("MINIO_ICEBERG_BUCKET", "flight-lakehouse")
        for b in [raw_b, iceberg_b]:
            if not client.bucket_exists(b):
                client.make_bucket(b)
                print(f"Created bucket: {b}")
            else:
                print(f"Bucket exists: {b}")
        print("MinIO storage initialized successfully!")
        break
    except Exception as e:
        print(f"Attempt {attempt}/30: Waiting for MinIO... ({e})")
        time.sleep(2)
EOF

echo "=== Airflow initialization completed successfully! ==="

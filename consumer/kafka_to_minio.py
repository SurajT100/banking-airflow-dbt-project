import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import tempfile
import time
last_flush_time = time.time()
flush_interval = 10  # seconds

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Kafka consumer
# -----------------------------
consumer = KafkaConsumer(
    'banking_server_clean.public.customers',
    'banking_server_clean.public.accounts',
    'banking_server_clean.public.transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# -----------------------------
# MinIO (S3)
# -----------------------------
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
if bucket not in existing_buckets:
    s3.create_bucket(Bucket=bucket)

# -----------------------------
# Schema enforcement
# -----------------------------
def enforce_schema(df, table_name):

    if table_name == "transactions":
        schema = {
            "id": "int64",
            "account_id": "int64",
            "txn_type": "string",
            "amount": "float64",
            "related_account_id": "float64",
            "status": "string",
            "created_at": "string"
        }

    elif table_name == "accounts":
        schema = {
            "id": "int64",
            "customer_id": "int64",
            "account_type": "string",
            "balance": "float64",
            "currency": "string",
            "created_at": "string"
        }

    elif table_name == "customers":
        schema = {
            "id": "int64",
            "first_name": "string",
            "last_name": "string",
            "email": "string",
            "created_at": "string"
        }

    else:
        return df

    for col, dtype in schema.items():
        if col in df.columns:
            if dtype.startswith("float") or dtype.startswith("int"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)

    return df

# -----------------------------
# Write to MinIO
# -----------------------------
def write_to_minio(table_name, records):

    if not records:
        return

    df = pd.DataFrame(records)

    # Fix schema
    df = enforce_schema(df, table_name)

    # Add ingestion timestamp
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()

    # Partition date
    date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # File name
    file_name = f"{table_name}_{datetime.now(timezone.utc).strftime('%H%M%S%f')}.parquet"

    # Windows-safe temp path
    local_path = os.path.join(tempfile.gettempdir(), file_name)

    # Write parquet
    df.to_parquet(local_path, engine='pyarrow', index=False)

    # Upload to MinIO
    s3_key = f"{table_name}/date={date_str}/{file_name}"
    s3.upload_file(local_path, bucket, s3_key)

    # Cleanup
    os.remove(local_path)

    print(f"✅ Uploaded {len(df)} records → s3://{bucket}/{s3_key}")

# -----------------------------
# Streaming loop
# -----------------------------
batch_size = 50

buffer = {
    'banking_server_clean.public.customers': [],
    'banking_server_clean.public.accounts': [],
    'banking_server_clean.public.transactions': []
}

print("🚀 Kafka consumer started...")

for message in consumer:

    topic = message.topic
    event = message.value

    payload = event.get("payload", {})
    record = payload.get("after")

    if record:
        buffer[topic].append(record)

        # Debug transactions
        if topic.endswith("transactions"):
            print(f"DEBUG amount → {record.get('amount')} ({type(record.get('amount'))})")

    current_time = time.time()

    if len(buffer[topic]) >= batch_size or (current_time - last_flush_time) > flush_interval:
    
        if buffer[topic]:  # prevent empty writes
            table_name = topic.split('.')[-1]
            write_to_minio(table_name, buffer[topic])
            buffer[topic] = []
            last_flush_time = current_time
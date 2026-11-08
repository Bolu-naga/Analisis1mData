"""Membuat bucket raw/warehouse di MinIO (aman dijalankan berulang)."""
import os
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client(
    "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "labadmin"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "labpassword123"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
)
for bucket in ("raw", "warehouse"):
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket dibuat: {bucket}")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"Bucket tersedia: {bucket}")
        else:
            raise
print("OK: MinIO siap untuk data mentah dan tabel Iceberg.")

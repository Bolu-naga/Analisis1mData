"""Memeriksa jumlah objek dan sampel baris di bucket raw MinIO."""
import argparse
import os
import boto3

p = argparse.ArgumentParser()
p.add_argument("--run-id", default="kelas-mg7-a")
a = p.parse_args()
s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "labadmin"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "labpassword123"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
prefix = f"events/run_id={a.run_id}/"
keys = [o for page in s3.get_paginator("list_objects_v2").paginate(Bucket="raw", Prefix=prefix)
        for o in page.get("Contents", [])]
print("OBJECT_COUNT", len(keys), "TOTAL_BYTES", sum(o["Size"] for o in keys))
assert keys, "Belum ada objek raw. Jalankan consumer dahulu."
first = keys[0]["Key"]
text = s3.get_object(Bucket="raw", Key=first)["Body"].read(600).decode("utf-8")
print("CONTOH_KEY", first, "\nCONTOH_ISI\n", text)

"""MG5->MG6: baca event Kafka, simpan objek JSONL ke bucket raw pada MinIO.

Semantik: at-least-once. Offset di-commit sesudah put_object sukses.
Jika terjadi crash, replay dapat memunculkan duplikasi; MG6 mengatasi melalui dropDuplicates.
"""
import argparse
import json
import os
import time
import boto3
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

parser = argparse.ArgumentParser()
parser.add_argument("--expected", type=int, default=100000)
parser.add_argument("--run-id", type=str, default="kelas-mg7-a")
parser.add_argument("--max-idle-seconds", type=int, default=60)
args = parser.parse_args()
assert args.expected > 0

s3 = boto3.client(
    "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "labadmin"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "labpassword123"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
)
consumer = KafkaConsumer(
    "ecommerce.orders", bootstrap_servers=os.getenv("BROKER", "redpanda:9092"),
    group_id=f"lab-raw-{args.run_id}", enable_auto_commit=False,
    auto_offset_reset="earliest", value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    max_poll_records=3000,
)
accepted = 0
last_progress = time.monotonic()
try:
    while accepted < args.expected:
        batches = consumer.poll(timeout_ms=2000)
        if not batches:
            if time.monotonic() - last_progress > args.max_idle_seconds:
                raise TimeoutError("Tidak ada pesan baru. Periksa jumlah --expected dan --run-id.")
            continue
        for tp, messages in batches.items():
            if accepted >= args.expected:
                break
            selected = [m for m in messages if m.value.get("run_id") == args.run_id]
            selected = selected[:args.expected - accepted]
            if selected:
                key = (
                    f"events/run_id={args.run_id}/partition={tp.partition}/"
                    f"offset-{selected[0].offset:012d}-{selected[-1].offset:012d}.jsonl"
                )
                body = "\n".join(json.dumps(m.value) for m in selected) + "\n"
                s3.put_object(Bucket="raw", Key=key, Body=body.encode("utf-8"),
                              ContentType="application/x-ndjson")
                accepted += len(selected)
                last_progress = time.monotonic()
                print(f"MINIO_OK s3://raw/{key} (event diterima kumulatif={accepted})")
            # Commit hanya offset yang telah terlewati melalui batch ini;
            # data run-id lain dilewati, tetapi offset-nya tetap di-commit.
            # Jika selected dipotong karena target tercapai, commit sampai
            # selected terakhir; jika tidak, commit sampai ujung batch.
            if selected and accepted >= args.expected:
                next_offset = selected[-1].offset + 1
            else:
                next_offset = messages[-1].offset + 1
            consumer.commit(offsets={tp: OffsetAndMetadata(next_offset, "")})
finally:
    consumer.close()
print(f"CONSUMER_OK run_id={args.run_id} jumlah_diterima={accepted}")

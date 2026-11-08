"""MG5: kirim event transaksi JSON sintetis ke Kafka-compatible Redpanda."""
import argparse
from datetime import date, timedelta
import json
import os
import time
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--rows", type=int, default=100000)
parser.add_argument("--run-id", type=str, default="kelas-mg7-a")
args = parser.parse_args()
assert args.rows > 0 and args.run_id, "rows harus >0 dan run-id tidak boleh kosong"

producer = KafkaProducer(
    bootstrap_servers=os.getenv("BROKER", "redpanda:9092"),
    key_serializer=lambda value: value.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all", retries=5,
)
start = time.perf_counter()
for i in range(args.rows):
    city = "Denpasar" if i % 10 < 8 else ("Jakarta" if i % 10 == 8 else "Surabaya")
    qty = i % 3 + 1
    price = (i * 37) % 900 + 100
    event = {
        "run_id": args.run_id,
        "order_id": i,
        "event_time": (date(2026, 9, 1) + timedelta(days=i % 30)).isoformat() + "T12:00:00",
        "city": "" if i % 997 == 0 else city,       # sengaja ada nilai tidak valid
        "product_id": (i * 17 + 3) % 200 + 1,
        "quantity": qty,
        "amount": None if i % 1009 == 0 else price * qty,  # sengaja ada null
    }
    producer.send("ecommerce.orders", key=str(event["product_id"]), value=event)
    if (i + 1) % 10000 == 0:
        producer.flush()
        print(f"Terkirim {i + 1:,}/{args.rows:,}")
producer.flush()
producer.close()
elapsed = time.perf_counter() - start
print(f"PRODUCER_OK run_id={args.run_id} jumlah={args.rows} durasi={elapsed:.2f}s")
print(f"Throughput perkiraan = {args.rows / max(elapsed, .001):,.0f} events/detik")

# Paket Praktikum Terintegrasi Minggu 5–7 (Analitik dan Skalabilitas Data)

**Lingkungan demo lokal:** Redpanda (Kafka-compatible, 1 broker), Python producer/consumer, MinIO, Apache Spark 3.5.3 (local[4]), Iceberg REST Catalog dan Iceberg tables. Semua contoh memakai data sintetis dan kredensial khusus kelas; **jangan gunakan password ini untuk server publik**.

## Kebutuhan
- Docker Desktop + Docker Compose v2 (untuk Windows, gunakan WSL2 backend), Internet saat pull image/JAR, RAM dianjurkan minimal 12–16 GB, ruang disk bebas 10 GB.
- Terminal PowerShell/Bash di direktori paket ini; perintah tidak mengharuskan Python/Java terpasang di host.

## Langkah cepat (smoke test 20.000 event)
```sh
docker compose up -d --build
docker compose ps
docker compose exec tools python scripts/00_setup.py
docker compose exec redpanda rpk topic create ecommerce.orders -p 3 -r 1
docker compose exec tools python scripts/01_producer.py --rows 20000 --run-id smoke-a
docker compose exec tools python scripts/02_consumer_raw_minio.py --expected 20000 --run-id smoke-a
docker compose exec tools python scripts/03_inspect_raw.py --run-id smoke-a
docker compose exec spark bash scripts/run_spark.sh --run-id smoke-a --shuffle-partitions 8 --salt-buckets 16 --pause
# Setelah Spark selesai:
docker compose exec tools python scripts/05_audit_hasil.py --run-id smoke-a
```
URL host: Kafka console `http://localhost:8088`, MinIO console `http://localhost:9001`, Iceberg REST API `http://localhost:8181/v1/config`, Spark Web UI `http://localhost:4040` **hanya selama job Spark hidup**.

## Latihan skala lebih besar: 1.000.000 event
Gunakan run-id **berbeda** agar tidak tercampur dengan data smoke test:
```sh
docker compose exec tools python scripts/01_producer.py --rows 1000000 --run-id kelas-1m
docker compose exec tools python scripts/02_consumer_raw_minio.py --expected 1000000 --run-id kelas-1m
docker compose exec spark bash scripts/run_spark.sh --run-id kelas-1m --shuffle-partitions 8 --salt-buckets 16 --pause
```

## Membaca kode hingga paham
- `PANDUAN_BACA_KODE_PER_SYNTAX.md`: penjelasan CLI, Docker, Kafka, consumer offset, PySpark, shuffle, salting, join, dan Iceberg.
- `PANDUAN_DOSEN_MILESTONE_2A.md`: kesiapan kelas, bukti unjuk kerja, rubrik, dan batasan sesuai RPS.
- `scripts/05_audit_hasil.py`: memeriksa kelengkapan dan konsistensi artefak setelah Spark selesai.

## Apa yang harus dikumpulkan?
- Repositori ini dan file `docker-compose.yml`; bukti kode/proses Kafka->MinIO->Spark->Iceberg.
- Folder `hasil/<run-id>/` berisi `benchmarks.csv`, `partition_sizes.csv`, `quality.json`, semua `plan_*.txt`, dan `plan_partition_pruning.txt`.
- Screenshot Spark UI (Jobs, Stages, SQL), MinIO `raw` dan `warehouse`, tabel/snapshot Iceberg.
- PDF laporan analitis (template ada di `laporan/TEMPLATE_LAPORAN.md`).

## Reproduksibilitas dan batasan
- Demo memakai **1 broker** Redpanda (replication factor 1), **belum** merepresentasikan uji failover multi-broker pada RPS Mg5. Untuk demonstrasi failover perlu sedikitnya tiga broker dan replikasi >1, serta pengujian terpisah. Target throughput MG5 >=10.000 event/detik bukan jaminan tercapai dengan skrip ini.
- Spark `local[4]` menjalankan empat thread pada satu host/container. Konsep stage, task, shuffle dan UI tetap dapat diamati, tetapi bukan klaster multi-worker fisik.
- Optimasi manual (salting/broadcast) **tidak dijamin selalu lebih cepat**; ukur dan jelaskan alasannya.
- Kafka consumer bersemantik *at-least-once*, sedangkan tabel Iceberg dibuat ulang dengan `createOrReplace()` untuk eksperimen kelas. Produksi membutuhkan penanganan retry, schema evolution, keamanan dan strategi idempotensi yang lebih matang.
- Pada eksekusi pertama Spark mengunduh dependensi JAR dari Maven Central; tanpa koneksi internet tugas tidak dapat memulai Spark. Periksa log jika terjadi versi/artifact mismatch.

## Reset (menghapus seluruh data lab)
```sh
docker compose down -v
```
HATI-HATI: `-v` akan menghapus data Kafka, MinIO dan cache JAR proyek.

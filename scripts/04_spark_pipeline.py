"""MG6-MG7: MinIO raw -> Spark cleaning -> analisis DAG/shuffle/skew/join -> Iceberg curated.

Contoh:
  bash scripts/run_spark.sh --run-id kelas-mg7-a --shuffle-partitions 8 \
    --salt-buckets 16 --pause
"""
import argparse
import csv
import io
import json
import time
from contextlib import redirect_stdout
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               LongType, IntegerType)


def explain_string(df):
    capture = io.StringIO()
    with redirect_stdout(capture):
        df.explain(mode="formatted")
    return capture.getvalue()


def partition_sizes(df):
    """Audit pendidikan: memakai RDD API untuk menghitung baris per partisi."""
    return sorted(df.rdd.mapPartitionsWithIndex(
        lambda idx, it: [(idx, sum(1 for _ in it))]
    ).collect())


def rows_to_dict(rows, keys, metrics):
    return {tuple(r[k] for k in keys): tuple(int(r[m]) for m in metrics)
            for r in rows}


def pause(enabled, message):
    if enabled:
        input(f"\n[PAUSE] {message}\nBuka http://localhost:4040, lalu tekan Enter ... ")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", default="kelas-mg7-a")
    p.add_argument("--shuffle-partitions", type=int, default=8)
    p.add_argument("--salt-buckets", type=int, default=16)
    p.add_argument("--pause", action="store_true")
    p.add_argument("--skip-iceberg", action="store_true")
    args = p.parse_args()
    if args.shuffle_partitions <= 0 or args.salt_buckets <= 0:
        p.error("Jumlah partisi dan salt-buckets harus positif.")

    # Nama run-id sekaligus nama folder output. Hindari '/', spasi, dan karakter liar.
    if not args.run_id.replace("-", "").replace("_", "").isalnum():
        p.error("--run-id hanya boleh mengandung huruf, angka, '-' atau '_'.")
    out = Path("/opt/lab/hasil") / args.run_id
    out.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName(f"MG7_Optimasi_{args.run_id}")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "false")  # uji manual sebelum AQE
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # baseline non-broadcast
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    print("Spark version:", spark.version)
    print("Spark UI:", sc.uiWebUrl)
    print("AQE:", spark.conf.get("spark.sql.adaptive.enabled"))
    print("Auto broadcast threshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
    print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

    # Schema eksplisit mencegah type inference berulang pada banyak file JSONL.
    schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("order_id", LongType(), True),
        StructField("event_time", StringType(), True),
        StructField("city", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("amount", LongType(), True),
    ])
    raw_path = f"s3a://raw/events/run_id={args.run_id}/partition=*/*.jsonl"
    raw = spark.read.schema(schema).json(raw_path)
    raw_count = raw.count()
    print("RAW_PATH", raw_path, "RAW_COUNT", raw_count)
    if raw_count == 0:
        raise ValueError("Data raw kosong. Ulangi tahap Kafka -> MinIO.")

    # Batch ETL: standardisasi kota, tipe tanggal, dan aturan kualitas data.
    cleaned = (
        raw.withColumn("city", F.initcap(F.trim(F.col("city"))))
           .withColumn("sale_date", F.to_date("event_time", "yyyy-MM-dd'T'HH:mm:ss"))
           .filter(F.col("order_id").isNotNull())
           .filter(F.col("city") != "")
           .filter(F.col("sale_date").isNotNull())
           .filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))
           .filter(F.col("quantity") > 0)
           .filter(F.col("product_id").between(1, 200))
           .dropDuplicates(["run_id", "order_id"])
           .select("run_id", "order_id", "sale_date", "city", "product_id",
                   "quantity", "amount")
           .persist(StorageLevel.MEMORY_AND_DISK)
    )
    clean_count = cleaned.count()  # materialisasi cache agar tes adil
    invalid_count = raw_count - clean_count
    print("CLEAN_COUNT", clean_count, "REJECTED_OR_DUPLICATE_COUNT", invalid_count)
    assert clean_count > 0, "Seluruh baris terhapus akibat aturan cleaning."
    pause(args.pause, "Lihat SQL/Jobs untuk membaca raw dan pembersihan data.")

    # Dimension table: data referensi produk kecil (200 baris).
    dim = (
        spark.range(1, 201).select(F.col("id").cast("int").alias("product_id"))
        .withColumn("category",
            F.when((F.col("product_id") % 4) == 0, "Elektronik")
             .when((F.col("product_id") % 4) == 1, "Fashion")
             .when((F.col("product_id") % 4) == 2, "Rumah Tangga")
             .otherwise("Olahraga"))
    )
    metrics = []

    def benchmark(tag, query, group_keys, value_keys):
        """Catat rencana fisik serta waktu action yang benar-benar mengeksekusi Spark."""
        (out / f"plan_{tag}.txt").write_text(explain_string(query), encoding="utf-8")
        sc.setJobGroup(tag, f"MG7 {tag}", interruptOnCancel=True)
        t0 = time.perf_counter()
        result = query.collect()   # ACTION: memicu DAG; hasil agregat relatif kecil
        seconds = time.perf_counter() - t0
        sc.setLocalProperty("spark.jobGroup.id", None)
        sc.setLocalProperty("spark.job.description", None)
        sc.setLocalProperty("spark.job.interruptOnCancel", None)
        metrics.append({"experiment": tag, "seconds": round(seconds, 4)})
        print(f"BENCHMARK {tag}: {seconds:.3f}s, rows={len(result)}")
        return rows_to_dict(result, group_keys, value_keys)

    # A: wide transformation pada agregasi groupBy.
    basic = cleaned.groupBy("city").agg(
        F.count("*").alias("orders"), F.sum("amount").alias("gross_sales"))
    result_a = benchmark("A_groupby_baseline", basic, ["city"], ["orders", "gross_sales"])
    pause(args.pause, "Amati Exchange/hashaggregate/shuffle pada SQL dan Stages.")

    # B: paksa redistribusi berdasarkan key tidak seimbang -> skew terlihat.
    skewed = cleaned.repartition(args.shuffle_partitions, "city").persist(
        StorageLevel.MEMORY_AND_DISK)
    skewed.count()
    before_sizes = partition_sizes(skewed)
    print("PARTITION_BEFORE_SALTING", before_sizes)
    skew_query = skewed.groupBy("city").agg(
        F.count("*").alias("orders"), F.sum("amount").alias("gross_sales"))
    result_b = benchmark("B_skew_before", skew_query, ["city"], ["orders", "gross_sales"])
    pause(args.pause, "Di Stages, bandingkan durasi Task dan variasi shuffle read.")

    # C: salting berbasis order_id; setiap city dibagi 16 sub-key.
    salted = (
        cleaned.withColumn("salt", F.pmod(F.xxhash64("order_id"),
                                           F.lit(args.salt_buckets)))
               .repartition(args.shuffle_partitions, "city", "salt")
               .persist(StorageLevel.MEMORY_AND_DISK)
    )
    salted.count()
    after_sizes = partition_sizes(salted)
    print("PARTITION_AFTER_SALTING", after_sizes)
    partial = salted.groupBy("city", "salt").agg(
        F.count("*").alias("orders_partial"),
        F.sum("amount").alias("sales_partial"))
    salted_query = partial.groupBy("city").agg(
        F.sum("orders_partial").alias("orders"),
        F.sum("sales_partial").alias("gross_sales"))
    result_c = benchmark("C_skew_after_salting", salted_query,
                         ["city"], ["orders", "gross_sales"])
    assert result_a == result_b == result_c, "FAIL: agregasi sebelum/sesudah salting berbeda!"
    print("VALIDASI_SALTING_OK output agregasi identik.")
    with (out / "partition_sizes.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["scenario", "partition_id", "rows"])
        for scenario, pairs in (("skew", before_sizes), ("salted", after_sizes)):
            for partition_id, n in pairs:
                writer.writerow([scenario, partition_id, n])
    pause(args.pause, "Bandingkan distribusi partisi dan diskusikan overhead salting.")

    # D: penentuan join strategy yang eksplisit.
    def summarize_join(joined):
        return joined.groupBy("category").agg(
            F.count("*").alias("orders"), F.sum("amount").alias("gross_sales"))

    merge_join = cleaned.join(dim.hint("merge"), "product_id", "inner")
    broadcast_join = cleaned.join(F.broadcast(dim), "product_id", "inner")
    result_d = benchmark("D_sort_merge_join", summarize_join(merge_join),
                         ["category"], ["orders", "gross_sales"])
    result_e = benchmark("E_broadcast_join", summarize_join(broadcast_join),
                         ["category"], ["orders", "gross_sales"])
    assert result_d == result_e, "FAIL: hasil join tidak identik!"
    assert sum(x[0] for x in result_e.values()) == clean_count
    print("VALIDASI_JOIN_OK hasil agregasi identik dan jumlah baris cocok.")
    pause(args.pause, "Bandingkan SortMergeJoin/Exchange vs BroadcastHashJoin.")

    if not args.skip_iceberg:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.curated")
        # Contoh tabel fakta detail, partisi tanggal memungkinkan partition pruning.
        cleaned.writeTo("lake.curated.orders_clean").using("iceberg") \
            .partitionedBy("sale_date").createOrReplace()
        # Data mart ringkas: metrik bisnis menurut tanggal, kota, kategori produk.
        sales_summary = (
            broadcast_join.groupBy("sale_date", "city", "category")
            .agg(F.count("*").alias("orders"),
                 F.sum("amount").alias("gross_sales"))
        )
        sales_summary.writeTo("lake.curated.sales_summary").using("iceberg") \
            .partitionedBy("sale_date").createOrReplace()
        iceberg_count = spark.table("lake.curated.orders_clean").count()
        assert iceberg_count == clean_count, "FAIL: jumlah baris Iceberg tidak cocok!"
        print("ICEBERG_OK orders_clean count", iceberg_count)
        spark.sql("SELECT * FROM lake.curated.orders_clean.snapshots").show(5, truncate=False)
        pruning = spark.table("lake.curated.orders_clean").filter(
            F.col("sale_date") == F.lit("2026-09-05").cast("date"))
        (out / "plan_partition_pruning.txt").write_text(
            explain_string(pruning), encoding="utf-8")
        print("PRUNING tanggal 2026-09-05, rows", pruning.count())
        pause(args.pause, "Lihat tab SQL dan plan_partition_pruning.txt setelah menulis Iceberg.")

    with (out / "benchmarks.csv").open("w", newline="", encoding="utf-8") as h:
        writer = csv.DictWriter(h, fieldnames=["experiment", "seconds"])
        writer.writeheader()
        writer.writerows(metrics)
    (out / "quality.json").write_text(json.dumps({
        "run_id": args.run_id, "raw_count": raw_count, "clean_count": clean_count,
        "invalid_or_duplicates": invalid_count,
        "partition_skew": before_sizes, "partition_salted": after_sizes,
        "all_validations_passed": True,
    }, indent=2), encoding="utf-8")
    print("SEMUA VALIDASI LULUS. Bukti: /opt/lab/hasil/" + args.run_id)
    pause(args.pause, "Ambil tangkapan layar sebelum aplikasi Spark berhenti.")
    skewed.unpersist()
    salted.unpersist()
    cleaned.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

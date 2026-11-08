"""MG7: audit berkas output pipeline secara statis, tanpa mengubah tabel Iceberg.

Contoh: python scripts/05_audit_hasil.py --run-id smoke-a
Jalankan setelah skrip 04_spark_pipeline.py selesai; laporan UI harus diperiksa manual.
"""
import argparse
import csv
import json
from pathlib import Path

p = argparse.ArgumentParser(description="Periksa artefak Milestone 2a")
p.add_argument("--run-id", required=True, help="harus identik dengan run-id producer/consumer/Spark")
a = p.parse_args()
if not a.run_id.replace("-", "").replace("_", "").isalnum():
    p.error("run-id hanya boleh memuat huruf/angka/-/_")

root = Path("/opt/lab/hasil") if Path("/opt/lab/hasil").exists() else Path(__file__).resolve().parents[1] / "hasil"
folder = root / a.run_id
required = ["quality.json", "benchmarks.csv", "partition_sizes.csv",
            "plan_A_groupby_baseline.txt", "plan_B_skew_before.txt",
            "plan_C_skew_after_salting.txt", "plan_D_sort_merge_join.txt",
            "plan_E_broadcast_join.txt", "plan_partition_pruning.txt"]
missing = [name for name in required if not (folder / name).is_file()]
if missing:
    raise SystemExit(f"GAGAL: file belum ada di {folder}: {', '.join(missing)}")

quality = json.loads((folder / "quality.json").read_text(encoding="utf-8"))
assert quality["run_id"] == a.run_id
assert quality["raw_count"] >= quality["clean_count"] > 0
assert quality["raw_count"] == quality["clean_count"] + quality["invalid_or_duplicates"]
assert quality["all_validations_passed"] is True
print(f"QUALITY_OK raw={quality['raw_count']:,} clean={quality['clean_count']:,} "
      f"invalid/duplicate={quality['invalid_or_duplicates']:,}")

with (folder / "benchmarks.csv").open(encoding="utf-8", newline="") as f:
    benchmarks = {r["experiment"]: float(r["seconds"]) for r in csv.DictReader(f)}
needed = ["A_groupby_baseline", "B_skew_before", "C_skew_after_salting",
          "D_sort_merge_join", "E_broadcast_join"]
assert all(k in benchmarks and benchmarks[k] >= 0 for k in needed), "Benchmark kurang atau tidak valid"
print("BENCHMARKS_OK")
for k in needed:
    print(f"  {k}: {benchmarks[k]:.4f} detik")

with (folder / "partition_sizes.csv").open(encoding="utf-8", newline="") as f:
    parts = list(csv.DictReader(f))
for name in ["skew", "salted"]:
    sizes = [int(r["rows"]) for r in parts if r["scenario"] == name]
    assert sizes and sum(sizes) == quality["clean_count"], f"Distribusi partisi {name} tidak cocok"
    # max/mean dari SEMUA partisi, termasuk partisi kosong (indikator ketimpangan).
    skew_index = max(sizes) / (sum(sizes) / len(sizes))
    print(f"PARTITIONS_{name.upper()} partisi={len(sizes)} kosong={sizes.count(0)} "
          f"max={max(sizes):,} skew_index={skew_index:.2f}")

merge = (folder / "plan_D_sort_merge_join.txt").read_text(encoding="utf-8")
broadcast = (folder / "plan_E_broadcast_join.txt").read_text(encoding="utf-8")
assert "SortMergeJoin" in merge, "Plan baseline tidak mengandung SortMergeJoin"
assert "BroadcastHashJoin" in broadcast, "Plan optimasi tidak mengandung BroadcastHashJoin"
print("JOIN_PLAN_OK SortMergeJoin dan BroadcastHashJoin terdeteksi")

def speedup(before, after):
    return before / after if after > 0 else float("inf")

print(f"JOIN_SPEEDUP {speedup(benchmarks['D_sort_merge_join'], benchmarks['E_broadcast_join']):.2f}x")
print(f"SKEW_SPEEDUP {speedup(benchmarks['B_skew_before'], benchmarks['C_skew_after_salting']):.2f}x")
print("AUDIT_OK: artefak secara internal konsisten. Screenshot dan analisis Spark UI tetap wajib diperiksa dosen.")

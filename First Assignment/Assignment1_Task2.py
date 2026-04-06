import csv
import os
import math
import hashlib
from datetime import datetime
from multiprocessing import Process
from typing import Optional
from collections import defaultdict
from datetime import timedelta

import glob
import itertools


# =========================
# Helpers
# =========================

def parse_timestamp(ts_str: str) -> Optional[datetime]:
    ts_str = ts_str.strip()
    if not ts_str:
        return None

    formats = [
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue

    return None


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


def km_to_nm(km):
    return km / 1.852


def stable_bucket_for_mmsi(mmsi: str, num_buckets: int) -> int:
    """
    Stable hash so the same MMSI always goes to the same bucket,
    across different Python runs/platforms.
    """
    digest = hashlib.md5(mmsi.encode("utf-8")).hexdigest()
    return int(digest, 16) % num_buckets


# =========================
# Task 2 - Map step
# Partition raw AIS rows by MMSI
# =========================

def partition_ais_file(input_csv_path, bucket_dir, num_buckets=8):
    print(f"[Partition] Reading input file: {input_csv_path}")
    os.makedirs(bucket_dir, exist_ok=True)

    bucket_paths = [
        os.path.join(bucket_dir, f"bucket_{i}.csv")
        for i in range(num_buckets)
    ]

    writers = []
    files = []

    fieldnames = ["MMSI", "timestamp", "latitude", "longitude"]

    try:
        for path in bucket_paths:
            f = open(path, "w", newline="", encoding="utf-8")
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            files.append(f)
            writers.append(writer)

        rows_read = 0
        rows_written = 0

        with open(input_csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                rows_read += 1

                mmsi = row.get("MMSI", "").strip()
                ts_str = row.get("# Timestamp", "").strip()
                lat_str = row.get("Latitude", "").strip()
                lon_str = row.get("Longitude", "").strip()

                if not mmsi:
                    continue

                ts = parse_timestamp(ts_str)
                if ts is None:
                    continue

                try:
                    lat = float(lat_str)
                    lon = float(lon_str)
                except ValueError:
                    continue

                bucket_id = stable_bucket_for_mmsi(mmsi, num_buckets)

                writers[bucket_id].writerow({
                    "MMSI": mmsi,
                    "timestamp": ts.isoformat(sep=" "),
                    "latitude": lat,
                    "longitude": lon,
                })
                rows_written += 1

        print(
            f"[Partition] {os.path.basename(input_csv_path)} | "
            f"rows_read={rows_read}, rows_written={rows_written}, buckets={num_buckets}"
        )

    finally:
        for f in files:
            f.close()

# =========================
# Task 2 - Reduce step
# Chronological vessel-state tracking inside isolated workers
# =========================

def process_bucket(bucket_id, bucket_csv_path, output_csv_path):
    print(f"[Worker {bucket_id}] Processing {bucket_csv_path}")

    if not os.path.exists(bucket_csv_path):
        print(f"[Worker {bucket_id}] Missing bucket file, skipping.")
        return

    vessels = {}
    rows_read = 0

    with open(bucket_csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            rows_read += 1

            mmsi = row.get("MMSI", "").strip()
            ts_str = row.get("# Timestamp", "").strip()
            lat_str = row.get("Latitude", "").strip()
            lon_str = row.get("Longitude", "").strip()
            draught = row.get("Draught", "").strip()

            if not mmsi:
                continue

            ts = parse_timestamp(ts_str)
            if ts is None:
                continue

            try:
                lat = float(lat_str)
                lon = float(lon_str)
                drt = float(draught)
            except ValueError:
                continue

            vessels.setdefault(mmsi, []).append((ts, lat, lon, drt))

        fieldnames = [
            "MMSI",
            "event_index",
            "timestamp",
            "latitude",
            "longitude",
            "draught",        # ← no trailing space
            "prev_timestamp",
            "prev_latitude",
            "prev_longitude",
            "prev_draught",   # ← no trailing space
            "draught_changes",
            "delta_seconds",
            "distance_from_prev_nm",
            "was_dark",
            "suspicious_draft_change",
        ]

    vessel_count = 0
    output_rows = 0
    was_dark_rows = 0
    draft_changes_rows = 0

    with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for mmsi, points in vessels.items():
            vessel_count += 1

            # Chronological state tracking
            points.sort(key=lambda x: x[0])

            prev_ts = None
            prev_lat = None
            prev_lon = None
            prev_drt = None
            for idx, (ts, lat, lon, drt) in enumerate(points, start=1):
                if prev_ts is None:
                    delta_seconds = ""
                    distance_nm = ""
                    prev_ts_str = ""
                    prev_lat_out = ""
                    prev_lon_out = ""
                    prev_drt_out = ""
                    draught_changes = ""
                    was_dark = "NO"
                    suspicious_draft_change = "NO"
                else:
                    delta_seconds = int((ts - prev_ts).total_seconds())
                    distance_nm = round(
                        km_to_nm(haversine_km(prev_lat, prev_lon, lat, lon)),
                        6
                    )
                    draught_changes = ((drt - prev_drt) / prev_drt) * 100
                    prev_ts_str = prev_ts.isoformat(sep=" ")
                    prev_lat_out = prev_lat
                    prev_lon_out = prev_lon
                    prev_drt_out = prev_drt
                    sog = distance_nm / (delta_seconds / 3600) if delta_seconds > 0 else 0
                    gap_hours = delta_seconds / 3600
                    # ANOMALY A Detection
                    if gap_hours > 4:
                        was_dark = "YES"
                        was_dark_rows += 1
                    else:
                        was_dark = "NO"

                    # ANOMALY C Detection
                    if gap_hours >= 2 and abs(draught_changes) >= 5:
                       suspicious_draft_change = "YES"
                       draft_changes_rows += 1
                    else:
                       suspicious_draft_change = "NO"

                writer.writerow({
                    "MMSI": mmsi,
                    "event_index": idx,
                    "timestamp": ts.isoformat(sep=" "),
                    "latitude": lat,
                    "longitude": lon,
                    "draught": drt,
                    "prev_timestamp": prev_ts_str,
                    "prev_latitude": prev_lat_out,
                    "prev_longitude": prev_lon_out,
                    "prev_draught": prev_drt_out,
                    "draught_changes": draught_changes,
                    "delta_seconds": delta_seconds,
                    "distance_from_prev_nm": distance_nm,
                    "was_dark": was_dark,
                    "suspicious_draft_change": suspicious_draft_change,
                })
                output_rows += 1

                prev_ts = ts
                prev_lat = lat
                prev_lon = lon
                prev_drt = drt

        print(
        f"[Worker {bucket_id}] rows_read={rows_read}, "
        f"vessels={vessel_count}, output_rows={output_rows}, Anomaly_A_detected_in={was_dark_rows}, Anomaly_C_detected_in={draft_changes_rows}"
    )


def run_parallel_reduce(bucket_dir, reduced_dir, num_buckets=8):
    os.makedirs(reduced_dir, exist_ok=True)

    workers = []

    for bucket_id in range(num_buckets):
        bucket_csv_path = os.path.join(bucket_dir, f"bucket_{bucket_id}.csv")
        output_csv_path = os.path.join(reduced_dir, f"reduced_bucket_{bucket_id}.csv")

        p = Process(
            target=process_bucket,
            args=(bucket_id, bucket_csv_path, output_csv_path)
        )
        p.start()
        workers.append(p)

    for p in workers:
        p.join()
        if p.exitcode != 0:
            print(f"[Main] Worker PID {p.pid} exited with code {p.exitcode}")


# =========================
# Optional final merge
# =========================

def merge_reduced_outputs(reduced_dir, final_output_csv):
    print(f"[Merge] Merging files from: {reduced_dir}")
    all_paths = [
        os.path.join(reduced_dir, name)
        for name in os.listdir(reduced_dir)
        if name.endswith(".csv")
    ]
    all_paths.sort()

    fieldnames = [
        "MMSI",
        "event_index",
        "timestamp",
        "latitude",
        "longitude",
        "draught",        # ← no trailing space
        "prev_timestamp",
        "prev_latitude",
        "prev_longitude",
        "prev_draught",   # ← no trailing space
        "draught_changes",
        "delta_seconds",
        "distance_from_prev_nm",
        "was_dark",
        "suspicious_draft_change",
    ]

    total_rows = 0

    with open(final_output_csv, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()

        for path in all_paths:
            with open(path, "r", newline="", encoding="utf-8") as in_f:
                reader = csv.DictReader(in_f)
                for row in reader:
                    writer.writerow(row)
                    total_rows += 1

    print(f"[Merge] Wrote {total_rows} chronological AIS rows to {final_output_csv}")

# =========================
# End-to-end runner
# =========================

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))

    files = [
        "aisdk-2026-02-27",
        "aisdk-2026-02-28",
    ]

    num_buckets = 8

    for file_name in files:
        print("=" * 80)
        print(f"Analyzing partition set: {file_name}", flush=True)

        bucket_dir = os.path.join(base_dir, "partitions", file_name)
        reduced_dir = os.path.join(base_dir, "reduced", file_name)
        final_output_csv = os.path.join(
            base_dir,
            "reduced",
            f"{file_name}_chronological.csv"
        )

        if not os.path.exists(bucket_dir):
            print(f"Skipping missing bucket directory: {bucket_dir}", flush=True)
            continue

        run_parallel_reduce(
            bucket_dir=bucket_dir,
            reduced_dir=reduced_dir,
            num_buckets=num_buckets,
        )

        merge_reduced_outputs(
            reduced_dir=reduced_dir,
            final_output_csv=final_output_csv,
        )
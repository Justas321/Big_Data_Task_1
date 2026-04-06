import csv
import os
from multiprocessing import Process

from Task2.utils import (
    parse_timestamp,
    haversine_km,
    km_to_nm,
    safe_float,
    detect_column,
)

REDUCED_FIELDNAMES = [
    "MMSI",
    "event_index",
    "timestamp",
    "latitude",
    "longitude",
    "prev_timestamp",
    "prev_latitude",
    "prev_longitude",
    "delta_seconds",
    "distance_from_prev_nm",
    "SOG",
    "Draught",
]


def process_bucket(bucket_id, bucket_csv_path, output_csv_path):
    print(f"[Worker {bucket_id}] Processing {bucket_csv_path}", flush=True)

    if not os.path.exists(bucket_csv_path):
        print(f"[Worker {bucket_id}] Missing bucket file, skipping.", flush=True)
        return

    vessels = {}
    rows_read = 0
    rows_kept = 0
    rows_rejected = 0

    with open(bucket_csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            print(f"[Worker {bucket_id}] Empty bucket file.", flush=True)
            return

        header = {name: name for name in reader.fieldnames}

        mmsi_col = detect_column(header, ["MMSI"])
        ts_col = detect_column(header, ["# Timestamp", "timestamp"])
        lat_col = detect_column(header, ["Latitude", "latitude"])
        lon_col = detect_column(header, ["Longitude", "longitude"])
        sog_col = detect_column(header, ["SOG", "sog"])
        draught_col = detect_column(header, ["Draught", "draught"])

        if not all([mmsi_col, ts_col, lat_col, lon_col]):
            raise ValueError(
                f"Bucket {bucket_id} missing required columns. "
                f"Found columns: {reader.fieldnames}"
            )

        for row in reader:
            rows_read += 1

            mmsi = (row.get(mmsi_col) or "").strip()
            ts_str = (row.get(ts_col) or "").strip()
            lat = safe_float(row.get(lat_col))
            lon = safe_float(row.get(lon_col))
            sog = safe_float(row.get(sog_col)) if sog_col else None
            draught = safe_float(row.get(draught_col)) if draught_col else None

            if not mmsi:
                rows_rejected += 1
                continue

            ts = parse_timestamp(ts_str)
            if ts is None or lat is None or lon is None:
                rows_rejected += 1
                continue

            vessels.setdefault(mmsi, []).append((ts, lat, lon, sog, draught))
            rows_kept += 1

    vessel_count = 0
    output_rows = 0

    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

    with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REDUCED_FIELDNAMES)
        writer.writeheader()

        for mmsi, points in vessels.items():
            vessel_count += 1
            points.sort(key=lambda x: x[0])

            prev_ts = None
            prev_lat = None
            prev_lon = None

            for idx, (ts, lat, lon, sog, draught) in enumerate(points, start=1):
                if prev_ts is None:
                    delta_seconds = ""
                    distance_nm = ""
                    prev_ts_str = ""
                    prev_lat_out = ""
                    prev_lon_out = ""
                else:
                    delta_seconds = int((ts - prev_ts).total_seconds())
                    distance_nm = round(
                        km_to_nm(haversine_km(prev_lat, prev_lon, lat, lon)),
                        6
                    )
                    prev_ts_str = prev_ts.isoformat(sep=" ")
                    prev_lat_out = prev_lat
                    prev_lon_out = prev_lon

                writer.writerow({
                    "MMSI": mmsi,
                    "event_index": idx,
                    "timestamp": ts.isoformat(sep=" "),
                    "latitude": lat,
                    "longitude": lon,
                    "prev_timestamp": prev_ts_str,
                    "prev_latitude": prev_lat_out,
                    "prev_longitude": prev_lon_out,
                    "delta_seconds": delta_seconds,
                    "distance_from_prev_nm": distance_nm,
                    "SOG": sog,
                    "Draught": draught,
                })
                output_rows += 1

                prev_ts = ts
                prev_lat = lat
                prev_lon = lon

    print(
        f"[Worker {bucket_id}] rows_read={rows_read}, rows_kept={rows_kept}, "
        f"rows_rejected={rows_rejected}, vessels={vessel_count}, "
        f"output_rows={output_rows}",
        flush=True
    )


def run_parallel_reduce(task1_bucket_dir, reduced_dir, num_buckets=8):
    os.makedirs(reduced_dir, exist_ok=True)

    workers = []

    for bucket_id in range(num_buckets):
        bucket_csv_path = os.path.join(task1_bucket_dir, f"bucket_{bucket_id}.csv")
        output_csv_path = os.path.join(reduced_dir, f"reduced_bucket_{bucket_id}.csv")

        p = Process(
            target=process_bucket,
            args=(bucket_id, bucket_csv_path, output_csv_path)
        )
        p.start()
        workers.append(p)

    failed = []

    for p in workers:
        p.join()
        if p.exitcode != 0:
            failed.append((p.pid, p.exitcode))

    if failed:
        details = ", ".join(f"PID {pid} exitcode={code}" for pid, code in failed)
        raise RuntimeError(f"Reduce worker(s) failed: {details}")


def find_dataset_dirs(task1_output_dir):
    if not os.path.isdir(task1_output_dir):
        return []

    dataset_dirs = []
    for name in os.listdir(task1_output_dir):
        full_path = os.path.join(task1_output_dir, name)
        if os.path.isdir(full_path):
            dataset_dirs.append(full_path)

    return sorted(dataset_dirs)


if __name__ == "__main__":
    task2_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(task2_dir)

    task1_output_dir = os.path.join(project_root, "Task1", "output")
    reduced_root = os.path.join(task2_dir, "output")

    os.makedirs(reduced_root, exist_ok=True)

    num_buckets = 8
    dataset_dirs = find_dataset_dirs(task1_output_dir)

    if not dataset_dirs:
        print(f"No output directories found in: {task1_output_dir}")
        raise SystemExit(1)

    for dataset_dir in dataset_dirs:
        dataset_name = os.path.basename(dataset_dir)
        reduced_dir = os.path.join(reduced_root, dataset_name)

        print(f"Processing bucket set: {dataset_name}", flush=True)
        print(f"Input buckets: {dataset_dir}", flush=True)
        print(f"Reduced output: {reduced_dir}", flush=True)

        run_parallel_reduce(
            task1_bucket_dir=dataset_dir,
            reduced_dir=reduced_dir,
            num_buckets=num_buckets,
        )
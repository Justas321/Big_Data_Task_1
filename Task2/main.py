import csv
import os
import tempfile
from multiprocessing import Process
import heapq

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

SORTED_INPUT_FIELDNAMES = [
    "MMSI",
    "timestamp",
    "latitude",
    "longitude",
    "SOG",
    "Draught",
]

def process_bucket(bucket_id, bucket_csv_path, output_csv_path):
    print(f"[Worker {bucket_id}] Processing {bucket_csv_path}", flush=True)

    if not os.path.exists(bucket_csv_path):
        print(f"[Worker {bucket_id}] Missing bucket file, skipping.", flush=True)
        return

    rows_read = 0
    rows_kept = 0
    rows_rejected = 0
    vessel_count = 0
    output_rows = 0

    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f"reduce_bucket_{bucket_id}_") as temp_dir:
        temp_unsorted = os.path.join(temp_dir, f"bucket_{bucket_id}_unsorted.csv")
        temp_sorted = os.path.join(temp_dir, f"bucket_{bucket_id}_sorted.csv")

        with open(bucket_csv_path, "r", newline="", encoding="utf-8") as f_in, \
             open(temp_unsorted, "w", newline="", encoding="utf-8") as f_tmp:

            reader = csv.DictReader(f_in)

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

            writer_tmp = csv.DictWriter(f_tmp, fieldnames=SORTED_INPUT_FIELDNAMES)

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

                writer_tmp.writerow({
                    "MMSI": mmsi,
                    "timestamp": ts.isoformat(sep=" "),
                    "latitude": lat,
                    "longitude": lon,
                    "SOG": sog,
                    "Draught": draught,
                })
                rows_kept += 1

        if rows_kept == 0:
            with open(output_csv_path, "w", newline="", encoding="utf-8") as f_out:
                writer = csv.DictWriter(f_out, fieldnames=REDUCED_FIELDNAMES)
                writer.writeheader()

            print(
                f"[Worker {bucket_id}] rows_read={rows_read}, rows_kept={rows_kept}, "
                f"rows_rejected={rows_rejected}, vessels=0, output_rows=0",
                flush=True
            )
            return
        
        chunk_files = []
        chunk_size = 200000

        with open(temp_unsorted, "r", newline="", encoding="utf-8") as f_unsorted:
            reader = csv.DictReader(f_unsorted, fieldnames=SORTED_INPUT_FIELDNAMES)
            chunk = []
            chunk_index = 0

            for row in reader:
                chunk.append(row)

                if len(chunk) >= chunk_size:
                    chunk.sort(key=lambda r: (r["MMSI"], r["timestamp"]))
                    chunk_path = os.path.join(
                        temp_dir, f"bucket_{bucket_id}_chunk_{chunk_index}.csv"
                    )
                    with open(chunk_path, "w", newline="", encoding="utf-8") as f_chunk:
                        writer = csv.DictWriter(
                            f_chunk, fieldnames=SORTED_INPUT_FIELDNAMES
                        )
                        for item in chunk:
                            writer.writerow(item)

                    chunk_files.append(chunk_path)
                    chunk_index += 1
                    chunk = []

            if chunk:
                chunk.sort(key=lambda r: (r["MMSI"], r["timestamp"]))
                chunk_path = os.path.join(
                    temp_dir, f"bucket_{bucket_id}_chunk_{chunk_index}.csv"
                )
                with open(chunk_path, "w", newline="", encoding="utf-8") as f_chunk:
                    writer = csv.DictWriter(
                        f_chunk, fieldnames=SORTED_INPUT_FIELDNAMES
                    )
                    for item in chunk:
                        writer.writerow(item)

                chunk_files.append(chunk_path)

        chunk_readers = []
        chunk_handles = []
        heap = []

        try:
            for i, chunk_path in enumerate(chunk_files):
                f_chunk = open(chunk_path, "r", newline="", encoding="utf-8")
                chunk_handles.append(f_chunk)
                reader = csv.DictReader(f_chunk, fieldnames=SORTED_INPUT_FIELDNAMES)
                chunk_readers.append(reader)

                first_row = next(reader, None)
                if first_row is not None:
                    heapq.heappush(
                        heap,
                        ((first_row["MMSI"], first_row["timestamp"]), i, first_row)
                    )

            with open(temp_sorted, "w", newline="", encoding="utf-8") as f_sorted:
                writer = csv.DictWriter(f_sorted, fieldnames=SORTED_INPUT_FIELDNAMES)

                while heap:
                    _, reader_index, row = heapq.heappop(heap)
                    writer.writerow(row)

                    next_row = next(chunk_readers[reader_index], None)
                    if next_row is not None:
                        heapq.heappush(
                            heap,
                            ((next_row["MMSI"], next_row["timestamp"]), reader_index, next_row)
                        )
        finally:
            for f_chunk in chunk_handles:
                f_chunk.close()

        with open(temp_sorted, "r", newline="", encoding="utf-8") as f_sorted, \
             open(output_csv_path, "w", newline="", encoding="utf-8") as f_out:

            reader = csv.DictReader(f_sorted, fieldnames=SORTED_INPUT_FIELDNAMES)
            writer = csv.DictWriter(f_out, fieldnames=REDUCED_FIELDNAMES)
            writer.writeheader()

            current_mmsi = None
            event_index = 0
            prev_ts = None
            prev_lat = None
            prev_lon = None

            for row in reader:
                mmsi = row["MMSI"]
                ts = parse_timestamp(row["timestamp"])
                lat = safe_float(row["latitude"])
                lon = safe_float(row["longitude"])
                sog = safe_float(row["SOG"])
                draught = safe_float(row["Draught"])

                if ts is None or lat is None or lon is None:
                    continue

                if mmsi != current_mmsi:
                    current_mmsi = mmsi
                    vessel_count += 1
                    event_index = 1
                    prev_ts = None
                    prev_lat = None
                    prev_lon = None
                else:
                    event_index += 1

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
                    "event_index": event_index,
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
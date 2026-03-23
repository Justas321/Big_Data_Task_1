import csv
import time
import os
import hashlib
from multiprocessing import Process, Queue
import psutil

INVALID_MMSI_SET = {
    "000000000",
    "111111111",
    "123456789",
    "999999999",
}

def get_memory_usage_mb(pid=None):
    if pid is None:
        pid = os.getpid()
    process = psutil.Process(pid)
    return process.memory_info().rss / (1024 * 1024)

def is_valid_mmsi(mmsi: str) -> bool:
    if mmsi is None:
        return False

    mmsi = mmsi.strip()

    if len(mmsi) != 9 or not mmsi.isdigit():
        return False

    if mmsi in INVALID_MMSI_SET:
        return False

    if len(set(mmsi)) == 1:
        return False

    mid = int(mmsi[:3])
    if mid < 201 or mid > 775:
        return False

    return True

def stable_bucket_id(mmsi: str, num_buckets: int) -> int:
    digest = hashlib.md5(mmsi.encode("utf-8")).hexdigest()
    return int(digest, 16) % num_buckets


def bucket_worker(bucket_id, task_queue, output_dir):
    worker_pid = os.getpid()
    peak_mem_mb = get_memory_usage_mb(worker_pid)
    total_rows_written = 0
    chunks_received = 0

    output_path = os.path.join(output_dir, f"bucket_{bucket_id}.csv")
    header_written = os.path.exists(output_path) and os.path.getsize(output_path) > 0

    while True:
        chunk = task_queue.get()
        if chunk is None:
            break

        if not chunk:
            continue

        chunks_received += 1
        peak_mem_mb = max(peak_mem_mb, get_memory_usage_mb(worker_pid))

        with open(output_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=chunk[0].keys())

            if not header_written:
                writer.writeheader()
                header_written = True

            writer.writerows(chunk)

        total_rows_written += len(chunk)
        peak_mem_mb = max(peak_mem_mb, get_memory_usage_mb(worker_pid))

    print(
        f"[Worker {bucket_id} | PID {worker_pid}] "
        f"chunks={chunks_received}, rows={total_rows_written}, "
        f"peak_rss_mb={peak_mem_mb:.2f}"
    )

def stream_partition_csv_parallel(input_csv, output_dir, num_buckets=4, flush_every=5000):
    os.makedirs(output_dir, exist_ok=True)

    start_time = time.time()
    main_peak_mem_mb = get_memory_usage_mb()

    queues = [Queue(maxsize=8) for _ in range(num_buckets)]
    workers = []

    for bucket_id in range(num_buckets):
        p = Process(target=bucket_worker, args=(bucket_id, queues[bucket_id], output_dir))
        p.start()
        workers.append(p)

    buffers = [[] for _ in range(num_buckets)]

    total_rows_read = 0
    total_rows_valid = 0
    total_rows_rejected = 0
    rejected_bad_mmsi = 0
    rejected_bad_coords = 0
    rejected_bad_timestamp = 0
    bucket_row_counts = [0] * num_buckets
    bucket_chunk_counts = [0] * num_buckets

    with open(input_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_rows_read += 1

            if total_rows_read % 200000 == 0:
                print(f"Processed {total_rows_read} rows...")
                main_peak_mem_mb = max(main_peak_mem_mb, get_memory_usage_mb())

            mmsi = row.get("MMSI", "").strip()
            ts = row.get("# Timestamp", "").strip()
            lat = row.get("Latitude", "").strip()
            lon = row.get("Longitude", "").strip()

            if not is_valid_mmsi(mmsi):
                total_rows_rejected += 1
                rejected_bad_mmsi += 1
                continue

            if not ts:
                total_rows_rejected += 1
                rejected_bad_timestamp += 1
                continue

            try:
                lat_val = float(lat)
                lon_val = float(lon)
                if not (-90 <= lat_val <= 90 and -180 <= lon_val <= 180):
                    total_rows_rejected += 1
                    rejected_bad_coords += 1
                    continue
            except Exception:
                total_rows_rejected += 1
                rejected_bad_coords += 1
                continue

            total_rows_valid += 1

            bucket_id = stable_bucket_id(mmsi, num_buckets)
            buffers[bucket_id].append(row)
            bucket_row_counts[bucket_id] += 1

            if len(buffers[bucket_id]) >= flush_every:
                queues[bucket_id].put(buffers[bucket_id])
                bucket_chunk_counts[bucket_id] += 1
                buffers[bucket_id] = []

    for bucket_id in range(num_buckets):
        if buffers[bucket_id]:
            queues[bucket_id].put(buffers[bucket_id])
            bucket_chunk_counts[bucket_id] += 1

    for q in queues:
        q.put(None)

    for p in workers:
        p.join()

    elapsed = time.time() - start_time
    main_peak_mem_mb = max(main_peak_mem_mb, get_memory_usage_mb())

    print("\n=== Partitioning Summary ===")
    print(f"Total rows read:      {total_rows_read}")
    print(f"Valid rows kept:      {total_rows_valid}")
    print(f"Rejected rows:        {total_rows_rejected}")
    print(f"  - bad MMSI:         {rejected_bad_mmsi}")
    print(f"  - bad timestamp:    {rejected_bad_timestamp}")
    print(f"  - bad coordinates:  {rejected_bad_coords}")
    print(f"Elapsed time (sec):   {elapsed:.2f}")
    print(f"Main peak RSS (MB):   {main_peak_mem_mb:.2f}")

    print("\n=== Bucket Distribution ===")
    for i in range(num_buckets):
        print(
            f"Bucket {i}: rows={bucket_row_counts[i]}, "
            f"chunks_sent={bucket_chunk_counts[i]}"
        )

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    files = [
        os.path.join(base_dir, "aisdk-2026-02-27.csv"),
        os.path.join(base_dir, "aisdk-2026-02-28.csv"),
    ]

    for file_path in files:
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        out_dir = os.path.join(base_dir, "partitions", file_name)

        print("=" * 80)
        print("Processing:", file_path)

        if not os.path.exists(file_path):
            print(f"Skipping missing file: {file_path}")
            continue

        stream_partition_csv_parallel(
            input_csv=file_path,
            output_dir=out_dir,
            num_buckets=8,
            flush_every=5000,
        )
import csv
import time
import os
from multiprocessing import Process, Queue

from Task1.utils import (
    get_memory_usage_mb,
    is_valid_mmsi,
    stable_bucket_id,
    has_valid_coordinates,
)


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
        process = Process(
            target=bucket_worker,
            args=(bucket_id, queues[bucket_id], output_dir)
        )
        process.start()
        workers.append(process)

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
            timestamp = row.get("# Timestamp", "").strip()
            latitude = row.get("Latitude", "").strip()
            longitude = row.get("Longitude", "").strip()

            if not is_valid_mmsi(mmsi):
                total_rows_rejected += 1
                rejected_bad_mmsi += 1
                continue

            if not timestamp:
                total_rows_rejected += 1
                rejected_bad_timestamp += 1
                continue

            if not has_valid_coordinates(latitude, longitude):
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

    for queue in queues:
        queue.put(None)

    for process in workers:
        process.join()

    failed_workers = [p for p in workers if p.exitcode != 0]
    if failed_workers:
        failed_info = ", ".join(
            f"PID {p.pid} exitcode={p.exitcode}" for p in failed_workers
        )
        raise RuntimeError(f"Worker(s) failed: {failed_info}")

    elapsed = time.time() - start_time
    main_peak_mem_mb = max(main_peak_mem_mb, get_memory_usage_mb())

    print("\nPartitioning Summary:")
    print(f"Total rows read: {total_rows_read}")
    print(f"Valid rows kept: {total_rows_valid}")
    print(f"Total rows rejected: {total_rows_rejected}")
    print(f"  for bad MMSI: {rejected_bad_mmsi}")
    print(f"  for bad timestamp: {rejected_bad_timestamp}")
    print(f"  for bad coordinates: {rejected_bad_coords}")
    print(f"Elapsed time (sec): {elapsed:.2f}")
    print(f"Main peak RSS (MB): {main_peak_mem_mb:.2f}")

    print("\nBucket Distribution:")
    for bucket_id in range(num_buckets):
        print(
            f"Bucket {bucket_id}: rows={bucket_row_counts[bucket_id]}, "
            f"chunks_sent={bucket_chunk_counts[bucket_id]}"
        )


def find_csv_files(input_dir):
    if not os.path.isdir(input_dir):
        return []

    csv_files = []
    for name in os.listdir(input_dir):
        full_path = os.path.join(input_dir, name)
        if os.path.isfile(full_path) and name.lower().endswith(".csv"):
            csv_files.append(full_path)

    return sorted(csv_files)


if __name__ == "__main__":
    task_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(task_dir)

    input_dir = os.path.join(project_root, "data")
    output_base_dir = os.path.join(task_dir, "output")

    os.makedirs(output_base_dir, exist_ok=True)

    files = find_csv_files(input_dir)

    if not files:
        print(f"No CSV files found in input directory: {input_dir}")

    for file_path in files:
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        output_dir = os.path.join(output_base_dir, file_name)

        print("Processing:", file_path)
        print("Output to: ", output_dir)

        stream_partition_csv_parallel(
            input_csv=file_path,
            output_dir=output_dir,
            num_buckets=8,
            flush_every=5000,
        )

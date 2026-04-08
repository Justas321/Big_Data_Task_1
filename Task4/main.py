import time
import matplotlib.pyplot as plt
import os
import csv

from Task1.main import stream_partition_csv_parallel, find_csv_files
from Task1.utils import is_valid_mmsi, get_memory_usage_mb
from Task2.main import process_bucket, run_parallel_reduce, find_dataset_dirs

# ---------------------------------------------------------
# Part 0: Memory profiling should be done: firtly install necessesary libraries
# pip install memory-profiler matplotlib
# Run script mprof run script.py
# Plot the results: mprof plot
# ---------------------------------------------------------

# ---------------------------------------------------------
# Part 1: Sequential Analysis
# ---------------------------------------------------------
def stream_partition_sequential(input_csv, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    output_csv = os.path.join(output_dir, "bucket_0.csv")
    main_peak_mem_mb = get_memory_usage_mb()
    total_rows_read = 0
    total_rows_valid = 0
    total_rows_rejected = 0
    rejected_bad_mmsi = 0
    rejected_bad_coords = 0
    rejected_bad_timestamp = 0

    with open(input_csv, "r", newline="", encoding="utf-8") as f, \
        open(output_csv, "w", newline="", encoding="utf-8") as o:
        reader = csv.DictReader(f)
        writer = csv.DictWriter(o, fieldnames=reader.fieldnames)
        writer.writeheader()

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
            writer.writerow(row)
    main_peak_mem_mb = max(main_peak_mem_mb, get_memory_usage_mb())

    print("\n=== Partitioning Summary ===")
    print(f"Total rows read:      {total_rows_read}")
    print(f"Valid rows kept:      {total_rows_valid}")
    print(f"Rejected rows:        {total_rows_rejected}")
    print(f"  - bad MMSI:         {rejected_bad_mmsi}")
    print(f"  - bad timestamp:    {rejected_bad_timestamp}")
    print(f"  - bad coordinates:  {rejected_bad_coords}")
    print(f"Main peak RSS (MB):   {main_peak_mem_mb:.2f}")



def run_sequential_reduce(task1_bucket_dir, reduced_dir, num_buckets=8):

    for bucket_id in range(num_buckets):
        bucket_csv_path = os.path.join(task1_bucket_dir, f"bucket_{bucket_id}.csv")
        output_csv_path = os.path.join(reduced_dir, f"reduced_bucket_{bucket_id}.csv")
        process_bucket(bucket_id, bucket_csv_path, output_csv_path)    



# ---------------------------------------------------------
# Part 2: Speedup Analysis
# ---------------------------------------------------------
def analyze_speedup():
    print("--- Starting Speedup Analysis ---")

    task_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(task_dir)

    input_dir = os.path.join(project_root, "data")
    output_base_dir = os.path.join(task_dir, "output")
    task1_output_dir = os.path.join(project_root, "Task4", "output")
    reduced_seq_root = os.path.join(output_base_dir, "reduced", "sequential_partitions")
    reduced_par_root = os.path.join(output_base_dir, "reduced", "parallel_partitions")

    os.makedirs(output_base_dir, exist_ok=True)
    os.makedirs(reduced_seq_root, exist_ok=True)
    os.makedirs(reduced_par_root, exist_ok=True)

    num_buckets = 8
    dataset_dirs = find_dataset_dirs(task1_output_dir)
    if not dataset_dirs:
        print(f"No output directories found in: {task1_output_dir}")
        raise SystemExit(1)

    files = find_csv_files(input_dir)
    if not files:
        print(f"No CSV files found in input directory: {input_dir}")

    
    # --- Sequential Analysis ---
    print("--- Starting Sequential Analysis ---")
    start_seq = time.perf_counter()

    for file_path in files:
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        reduced_seq_dir_for_file = os.path.join(reduced_seq_root, file_name)

        print("=" * 80)
        print("Processing:", file_path)

        if not os.path.exists(file_path):
            print(f"Skipping missing file: {file_path}")
            continue

        stream_partition_sequential(
            input_csv=file_path,
            output_dir=reduced_seq_dir_for_file,
        )

        for dataset_dir in dataset_dirs:
            dataset_name = os.path.basename(dataset_dir)
            reduced_dir = os.path.join(reduced_seq_root, file_name)
            bucket_source_dir = os.path.join(task1_output_dir, dataset_name, "sequential_partitions", file_name)

            os.makedirs(reduced_dir, exist_ok=True)

            print(f"Processing bucket set: {dataset_name}", flush=True)
            print(f"Input buckets: {bucket_source_dir}", flush=True)
            print(f"Reduced output: {reduced_dir}", flush=True)

            run_sequential_reduce(
                task1_bucket_dir=bucket_source_dir,
                reduced_dir=reduced_dir,
                num_buckets=num_buckets,
            )

    t_seq = time.perf_counter() - start_seq
    print(f"Sequential Time: {t_seq:.4f} seconds")

    # Measure Parallel Time 
    print("--- Starting Parallel Analysis ---")    
    start_par = time.perf_counter()
    for file_path in files:
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        reduced_par_dir_for_file = os.path.join(reduced_par_root, file_name)

        print("=" * 80)
        print("Processing:", file_path)

        if not os.path.exists(file_path):
            print(f"Skipping missing file: {file_path}")
            continue
        
        stream_partition_csv_parallel(
            input_csv=file_path,
            output_dir=reduced_par_dir_for_file,
            num_buckets=num_buckets,
            flush_every=5000,
        )

        for dataset_dir in dataset_dirs:
            dataset_name = os.path.basename(dataset_dir)
            reduced_dir = os.path.join(reduced_par_root, file_name)
            bucket_source_dir = os.path.join(task1_output_dir, dataset_name, "parallel_partitions", file_name)

            os.makedirs(reduced_dir, exist_ok=True)

            print(f"Processing bucket set: {dataset_name}", flush=True)
            print(f"Input buckets: {bucket_source_dir}", flush=True)
            print(f"Reduced output: {reduced_dir}", flush=True)

            run_parallel_reduce(
                task1_bucket_dir=bucket_source_dir,
                reduced_dir=reduced_dir,
                num_buckets=num_buckets,
            )
    
    t_par = time.perf_counter() - start_par
    print(f"Parallel Time: {t_par:.4f} seconds")

    # Formula: S = T(sequential) / T(parallel)
    speedup = t_seq / t_par
    print(f"Speedup Factor (S): {speedup:.2f}x\n")

# ---------------------------------------------------------
# Part 3: Chunk Optimization
# ---------------------------------------------------------
def optimize_chunks():
    print("--- Starting Chunk Optimization ---")
    chunk_sizes = [5000, 10000, 50000, 100000, 250000, 500000]
    execution_times = []
    task_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(task_dir)
    input_dir = os.path.join(project_root, "data")
    output_base_dir = os.path.join(task_dir, "output")
    files = find_csv_files(input_dir)

    if not files:
        print(f"No CSV files found in input directory: {input_dir}")

    for size in chunk_sizes:
        print(f"Testing chunk size: {size}")
        start_time = time.perf_counter()
        
        for file_path in files:
            file_name = os.path.splitext(os.path.basename(file_path))[0]
            out_dir = os.path.join(output_base_dir, f"{size}_partitions", file_name)

            print("=" * 80)
            print("Processing:", file_path)

            if not os.path.exists(file_path):
                print(f"Skipping missing file: {file_path}")
                continue

            stream_partition_csv_parallel(
                input_csv=file_path,
                output_dir=out_dir,
                num_buckets=8,
                flush_every=size,
            )

            run_parallel_reduce(
                task1_bucket_dir=out_dir,
                reduced_dir=out_dir,
                num_buckets=8,
            )
        
        elapsed = time.perf_counter() - start_time
        execution_times.append(elapsed)

    plt.figure(figsize=(10, 6))
    plt.plot(chunk_sizes, execution_times, marker='o', linestyle='-', color='b')
    plt.title('Impact of Chunk Size on Parallel Execution Time')
    plt.xlabel('Rows per Chunk')
    plt.ylabel('Execution Time (seconds)')
    plt.grid(True)
    
    plt.savefig('chunk_optimization_plot.png')
    print("Plot saved as 'chunk_optimization_plot.png'")
    plt.show()

if __name__ == "__main__":
    analyze_speedup()
    optimize_chunks()
    
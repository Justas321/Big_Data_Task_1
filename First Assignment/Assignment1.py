import polars as pl
import time
import os
from multiprocessing import Pool
import psutil

# Function fixes MB
def fix_mb (value):
    return value/(1024*1024)

# Function to check if column has a valid MMSI - Later used in read_csv_in_chunks file or MMSI column
def is_valid_mmsi_expr(col_name: str):
    mmsi = pl.col(col_name).cast(pl.String).str.strip_chars()
    mid = mmsi.str.slice(0, 3).cast(pl.Int32)
    
    return (
        mmsi.is_not_null() &
        mmsi.str.contains(r"^\d{9}$") &
        (mmsi != "000000000") &
        mid.is_between(201, 775)
    )


# Function that reads csv in chunks already filtering out invalid MMSI
# Shortly to describe logic:
# Firstly scans a file and already filters out not valid MMSI
# Secondly loops through polars batches (Through 60000 rows at the time)
# Thirdly it creates temporary csv file and writes there filtered rows
# Fourthly if it is first chunk it writes in csv, if it is not, it appends ('a', 'w' logic)
def read_csv_in_chunks(file_path, chunk_size: int = 60000):
    temp_output = f"temp_{os.path.basename(file_path)}"
    lf = (pl.scan_csv(file_path).filter(is_valid_mmsi_expr("MMSI")))
    lf = lf.rename({"# Timestamp": "Timestamp"})
    lf = lf.with_columns(pl.col("Timestamp").str.to_datetime())
    lf = lf.sort(["MMSI", "Timestamp"])
    first_chunk = True
    for i, df_chunk in enumerate(lf.collect_batches(chunk_size=chunk_size), start=1):
        with open(temp_output, "a" if not first_chunk else "w", newline="", encoding="utf-8") as f:
            df_chunk.write_csv(f, include_header=first_chunk)

        print(f"Batch {i}: {df_chunk.height} rows")
        first_chunk = False
    return temp_output

# Runs code in parallel using cpu_cores / 2 workers
# It merges two temp files in to one big csv
def run_code_in_parallel(files, final_output):
    start = time.perf_counter()
    workers = os.cpu_count() // 2
    print(f"Our script will be using {workers} workers")
    process = psutil.Process()
    with Pool(processes=workers) as pool:
        temp_files = pool.map(read_csv_in_chunks, files)
        mem_rss = process.memory_info().rss

    if temp_files:
        pl.scan_csv(temp_files).sink_csv(final_output)

    duration = time.perf_counter() - start
    print(f"Total time: {duration:.2f}s",
           f"Memory RSS {fix_mb(mem_rss):.2f} MB")

if __name__ == "__main__":
    files = ["aisdk-2026-02-27.csv", "aisdk-2026-02-28.csv"]
    output = "filtered_ship_data.csv"
    run_code_in_parallel(files, output)
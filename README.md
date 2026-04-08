# Big Data Assignment 1 - Shadow Fleet Detection on AIS Streams

## Launching Process
Run all commands from the project root.

### 1) Environment Setup
Install Python 3.10+ and create a virtual environment:

Windows:
```bash
python -m venv .venv
.venv\Scripts\activate
```

macOS / Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install required packages. Run commands from project root:
```bash
pip install -r requirements.txt
```

### 2) Input Data
Download Maritime AIS CSV files from [http://aisdata.ais.dk/](http://aisdata.ais.dk/) and place your downloaded data in `data/`. Make sure to extract the .csv files from .zip

Example:
```text
data/aisdk-2026-02-27.csv
data/aisdk-2026-02-28.csv
```

### 3) Run the Pipeline
Execute stages in this exact order:

```bash
python -m Task1.main
python -m Task2.main
python -m Task3.main
python -m Task4.main
```

### 4) Outputs
```text
Task1/output/<dataset>/bucket_0.csv ... bucket_7.csv
Task2/output/<dataset>/reduced_bucket_0.csv ... reduced_bucket_7.csv
Task3/output/<dataset>/findings/acd_findings_bucket_*.csv
Task3/output/<dataset>/anomaly_b/b_findings.csv
Task3/output/<dataset>/anomaly_b/b_summary.csv
Task3/output/<dataset>/final_scores.csv
```

## Introduction
This repository implements a parallel, low-memory analytics pipeline for gigabyte-scale AIS data.

## Dataset
- Source dataset: Danish Maritime data ([http://aisdata.ais.dk/](http://aisdata.ais.dk/)).
- Two-day subset selected for analysis (2026-02-27, 2026-02-28)

## Implementation

### Task1 - Streaming Partitioner
Implementation:
- Reads each AIS CSV row-by-row using `csv.DictReader`.
- Applies strict data validation before partitioning:
  - MMSI format and known invalid/default MMSI filtering,
  - timestamp presence,
  - coordinate range validation.
- Uses deterministic partitioning by vessel identity.
- Uses `multiprocessing.Process` workers (one per bucket) and bounded `Queue(maxsize=8)`.

### Task2 - Parallel Reduce
Implementation:
- Spawns one reducer process per bucket produced by Stage 1.
- Normalizes selected columns and parses timestamps.
- Performs external sorting to keep memory bounded:
  1. Writes normalized temporary stream.
  2. Sorts fixed-size chunks (`200000` rows) by `MMSI` and `timestamp` values.
  3. Merges sorted chunks with `heapq` into a single ordered stream.
- Computes vessel state transitions per event:
  - `event_index`,
  - previous `timestamp`/`coordinates`,
  - `delta_seconds`,
  - `distance_from_prev_nm`,
  - collects `SOG` and `Draught` values.

Result:
- Chronologically ordered vessel events with derived movement features for anomaly analytics.

### Task 3 - Anomaly Detection and DFSI score calculation
Implementation:
- Runs A/C/D detection in parallel across reduced buckets.
- Implements B detection as a separate subsystem to merge buckets:
  - creates 15-minute low-speed vessel summaries (`SOG < 1`),
  - finds vessel pairs within 500 meters,
  - requires continuity longer than 2 hours.

Implemented thresholds:
- Anomaly A: AIS gap `> 4h` and vessel movement during the gap.
- Anomaly B: two distinct MMSI within `500m`, `SOG < 1 kn`, duration `> 2h`.
- Anomaly C: draught change `> 5%` during blackout `> 2h`.
- Anomaly D: implied speed `> 60 kn` between consecutive points.

### Task 4 - Performance Evaluation
Implementation:
- sequential vs parallel speedup experiments,
- chunk-size sensitivity experiments,
- memory profiling workflow (`memory_profiler`, `mprof`) and plotting.

### Presentation Preparation
Implementation:
- provides html maps of vessel trajectories for selected MMSI (highest calculated DFSI vessels for both tested days),
- loads reduced AIS tracks from Task2 outputs,
- reads DFSI values and anomaly counts from Task3 final scores,
- plots vessel paths with hourly position markers and results in a legend.

## Repository Structure
```text
Task1/
  main.py
  utils.py
Task2/
  main.py
  utils.py
Task3/
  main.py
  anomaly_b.py
  utils.py
Task4/
  main.py
Presentation/
data/
requirements.txt
```

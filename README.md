# Big Data Assignment 1

## Setup

Install Python 3.10+.

Create and activate a virtual environment.

### Windows
```bash
python -m venv .venv
.venv\Scripts\activate
```
macOS / Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Data

Download AIS CSV data from:

http://aisdata.ais.dk/

Put the files into:
data

Example:
```bash
data/aisdk-2026-02-27.csv
data/aisdk-2026-02-28.csv
```

## Task 1

From the project root, run:
```bash
python -m Task1.main.py
```
Task 1 reads all .csv files in data directory and writes partitioned output to:

Task1/output/

## Task 2

From the project root, run:
```bash
python -m Task2.main
```
Task 2 reads bucket files produced by Task 1 from:
```bash
Task1/output/
```
and writes reduced output to:
```bash
Task2/output/
```
Example:
```bash
Task2/output/aisdk-2026-02-27/reduced_bucket_0.csv
Task2/output/aisdk-2026-02-27/reduced_bucket_1.csv
Task2/output/aisdk-2026-02-27/reduced_bucket_2.csv
```
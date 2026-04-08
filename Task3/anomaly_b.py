import csv
import os
from collections import defaultdict
from datetime import datetime, timedelta

from Task2.utils import parse_timestamp, safe_float
from Task3.utils import distance_nm

B_MAX_SOG = 1.0
B_MAX_DISTANCE_METERS = 500.0
B_MIN_DURATION_HOURS = 2.0

WINDOW_MINUTES = 15
WINDOW_DURATION = timedelta(minutes=WINDOW_MINUTES)
MIN_REQUIRED_WINDOWS = int((B_MIN_DURATION_HOURS * 60) / WINDOW_MINUTES) + 1

# ~1.1 km latitude cell size.
GRID_DEGREES = 0.01

WINDOW_FIELDNAMES = [
    "window_start",
    "MMSI",
    "avg_latitude",
    "avg_longitude",
    "avg_SOG",
    "point_count",
]

B_FINDING_FIELDS = [
    "MMSI",
    "other_MMSI",
    "anomaly_type",
    "start_timestamp",
    "end_timestamp",
    "duration_hours",
    "min_distance_m",
    "details",
]


def floor_to_window(ts):
    minute = (ts.minute // WINDOW_MINUTES) * WINDOW_MINUTES
    return ts.replace(minute=minute, second=0, microsecond=0)


def window_filename(window_start):
    return f"window_{window_start.strftime('%Y%m%d_%H%M')}.csv"


def parse_window_filename(filename):
    name = filename.replace("window_", "").replace(".csv", "")
    return datetime.strptime(name, "%Y%m%d_%H%M")


def grid_cell(lat, lon):
    return int(lat / GRID_DEGREES), int(lon / GRID_DEGREES)


def neighboring_cells(cell_x, cell_y):
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            yield (cell_x + dx, cell_y + dy)


def pair_key(m1, m2):
    return (m1, m2) if m1 < m2 else (m2, m1)


def build_b_window_summaries(task2_dataset_dir, window_dir):
    os.makedirs(window_dir, exist_ok=True)
    for name in os.listdir(window_dir):
        if name.startswith("window_") and name.endswith(".csv"):
            os.remove(os.path.join(window_dir, name))

    writers = {}
    handles = {}

    rows_read = 0
    low_speed_rows = 0
    summaries_written = 0

    def get_writer(window_start):
        filename = window_filename(window_start)
        path = os.path.join(window_dir, filename)

        if path not in writers:
            f = open(path, "w", newline="", encoding="utf-8")
            writer = csv.DictWriter(f, fieldnames=WINDOW_FIELDNAMES)
            writer.writeheader()
            handles[path] = f
            writers[path] = writer

        return writers[path]

    def flush_summary(mmsi, window_start, count, sum_lat, sum_lon, sum_sog):
        nonlocal summaries_written

        if mmsi is None or window_start is None or count == 0:
            return

        writer = get_writer(window_start)
        writer.writerow({
            "window_start": window_start.isoformat(sep=" "),
            "MMSI": mmsi,
            "avg_latitude": sum_lat / count,
            "avg_longitude": sum_lon / count,
            "avg_SOG": sum_sog / count,
            "point_count": count,
        })
        summaries_written += 1

    try:
        for name in sorted(os.listdir(task2_dataset_dir)):
            if not (name.startswith("reduced_bucket_") and name.endswith(".csv")):
                continue

            path = os.path.join(task2_dataset_dir, name)

            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                current_mmsi = None
                current_window = None
                count = 0
                sum_lat = 0.0
                sum_lon = 0.0
                sum_sog = 0.0

                for row in reader:
                    rows_read += 1

                    sog = safe_float(row.get("SOG"))
                    if sog is None or sog >= B_MAX_SOG:
                        continue

                    mmsi = (row.get("MMSI") or "").strip()
                    ts = parse_timestamp(row.get("timestamp"))
                    lat = safe_float(row.get("latitude"))
                    lon = safe_float(row.get("longitude"))

                    if not mmsi or ts is None or lat is None or lon is None:
                        continue

                    low_speed_rows += 1
                    win = floor_to_window(ts)

                    if mmsi != current_mmsi or win != current_window:
                        flush_summary(current_mmsi, current_window, count, sum_lat, sum_lon, sum_sog)

                        current_mmsi = mmsi
                        current_window = win
                        count = 1
                        sum_lat = lat
                        sum_lon = lon
                        sum_sog = sog
                    else:
                        count += 1
                        sum_lat += lat
                        sum_lon += lon
                        sum_sog += sog

                flush_summary(current_mmsi, current_window, count, sum_lat, sum_lon, sum_sog)

    finally:
        for f in handles.values():
            f.close()

    print(
        f"Window summaries built: rows_read={rows_read}, "
        f"low_speed_rows={low_speed_rows}, summaries_written={summaries_written}",
        flush=True,
    )


def finalize_pair(pair, state, findings_writer, vessel_counts):
    if state["window_count"] < MIN_REQUIRED_WINDOWS:
        return

    duration_hours = (state["window_count"] * WINDOW_MINUTES) / 60.0
    m1, m2 = pair

    vessel_counts[m1] += 1
    vessel_counts[m2] += 1

    findings_writer.writerow({
        "MMSI": m1,
        "other_MMSI": m2,
        "anomaly_type": "B",
        "start_timestamp": state["start_window"].isoformat(sep=" "),
        "end_timestamp": (state["end_window"] + WINDOW_DURATION).isoformat(sep=" "),
        "duration_hours": round(duration_hours, 4),
        "min_distance_m": round(state["min_distance_m"], 3),
        "details": "Loitering / possible transfer",
    })

    findings_writer.writerow({
        "MMSI": m2,
        "other_MMSI": m1,
        "anomaly_type": "B",
        "start_timestamp": state["start_window"].isoformat(sep=" "),
        "end_timestamp": (state["end_window"] + WINDOW_DURATION).isoformat(sep=" "),
        "duration_hours": round(duration_hours, 4),
        "min_distance_m": round(state["min_distance_m"], 3),
        "details": "Loitering / possible transfer",
    })


def detect_b_from_windows(window_dir, findings_csv_path, summary_csv_path):
    os.makedirs(os.path.dirname(findings_csv_path), exist_ok=True)

    window_files = sorted(
        name for name in os.listdir(window_dir)
        if name.startswith("window_") and name.endswith(".csv")
    )

    active_pairs = {}
    vessel_counts = defaultdict(int)

    with open(findings_csv_path, "w", newline="", encoding="utf-8") as out_f:
        findings_writer = csv.DictWriter(out_f, fieldnames=B_FINDING_FIELDS)
        findings_writer.writeheader()

        for filename in window_files:
            window_start = parse_window_filename(filename)
            path = os.path.join(window_dir, filename)

            rows = []
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mmsi = (row.get("MMSI") or "").strip()
                    lat = safe_float(row.get("avg_latitude"))
                    lon = safe_float(row.get("avg_longitude"))

                    if not mmsi or lat is None or lon is None:
                        continue

                    rows.append({
                        "MMSI": mmsi,
                        "latitude": lat,
                        "longitude": lon,
                    })

            grid = defaultdict(list)
            seen_pairs = {}

            for row in rows:
                lat = row["latitude"]
                lon = row["longitude"]
                mmsi = row["MMSI"]
                cell_x, cell_y = grid_cell(lat, lon)

                for nx, ny in neighboring_cells(cell_x, cell_y):
                    for other in grid.get((nx, ny), []):
                        if other["MMSI"] == mmsi:
                            continue

                        if abs(lat - other["latitude"]) > 0.01:
                            continue
                        if abs(lon - other["longitude"]) > 0.02:
                            continue

                        dist_m = distance_nm(
                            lat, lon,
                            other["latitude"], other["longitude"]
                        ) * 1852.0

                        if dist_m > B_MAX_DISTANCE_METERS:
                            continue

                        pair = pair_key(mmsi, other["MMSI"])
                        if pair not in seen_pairs:
                            seen_pairs[pair] = dist_m
                        else:
                            seen_pairs[pair] = min(seen_pairs[pair], dist_m)

                grid[(cell_x, cell_y)].append(row)

            updated_pairs = set()

            for pair, dist_m in seen_pairs.items():
                if pair not in active_pairs:
                    active_pairs[pair] = {
                        "start_window": window_start,
                        "end_window": window_start,
                        "window_count": 1,
                        "min_distance_m": dist_m,
                    }
                else:
                    prev_end = active_pairs[pair]["end_window"]

                    if window_start - prev_end == WINDOW_DURATION:
                        active_pairs[pair]["end_window"] = window_start
                        active_pairs[pair]["window_count"] += 1
                        active_pairs[pair]["min_distance_m"] = min(
                            active_pairs[pair]["min_distance_m"], dist_m
                        )
                    else:
                        finalize_pair(pair, active_pairs[pair], findings_writer, vessel_counts)
                        active_pairs[pair] = {
                            "start_window": window_start,
                            "end_window": window_start,
                            "window_count": 1,
                            "min_distance_m": dist_m,
                        }

                updated_pairs.add(pair)

            stale_pairs = []
            for pair, state in active_pairs.items():
                if pair not in updated_pairs and state["end_window"] < window_start:
                    stale_pairs.append(pair)

            for pair in stale_pairs:
                finalize_pair(pair, active_pairs[pair], findings_writer, vessel_counts)
                del active_pairs[pair]

    with open(findings_csv_path, "a", newline="", encoding="utf-8") as out_f:
        findings_writer = csv.DictWriter(out_f, fieldnames=B_FINDING_FIELDS)
        for pair, state in list(active_pairs.items()):
            finalize_pair(pair, state, findings_writer, vessel_counts)

    with open(summary_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["MMSI", "B_count"])
        writer.writeheader()

        for mmsi in sorted(vessel_counts.keys()):
            writer.writerow({
                "MMSI": mmsi,
                "B_count": vessel_counts[mmsi],
            })

    print(
        f"Detection finished: window_files={len(window_files)}, vessels={len(vessel_counts)}",
        flush=True,
    )

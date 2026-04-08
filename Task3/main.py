import csv
import os
from multiprocessing import Process
from collections import defaultdict

from Task3.anomaly_b import (
    build_b_window_summaries,
    detect_b_from_windows,
)
from Task2.utils import (
    parse_timestamp,
    safe_float,
    detect_column,
)
from Task3.utils import (
    distance_nm,
    compute_gap_hours,
    compute_implied_speed_knots,
    compute_draught_change_pct,
)

NUM_BUCKETS = 8

A_MIN_GAP_HOURS = 4.0
A_MIN_MOVEMENT_NM = 2.0

C_MIN_GAP_HOURS = 2.0
C_MIN_DRAUGHT_CHANGE_PCT = 5.0

D_MAX_SPEED_KNOTS = 60.0

FINDING_FIELDS = [
    "MMSI",
    "anomaly_type",
    "start_timestamp",
    "end_timestamp",
    "start_latitude",
    "start_longitude",
    "end_latitude",
    "end_longitude",
    "gap_hours",
    "distance_nm",
    "implied_speed_knots",
    "draught_before",
    "draught_after",
    "draught_change_pct",
    "details",
]


def detect_anomaly_a(gap_hours, dist_nm):
    return gap_hours > A_MIN_GAP_HOURS and dist_nm >= A_MIN_MOVEMENT_NM


def detect_anomaly_c(gap_hours, prev_draught, curr_draught):
    if gap_hours <= C_MIN_GAP_HOURS:
        return False, None

    draught_change_pct = compute_draught_change_pct(prev_draught, curr_draught)
    if draught_change_pct is None:
        return False, None

    return draught_change_pct > C_MIN_DRAUGHT_CHANGE_PCT, draught_change_pct


def detect_anomaly_d(implied_speed_knots):
    return implied_speed_knots > D_MAX_SPEED_KNOTS


def write_finding(
    writer,
    mmsi,
    anomaly_type,
    prev_ts,
    ts,
    prev_lat,
    prev_lon,
    lat,
    lon,
    gap_hours,
    dist_nm,
    implied_speed_knots,
    draught_before="",
    draught_after="",
    draught_change_pct="",
    details="",
):
    writer.writerow({
        "MMSI": mmsi,
        "anomaly_type": anomaly_type,
        "start_timestamp": prev_ts.isoformat(sep=" "),
        "end_timestamp": ts.isoformat(sep=" "),
        "start_latitude": prev_lat,
        "start_longitude": prev_lon,
        "end_latitude": lat,
        "end_longitude": lon,
        "gap_hours": round(gap_hours, 4),
        "distance_nm": round(dist_nm, 4),
        "implied_speed_knots": round(implied_speed_knots, 4),
        "draught_before": draught_before,
        "draught_after": draught_after,
        "draught_change_pct": "" if draught_change_pct == "" else round(draught_change_pct, 4),
        "details": details,
    })


def calculate_dfsi(max_gap_hours, total_impossible_jump_nm, c_count):
    return (max_gap_hours / 2.0) + (total_impossible_jump_nm / 10.0) + (c_count * 15.0)


def process_bucket(bucket_id, reduced_csv_path, findings_csv_path, summary_csv_path):
    print(f"[Worker {bucket_id}] Processing {reduced_csv_path}", flush=True)

    if not os.path.exists(reduced_csv_path):
        print(f"[Worker {bucket_id}] Missing reduced bucket, skipping.", flush=True)
        return

    vessel_stats = defaultdict(lambda: {
        "A_count": 0,
        "C_count": 0,
        "D_count": 0,
        "max_gap_hours": 0.0,
        "total_impossible_jump_nm": 0.0,
    })

    prev_draught_by_mmsi = {}
    rows_read = 0
    findings_written = 0

    os.makedirs(os.path.dirname(findings_csv_path), exist_ok=True)

    with open(reduced_csv_path, "r", newline="", encoding="utf-8") as in_f, \
         open(findings_csv_path, "w", newline="", encoding="utf-8") as out_f:

        reader = csv.DictReader(in_f)
        writer = csv.DictWriter(out_f, fieldnames=FINDING_FIELDS)
        writer.writeheader()

        if not reader.fieldnames:
            print(f"[Task3 Worker {bucket_id}] Empty file.", flush=True)
            return

        header = {name: name for name in reader.fieldnames}

        mmsi_col = detect_column(header, ["MMSI"])
        ts_col = detect_column(header, ["timestamp"])
        lat_col = detect_column(header, ["latitude"])
        lon_col = detect_column(header, ["longitude"])
        prev_ts_col = detect_column(header, ["prev_timestamp"])
        prev_lat_col = detect_column(header, ["prev_latitude"])
        prev_lon_col = detect_column(header, ["prev_longitude"])
        draught_col = detect_column(header, ["Draught"])

        if not all([mmsi_col, ts_col, lat_col, lon_col, prev_ts_col, prev_lat_col, prev_lon_col]):
            raise ValueError(
                f"Bucket {bucket_id} missing required columns. Found columns: {reader.fieldnames}"
            )

        for row in reader:
            rows_read += 1

            mmsi = (row.get(mmsi_col) or "").strip()
            ts = parse_timestamp(row.get(ts_col))
            lat = safe_float(row.get(lat_col))
            lon = safe_float(row.get(lon_col))
            prev_ts = parse_timestamp(row.get(prev_ts_col))
            prev_lat = safe_float(row.get(prev_lat_col))
            prev_lon = safe_float(row.get(prev_lon_col))
            draught = safe_float(row.get(draught_col)) if draught_col else None

            if not mmsi or ts is None or lat is None or lon is None:
                continue

            if prev_ts is None or prev_lat is None or prev_lon is None:
                if draught is not None:
                    prev_draught_by_mmsi[mmsi] = draught
                continue

            gap_hours = compute_gap_hours(prev_ts, ts)
            if gap_hours <= 0:
                if draught is not None:
                    prev_draught_by_mmsi[mmsi] = draught
                continue

            dist_nm = distance_nm(prev_lat, prev_lon, lat, lon)
            implied_speed_knots = compute_implied_speed_knots(dist_nm, gap_hours)
            prev_draught = prev_draught_by_mmsi.get(mmsi)

            # A detection
            if detect_anomaly_a(gap_hours, dist_nm):
                write_finding(
                    writer=writer,
                    mmsi=mmsi,
                    anomaly_type="A",
                    prev_ts=prev_ts,
                    ts=ts,
                    prev_lat=prev_lat,
                    prev_lon=prev_lon,
                    lat=lat,
                    lon=lon,
                    gap_hours=gap_hours,
                    dist_nm=dist_nm,
                    implied_speed_knots=implied_speed_knots,
                    details="A anomaly type",
                )
                vessel_stats[mmsi]["A_count"] += 1
                vessel_stats[mmsi]["max_gap_hours"] = max(vessel_stats[mmsi]["max_gap_hours"], gap_hours)
                findings_written += 1

            # C anomaly
            is_c, draught_change_pct = detect_anomaly_c(gap_hours, prev_draught, draught)
            if is_c:
                write_finding(
                    writer=writer,
                    mmsi=mmsi,
                    anomaly_type="C",
                    prev_ts=prev_ts,
                    ts=ts,
                    prev_lat=prev_lat,
                    prev_lon=prev_lon,
                    lat=lat,
                    lon=lon,
                    gap_hours=gap_hours,
                    dist_nm=dist_nm,
                    implied_speed_knots=implied_speed_knots,
                    draught_before=prev_draught,
                    draught_after=draught,
                    draught_change_pct=draught_change_pct,
                    details="C anomaly type",
                )
                vessel_stats[mmsi]["C_count"] += 1
                findings_written += 1

            # D anomaly
            if detect_anomaly_d(implied_speed_knots):
                write_finding(
                    writer=writer,
                    mmsi=mmsi,
                    anomaly_type="D",
                    prev_ts=prev_ts,
                    ts=ts,
                    prev_lat=prev_lat,
                    prev_lon=prev_lon,
                    lat=lat,
                    lon=lon,
                    gap_hours=gap_hours,
                    dist_nm=dist_nm,
                    implied_speed_knots=implied_speed_knots,
                    details="D anomaly type",
                )
                vessel_stats[mmsi]["D_count"] += 1
                vessel_stats[mmsi]["total_impossible_jump_nm"] += dist_nm
                findings_written += 1

            if draught is not None:
                prev_draught_by_mmsi[mmsi] = draught

    with open(summary_csv_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "MMSI",
            "A_count",
            "C_count",
            "D_count",
            "max_gap_hours",
            "total_impossible_jump_nm",
            "DFSI",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for mmsi in sorted(vessel_stats.keys()):
            stats = vessel_stats[mmsi]
            dfsi = calculate_dfsi(
                max_gap_hours=stats["max_gap_hours"],
                total_impossible_jump_nm=stats["total_impossible_jump_nm"],
                c_count=stats["C_count"],
            )

            writer.writerow({
                "MMSI": mmsi,
                "A_count": stats["A_count"],
                "C_count": stats["C_count"],
                "D_count": stats["D_count"],
                "max_gap_hours": round(stats["max_gap_hours"], 4),
                "total_impossible_jump_nm": round(stats["total_impossible_jump_nm"], 4),
                "DFSI": round(dfsi, 4),
            })

    print(
        f"[Task3 Worker {bucket_id}] rows_read={rows_read}, findings_written={findings_written}, vessels={len(vessel_stats)}",
        flush=True
    )


def run_parallel_task3(task2_dataset_dir, task3_dataset_dir, num_buckets=NUM_BUCKETS):
    os.makedirs(task3_dataset_dir, exist_ok=True)

    findings_dir = os.path.join(task3_dataset_dir, "findings")
    summaries_dir = os.path.join(task3_dataset_dir, "summaries")
    os.makedirs(findings_dir, exist_ok=True)
    os.makedirs(summaries_dir, exist_ok=True)

    workers = []

    for bucket_id in range(num_buckets):
        reduced_csv_path = os.path.join(task2_dataset_dir, f"reduced_bucket_{bucket_id}.csv")
        findings_csv_path = os.path.join(findings_dir, f"acd_findings_bucket_{bucket_id}.csv")
        summary_csv_path = os.path.join(summaries_dir, f"acd_summary_bucket_{bucket_id}.csv")

        p = Process(
            target=process_bucket,
            args=(bucket_id, reduced_csv_path, findings_csv_path, summary_csv_path)
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
        raise RuntimeError(f"Task3 worker(s) failed: {details}")


def merge_bucket_summaries(task3_dataset_dir, final_scores_csv):
    summaries_dir = os.path.join(task3_dataset_dir, "summaries")
    b_summary_csv = os.path.join(task3_dataset_dir, "anomaly_b", "b_summary.csv")

    totals = defaultdict(lambda: {
        "A_count": 0,
        "B_count": 0,
        "C_count": 0,
        "D_count": 0,
        "max_gap_hours": 0.0,
        "total_impossible_jump_nm": 0.0,
    })

    # Merge A/C/D summaries
    if os.path.isdir(summaries_dir):
        for name in sorted(os.listdir(summaries_dir)):
            if not name.endswith(".csv"):
                continue

            path = os.path.join(summaries_dir, name)

            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mmsi = (row.get("MMSI") or "").strip()
                    if not mmsi:
                        continue

                    totals[mmsi]["A_count"] += int(row.get("A_count") or 0)
                    totals[mmsi]["C_count"] += int(row.get("C_count") or 0)
                    totals[mmsi]["D_count"] += int(row.get("D_count") or 0)
                    totals[mmsi]["max_gap_hours"] = max(
                        totals[mmsi]["max_gap_hours"],
                        float(row.get("max_gap_hours") or 0.0)
                    )
                    totals[mmsi]["total_impossible_jump_nm"] += float(
                        row.get("total_impossible_jump_nm") or 0.0
                    )

    # Merge B summary
    if os.path.exists(b_summary_csv):
        with open(b_summary_csv, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi = (row.get("MMSI") or "").strip()
                if not mmsi:
                    continue

                totals[mmsi]["B_count"] += int(row.get("B_count") or 0)

    with open(final_scores_csv, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "MMSI",
            "A_count",
            "B_count",
            "C_count",
            "D_count",
            "max_gap_hours",
            "total_impossible_jump_nm",
            "DFSI",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for mmsi in sorted(totals.keys()):
            stats = totals[mmsi]
            dfsi = calculate_dfsi(
                max_gap_hours=stats["max_gap_hours"],
                total_impossible_jump_nm=stats["total_impossible_jump_nm"],
                c_count=stats["C_count"],
            )

            writer.writerow({
                "MMSI": mmsi,
                "A_count": stats["A_count"],
                "B_count": stats["B_count"],
                "C_count": stats["C_count"],
                "D_count": stats["D_count"],
                "max_gap_hours": round(stats["max_gap_hours"], 4),
                "total_impossible_jump_nm": round(stats["total_impossible_jump_nm"], 4),
                "DFSI": round(dfsi, 4),
            })


def find_dataset_dirs(task2_output_dir):
    if not os.path.isdir(task2_output_dir):
        return []

    dataset_dirs = []
    for name in os.listdir(task2_output_dir):
        full_path = os.path.join(task2_output_dir, name)
        if os.path.isdir(full_path):
            dataset_dirs.append(full_path)

    return sorted(dataset_dirs)


if __name__ == "__main__":
    task3_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(task3_dir)

    task2_output_root = os.path.join(project_root, "Task2", "output")
    task3_output_root = os.path.join(task3_dir, "output")

    os.makedirs(task3_output_root, exist_ok=True)

    dataset_dirs = find_dataset_dirs(task2_output_root)

    if not dataset_dirs:
        print(f"No Task2 dataset directories found in: {task2_output_root}")
        raise SystemExit(1)

    for dataset_dir in dataset_dirs:
        dataset_name = os.path.basename(dataset_dir)
        task3_dataset_dir = os.path.join(task3_output_root, dataset_name)

        print(f"Processing bucket set: {dataset_name}", flush=True)

        run_parallel_task3(
            task2_dataset_dir=dataset_dir,
            task3_dataset_dir=task3_dataset_dir,
            num_buckets=NUM_BUCKETS,
        )

        b_dir = os.path.join(task3_dataset_dir, "anomaly_b")
        os.makedirs(b_dir, exist_ok=True)

        b_findings_csv = os.path.join(b_dir, "b_findings.csv")
        b_summary_csv = os.path.join(b_dir, "b_summary.csv")

        print(f"Starting anomaly B window summarization for {dataset_name}", flush=True)
        build_b_window_summaries(
            task2_dataset_dir=dataset_dir,
            window_dir=b_dir,
        )

        print(f"Starting anomaly B detection for {dataset_name}", flush=True)
        detect_b_from_windows(
            window_dir=b_dir,
            findings_csv_path=b_findings_csv,
            summary_csv_path=b_summary_csv,
        )

        print(f"Finished anomaly B for {dataset_name}", flush=True)

        final_scores_csv = os.path.join(task3_dataset_dir, "final_scores.csv")
        merge_bucket_summaries(
            task3_dataset_dir=task3_dataset_dir,
            final_scores_csv=final_scores_csv,
        )
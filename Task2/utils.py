from datetime import datetime
from typing import Optional
import math


def parse_timestamp(ts_str: str) -> Optional[datetime]:
    ts_str = (ts_str or "").strip()
    if not ts_str:
        return None

    formats = [
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue

    return None


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(a))


def km_to_nm(km):
    return km / 1.852


def safe_float(value: str) -> Optional[float]:
    try:
        return float((value or "").strip())
    except (ValueError, AttributeError):
        return None


def detect_column(row: dict, candidates):
    for name in candidates:
        if name in row:
            return name
    return None

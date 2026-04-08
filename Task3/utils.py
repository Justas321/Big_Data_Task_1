from Task2.utils import haversine_km, km_to_nm


def distance_nm(lat1, lon1, lat2, lon2):
    return km_to_nm(haversine_km(lat1, lon1, lat2, lon2))


def compute_gap_hours(prev_ts, curr_ts):
    return (curr_ts - prev_ts).total_seconds() / 3600.0


def compute_implied_speed_knots(distance_nm_value, gap_hours):
    if gap_hours <= 0:
        return 0.0
    return distance_nm_value / gap_hours


def compute_draught_change_pct(prev_draught, curr_draught):
    if prev_draught is None or curr_draught is None:
        return None
    if prev_draught <= 0:
        return None
    return abs((curr_draught - prev_draught) / prev_draught) * 100.0

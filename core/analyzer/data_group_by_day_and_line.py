from datetime import datetime
from typing import Iterable, Dict, Any



def group_name_by_hour_and_line(
        records: Iterable[Dict[str, Any]],
        include_records: bool = True
) -> Dict[str, Any]:

    by_hour: Dict[str, Dict[str, Any]] = {f"{h:02d}": {} for h in range(24)}

    def is_fail(v: Any) -> bool:
        # Treat exactly "1" (int or str) as fail
        try:
            if isinstance(v, str):
                v = v.strip()
            return str(v) == "1"
        except Exception:
            return False

    for r in records:
        try:
            ts = r["collected_timestamp"]
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            hour = dt.strftime("%H")
            group = r.get("group_name", "")
            if not group:
                continue
        except (KeyError, ValueError):
            # Skip malformed records
            continue

        groups_for_hour = by_hour[hour]
        bucket = groups_for_hour.get(group)
        if bucket is None:
            bucket = {
                "count": 0,
                "units_pass": 0,
                "units_fail": 0,
            }
            if include_records:
                bucket["records"] = []
            groups_for_hour[group] = bucket

        # Update counters
        if is_fail(r.get("error_flag", 0)):
            bucket["units_fail"] += 1
        else:
            bucket["units_pass"] += 1
        bucket["count"] += 1

        # Append record
        if include_records:
            bucket["records"].append({
                "ppid": r.get("ppid"),
                "collected_timestamp": ts,
                "model_name": r.get("model_name"),
                "line_name": r.get("line_name"),
                "group_name": group,
                "next_station": r.get("next_station"),
                "error_flag": r.get("error_flag"),
            })

    # Sort records by timestamp (ascending) within each group/hour
    if include_records:
        for hour_groups in by_hour.values():
            for bucket in hour_groups.values():
                bucket["records"].sort(key=lambda x: x["collected_timestamp"])

    # Build 24_hours_by_group: {group_name: [ {hour: int, units_pass: int, units_fail: int}, ... ] }
    # Ensure each group has entries for all 24 hours (0-23)
    all_groups = set()
    for hour_groups in by_hour.values():
        all_groups.update(hour_groups.keys())

    data: Dict[str, Any] = {}
    for group in all_groups:
        # Initialize 24 hours with zeros
        data[group] = [{"hour": h, "units_pass": 0, "units_fail": 0} for h in range(24)]

    # Fill counts from by_hour
    for hour_str, hour_groups in by_hour.items():
        h = int(hour_str)
        for group, bucket in hour_groups.items():
            data[group][h]["units_pass"] += int(bucket.get("units_pass", 0))
            data[group][h]["units_fail"] += int(bucket.get("units_fail", 0))

    # logic for 24_hours_by_group
    return {"by_hour": by_hour, "hours_by_group": data}

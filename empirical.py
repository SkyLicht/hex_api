import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Dict, Any

from core.db.sfc_clon_db import SQLiteReadOnlyConnection

def group_group_name_by_hour(
        records: Iterable[Dict[str, Any]],
        include_records: bool = True
) -> Dict[str, Any]:
    """
    Return shape:
    {
      "by_hour": {
        "00": {
          "<group_name>": {
            "count": <int>,
            "units_pass": <int>,  # error_flag == 0
            "units_fail": <int>,  # error_flag == 1
            "records": [ ... ]    # present only if include_records is True
          },
          ...
        },
        ...
        "23": { ... }
      }
    }
    """
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

    return {"by_hour": by_hour}



if __name__ == "__main__":
    db = SQLiteReadOnlyConnection()

    res = db.execute_query("""SELECT ppid,
                                     group_name,
                                     next_station,
                                     collected_timestamp,
                                     model_name,
                                     line_name,
                                     error_flag
                              FROM records_table
                              WHERE date(collected_timestamp) = '2025-08-20'
                                AND line_name = 'J01'
                              ORDER BY collected_timestamp;""")


    resr = group_group_name_by_hour(res)

    print(json.dumps(resr, indent=4))

    json_str = json.dumps(resr, indent=4, ensure_ascii=False)
    Path("output/by_hour.json").write_text(json_str, encoding="utf-8")


# python
from __future__ import annotations
from typing import List, Dict, Any, Optional
from datetime import datetime
from collections import defaultdict
import statistics

DEFAULT_STATIONS = ['PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT', 'FT1', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING']

def _parse_dt(v: Any) -> Optional[datetime]:
    """
    Parse a timestamp that might be:
    - None
    - 'nan' or '' string
    - SQLite text 'YYYY-MM-DD HH:MM:SS'
    - ISO-like strings
    - datetime instance
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s or s.lower() == 'nan':
            return None
        # Try flexible parsing
        try:
            # fromisoformat accepts 'YYYY-MM-DD HH:MM:SS' as well
            return datetime.fromisoformat(s)
        except Exception:
            pass
        # Fallback to common format
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
    return None

def _safe_percentile(sorted_values: List[float], q: float) -> Optional[float]:
    n = len(sorted_values)
    if n == 0:
        return None
    if n == 1:
        return float(sorted_values[0])
    pos = (n - 1) * q
    lower = int(pos)
    upper = min(lower + 1, n - 1)
    frac = pos - lower
    return float(sorted_values[lower] * (1 - frac) + sorted_values[upper] * frac)

def _safe_percentiles(values: List[float]) -> Dict[str, Optional[float]]:
    if not values:
        return {"p25": None, "p75": None, "p90": None}
    s = sorted(values)
    return {
        "p25": _safe_percentile(s, 0.25),
        "p75": _safe_percentile(s, 0.75),
        "p90": _safe_percentile(s, 0.90),
    }

def compute_hourly_ct_table(
        process_flow_data: List[Dict[str, Any]],
        stations: Optional[List[str]] = None,
        max_cycle_seconds: int = 3600
) -> Dict[str, Any]:
    """
    Build a per-hour cycle-time table using the upstream station completion hour.
    - Uses timestamps (ignores *_sec fields) to compute CTs between adjacent stations.
    - Counts upstream events even if downstream is missing (incomplete PPIDs).
    - Drops non-positive and extreme cycle times (> max_cycle_seconds).
    """
    st = stations or DEFAULT_STATIONS
    pair_keys = [f"{a}_to_{b}" for a, b in zip(st[:-1], st[1:])]

    # Aggregation: date -> hour -> pair_key -> list of CTs + counters
    agg_cycles = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))      # List[float] diffs
    agg_up = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))          # upstream events
    agg_down = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))        # downstream present (valid CT)

    for rec in process_flow_data:
        for a, b in zip(st[:-1], st[1:]):
            a_ts = _parse_dt(rec.get(a))
            b_ts = _parse_dt(rec.get(b))
            if not a_ts:
                continue
            date_key = a_ts.strftime("%Y-%m-%d")
            hour_key = a_ts.hour
            key = f"{a}_to_{b}"

            # Count upstream completion this hour
            agg_up[date_key][hour_key][key] += 1

            # If we have downstream, compute CT
            if b_ts:
                diff = (b_ts - a_ts).total_seconds()
                if 0 < diff <= max_cycle_seconds:
                    agg_cycles[date_key][hour_key][key].append(diff)
                    agg_down[date_key][hour_key][key] += 1

    # Build result
    result: Dict[str, Any] = {
        "meta": {
            "stations": st,
            "max_cycle_seconds": max_cycle_seconds,
            "explanation": "Stats grouped by hour of upstream completion; incomplete PPIDs contribute to upstream_events but not sample_size."
        },
        "by_date": {}
    }

    for date_key in sorted(agg_up.keys()):
        hours_out: Dict[str, Any] = {}
        for hr in sorted(agg_up[date_key].keys()):
            station_pairs_stats: Dict[str, Any] = {}
            any_upstream = False

            for pair in pair_keys:
                up = agg_up[date_key][hr].get(pair, 0)
                down = agg_down[date_key][hr].get(pair, 0)
                cycles = agg_cycles[date_key][hr].get(pair, [])

                if up > 0:
                    any_upstream = True

                if cycles:
                    pct = _safe_percentiles(cycles)
                    avg = statistics.mean(cycles)
                    med = statistics.median(cycles)
                    std = statistics.stdev(cycles) if len(cycles) > 1 else 0.0
                    station_pairs_stats[pair] = {
                        "sample_size": len(cycles),
                        "average_seconds": round(avg, 2),
                        "mean_seconds": round(avg, 2),
                        "median_seconds": round(med, 2),
                        "std_dev_seconds": round(std, 2),
                        "p25_seconds": round(pct["p25"], 2) if pct["p25"] is not None else None,
                        "p75_seconds": round(pct["p75"], 2) if pct["p75"] is not None else None,
                        "p90_seconds": round(pct["p90"], 2) if pct["p90"] is not None else None,
                        "min_seconds": round(min(cycles), 2),
                        "max_seconds": round(max(cycles), 2),
                        "upstream_events": up,
                        "downstream_present": down
                    }
                else:
                    station_pairs_stats[pair] = {
                        "sample_size": 0,
                        "average_seconds": None,
                        "mean_seconds": None,
                        "median_seconds": None,
                        "std_dev_seconds": None,
                        "p25_seconds": None,
                        "p75_seconds": None,
                        "p90_seconds": None,
                        "min_seconds": None,
                        "max_seconds": None,
                        "upstream_events": up,
                        "downstream_present": down
                    }

            if any_upstream:
                hours_out[f"{hr:02d}:00"] = {
                    "hour": hr,
                    "station_pairs": station_pairs_stats
                }

        result["by_date"][date_key] = {"hours": hours_out}

    return result

# Example usage:
# data = get_expected_packing_query(database, line_name)  # returns List[Dict[str, Any]]
# hourly = compute_hourly_ct_table(data, max_cycle_seconds=7200)
# return hourly
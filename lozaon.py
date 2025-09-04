# Python
from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Counter, Mapping, List
from zoneinfo import ZoneInfo

from core.db.ppid_record_db import SQLiteReadOnlyConnection


# def import_production_excel_to_sqlite(
#         file: str | Path,
#         database_path: str | Path,
#         batch_size: int = 1_000,
#         convert_to_local: bool = False,
# ) -> int:
#     """
#     Import the entire Excel/CSV file into SQLite (no row limit), in batches.
#
#     - Streams rows and inserts with INSERT OR IGNORE.
#     - Uses a single transaction with PRAGMAs for speed.
#     - Counts actual inserted rows via total_changes delta.
#
#     Args:
#         file: Path to .xlsx/.xls or delimited text (.csv/.tsv).
#         database_path: Path to SQLite DB file.
#         batch_size: Number of rows per executemany batch.
#         convert_to_local: If True, treat source as UTC/GMT and convert to local time.
#
#     Returns:
#         int: Inserted rows (duplicates ignored).
#     """
#     file = Path(file)
#     database_path = Path(database_path)
#
#     conn = sqlite3.connect(str(database_path))
#     try:
#         _ensure_records_table(conn)
#         conn.execute("PRAGMA journal_mode=WAL;")
#         conn.execute("PRAGMA synchronous=NORMAL;")
#         conn.execute("PRAGMA temp_store=MEMORY;")
#         conn.execute("PRAGMA cache_size=-32000;")
#
#         insert_sql = """
#                      INSERT OR IGNORE INTO records_table (
#                          id,
#                          ppid,
#                          work_order,
#                          collected_timestamp,
#                          employee_name,
#                          group_name,
#                          line_name,
#                          station_name,
#                          model_name,
#                          error_flag,
#                          next_station
#                      )
#                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
#                      """
#
#         rows_iter = _read_rows_with_normalized_headers(file)
#         batch: list[tuple] = []
#
#         before_total = conn.total_changes
#         conn.execute("BEGIN")
#         try:
#             for src in rows_iter:
#                 try:
#                     mapped = _map_row_to_db_fields(src, convert_to_local=convert_to_local)
#                     if mapped is None:
#                         continue
#                     batch.append(mapped)
#
#                     if len(batch) >= batch_size:
#                         conn.executemany(insert_sql, batch)
#                         batch.clear()
#                 except Exception:
#                     # Skip malformed rows
#                     continue
#
#             if batch:
#                 conn.executemany(insert_sql, batch)
#                 batch.clear()
#
#             conn.commit()
#         except Exception:
#             conn.rollback()
#             raise
#
#         inserted_total = conn.total_changes - before_total
#         return inserted_total
#
#     finally:
#         conn.close()
#
#
# def _read_rows_with_normalized_headers(file: Path) -> Iterable[Dict[str, Any]]:
#     """
#     Yield rows as dicts with normalized uppercase underscore headers.
#     Supports .xlsx/.xls via pandas, and .csv/.tsv via csv module.
#     """
#     def normalize(name: str) -> str:
#         return name.strip().upper().replace(" ", "_")
#
#     if file.suffix.lower() in {".xlsx", ".xls"}:
#         try:
#             import pandas as pd
#         except ImportError as e:
#             raise RuntimeError("Reading Excel requires pandas. Please install it and retry.") from e
#
#         df = pd.read_excel(file)
#         df.columns = [normalize(str(c)) for c in df.columns]
#         for _, row in df.iterrows():
#             yield {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
#     else:
#         with file.open("r", encoding="utf-8", newline="") as f:
#             sample = f.read(2048)
#             f.seek(0)
#             try:
#                 dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
#             except csv.Error:
#                 dialect = csv.getdialect("excel")
#             reader = csv.DictReader(f, dialect=dialect)
#             if reader.fieldnames is None:
#                 return
#             reader.fieldnames = [normalize(str(c)) for c in reader.fieldnames]
#             for row in reader:
#                 yield {k: v for k, v in row.items()}
#
#
# def _ensure_records_table(conn: sqlite3.Connection) -> None:
#     conn.execute("""
#                  CREATE TABLE IF NOT EXISTS records_table (
#                                                               id TEXT PRIMARY KEY,
#                                                               ppid TEXT NOT NULL CHECK(length(ppid) <= 23),
#                                                               work_order TEXT NOT NULL CHECK(length(work_order) <= 12),
#                                                               collected_timestamp DATETIME NOT NULL,
#                                                               employee_name TEXT NOT NULL CHECK(length(employee_name) <= 16),
#                                                               group_name TEXT NOT NULL CHECK(length(group_name) <= 23),
#                                                               line_name TEXT NOT NULL CHECK(length(line_name) <= 3),
#                                                               station_name TEXT NOT NULL CHECK(length(station_name) <= 23),
#                                                               model_name TEXT NOT NULL CHECK(length(model_name) <= 5),
#                                                               error_flag INTEGER NOT NULL DEFAULT 0,
#                                                               next_station TEXT CHECK(length(next_station) <= 16),
#                                                               UNIQUE(ppid, collected_timestamp, line_name, station_name, group_name) ON CONFLICT IGNORE
#                  ) WITHOUT ROWID;
#                  """)
#
#
# def _parse_timestamp(ts: Any, convert_to_local: bool = False) -> str:
#     """
#     Parse timestamps including 'Wed, 20 Aug 2025 12:00:01 GMT'.
#     Returns 'YYYY-MM-DD HH:MM:SS'. No TZ conversion unless convert_to_local=True.
#     """
#     if ts is None:
#         raise ValueError("Missing timestamp")
#     s = str(ts).strip()
#     sup = s.upper()
#
#     fmts = [
#         "%a, %d %b %Y %H:%M:%S %Z",  # with TZ label
#         "%a, %d %b %Y %H:%M:%S",     # without TZ
#         "%Y-%m-%d %H:%M:%S",
#         "%m/%d/%Y %H:%M:%S",
#         "%d/%m/%Y %H:%M:%S",
#     ]
#
#     last_err: Optional[Exception] = None
#     for fmt in fmts:
#         try:
#             dt = datetime.strptime(s, fmt)
#             if convert_to_local:
#                 src_tz = ZoneInfo("UTC") if ("GMT" in sup) else ZoneInfo("UTC")
#                 dt_local = dt.replace(tzinfo=src_tz).astimezone()
#                 return dt_local.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
#             return dt.strftime("%Y-%m-%d %H:%M:%S")
#         except ValueError as e:
#             last_err = e
#             continue
#
#     if sup.endswith(" GMT"):
#         s2 = s[:-4].rstrip()
#         try:
#             dt = datetime.strptime(s2, "%a, %d %b %Y %H:%M:%S")
#             if convert_to_local:
#                 dt_local = dt.replace(tzinfo=ZoneInfo("UTC")).astimezone()
#                 return dt_local.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
#             return dt.strftime("%Y-%m-%d %H:%M:%S")
#         except ValueError:
#             pass
#
#     raise ValueError(f"Unrecognized timestamp format: {s}") from last_err
#
#
# def _coerce_int01(v: Any) -> int:
#     try:
#         iv = int(str(v).strip())
#         return 1 if iv == 1 else 0
#     except Exception:
#         return 0
#
#
# def _safe_str(v: Any, max_len: int) -> str:
#     s = "" if v is None else str(v).strip()
#     return s[:max_len]
#
#
# def _make_id(ppid: str, ts: str, line: str, station: str, group: str) -> str:
#     key = f"{ppid}|{ts}|{line}|{station}|{group}"
#     return hashlib.sha1(key.encode("utf-8")).hexdigest()
#
#
# def _map_row_to_db_fields(r: Dict[str, Any], convert_to_local: bool = False) -> Optional[tuple]:
#     """
#     Map a source row into the target table tuple in correct order.
#     Applies length constraints and parsing.
#     """
#     ppid = _safe_str(r.get("SERIAL_NUMBER"), 23)
#     if not ppid:
#         return None
#
#     collected_timestamp = _parse_timestamp(
#         r.get("IN_STATION_TIME") or r.get("COLLECTED_TIMESTAMP"),
#         convert_to_local=convert_to_local,
#         )
#     group_name = _safe_str(r.get("GROUP_NAME"), 23)
#     line_name = _safe_str(r.get("LINE_NAME"), 3)
#     station_name = _safe_str(r.get("STATION_NAME"), 23)
#     model_name = _safe_str(r.get("MODEL_NAME"), 5)
#     next_station = _safe_str(r.get("NEXT_STATION"), 16)
#     error_flag = _coerce_int01(r.get("ERROR_FLAG"))
#
#     work_order = _safe_str(r.get("WORK_ORDER") or "", 12)
#     employee_name = _safe_str(r.get("EMPLOYEE_NAME") or "", 16)
#
#     rec_id = _make_id(ppid, collected_timestamp, line_name, station_name, group_name)
#
#     return (
#         rec_id,
#         ppid,
#         work_order,
#         collected_timestamp,
#         employee_name,
#         group_name,
#         line_name,
#         station_name,
#         model_name,
#         error_flag,
#         next_station,
#     )
#

def group_t_from_by_hour_next_hour_pass(records: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """
    Group by t_from hour and count records where t_to >= the next-hour boundary after t_from.

    Input items must have keys: 'ppid', 't_from', 't_to' (YYYY-MM-DD HH:MM:SS).
    'dwell_min' is optional; if missing, it will be computed.

    Returns JSON-friendly dict:
    {
      "hours": {
        "00": { "count": int, "ppids": [ {"ppid": str, "dwell_min": int}, ... ] },
        ...
        "23": { "count": int, "ppids": [ ... ] }
      },
      "total": int
    }
    """
    hours: Dict[str, Dict[str, Any]] = {
        f"{h:02d}": {"count": 0, "ppids": []} for h in range(24)
    }

    def parse(ts: str) -> datetime:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")

    for rec in records:
        try:
            ppid = str(rec["ppid"])
            t_from = parse(str(rec["t_from"]))
            t_to = parse(str(rec["t_to"]))
        except Exception:
            # Skip malformed/partial records
            continue

        next_hour_boundary = t_from.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

        if t_to >= next_hour_boundary:
            # Use provided dwell_min if present; otherwise compute it
            if "dwell_min" in rec and rec["dwell_min"] is not None:
                try:
                    dwell_min = int(rec["dwell_min"])
                except Exception:
                    dwell_min = int(round((t_to - t_from).total_seconds() / 60.0))
            else:
                dwell_min = int(round((t_to - t_from).total_seconds() / 60.0))

            hour_key = t_from.strftime("%H")
            hours[hour_key]["ppids"].append({"ppid": ppid, "dwell_min": dwell_min, "from": rec["t_from"], "to": rec["t_to"]})

    total = 0
    for hour_key in hours:
        count = len(hours[hour_key]["ppids"])
        hours[hour_key]["count"] = count
        total += count

    return {"hours": hours, "total": total}

def merge_hourly_counts(
        hour_by_hour_smt: Iterable[Mapping[str, Any]],
        hour_by_hour_packing: Iterable[Mapping[str, Any]],
        pass_result: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    Merge hour_by_hour_count with the result from group_t_from_by_hour_next_hour_pass(res).

    Inputs:
      - hour_by_hour_count: iterable of {"hour": "HH" or int, "record_count": int}
      - pass_result: {"hours": {"HH": {"count": int, "ppids": [...]}, ...}, "total": int}

    Output:
      {
        "hours": {
          "00": { "count": <pass_count>, "acutal_packing": <record_count>, "ppids": [...] },
          ...
          "23": { ... }
        },
        "total": <same as pass_result["total"]>
      }
    """
    # Start with the pass_result hours as the base
    base_hours: Dict[str, Dict[str, Any]] = pass_result.get("hours", {})

    # Build merged structure with default acutal_packing = 0
    merged_hours: Dict[str, Dict[str, Any]] = {}
    for h in [f"{i:02d}" for i in range(24)]:
        bucket = base_hours.get(h, {"count": 0, "ppids": []})
        merged_hours[h] = {
            "units_held": int(bucket.get("count", 0)),
            'actual_smt': 0,
            'actual_packing': 0,
            "ppids": list(bucket.get("ppids", [])),
        }

    # Helper to normalize hour keys
    def norm_hour(h: Any) -> str:
        try:
            if isinstance(h, int):
                return f"{h:02d}"
            s = str(h).strip()
            if s.isdigit():
                return f"{int(s):02d}"
            # If it's already "HH" keep it; otherwise ignore
            return s if len(s) == 2 and s.isdigit() else None
        except Exception:
            return None

    # Overlay acutal_packing from hour_by_hour_count
    for item in hour_by_hour_packing:
        h_raw = item.get("hour")
        hh = norm_hour(h_raw)
        if hh is None or hh not in merged_hours:
            continue
        try:
            rc = int(item.get("record_count", 0))
        except Exception:
            rc = 0
        merged_hours[hh]['actual_packing'] = rc
    for item in hour_by_hour_smt:
        h_raw = item.get("hour")
        hh = norm_hour(h_raw)
        if hh is None or hh not in merged_hours:
            continue
        try:
            rc = int(item.get("record_count", 0))
        except Exception:
            rc = 0
        merged_hours[hh]['actual_smt'] = rc

    return {
        "hours": merged_hours,
        "total": int(pass_result.get("total", 0))
    }
def fetch_output_hour_counts(
        db, target_date: str, line_name: str, group_name: str = "PACKING"
) -> List[Dict[str, Any]]:
    """
    Return hour-by-hour counts for a given date, line, and group (default PACKING).

    Args:
        db: Database connection with execute_query method.
        target_date: 'YYYY-MM-DD'
        line_name: e.g., 'J01'
        group_name: e.g., 'PACKING' (default)

    Returns:
        List[Dict[str, Any]] like:
        [{'hour': '00', 'record_count': 40}, ...]
    """
    query = """
            SELECT
                strftime('%H', collected_timestamp) AS hour,
                COUNT(*) AS record_count
            FROM records_table
            WHERE date(collected_timestamp) = ?
              AND line_name = ?
              AND group_name = ?
                AND error_flag = 0
            GROUP BY strftime('%H', collected_timestamp)
            ORDER BY hour; \
            """
    return db.execute_query(query, (target_date, line_name, group_name))


def fetch_final_to_packing_dwell(
        db, target_date: str, line_name: str
) -> List[Dict[str, Any]]:
    """
    Return pairs of FINAL INSPECT -> PACKING per PPID for a date and line, with dwell_min.

    Args:
        db: Database connection with execute_query method.
        target_date: 'YYYY-MM-DD'
        line_name: e.g., 'J01'

    Returns:
        List[Dict[str, Any]] like:
        [
          {
            'ppid': '...',
            't_from': 'YYYY-MM-DD HH:MM:SS',
            't_to': 'YYYY-MM-DD HH:MM:SS',
            'dwell_min': 90
          },
          ...
        ]
    """
    query = """
            WITH
                base AS (
                    SELECT *
                    FROM records_table
                ),
                from_ev AS (
                    SELECT ppid, MIN(collected_timestamp) AS t_from
                    FROM base
                    WHERE group_name = 'FINAL INSPECT'
                      AND date(collected_timestamp) = ?
                      AND line_name  = ?
                      AND error_flag = 0
                    GROUP BY ppid
                ),
                to_ev AS (
                    SELECT r.ppid, MIN(r.collected_timestamp) AS t_to
                    FROM base r
                             JOIN from_ev f ON f.ppid = r.ppid
                    WHERE r.group_name = 'PACKING'
                      AND r.line_name  = ?
                      AND r.error_flag = 0
                      AND r.collected_timestamp > f.t_from
                    GROUP BY r.ppid
                ),
                pairs AS (
                    SELECT f.ppid, f.t_from, k.t_to
                    FROM from_ev f
                             JOIN to_ev   k USING (ppid)
                )
            SELECT
                p.ppid,
                p.t_from,
                p.t_to,
                CAST(ROUND((julianday(p.t_to) - julianday(p.t_from)) * 1440) AS INT) AS dwell_min
            FROM pairs p
            ORDER BY t_from; \
            """
    return db.execute_query(query, (target_date, line_name, line_name))
def list_dates_in_range(start_date: str, end_date: str) -> List[str]:
    """
    Return a list of date strings ('YYYY-MM-DD') from start_date to end_date inclusive.

    Args:
        start_date: 'YYYY-MM-DD'
        end_date:   'YYYY-MM-DD'

    Raises:
        ValueError: If start_date > end_date or format is invalid.

    Example:
        list_dates_in_range("2025-08-10", "2025-08-19")
        -> ["2025-08-10", "2025-08-11", ..., "2025-08-19"]
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if start > end:
        raise ValueError("start_date must be less than or equal to end_date")

    total_days = (end - start).days
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(total_days + 1)]


# python
from typing import Iterable, Mapping, Any, Dict, List, Optional, Tuple
import os

def build_ppid_hold_rows(
        merged: Mapping[str, Any],
        date_str: str,
        line_name: str,
        ppid_times: Optional[Iterable[Mapping[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Build a flat list of rows from the merged structure, one row per PPID.

    Columns per row:
      date, line, hour, units_held, actual_smt, actual_packing, ppid, dwell_min, from, to

    Args:
      merged: Output of merge_hourly_counts(...)
              {
                "hours": {
                  "HH": {
                    "units_held": int,
                    "actual_smt": int,
                    "actual_packing": int,
                    "ppids": [ {"ppid": str, "dwell_min": int, "t_from": "...", "t_to": "..."}, ... ]
                  }, ...
                },
                "total": int
              }
      date_str: 'YYYY-MM-DD'
      line_name: line identifier (e.g., 'J01')
      ppid_times: Optional iterable with items containing keys {'ppid','t_from','t_to'}.
                  If provided, used to fill missing 'from'/'to' per PPID.

    Returns:
      List of dict rows ready to export.
    """
    # Optional lookup for from/to by PPID
    times_lookup: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    if ppid_times:
        for it in ppid_times:
            try:
                k = str(it.get("ppid"))
                if not k:
                    continue
                times_lookup[k] = (it.get("t_from"), it.get("t_to"))
            except Exception:
                continue

    rows: List[Dict[str, Any]] = []
    hours = merged.get("hours", {}) if isinstance(merged, dict) else {}

    for hour, bucket in hours.items():
        units_held = int(bucket.get("units_held", 0))
        actual_smt = int(bucket.get("actual_smt", 0))
        actual_packing = int(bucket.get("actual_packing", 0))
        ppids_list = bucket.get("ppids", []) or []

        for p in ppids_list:
            # p can be a dict {"ppid":..., "dwell_min":..., "t_from":..., "t_to":...} or a raw string PPID
            if isinstance(p, dict):
                ppid = p.get("ppid")
                dwell_min = p.get("dwell_min")
                t_from = p.get("t_from") or p.get("from")
                t_to = p.get("t_to") or p.get("to")
            else:
                ppid = p
                dwell_min, t_from, t_to = None, None, None

            if (t_from is None or t_to is None) and ppid in times_lookup:
                lt_from, lt_to = times_lookup[ppid]
                t_from = t_from or lt_from
                t_to = t_to or lt_to

            rows.append({
                "date": date_str,
                "line": line_name,
                "hour": str(hour),
                "units_held": units_held,
                "actual_smt": actual_smt,
                "actual_packing": actual_packing,
                "ppid": ppid,
                "dwell_min": dwell_min,
                "from": t_from,
                "to": t_to,
            })

    return rows


def export_ppid_hold_rows_to_excel(rows: List[Dict[str, Any]], file_path: str) -> str:
    """
    Export rows to an Excel file (.xlsx). Falls back to CSV if the Excel engine is unavailable.

    Args:
      rows: List of dictionaries (use build_ppid_hold_rows to create them)
      file_path: Target file path (e.g., 'C:/path/report.xlsx')

    Returns:
      The path written (may be a .csv if Excel engine not available).
    """
    # Desired column order
    columns = [
        "date", "line", "hour",
        "units_held", "actual_smt", "actual_packing",
        "ppid", "dwell_min", "from", "to"
    ]

    try:
        import pandas as pd
        df = pd.DataFrame(rows)
        # Ensure column order (missing columns will be added empty)
        for col in columns:
            if col not in df.columns:
                df[col] = None
        df = df[columns]

        # Try Excel first
        if file_path.lower().endswith(".xlsx"):
            try:
                df.to_excel(file_path, index=False)
                return file_path
            except ImportError:
                # Fallback to CSV if Excel engine not installed
                csv_path = os.path.splitext(file_path)[0] + ".csv"
                df.to_csv(csv_path, index=False, encoding="utf-8")
                return csv_path
        else:
            # If not .xlsx, write CSV
            df.to_csv(file_path, index=False, encoding="utf-8")
            return file_path
    except Exception as e:
        raise RuntimeError(f"Failed to export report: {e}")


def export_hold_report_from_merged(
        merged: Mapping[str, Any],
        date_str: str,
        line_name: str,
        file_path: str,
        ppid_times: Optional[Iterable[Mapping[str, Any]]] = None,
) -> str:
    """
    Convenience wrapper: build rows from merged and export to Excel (or CSV fallback).

    Args:
      merged: Result of merge_hourly_counts(...)
      date_str: 'YYYY-MM-DD'
      line_name: e.g., 'J01'
      file_path: output path (e.g., 'C:/reports/hold_report.xlsx')
      ppid_times: Optional iterable to enrich missing 'from'/'to' by PPID

    Returns:
      Path written (xlsx or csv).
    """
    rows = build_ppid_hold_rows(merged, date_str, line_name, ppid_times=ppid_times)
    return export_ppid_hold_rows_to_excel(rows, file_path)
def run_hold_report_over_range(
        start_date: str,
        end_date: str,
        line_name: str,
        output_dir: str = "output",
) -> str:
    """
    Build a hold report over a date range (inclusive) and export to Excel.

    Output columns per row:
      date, line, hour, units_held, actual_smt, actual_packing, ppid, dwell_min, from, to

    Returns:
      Path to the written Excel (or CSV fallback) file.
    """
    # 1) Prepare date list and DB
    dates: List[str] = list_dates_in_range(start_date, end_date)
    db = SQLiteReadOnlyConnection()

    all_rows = []

    # 2) Loop days, query & merge, flatten to rows
    for day in dates:
        print(f"Processing day: {day}")

        hour_by_hour_smt_count = fetch_output_hour_counts(db, day, line_name, "AOI T2")
        hour_by_hour_packing_count = fetch_output_hour_counts(db, day, line_name, "PACKING")
        packing_dwell = fetch_final_to_packing_dwell(db, day, line_name)

        result = group_t_from_by_hour_next_hour_pass(packing_dwell)
        merged = merge_hourly_counts(hour_by_hour_smt_count, hour_by_hour_packing_count, result)

        # Optional: inspect merged day summary
        # print(json.dumps(merged, indent=2))

        # Flatten to per-PPID rows; pass packing_dwell to enrich 'from'/'to'
        rows = build_ppid_hold_rows(merged, day, line_name, ppid_times=packing_dwell)
        all_rows.extend(rows)

    if not all_rows:
        raise RuntimeError("No rows generated for the given range and line.")

    # 3) Export
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    outfile = Path(output_dir) / f"hold_report_{line_name}_{dates[0]}_to_{dates[-1]}.xlsx"
    written_path = export_ppid_hold_rows_to_excel(all_rows, str(outfile))
    print(f"Report written: {written_path}")
    return written_path

if __name__ == "__main__":
    # Example
    # inserted = import_production_excel_to_sqlite(
    #     file="C:/db/sfc_data3.csv",
    #     database_path="C:/db/production.db",
    #     batch_size=5_000,
    #     convert_to_local=False,
    # )
    # print(f"Inserted rows: {inserted}")



    db = SQLiteReadOnlyConnection()


        # Or generate a single Excel for the whole range:
    run_hold_report_over_range("2025-07-28", "2025-08-19", "J03", output_dir="output")


    #
    #
    #
    # hour_by_hour_count = fetch_packing_hour_counts(db, "2025-08-19", "J01")
    #
    #
    # packing_dwell = fetch_final_to_packing_dwell(db, "2025-08-19", "J01")
    #
    # result = group_t_from_by_hour_next_hour_pass(packing_dwell)
    # merged = merge_hourly_counts(hour_by_hour_count, result)
    # print(json.dumps(merged, indent=2))



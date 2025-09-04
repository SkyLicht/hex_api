
import pandas as pd
import sqlite3
import hashlib
from datetime import datetime
from pathlib import Path

def process_csv_data(file_path):
    """
    Read and process CSV data according to requirements:
    1. Replace spaces with underscores in GROUP_NAME
    2. Replace spaces with underscores in NEXT_STATION
    3. Parse IN_STATION_TIME to YYYY-MM-DD HH:MM:SS format

    Args:
        file_path (str): Path to the CSV file

    Returns:
        pandas.DataFrame: Processed DataFrame or None if error
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Process the data
        processed_df = df.copy()

        # 1. Replace spaces with underscores in GROUP_NAME
        if 'GROUP_NAME' in processed_df.columns:
            processed_df['GROUP_NAME'] = processed_df['GROUP_NAME'].astype(str).str.replace(' ', '_')

        # 2. Replace spaces with underscores in NEXT_STATION
        if 'NEXT_STATION' in processed_df.columns:
            processed_df['NEXT_STATION'] = processed_df['NEXT_STATION'].astype(str).str.replace(' ', '_')

        # 3. Parse IN_STATION_TIME to YYYY-MM-DD HH:MM:SS format
        if 'IN_STATION_TIME' in processed_df.columns:
            processed_df['IN_STATION_TIME'] = processed_df['IN_STATION_TIME'].apply(_parse_timestamp_to_format)

        return processed_df

    except Exception as e:
        return None

def insert_processed_data_to_db(processed_data, db_path, batch_size=2000):
    """
    Insert processed CSV data into SQLite database efficiently using batches.
    Optimized for M2 Mac with 3500 MB RAM.

    Args:
        processed_data (pandas.DataFrame): Processed DataFrame from process_csv_data
        db_path (str): Path to SQLite database file
        batch_size (int): Number of records per batch (default: 2000 for M2 optimization)

    Returns:
        dict: Results summary with inserted count and any errors
    """
    if processed_data is None or processed_data.empty:
        return {"success": False, "error": "No data to insert", "inserted_count": 0}

    db_path = Path(db_path)

    try:
        # Connect to database
        conn = sqlite3.connect(str(db_path))

        # Optimize SQLite for bulk inserts on M2 Mac
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-64000;")  # 64MB cache for M2
        conn.execute("PRAGMA mmap_size=268435456;")  # 256MB memory map

        # Ensure the table exists
        _ensure_records_table(conn)

        # Prepare insert statement
        insert_sql = """
                     INSERT OR IGNORE INTO records_table (
                         id,
                         ppid,
                         work_order,
                         collected_timestamp,
                         employee_name,
                         group_name,
                         line_name,
                         station_name,
                         model_name,
                         error_flag,
                         next_station
                     )
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                     """

        # Process data in batches
        total_rows = len(processed_data)
        inserted_count = 0
        batch_data = []

        # Track initial total_changes
        initial_changes = conn.total_changes

        # Begin transaction
        conn.execute("BEGIN TRANSACTION;")

        try:
            for index, row in processed_data.iterrows():
                # Map CSV columns to database fields
                mapped_row = _map_csv_row_to_db_fields(row)
                if mapped_row is None:
                    continue

                batch_data.append(mapped_row)

                # Insert batch when it reaches batch_size
                if len(batch_data) >= batch_size:
                    conn.executemany(insert_sql, batch_data)
                    batch_data.clear()

            # Insert remaining rows
            if batch_data:
                conn.executemany(insert_sql, batch_data)
                batch_data.clear()

            # Commit transaction
            conn.commit()

            # Calculate actual inserted rows
            inserted_count = conn.total_changes - initial_changes

            return {
                "success": True,
                "total_rows_processed": total_rows,
                "inserted_count": inserted_count,
                "skipped_count": total_rows - inserted_count,
                "batch_size_used": batch_size
            }

        except Exception as e:
            conn.rollback()
            return {
                "success": False,
                "error": f"Transaction error: {str(e)}",
                "inserted_count": 0
            }

    except Exception as e:
        return {
            "success": False,
            "error": f"Database connection error: {str(e)}",
            "inserted_count": 0
        }
    finally:
        if 'conn' in locals():
            conn.close()

def _ensure_records_table(conn):
    """Create the records_table if it doesn't exist."""
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS records_table (
                                                              id TEXT PRIMARY KEY,
                                                              ppid TEXT NOT NULL CHECK(length(ppid) <= 23),
                                                              work_order TEXT NOT NULL CHECK(length(work_order) <= 12),
                                                              collected_timestamp DATETIME NOT NULL,
                                                              employee_name TEXT NOT NULL CHECK(length(employee_name) <= 16),
                                                              group_name TEXT NOT NULL CHECK(length(group_name) <= 23),
                                                              line_name TEXT NOT NULL CHECK(length(line_name) <= 3),
                                                              station_name TEXT NOT NULL CHECK(length(station_name) <= 23),
                                                              model_name TEXT NOT NULL CHECK(length(model_name) <= 5),
                                                              error_flag INTEGER NOT NULL DEFAULT 0,
                                                              next_station TEXT CHECK(length(next_station) <= 16),
                                                              UNIQUE(ppid, collected_timestamp, line_name, station_name, group_name) ON CONFLICT IGNORE
                 ) WITHOUT ROWID;
                 """)

def _map_csv_row_to_db_fields(row):
    """
    Map CSV row to database fields tuple.

    Args:
        row: Pandas Series representing a CSV row

    Returns:
        tuple: Mapped row data or None if invalid
    """
    try:
        # Extract and validate required fields
        ppid = _safe_str(row.get('SERIAL_NUMBER'), 23)
        if not ppid:
            return None

        collected_timestamp = str(row.get('IN_STATION_TIME', ''))
        if not collected_timestamp:
            return None

        work_order = _safe_str(row.get('MO_NUMBER', ''), 12)
        employee_name = _safe_str(row.get('EMP_NO', ''), 16)
        group_name = _safe_str(row.get('GROUP_NAME', ''), 23)
        line_name = _safe_str(row.get('LINE_NAME', ''), 3)
        station_name = _safe_str(row.get('STATION_NAME', ''), 23)
        model_name = _safe_str(row.get('MODEL_NAME', ''), 5)
        next_station = _safe_str(row.get('NEXT_STATION', ''), 16)
        error_flag = _coerce_int01(row.get('ERROR_FLAG', 0))

        # Generate unique ID
        record_id = _make_id(ppid, collected_timestamp, line_name, station_name, group_name)

        return (
            record_id,
            ppid,
            work_order,
            collected_timestamp,
            employee_name,
            group_name,
            line_name,
            station_name,
            model_name,
            error_flag,
            next_station
        )

    except Exception:
        return None

def _safe_str(value, max_len):
    """Convert value to string and truncate to max_len."""
    if value is None or pd.isna(value):
        return ""
    s = str(value).strip()
    return s[:max_len]

def _coerce_int01(value):
    """Convert value to 0 or 1 integer."""
    try:
        iv = int(str(value).strip())
        return 1 if iv == 1 else 0
    except Exception:
        return 0

def _make_id(ppid, timestamp, line, station, group):
    """Generate unique ID hash for the record."""
    key = f"{ppid}|{timestamp}|{line}|{station}|{group}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def print_sample_records(processed_data, num_records=10):
    """
    Print a sample of records from processed data.

    Args:
        processed_data (pandas.DataFrame): The processed DataFrame
        num_records (int): Number of records to display (default: 10)
    """
    if processed_data is None:
        print("Error: No data to display - processed_data is None")
        return

    if processed_data.empty:
        print("Error: DataFrame is empty")
        return

    # Print basic info
    print(f"Dataset Info:")
    print(f"- Total rows: {len(processed_data)}")
    print(f"- Total columns: {len(processed_data.columns)}")
    print(f"- Columns: {list(processed_data.columns)}")
    print()

    # Print sample records
    sample_size = min(num_records, len(processed_data))
    print(f"Sample of {sample_size} records:")
    print("=" * 100)

    # Display the records
    print(processed_data.head(sample_size).to_string(index=False))

def _parse_timestamp_to_format(timestamp_str):
    """
    Parse timestamp from 'Mon, 25 Aug 2025 06:13:44 GMT' format to '2025-08-25 06:13:44'.

    Args:
        timestamp_str: String timestamp in various formats

    Returns:
        str: Formatted timestamp as 'YYYY-MM-DD HH:MM:SS' or original if parsing fails
    """
    if pd.isna(timestamp_str) or timestamp_str is None:
        return None

    ts = str(timestamp_str).strip()

    # List of formats to try
    formats_to_try = [
        "%a, %d %b %Y %H:%M:%S %Z",    # Mon, 25 Aug 2025 06:13:44 GMT
        "%a, %d %b %Y %H:%M:%S",       # Mon, 25 Aug 2025 06:13:44
        "%Y-%m-%d %H:%M:%S",           # 2025-08-25 06:13:44
        "%m/%d/%Y %H:%M:%S",           # 08/25/2025 06:13:44
        "%d/%m/%Y %H:%M:%S",           # 25/08/2025 06:13:44
    ]

    for fmt in formats_to_try:
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    # Special handling for GMT suffix
    if ts.upper().endswith(" GMT"):
        try:
            ts_without_gmt = ts[:-4].strip()
            dt = datetime.strptime(ts_without_gmt, "%a, %d %b %Y %H:%M:%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    # If all formats fail, return the original string
    return ts

# ... existing code ...

if __name__ == "__main__":

    # Example usage:
    processed_data = process_csv_data("C:/Users/jorgeortiza/PycharmProjects/sfc_graph/sfc_data5.csv")
    result = insert_processed_data_to_db(processed_data, "C:/data/lbn_db/lllll.db")
    print(f"Insert result: {result}")
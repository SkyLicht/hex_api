from datetime import datetime

from core.db.ppid_record_db import SQLiteReadOnlyConnection


def getCurrentDayDeltasQuery (database:SQLiteReadOnlyConnection, group_name: str, line_name: str):
    # Get current date
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    # Create start and end timestamps for the entire day
    day_start = f"{current_date} 00:00:00"
    day_end = f"{current_date} 23:59:59"

    # SQL query using BETWEEN for the entire day range
    query = """SELECT * FROM records_table
               WHERE collected_timestamp BETWEEN ? AND ? AND group_name = ? AND line_name = ?
               ORDER BY collected_timestamp DESC"""
    results = database.execute_query(query, (day_start, day_end, group_name, line_name))
    return results

def get_wip_query(database:SQLiteReadOnlyConnection, group_name: str, line_name: str):
    query = """
            WITH latest_records AS (
        SELECT ppid,
               line_name,
               next_station,
               collected_timestamp,
               station_name,
               group_name,
               ROW_NUMBER() OVER (PARTITION BY ppid ORDER BY collected_timestamp DESC) as rn
        FROM records_table
        WHERE collected_timestamp >= datetime('now', '-4 hours', 'localtime')
    )
               SELECT *
               FROM latest_records
               WHERE rn = 1 AND line_name = ? AND next_station = ?; 
            """


    try:
        results = database.execute_query(query, (line_name, group_name))

        if not results:
            return None
        return results

    except Exception as e:
        print(e)
        return None




def get_final_inspection_to_packing_last_24_hours(database: SQLiteReadOnlyConnection, line_name: str):
    """
    Get the time difference between FINAL_INSPECT and PACKING stages for each PPID
    in the last 24 hours for a specific line.

    Args:
        database: SQLiteReadOnlyConnection instance
        line_name: Line name to filter by (e.g., 'J01', 'J02')

    Returns:
        List[Dict[str, Any]]: List of records with ppid, timestamps, and time differences
    """

    query = """
            SELECT
                ppid,
                final_inspect_ts,
                packing_ts,
                (packing_epoch - final_epoch) AS diff_seconds
            FROM (
                     SELECT
                         ppid,
                         MAX(CASE WHEN group_name = 'FINAL_INSPECT' THEN collected_timestamp END) AS final_inspect_ts,
                         MAX(CASE WHEN group_name = 'PACKING'       THEN collected_timestamp END) AS packing_ts,
                         MAX(CASE WHEN group_name = 'FINAL_INSPECT' THEN strftime('%s', collected_timestamp) END) AS final_epoch,
                         MAX(CASE WHEN group_name = 'PACKING'       THEN strftime('%s', collected_timestamp) END) AS packing_epoch
                     FROM records_table
                     WHERE group_name IN ('FINAL_INSPECT', 'PACKING')
                       AND collected_timestamp >= datetime('now', '-24 hours', 'localtime')
                       AND line_name = ?
                     GROUP BY ppid
                     HAVING COUNT(DISTINCT group_name) = 2
                 ) t
            ORDER BY packing_ts DESC \
            """

    return database.execute_query(query, (line_name,))



def get_final_inspection_to_packing_by_date(database: SQLiteReadOnlyConnection, line_name: str, target_date: str):
    """
    Get the time difference between FINAL_INSPECT and PACKING stages for each PPID
    for a specific date and line.

    Args:
        database: SQLiteReadOnlyConnection instance
        line_name: Line name to filter by (e.g., 'J01', 'J02')
        target_date: Target date in format 'YYYY-MM-DD' (e.g., '2025-08-11')

    Returns:
        List[Dict[str, Any]]: List of records with ppid, timestamps, and time differences
    """

    # Create start and end timestamps for the entire target day
    day_start = f"{target_date} 00:00:00"
    day_end = f"{target_date} 23:59:59"

    query = """
            SELECT
                ppid,
                final_inspect_ts,
                packing_ts,
                (packing_epoch - final_epoch) AS diff_seconds
            FROM (
                     SELECT
                         ppid,
                         MAX(CASE WHEN group_name = 'FINAL_INSPECT' THEN collected_timestamp END) AS final_inspect_ts,
                         MAX(CASE WHEN group_name = 'PACKING'       THEN collected_timestamp END) AS packing_ts,
                         MAX(CASE WHEN group_name = 'FINAL_INSPECT' THEN strftime('%s', collected_timestamp) END) AS final_epoch,
                         MAX(CASE WHEN group_name = 'PACKING'       THEN strftime('%s', collected_timestamp) END) AS packing_epoch
                     FROM records_table
                     WHERE group_name IN ('FINAL_INSPECT', 'PACKING')
                       AND collected_timestamp BETWEEN ? AND ?
                       AND line_name = ?
                     GROUP BY ppid
                     HAVING COUNT(DISTINCT group_name) = 2
                 ) t
            ORDER BY packing_ts DESC
            """

    return database.execute_query(query, (day_start, day_end, line_name))



def get_expected_packing_query(database: SQLiteReadOnlyConnection, line_name: str):
    """
    Get pivot table showing timestamps and time deltas between manufacturing stages
    for the last 8 hours for a specific line.

    Args:
        database: SQLiteReadOnlyConnection instance
        line_name: Line name to filter by (e.g., 'J01', 'J02')

    Returns:
        List[Dict[str, Any]]: List of records with ppid, timestamps, and time differences between stages
    """

    query = """
            WITH pivot AS (
                SELECT
                    ppid,
                    MAX(CASE WHEN group_name = 'PTH_INPUT'      THEN collected_timestamp END) AS PTH_INPUT,
                    MAX(CASE WHEN group_name = 'TOUCH_INSPECT'  THEN collected_timestamp END) AS TOUCH_INSPECT,
                    MAX(CASE WHEN group_name = 'TOUCH_UP'       THEN collected_timestamp END) AS TOUCH_UP,
                    MAX(CASE WHEN group_name = 'ICT'            THEN collected_timestamp END) AS ICT,
                    MAX(CASE WHEN group_name = 'FT1'             THEN collected_timestamp END) AS FT,
                    MAX(CASE WHEN group_name = 'FINAL_VI'       THEN collected_timestamp END) AS FINAL_VI,
                    MAX(CASE WHEN group_name = 'FINAL_INSPECT'  THEN collected_timestamp END) AS FINAL_INSPECT,
                    MAX(CASE WHEN group_name = 'PACKING'        THEN collected_timestamp END) AS PACKING
                FROM records_table
                WHERE collected_timestamp >= datetime('now', '-8 hours', 'localtime')
                  AND line_name = ?
                  AND group_name IN ('PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT', 'FT1', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING')
                GROUP BY ppid
            )
            SELECT
                ppid,

                -- timestamps
                PTH_INPUT,
                -- delta to next (in seconds)
                CASE WHEN PTH_INPUT IS NOT NULL AND TOUCH_INSPECT IS NOT NULL
                         THEN CAST(ROUND((julianday(TOUCH_INSPECT) - julianday(PTH_INPUT)) * 86400.0) AS INTEGER)
                     ELSE 'nan' END AS PTH_INPUT_to_TOUCH_INSPECT_sec,

                TOUCH_INSPECT,
                CASE WHEN TOUCH_INSPECT IS NOT NULL AND TOUCH_UP IS NOT NULL
                         THEN CAST(ROUND((julianday(TOUCH_UP) - julianday(TOUCH_INSPECT)) * 86400.0) AS INTEGER)
                     ELSE 'nan' END AS TOUCH_INSPECT_to_TOUCH_UP_sec,

                TOUCH_UP,
                CASE WHEN TOUCH_UP IS NOT NULL AND ICT IS NOT NULL
                         THEN CAST(ROUND((julianday(ICT) - julianday(TOUCH_UP)) * 86400.0) AS INTEGER)
                     ELSE 'nan' END AS TOUCH_UP_to_ICT_sec,

                ICT,
                CASE WHEN ICT IS NOT NULL AND FT IS NOT NULL
                         THEN CAST(ROUND((julianday(FT) - julianday(ICT)) * 86400.0) AS INTEGER)
                     ELSE 'nan' END AS ICT_to_FT_sec,

                FT,
                CASE WHEN FT IS NOT NULL AND FINAL_VI IS NOT NULL
                         THEN CAST(ROUND((julianday(FINAL_VI) - julianday(FT)) * 86400.0) AS INTEGER)
                     ELSE 'nan' END AS FT_to_FINAL_VI_sec,

                FINAL_VI,
                CASE WHEN FINAL_VI IS NOT NULL AND FINAL_INSPECT IS NOT NULL
                         THEN CAST(ROUND((julianday(FINAL_INSPECT) - julianday(FINAL_VI)) * 86400.0) AS INTEGER)
                     ELSE 'nan' END AS FINAL_VI_to_FINAL_INSPECT_sec,

                FINAL_INSPECT,
                CASE WHEN FINAL_INSPECT IS NOT NULL AND PACKING IS NOT NULL
                         THEN CAST(ROUND((julianday(PACKING) - julianday(FINAL_INSPECT)) * 86400.0) AS INTEGER)
                     ELSE 'nan' END AS FINAL_INSPECT_to_PACKING_sec,

                PACKING

            FROM pivot
            -- SQLite doesn't support NULLS LAST directly; this puts NULLs last:
            ORDER BY (PTH_INPUT IS NULL), PTH_INPUT
            """

    return database.execute_query(query, (line_name,))


def get_data_by_day_and_line(
        database: SQLiteReadOnlyConnection,
        line_name: str,
        target_date: str,
):
    query = """SELECT ppid,
                      group_name,
                      next_station,
                      collected_timestamp,
                      model_name,
                      line_name,
                      error_flag
               FROM records_table
               WHERE date(collected_timestamp) = ?
                 AND line_name = ?
               ORDER BY collected_timestamp;"""

    result = database.execute_query(query, (target_date, line_name))

    return result
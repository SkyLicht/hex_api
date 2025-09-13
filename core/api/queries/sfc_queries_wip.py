from core.db.sfc_clon_db import SQLiteReadOnlyConnection


def get_wip_by_hour_and_line_and_group(
        database: SQLiteReadOnlyConnection,
        group_name_a: str,
        group_name_b: str,
        line_name: str,
        date: str,
        start_hour: int,
        lookback_hours: int = 4,
):
    """
    Get WIP (Work In Progress) data by filtering records between two groups within a specific hour range.

    Args:
        database: SQLiteReadOnlyConnection instance
        group_name_a: First group name (e.g., 'FINAL_INSPECT')
        group_name_b: Second group name (e.g., 'PACKING')
        line_name: Line name to filter by (e.g., 'J01', 'J02')
        date: Date in format 'YYYY-MM-DD' (e.g., '2025-08-22')
        start_hour: Starting hour as integer (0-23)
        lookback_hours: Number of hours to look back from start_hour (default: 4)

    Returns:
        List[Dict[str, Any]]: List of records with ppid, line_name, and timestamps for each group
    """

    # Validate start_hour
    if not isinstance(start_hour, int) or start_hour < 0 or start_hour > 23:
        raise ValueError("start_hour must be an integer between 0 and 23")

    # Validate lookback_hours
    if not isinstance(lookback_hours, int) or lookback_hours < 1:
        raise ValueError("lookback_hours must be a positive integer")

    # Calculate the lookback start hour
    lookback_start_hour = start_hour - lookback_hours

    # Calculate end hour (start_hour + 1)
    end_hour = start_hour + 1

    # Handle day boundary crossing for lookback
    if lookback_start_hour < 0:
        # If we cross into the previous day, we need to handle it
        from datetime import datetime, timedelta

        current_date = datetime.strptime(date, '%Y-%m-%d')
        previous_date = current_date - timedelta(days=1)

        # Start time is in the previous day
        lookback_start_timestamp = f"{previous_date.strftime('%Y-%m-%d')} {lookback_start_hour + 24:02d}:00:00"
    else:
        # Both times are in the same day
        lookback_start_timestamp = f"{date} {lookback_start_hour:02d}:00:00"

    # Handle day boundary crossing for end hour
    if end_hour > 23:
        # If end hour goes into next day
        from datetime import datetime, timedelta

        current_date = datetime.strptime(date, '%Y-%m-%d')
        next_date = current_date + timedelta(days=1)

        end_timestamp = f"{next_date.strftime('%Y-%m-%d')} {end_hour - 24:02d}:00:00"
    else:
        end_timestamp = f"{date} {end_hour:02d}:00:00"

    print(lookback_start_timestamp)
    print(end_timestamp)
    print(lookback_hours)

    print(line_name)
    print(group_name_a)
    print(group_name_b)

    query = """
            WITH base AS (SELECT *
                          FROM records_table
                          WHERE line_name = ?
                            AND group_name IN (?, ?)
                            AND collected_timestamp >= ?
                            AND collected_timestamp < ?)
            SELECT ppid,
                   line_name,
                   MAX(CASE WHEN group_name = ? THEN collected_timestamp END) AS GROUP_A,
                   MAX(CASE WHEN group_name = ? THEN collected_timestamp END) AS GROUP_B,
                   ?                                                          as lookback_hours
            FROM base
            GROUP BY ppid, line_name
            HAVING GROUP_B IS NULL
            ORDER BY COALESCE(GROUP_A, GROUP_B) DESC; 
            """

    execute = database.execute_query(
        query,
        (line_name, group_name_a, group_name_b, lookback_start_timestamp, end_timestamp,
         group_name_a, group_name_b, lookback_hours)
    )

    return execute

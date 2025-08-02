from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any

from core.analyzer.delta_analyzer import PPIDDeltaAnalyzer
from core.db.ppid_record_db import SQLiteReadOnlyConnection, get_database

router = APIRouter(
    prefix="/ppid",
    tags=["ppid"],
)


@router.get("/get_current_records")
async def get_current_records(
        database: SQLiteReadOnlyConnection = Depends(get_database)
) -> List[Dict[str, Any]]:
    """
    Get current hour records from ppid_24_hours_table.
    Returns all records between the start and end of the current hour.
    """
    try:
        # Get current time
        now = datetime.now()

        # Get start of current hour (e.g., if it's 14:30, get 14:00:00)
        current_hour_start = now.replace(minute=0, second=0, microsecond=0)

        # Get end of current hour (e.g., if it's 14:30, get 14:59:59)
        current_hour_end = current_hour_start.replace(minute=59, second=59, microsecond=999999)

        # SQL query
        query = """SELECT * FROM ppid_24_hours_table
                   WHERE timestamp BETWEEN ? AND ?
                   ORDER BY timestamp DESC"""

        # Execute query with formatted timestamps
        results = database.execute_query(
            query,
            (
                current_hour_start.strftime("%Y-%m-%d %H:%M:%S"),
                current_hour_end.strftime("%Y-%m-%d %H:%M:%S")
            )
        )

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/get_ppid_by_date_hour_and_group_name")
async def get_ppid_by_date_hour_and_group_name(
        date: str,
        hour: str,
        group_name: str,
        line_name: str,
        database: SQLiteReadOnlyConnection = Depends(get_database)
) -> List[Dict[str, Any]]:
    """Get PPID records by date, hour and group name using BETWEEN query"""
    try:
        # Validate and parse date format (YYYY-MM-DD)
        try:
            parsed_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Expected format: YYYY-MM-DD (e.g., 2025-07-29)"
            )

        # Validate and parse hour format (HH:MM:SS)
        try:
            parsed_hour = datetime.strptime(hour, "%H:%M:%S")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid hour format. Expected format: HH:MM:SS (e.g., 09:00:00)"
            )

        # Create start and end timestamps for the hour range
        hour_start = f"{date} {hour}"

        # Get the hour from parsed_hour and create end time
        hour_obj = parsed_hour.time()
        next_hour = datetime.combine(parsed_date.date(), hour_obj) + timedelta(hours=1) - timedelta(seconds=1)
        hour_end = next_hour.strftime("%Y-%m-%d %H:%M:%S")

        # SQL query using BETWEEN for the hour range
        query = """SELECT * FROM ppid_24_hours_table
                   WHERE timestamp BETWEEN ? AND ? AND group_name = ? AND line_name = ?
                   ORDER BY timestamp DESC"""

        results = database.execute_query(query, (hour_start, hour_end, group_name,line_name))

        if not results:
            raise HTTPException(status_code=404, detail="No records found for the specified criteria")

        return results

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/get_ppid_current_day")
async def get_ppid_current_day(
        group_name: str,
        line_name: str,
        database: SQLiteReadOnlyConnection = Depends(get_database)
) -> List[Dict[str, Any]]:
    """Get all PPID records for the current day (00:00:00 to 23:59:59)"""
    try:
        # Get current date
        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")

        # Create start and end timestamps for the entire day
        day_start = f"{current_date} 00:00:00"
        day_end = f"{current_date} 23:59:59"

        # SQL query using BETWEEN for the entire day range
        query = """SELECT * FROM ppid_24_hours_table
                   WHERE timestamp BETWEEN ? AND ? AND group_name = ? AND line_name = ?
                   ORDER BY timestamp DESC"""

        results = database.execute_query(query, (day_start, day_end, group_name, line_name))

        if not results:
            raise HTTPException(status_code=404, detail="No records found for the current day")

        return results

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/get_ppid_current_day_deltas")
async def get_ppid_current_day_deltas(
        group_name: str,
        line_name: str,
        database: SQLiteReadOnlyConnection = Depends(get_database)
) -> dict[str, Any]:
    """Get all PPID records for the current day (00:00:00 to 23:59:59)"""

    print(group_name)
    try:
        # Get current date
        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")

        # Create start and end timestamps for the entire day
        day_start = f"{current_date} 00:00:00"
        day_end = f"{current_date} 23:59:59"


        # SQL query using BETWEEN for the entire day range
        query = """SELECT * FROM ppid_24_hours_table
                   WHERE timestamp BETWEEN ? AND ? AND group_name = ? AND line_name = ?
                   ORDER BY timestamp DESC"""

        results = database.execute_query(query, (day_start, day_end, group_name, line_name))

        if not results:
            raise HTTPException(status_code=404, detail="No records found for the current day")

        return PPIDDeltaAnalyzer(results).get_analysis_json()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")





@router.get("/get_current_12_wip_by_group_and_line")
async def get_current_12_wip_by_group_and_line(
        line_name: str,
        group_name: str,
        database: SQLiteReadOnlyConnection = Depends(get_database)
) -> List[Dict[str, Any]]:
    """
    Get the most recent WIP records for each PPID from the last 12 hours,
    filtered by line_name and group_name.
    Returns only the latest record for each PPID within the time window.
    """
    try:
        # SQL query to get the most recent record for each PPID from last 12 hours
        query = """
                SELECT
                    ppid,
                    timestamp,
                    employee,
                    section_name,
                    station_name,
                    model_name,
                    error_flag
                FROM (
                    SELECT
                    ppid,
                    timestamp,
                    employee,
                    section_name,
                    station_name,
                    model_name,
                    error_flag,
                    line_name,
                    group_name,
                    ROW_NUMBER() OVER (PARTITION BY ppid ORDER BY timestamp DESC) as rn
                    FROM ppid_24_hours_table
                    WHERE timestamp >= datetime('now','localtime', '-12 hours')
                    ) ranked
                WHERE rn = 1
                  AND line_name = ?
                  AND group_name = ?
                ORDER BY timestamp DESC \
                """

        results = database.execute_query(query, (line_name, group_name))

        if not results:
            raise HTTPException(status_code=404, detail="No WIP records found for the specified line and group in the last 12 hours")

        return results

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
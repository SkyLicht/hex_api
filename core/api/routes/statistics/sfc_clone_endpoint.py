from typing import List, Dict, Any, Coroutine

from fastapi import APIRouter, Depends, HTTPException

from core.analyzer.delta_analyzer import DeltaAnalyzer
from core.analyzer.ecpv3 import compute_hourly_ct_table
from core.analyzer.expected_packing import calculate_expected_packing_output, analyze_hourly_production_flow
from core.analyzer.expected_packing_v2 import analyze_hourly_cycle_times
from core.analyzer.pcb_held import analyze_production_hiding_patterns
from core.api.queries.sfc_queries import getCurrentDayDeltasQuery, get_wip_query, get_expected_packing_query, \
    get_final_inspection_to_packing_by_date
from core.db.ppid_record_db import SQLiteReadOnlyConnection, get_database

router = APIRouter(
    prefix="/sfc_clon",
    tags=["sfc_clon"],
)


@router.get("/getDeltasByGroupAndLine")
async def get_current_days_delta(
        group_name: str,
        line_name: str,
        database: SQLiteReadOnlyConnection = Depends(get_database)
) -> dict[str, Any]:
    try:
        result = getCurrentDayDeltasQuery(database, group_name, line_name)

        if not result:
            raise HTTPException(status_code=404, detail="No records found for the specified criteria")

        return DeltaAnalyzer(result).get_analysis_json()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/get_wip_by_group_and_line")
async def get_wip_by_group_and_line(
        group_name: str,
        line_name: str,
        database: SQLiteReadOnlyConnection = Depends(get_database)
) -> list[dict[str, Any]]:
    try:
        result = get_wip_query(database, group_name, line_name)

        if not result:
            return []

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
# get_packing_expected_by_line
@router.get("/get_production_hiding_patterns")
async def get_production_hiding_patterns(
        line_name: str,
        threshold_minutes: int = 3,
        database: SQLiteReadOnlyConnection = Depends(get_database)
) -> Dict[str, Any]:
    try:
        # result = get_final_inspection_to_packing_last_24_hours( database, line_name)
        result = get_final_inspection_to_packing_by_date( database, line_name, "2025-08-15")

        if not result:
            raise HTTPException(status_code=404, detail="No records found for the specified criteria")

        analyze = analyze_production_hiding_patterns(
            result,
            line_name,
            threshold_minutes
        )


        return analyze

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")



@router.get("/get_get_expected_packing_by_line")
async def get_expected_packing(
        line_name: str,
        database: SQLiteReadOnlyConnection = Depends(get_database)
):
    try:
        query_data = get_expected_packing_query(database, line_name)

        if not query_data:
            raise HTTPException(status_code=404, detail="No records found for the specified criteria")


        # For a specific hour analysis (10:00-11:00 AM on 2025-08-15)
        # result = calculate_expected_packing_output(
        #     process_flow_data=query_data,
        #     target_hour_start="2025-08-15 10:00:00",
        #     target_hour_end="2025-08-15 11:00:00"
        # )

        # result  = analyze_hourly_cycle_times(
        #     process_flow_data=query_data,
        #     analysis_date="2025-08-15"
        # )

        hourly = compute_hourly_ct_table(query_data, max_cycle_seconds=7200)


        return hourly

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
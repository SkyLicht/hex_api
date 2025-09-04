from typing import List, Dict, Any, Coroutine, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.params import Query

from core.analyzer.data_group_by_day_and_line import group_name_by_hour_and_line
from core.analyzer.delta_analyzer import DeltaAnalyzer
from core.analyzer.ecpv3 import compute_hourly_ct_table
from core.analyzer.pcb_held import analyze_production_hiding_patterns
from core.analyzer.wip_analyzer import wip_to_hour_summary
from core.api.queries.sfc_queries import getCurrentDayDeltasQuery, get_wip_query, get_expected_packing_query, \
    get_final_inspection_to_packing_by_date, get_data_by_day_and_line
from core.api.queries.sfc_queries_wip import get_wip_by_hour_and_line_and_group
from core.db.ppid_record_db import SQLiteReadOnlyConnection, get_database
from core.services.ECDFService import ECDFService

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





@router.get("/get_ecdf")
async def get_ecdf(
    line_name: str,
    stage_from: str = Query("PTH_INPUT", description="Origin station"),
    stage_to:   str = Query("PACKING",       description="Destination station"),
    date: Optional[str] = Query(None, description="YYYY-MM-DD (mutually exclusive with start_dt/end_dt)"),
    start_dt: Optional[str] = Query(None, description="YYYY-MM-DD HH:MM:SS"),
    end_dt:   Optional[str] = Query(None, description="YYYY-MM-DD HH:MM:SS"),
    anchor:   str = Query("start", pattern="^(start|end|both)$"),
    cap_minutes: int = 1440,
    censor_flow_errors: bool = True,
    censor_repairs: bool = True,
    grid_step: int = 10,
    grid_max: Optional[int] = Query(None),
    eval_at: Optional[List[int]] = Query(None, description="Repeatable ?eval_at=60&eval_at=120"),
    database: SQLiteReadOnlyConnection = Depends(get_database),
) -> Dict[str, Any]:
    """
    Pure ECDF info (no batch/hiding detection).
    Returns grid (t, F(t)), percentiles (p50,p90,p95,p99), count, and support(min,max).
    """
    try:

        svc = ECDFService(database, line_name)
        result = svc.get_ecdf(
            stage_from=stage_from,
            stage_to=stage_to,
            date=date,
            start_dt=start_dt,
            end_dt=end_dt,
            anchor=anchor,
            cap_minutes=cap_minutes,
            censor_flow_errors=censor_flow_errors,
            censor_repairs=censor_repairs,
            grid_step=grid_step,
            grid_max=grid_max,
            eval_at=eval_at,
        )
        if result.get("n", 0) == 0:
            raise HTTPException(status_code=404, detail="No records found for the specified criteria")

        return result

    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")




@router.get("/get_data_by_day_and_line")
async def get_data_by_day(
        date: str,
        line_name: str,
        database: SQLiteReadOnlyConnection = Depends(get_database),
):

    try:
        query_data = get_data_by_day_and_line(database, line_name, date)

        if not query_data:
            raise HTTPException(status_code=404, detail="No records found for the specified criteria")

        transform_data = group_name_by_hour_and_line(query_data)
        return transform_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/get_wip_by_hour")
async def get_wip_by_hour(
        date: str,
        hour: int,
        line_name: str,
        group_name_a: str,
        group_name_b: str,
        database: SQLiteReadOnlyConnection = Depends(get_database),
):
    try:

        query_data = get_wip_by_hour_and_line_and_group(database, group_name_a,group_name_b,line_name,date,hour)
        transform_data = wip_to_hour_summary(group_name_b,query_data)

        if transform_data is None:
            raise HTTPException(status_code=404, detail="No records found for the specified criteria")

        return transform_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

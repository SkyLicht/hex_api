from fastapi import APIRouter, Depends, HTTPException

from core.api.dependency import get_work_plan_repository
from core.api.requests.planner_request import CreateWorkPlanRequest
from core.data.repositories.planner.work_plan_repository import WorkPlanRepository

router = APIRouter(
    prefix="/planner",
    tags=["planner"],
)


@router.post("/create_work_plan")
async def create_work_plan(
        body: CreateWorkPlanRequest,
        repo: WorkPlanRepository = Depends(get_work_plan_repository),
):
    try:

        return repo.create_work_plan(body.to_orm())
    except PermissionError as e:

        raise HTTPException(status_code=403, detail="Permission denied.")
    except ValueError as e:

        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")
    finally:
        pass


@router.get("/get_work_plans_by_str_date")
async def get_work_plans_by_str_date(
        str_date: str,
        repo: WorkPlanRepository = Depends(get_work_plan_repository),
):
    try:
        _result = repo.get_work_plans_by_str_date(str_date)

        if _result:
            return _result
        else:
            return {"message": "No data found. Please check the input parameters.", "data": None, "status": 404,
                    "error": None}
    except PermissionError as e:

        raise HTTPException(status_code=403, detail="Permission denied.")
    except ValueError as e:

        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:

        raise HTTPException(status_code=500, detail="An unexpected error occurred.")
    finally:
        pass


@router.get("/get_work_plan_by_str_date_and_line_name")
async def get_work_plan_by_str_date_and_line_name(str_date: str, line_name: str,
                                                  repo: WorkPlanRepository = Depends(get_work_plan_repository)):
    try:
        _data = repo.get_work_plan_by_str_date_and_line_name(str_date, line_name)
        if _data:
            return {"message": "Data found successfully.", "data": _data, "status": 200, "error": None}
        # else:
        #     return {"message": "No data found. Please check the input parameters.", "data": None, "status": 404,
        #             "error": None}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail="Permission denied.")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

    finally:
        pass

#
# class WorkDayIDRequest(BaseModel):
#     work_day_id: str = Field(..., regex="^[a-f0-9]{24}$")  # Example for MongoDB ObjectId

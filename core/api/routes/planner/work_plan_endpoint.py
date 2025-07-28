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
        return repo.get_work_plans_by_str_date(str_date)
    except PermissionError as e:

        raise HTTPException(status_code=403, detail="Permission denied.")
    except ValueError as e:

        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:

        raise HTTPException(status_code=500, detail="An unexpected error occurred.")
    finally:
        pass

@router.get("/get_work_plans_with_relations_by_date")
async def get_work_plans_with_relations_by_date(
        str_date: str,
        repo: WorkPlanRepository = Depends(get_work_plan_repository),
):
    try:
        return repo.get_work_plans_with_platform_line_by_str_date(str_date)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail="Permission denied.")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")




#
# class WorkDayIDRequest(BaseModel):
#     work_day_id: str = Field(..., regex="^[a-f0-9]{24}$")  # Example for MongoDB ObjectId

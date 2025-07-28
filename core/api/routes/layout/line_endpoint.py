from fastapi import APIRouter, Depends, HTTPException

from core.api.dependency import get_line_repository
from core.data.repositories.layout.line_repository import LineRepository

router = APIRouter(
    prefix="/layout",
    tags=["layout"],
    responses={404: {"description": "Not found"}},
)


@router.get("/get_lines")
async def get_lines(
        line_repo: LineRepository = Depends(get_line_repository),
):
    try:

        return line_repo.get_factories_lines()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

from fastapi import APIRouter, Depends, HTTPException

from core.api.dependency import get_uph_repository
from core.api.requests.uph_record_request import CreateUPHRecordRequest
from core.data.repositories.planner.uph_record_repository import UPHRecordRepository

router = APIRouter(
    prefix="/uph",
    tags=["uph"]
)


@router.post("/create_uph")
async def create_uph(
        body: CreateUPHRecordRequest,
        repo: UPHRecordRepository = Depends(get_uph_repository),
):
    return repo.create_uph_record(body.to_orm())

# POST http://10.13.32.220:3010/api/v1/uph/create_uph
# Content-Type: application/json
#
# {
#   "platform_id": "t1iwmdwlx3d0k7z",
#   "line_id": "lxL6Pz3KSOmnefj",
#   "target_oee": 0.6,
#   "uph": 100,
#   "start_date": "2025-09-13 14:10:00",
#   "end_date": "2025-09-13 19:00:00"
# }



@router.get("/get_uph")
async def get_uph(
        page: int,
        page_size: int,
        repo: UPHRecordRepository = Depends(get_uph_repository),
):

    if page < 0:
        raise HTTPException(status_code=400, detail="Invalid page number")
    if page_size < 0:
        raise HTTPException(status_code=400, detail="Invalid page size")

    return repo.get_uph_record_page(page, page_size)


# GET http://10.13.32.220:3010/api/v1/uph/get_uph?page=1&page_size=20
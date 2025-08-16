from fastapi import APIRouter, HTTPException
from collections import defaultdict

from core.hbh.hbh_mackenzie_api import get_current_day_data_from_api

router = APIRouter(
    prefix="/hbh_api",
    tags=["hbh_api"],
)


@router.get("/get_current_day_records")
async def get_current_day_records():
    try:
        result = await get_current_day_data_from_api()

        if result is None:
            raise HTTPException(status_code=404, detail="No data found")

        # Parse the JSON string to list of dictionaries
        import json
        data_list = json.loads(result)

        # Group by line
        grouped_data = defaultdict(list)
        for record in data_list:
            line = record["line"]
            grouped_data[line].append(record)

        # Convert defaultdict to regular dict
        grouped_dict = dict(grouped_data)

        return grouped_dict
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
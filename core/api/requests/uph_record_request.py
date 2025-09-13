from datetime import datetime
from typing import Optional
from pydantic import Field, field_validator
from pydantic import BaseModel

from core.data.orm_models.work_plan_model_v1 import UPHRecordORM


class CreateUPHRecordRequest(BaseModel):
    """Request body to create UPH record"""
    platform_id: Optional[str] = Field(description="Platform identifier")
    line_id: Optional[str] = Field(description="Production line identifier")
    target_oee: Optional[float] = Field(description="Target OEE")
    uph: Optional[int] = Field(description="UPH")
    start_date: Optional[str] = Field(description="Date in YYYY-MM-DD HH:MM:SS format")
    end_date: Optional[str] = Field(description="Date in YYYY-MM-DD HH:MM:SS format")

    # @field_validator("start_date", mode='before')
    # def validate_start_date(cls, v):
    #     if isinstance(v, str):
    #         try:
    #             return datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
    #         except ValueError:
    #             raise ValueError("Start date must be in format YYYY-MM-DD HH:MM:SS")
    #     return v
    #
    # @field_validator("end_date", mode='before')
    # def validate_end_date(cls, v):
    #     if isinstance(v, str):
    #         try:
    #             return datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
    #         except ValueError:
    #             raise ValueError("End date must be in format YYYY-MM-DD HH:MM:SS format")
    #
    #     return v

    def to_orm(self):
        return UPHRecordORM(
            platform_id=self.platform_id,
            line_id=self.line_id,
            target_oee=self.target_oee,
            uph=self.uph,
            start_date=datetime.strptime(self.start_date, "%Y-%m-%d %H:%M:%S"),
            end_date=datetime.strptime(self.end_date, "%Y-%m-%d %H:%M:%S"),
        )

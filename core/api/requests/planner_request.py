from typing import Optional
from pydantic import BaseModel, Field

from core.data.orm_models.work_plan_model_v1 import WorkPlanModel


class CreateWorkPlanRequest(BaseModel):
    """Request body to create a work plan"""
    platform_id: Optional[str] = Field(None, description="Platform identifier")
    line_id: Optional[str] = Field(None, description="Production line identifier")
    planned_hours: Optional[float] = Field(None, description="Planned hours")
    target_oee: Optional[float] = Field(None, description="Target OEE")
    uph_i: Optional[int] = Field(None, description="UPH")
    start_hour: Optional[int] = Field(None, description="Start hour")
    end_hour: Optional[int] = Field(None, description="End hour")
    str_date: Optional[str] = Field(None, description="Date in YYYY-MM-DD format")
    week: Optional[int] = Field(None, description="Week number")
    head_count: Optional[int] = Field(None, description="Head count")
    ft: Optional[int] = Field(None, description="FT")
    ict: Optional[int] = Field(None, description="ICT")

    # Date must be in YYYY-MM-DD format
    # @field_validator('str_date', mode='before')
    # def parse_dates(cls, v):
    #     if isinstance(v, str):
    #         try:
    #             return datetime.strptime(v, '%Y-%m-%d')
    #         except ValueError:
    #             raise ValueError('Date must be in YYYY-MM-DD format')
    #     return v

    # All fields are required
    # @model_validator(mode='after')
    # def validate_parameters(self):
    #     if not all([self.work_day_id, self.platform_id, self.line_id, self.planned_hours, self.target_oee, self.uph_i,
    #                 self.start_hour, self.end_hour, self.date, self.str_date, self.week, self.head_count, self.ft,
    #                 self.ict]):
    #         raise ValueError('All fields are required')
    #     return self

    #
    # Return WorkPlanModel
    def to_orm(self):
        return WorkPlanModel(
            platform_id=self.platform_id,
            line_id=self.line_id,
            planned_hours=self.planned_hours,
            target_oee=self.target_oee,
            uph_i=self.uph_i,
            start_hour=self.start_hour,
            end_hour=self.end_hour,
            str_date=self.str_date,
            week=self.week,
            head_count=self.head_count,
            ft=self.ft,
            ict=self.ict
        )

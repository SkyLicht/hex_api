import datetime
from typing import List

from pydantic import BaseModel

from core.data.orm_models.work_plan_model_v1 import WorkPlanModel
from core.data.schemas.layout.line_schema import LineSmallSchema


class PlatformSchema(BaseModel):
    f_n: int
    platform: str
    uph: int
    in_service: bool
    components_list_id: str
    height: float
    id: str
    sku: str
    cost: float
    components: int
    width: float


class WorkPlanSchema(BaseModel):
    id: str
    platform_id: str
    line_id: str
    planned_hours: float
    target_oee: float
    uph: int
    start_hour: int
    end_hour: int
    str_date: str
    week: int
    head_count: int
    ft: int
    ict: int
    line: str
    factory: str
    uph_meta: int
    commit: int
    commit_full: int

    platform: PlatformSchema

    @staticmethod
    def work_plan_orm_to_schema(orm: WorkPlanModel) -> 'WorkPlanSchema':
        return WorkPlanSchema(
            id=orm.id,
            platform_id=orm.platform_id,
            line_id=orm.line_id,
            planned_hours=orm.planned_hours,
            target_oee=orm.target_oee,
            uph=orm.uph_i,
            start_hour=orm.start_hour,
            end_hour=orm.end_hour,
            str_date=orm.str_date,
            week=orm.week,
            head_count=orm.head_count,
            ft=orm.ft,
            ict=orm.ict,
            line=orm.line.name,
            factory=orm.line.factory.name,
            uph_meta=round(orm.target_oee * orm.platform.uph),
            commit=round(orm.planned_hours * (orm.target_oee * orm.platform.uph)),
            commit_full=round(orm.planned_hours * orm.platform.uph),
            platform=PlatformSchema(
                f_n=orm.platform.f_n,
                platform=orm.platform.platform,
                uph=orm.platform.uph,
                in_service=orm.platform.in_service,
                components_list_id=orm.platform.components_list_id,
                height=orm.platform.height,
                id=orm.platform.id,
                sku=orm.platform.sku,
                cost=orm.platform.cost,
                components=orm.platform.components,
                width=orm.platform.width,
            )
        )

    @staticmethod
    def work_plan_orm_list_to_schema_list(orm_list: [WorkPlanModel]) -> 'List[WorkPlanSchema]':
        return [WorkPlanSchema.work_plan_orm_to_schema(orm) for orm in orm_list]


class WorkPlanWithRelationsSchema(BaseModel):
    id: str
    platform_id: str
    line_id: str
    planned_hours: float
    target_oee: float
    uph_i: int
    start_hour: int
    end_hour: int
    str_date: str
    week: int
    head_count: int
    ft: int
    ict: int
    created_at: datetime
    updated_at: datetime
    platform: PlatformSchema
    line: LineSmallSchema

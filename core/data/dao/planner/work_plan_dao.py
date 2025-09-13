from typing import List

from sqlalchemy import desc, column
from sqlalchemy.orm import joinedload

from core.data.orm_models.work_plan_model_v1 import WorkPlanModel, LineModel


class WorkPlanDAO:
    def __init__(self, db):
        self.db = db

    def create_work_plan(self, work_plan: WorkPlanModel):
        try:
            self.db.add(work_plan)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise e

    def get_work_plan_by_line_id_and_str_date(self, line_id: str, str_date: str) -> WorkPlanModel:
        return (self.db.query(WorkPlanModel)
                .options(
            joinedload(WorkPlanModel.platform),
            joinedload(WorkPlanModel.line),
            joinedload(WorkPlanModel.line).joinedload(LineModel.factory)
        )
                .filter(WorkPlanModel.line_id == line_id, WorkPlanModel.str_date == str_date).first())


    def get_work_plan_by_str_date_and_line_name(self, str_date: str, line_name: str) -> WorkPlanModel | None:
        orm =  (self.db.query(WorkPlanModel)
                .options(
            joinedload(WorkPlanModel.platform),
            joinedload(WorkPlanModel.line),
            joinedload(WorkPlanModel.line).joinedload(LineModel.factory)
        )
                .join(LineModel, WorkPlanModel.line_id == LineModel.id)
                .filter(WorkPlanModel.str_date == str_date)
                .filter(LineModel.name == line_name)
                .first())
        return orm

    def get_work_plan_by_line_id(self, line_id: str) -> WorkPlanModel:
        return self.db.query(WorkPlanModel).filter(WorkPlanModel.line_id == line_id).order_by(
            desc(WorkPlanModel.str_date)).first()

    def get_work_plans_by_str_date(self, str_date) -> List[WorkPlanModel]:
        return (self.db.query(WorkPlanModel)
                .options(
            joinedload(WorkPlanModel.platform),
            joinedload(WorkPlanModel.line),
            joinedload(WorkPlanModel.line).joinedload(LineModel.factory)
        )
                .filter(WorkPlanModel.str_date == str_date).all())

    def get_work_plan_by_id(self, work_plan_id: str) -> List[WorkPlanModel]:
        return (self.db.query(WorkPlanModel)
                .options(
            joinedload(WorkPlanModel.platform),
            joinedload(WorkPlanModel.line),
            joinedload(WorkPlanModel.line).joinedload(LineModel.factory)
        )
                .filter(WorkPlanModel.id == work_plan_id).all())

    def get_work_plans_with_platform_line_by_str_date(self, str_date: str) -> List[WorkPlanModel]:
        return (self.db.query(WorkPlanModel)
                .options(
            joinedload(WorkPlanModel.platform),
            joinedload(WorkPlanModel.line)
        )
                .filter(WorkPlanModel.str_date == str_date)
                .all())
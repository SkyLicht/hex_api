from typing import List
from sqlalchemy.orm import Session

from core.data.dao.planner.work_plan_dao import WorkPlanDAO
from core.data.schemas.planner.planner_schema import WorkPlanSchema, WorkPlanWithRelationsSchema


class WorkPlanRepository:

    def __init__(self, session: Session):
        self.session = session
        self.work_plan_dao = WorkPlanDAO(session)


    def __enter__(self):
        # Perform setup actions, e.g., open a database connection
        #print("Entering context and setting up resources.")
        return self  # Return the object to be used in the with block

    def __exit__(self, exc_type, exc_value, traceback):
        # Perform cleanup actions, e.g., close the database connection
        #print("Exiting context and cleaning up resources.")
        # Handle exceptions if necessary; return True to suppress them, False to propagate
        return False

    def create_work_plan(self, work_plan):
        return self.work_plan_dao.create_work_plan(work_plan)

    def get_work_plans_by_str_date(self, str_date):
        return self.work_plan_dao.get_work_plans_by_str_date(str_date)


    def get_work_plans_by_id(self, work_plan_id) -> 'List[WorkPlanSchema]':
        orm_list = self.work_plan_dao.get_work_plan_by_id(work_plan_id)
        return WorkPlanSchema.work_plan_orm_list_to_schema_list(orm_list)

    def get_work_plans_with_platform_line_by_str_date(self, str_date: str) -> List[WorkPlanWithRelationsSchema]:
        orm_list = self.work_plan_dao.get_work_plans_with_platform_line_by_str_date(str_date)
        return [WorkPlanWithRelationsSchema.model_validate(work_plan) for work_plan in orm_list]

    

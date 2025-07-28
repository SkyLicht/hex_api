from core.data.dao.planner.platform_dao import PlatformDAO
from core.data.dao.planner.work_plan_dao import WorkPlanDAO


class PlatformRepository:
    def __init__(self, session):
        self.session = session
        self.platform_dao = PlatformDAO(session)
        self.work_plan_dao = WorkPlanDAO(session)

    def get_all_in_service(self):
        return self.platform_dao.get_all_in_service()

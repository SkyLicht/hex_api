from typing import List

from core.data.orm_models.work_plan_model_v1 import PlatformModel


class PlatformDAO:
    def __init__(self, session):
        self.session = session

    def create_all(self, platform: List[PlatformModel]):
        try:
            self.session.add_all(platform)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def get_all_in_service(self):
        return self.session.query(PlatformModel).filter(PlatformModel.in_service == True).all()

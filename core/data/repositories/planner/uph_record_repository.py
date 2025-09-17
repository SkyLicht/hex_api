from core.data.dao.planner.uph_record_dao import UPHRecordDAO
from core.data.orm_models.work_plan_model_v1 import UPHRecordORM


class UPHRecordRepository:

    def __init__(self, session):
        self.session = session
        self.dao = UPHRecordDAO(session)

    def create_uph_record(self, orm: UPHRecordORM):
        return self.dao.create(orm)

    def get_uph_record_page(self, page: int, per_page: int):
        return self.dao.get_page(page, per_page)

    def delete_uph_record(self, uph_id: str)-> bool:
        return self.dao.delete(uph_id)

    def get_last_record(self):
        return self.dao.get_line_unique()

    def get_all_by_line_name(self, line_name: str):
        return self.dao.get_by_line_name(line_name)
from sqlalchemy.orm import Session

from core.data.orm_models.work_plan_model_v1 import UPHRecordORM, PlatformModel, LineModel


class UPHRecordDAO:
    def __init__(self, db: Session):
        self.db = db

    def create(self, orm: UPHRecordORM):

        platform = self.db.query(PlatformModel).get(orm.platform_id)
        if platform is None:
            raise ValueError(f"Platform {orm.platform_id} does not exist")

        line = self.db.query(LineModel).get(orm.line_id)
        if line is None:
            raise ValueError(f"Line {orm.line_id} does not exist")

        try:
            self.db.add(orm)
            self.db.commit()

            return orm.id

        except Exception as e:
            self.db.rollback()
            raise e


    def get_page(self, page:int,page_size:int):
        offset = (page - 1) * page_size
        return self.db.query(UPHRecordORM).offset(offset).limit(page_size).all()
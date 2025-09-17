from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload, aliased

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

    def get_page(self, page: int, page_size: int):
        offset = (page - 1) * page_size
        return self.db.query(UPHRecordORM).options(
            joinedload(UPHRecordORM.platform),
            joinedload(UPHRecordORM.line)
        ).offset(offset).limit(page_size).all()

    def delete(self, uph_id: str) -> bool:
        uph_record = self.db.query(UPHRecordORM).get(uph_id)
        if uph_record is None:
            raise ValueError(f"UPH record {uph_id} does not exist")

        try:
            self.db.delete(uph_record)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            raise e

    def get_line_unique(self):
        # Create a subquery with row_number

        ranked_subquery = (
            self.db.query(
                UPHRecordORM,
                func.row_number().over(
                    partition_by=UPHRecordORM.line_id,
                    order_by=UPHRecordORM.end_date.desc()
                ).label("rn")
            )
            .subquery()
        )

        # Alias the subquery for ORM mapping
        ranked_alias = aliased(UPHRecordORM, ranked_subquery)

        # Query only rows where rn == 1 (latest per line_id)
        return self.db.query(ranked_alias).options(
            joinedload(ranked_alias.platform),
            joinedload(ranked_alias.line)
        ).filter(ranked_subquery.c.rn == 1).all()

    def get_by_line_name(self, line_name: str):

        _line = self.db.query(LineModel).filter_by(name = line_name).first()
        if _line is None:
            raise ValueError(f"Line {line_name} does not exist")


        return (self.db.query(UPHRecordORM).options(
            joinedload(UPHRecordORM.platform),
            joinedload(UPHRecordORM.line))
                .filter(UPHRecordORM.line_id == _line.id)
                .order_by(UPHRecordORM.start_date.desc())
                .all()
            )

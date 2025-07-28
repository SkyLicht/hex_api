from typing import List

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from core.data.orm_models.work_plan_model_v1 import LineModel


class LineDAO:
    def __init__(self, session):
        self.session = session

    def create_line(self, line: LineModel):
        try:
            self.session.add(line)
            self.session.commit()

        except SQLAlchemyError as e:
            self.session.rollback()
            raise

    def create_all(self, lines: List[LineModel]):
        try:
            self.session.add_all(lines)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            raise

    def create_one_by_one(self, lines: List[LineModel]):
        for line in lines:
            try:
                self.session.add(line)
                self.session.commit()
            except SQLAlchemyError as e:
                self.session.rollback()
                raise

    def get_all(self) -> List[LineModel]:
        return self.session.query(LineModel).all()

    def get_all_with_factory(self):
        return (self.session.query(LineModel)
                .options(joinedload(LineModel.factory))
                .all())

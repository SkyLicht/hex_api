from typing import List

from sqlalchemy.exc import SQLAlchemyError

from core.data.orm_models.work_plan_model_v1 import FactoryModel


class FactoryDAO:
    def __init__(self, session):
        self.session = session

    # add try except block
    def create_factory(self, factory: FactoryModel):
        self.session.add(factory)
        self.session.commit()

    def get_factory_by_name(self, name: str) -> FactoryModel:
        return self.session.query(FactoryModel).filter(FactoryModel.name == name).first()

    def get_all_factories(self) -> List[FactoryModel]:
        return self.session.query(FactoryModel).all()

    def delete_factory(self, factory: FactoryModel):
        self.session.delete(factory)
        self.session.commit()

    def update_factory(self, factory: FactoryModel):
        self.session.commit()

    def create_all(self, factories: List[FactoryModel]):
        try:
            self.session.add_all(factories)
            self.session.commit()

        except SQLAlchemyError as e:
            self.session.rollback()

            raise

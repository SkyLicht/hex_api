from typing import List

from fastapi import Depends, Request
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from core.data.repositories.layout.line_repository import LineRepository
from core.data.repositories.planner.platform_repository import PlatformRepository
from core.data.repositories.planner.uph_record_repository import UPHRecordRepository
from core.data.repositories.planner.work_plan_repository import WorkPlanRepository
from core.db.ie_tool_db import IETOOLDBConnection


def get_db(request: Request):
    return request.state.db


def get_scoped_db_session():
    db = IETOOLDBConnection().ScopedSession  # Get the ScopedSession instance

    try:
        yield db  # Provide the session to the caller
    except SQLAlchemyError as e:
        db.rollback()  # Rollback transaction in case of an exception
        raise e  # Re-raise the exception
    finally:
        db.remove()  # Call remove() to clear the thread-local session


def get_work_plan_repository(db: Session = Depends(get_db)):
    with WorkPlanRepository(db) as repo:
        yield repo


def get_platform_repository(db: Session = Depends(get_db)):
    with PlatformRepository(db) as repo:
        yield repo


def get_uph_repository(db: Session = Depends(get_scoped_db_session)):
    return UPHRecordRepository(db)


def get_line_repository(
        db: Session = Depends(get_db),
):
    with LineRepository(db) as repo:
        yield repo

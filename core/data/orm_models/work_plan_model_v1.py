from core.db.ie_tool_db import IEToolBase
from datetime import datetime, timezone

from sqlalchemy import Column, String, ForeignKey, Boolean, DateTime, Index, Integer, Float, \
    CheckConstraint, UniqueConstraint
from sqlalchemy.orm import relationship

from core.utils.generate import generate_custom_id


class FactoryModel(IEToolBase):
    """
    A factory or manufacturing facility where production lines are located.
    """
    __tablename__ = 'planner_factory'

    id = Column(String(16), primary_key=True, default=generate_custom_id)
    name = Column(String, unique=True, nullable=False)

    # Add cascade deletion for related lines
    lines = relationship("LineModel", back_populates="factory", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<PlannerFactory(name={self.name})>"


class LineModel(IEToolBase):
    __tablename__ = 'planner_lines'

    id = Column(String(16), primary_key=True, default=generate_custom_id, unique=True, nullable=False)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    factory_id = Column(String(16), ForeignKey('planner_factory.id', ondelete='CASCADE'), nullable=False)

    created_at = Column(DateTime, default=datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc),
                        nullable=False)

    # Add unique constraint for name within a factory
    __table_args__ = (
        UniqueConstraint('name', 'factory_id', name='uq_line_name_per_factory'),
    )

    # Update relationship definitions
    factory = relationship("FactoryModel", back_populates="lines")
    work_plans = relationship("WorkPlanModel", back_populates="line", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<LineModel(name={self.name}, factory_id={self.factory_id})>"


class PlatformModel(IEToolBase):
    """
    Represents a product platform or model, including SKU, cost, and UPH.
    """
    __tablename__ = 'planner_platform'

    id = Column(String(16), primary_key=True, default=generate_custom_id)
    f_n = Column(Integer, nullable=False)
    platform = Column(String, nullable=False)
    sku = Column(String, nullable=False)
    uph = Column(Integer, nullable=False)
    cost = Column(Float, nullable=False)
    in_service = Column(Boolean, nullable=False, default=True)
    components = Column(Integer, nullable=False)
    components_list_id = Column(String, nullable=True)
    width = Column(Float, nullable=True)
    height = Column(Float, nullable=True)

    created_at = Column(DateTime, default=datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc),
                        nullable=False)

    __table_args__ = (
        Index('idx_planner_platform_cost_in_service', 'cost', 'in_service'),
        # Add unique constraint for platform and SKU combination
        UniqueConstraint('platform', 'sku', name='uq_platform_sku'),
    )

    # Add cascade deletion for related work plans
    work_plans = relationship("WorkPlanModel", back_populates="platform", cascade="all, delete-orphan")

    def __repr__(self):
        return (
            f"<PlannerPlatform(id={self.id}, f_n={self.f_n}, platform={self.platform}, sku={self.sku}, "
            f"uph={self.uph}, cost={self.cost}, in_service={self.in_service})>"
        )


class WorkPlanModel(IEToolBase):
    """
    Represents a plan for a particular day, line, and platform combination:
    - The shift hours planned
    - The target OEE
    - The estimated or planned UPH
    """
    __tablename__ = 'planner_work_plan'

    id = Column(String(16), primary_key=True, default=generate_custom_id)
    platform_id = Column(String(16), ForeignKey('planner_platform.id', ondelete='CASCADE'), nullable=False)
    line_id = Column(String(16), ForeignKey('planner_lines.id', ondelete='CASCADE'), nullable=False)
    planned_hours = Column(Float, nullable=False)
    target_oee = Column(Float, nullable=False)
    uph_i = Column(Integer, nullable=False)
    start_hour = Column(Integer, nullable=False)
    end_hour = Column(Integer, nullable=False)
    str_date = Column(String(10), nullable=False)
    week = Column(Integer, nullable=False)
    head_count = Column(Integer, nullable=False)
    ft = Column(Integer, nullable=False)
    ict = Column(Integer, nullable=False)

    created_at = Column(DateTime, default=datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc),
                        nullable=False)

    __table_args__ = (
        CheckConstraint('start_hour >= 0 AND start_hour < 24', name='check_planner_workplan_start_hour'),
        CheckConstraint('end_hour > start_hour AND end_hour <= 24', name='check_planner_workplan_end_hour'),
        CheckConstraint('target_oee >= 0.1 AND target_oee <= 1.0', name='check_planner_workplan_target_oee'),
        CheckConstraint('planned_hours > 0', name='check_planner_workplan_planned_hours'),
        # Add week validation
        CheckConstraint('week >= 1 AND week <= 53', name='check_planner_workplan_week'),
        # Add unique constraint to prevent duplicate plans for same line and date
        UniqueConstraint('line_id', 'str_date', 'start_hour', name='uq_workplan_line_date_start'),
        Index('idx_planner_work_plan_line_date', 'line_id', 'str_date'),
    )

    line = relationship("LineModel", back_populates="work_plans")
    platform = relationship("PlatformModel", back_populates="work_plans")

    def __repr__(self):
        return (
            f"<PlannerWorkPlan(id={self.id}, platform_id={self.platform_id}, line_id={self.line_id}, "
            f"planned_hours={self.planned_hours}, target_oee={self.target_oee}, uph_i={self.uph_i}, "
            f"start_hour={self.start_hour}, end_hour={self.end_hour}, str_date={self.str_date}, week={self.week}, "
            f"head_count={self.head_count}, ft={self.ft}, ict={self.ict})>"
        )


class UPHRecordORM(IEToolBase):
    __tablename__ = 'uph_records'

    id = Column(String(16), primary_key=True, default=generate_custom_id)
    platform_id = Column(String(16), ForeignKey('planner_platform.id', ondelete='CASCADE'), nullable=False)
    line_id = Column(String(16), ForeignKey('planner_lines.id', ondelete='CASCADE'), nullable=False)

    uph = Column(Integer, nullable=False)
    target_oee = Column(Float, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('idx_uph_records_line_id', 'line_id'),
        Index('idx_uph_records_platform_id', 'platform_id'),
    )

    # line = relationship("LineModel", back_populates="uph_records")
    # platform = relationship("PlatformModel", back_populates="uph_records")

    def __repr__(self):
        return (
            f"<UPHRecords"
            f"id={self.id}, platform_id={self.platform_id}, line_id={self.line_id}, "
            f"uph={self.uph}, target_oee={self.target_oee}, start_date={self.start_date}, end_date={self.end_date}>"
        )

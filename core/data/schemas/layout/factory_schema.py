from pydantic import BaseModel

from core.data.schemas.layout.line_schema import LineSmallSchema


class FactoryWithLinesSchema(BaseModel):
    id: str
    name: str
    lines: list[LineSmallSchema]
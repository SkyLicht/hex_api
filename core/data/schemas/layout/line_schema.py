from pydantic import BaseModel

class LineSmallSchema(BaseModel):
    id: str
    is_active: bool
    description: str
    name: str
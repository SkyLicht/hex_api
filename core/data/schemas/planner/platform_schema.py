from pydantic import BaseModel


class PlatformSchema(BaseModel):
    id: str
    f_n: int
    platform: str
    sku: str
    uph: int
    cost: float
    in_service: bool
    components: int
    components_list_id: str | None
    width: float | None
    height: float | None

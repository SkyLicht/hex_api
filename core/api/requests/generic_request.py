from pydantic import BaseModel, Field, field_validator


class PageRequest(BaseModel):
    """Request body to page """
    page: int = Field(..., description="Page number")
    page_size: int = Field(..., description="Page size")

    @field_validator("page", mode='before')
    def validate_page(cls, value):
        if value < 1:
            raise ValueError("Page size must be greater than 0")
        return value

    @field_validator("page_size", mode='before')
    def validate_page_size(cls, value):
        if value < 1:
            raise ValueError("Page size must be greater than 0")
        return value


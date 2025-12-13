from pydantic import BaseModel, ConfigDict


class APIModel(BaseModel):
    """Base request/response schema with strict field handling."""

    model_config = ConfigDict(extra="forbid")


class ORMModel(APIModel):
    """Response schema that supports ORM objects."""

    model_config = ConfigDict(from_attributes=True, extra="forbid")

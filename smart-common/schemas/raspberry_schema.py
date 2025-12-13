from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID

from pydantic import ConfigDict

from app.schemas.base import APIModel, ORMModel
from app.schemas.device_schema import DeviceOut


# ====== BASE ======
class RaspberryBase(APIModel):
    name: str
    description: Optional[str] = None
    software_version: Optional[str] = None
    max_devices: int = 1


# ====== CREATE ======
class RaspberryCreate(RaspberryBase):
    user_id: Optional[int] = None
    inverter_id: Optional[int] = None


# ====== UPDATE ======
class RaspberryUpdate(RaspberryBase):
    inverter_id: Optional[int] = None
    user_id: Optional[int] = None


# ====== OUTPUT ======
class RaspberryOut(RaspberryBase, ORMModel):
    uuid: UUID
    id: int
    user_id: Optional[int]
    inverter_id: Optional[int]

    model_config = ConfigDict(from_attributes=True, extra="forbid")


# ====== OUTPUT ======
class RaspberryCreateOut(RaspberryBase, ORMModel):
    uuid: UUID
    id: int
    secret_plain: str
    user_id: Optional[int]
    inverter_id: Optional[int]

    model_config = ConfigDict(from_attributes=True, extra="forbid")


class RaspberryFullOut(RaspberryOut):
    devices: list[DeviceOut]

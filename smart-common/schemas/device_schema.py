from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import Field

from app.constans.device_mode import DeviceMode
from app.schemas.base import APIModel, ORMModel


class DeviceBase(APIModel):
    name: str = Field(..., description="Nazwa urządzenia")
    device_number: Optional[int] = Field(
        None, description="Numer pinu GPIO (ustalany automatycznie)"
    )
    mode: DeviceMode = Field(default=DeviceMode.MANUAL, description="Tryb pracy")
    rated_power_kw: Optional[float] = Field(None, description="Deklarowana moc urządzenia (W)")
    threshold_kw: Optional[float] = None
    hysteresis_w: Optional[float] = 100
    schedule: Optional[Any] = None


class DeviceCreate(DeviceBase):
    raspberry_id: int = Field(
        ..., description="ID Raspberry, do którego przypisane jest urządzenie"
    )


class DeviceUpdate(DeviceBase):
    is_on: Optional[bool] = None
    raspberry_id: Optional[int] = None
    user_id: Optional[int] = None


class DeviceOut(DeviceBase, ORMModel):
    id: int
    uuid: UUID
    user_id: Optional[int]
    raspberry_id: Optional[int]
    is_on: bool
    last_update: datetime

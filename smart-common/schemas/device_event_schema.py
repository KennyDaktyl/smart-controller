from datetime import datetime
from typing import Optional

from app.schemas.base import APIModel, ORMModel


class DeviceEventCreate(APIModel):
    device_id: int
    pin_state: bool
    trigger_reason: Optional[str] = None
    power_kw: Optional[float] = None
    timestamp: Optional[datetime] = None


class DeviceEventOut(ORMModel):
    id: int
    device_id: int
    state: str
    pin_state: bool
    trigger_reason: Optional[str] = None
    power_kw: Optional[float] = None
    timestamp: datetime


class DeviceEventSeriesOut(APIModel):
    events: list[DeviceEventOut]
    total_minutes_on: int
    energy_kwh: Optional[float] = None
    rated_power_kw: Optional[float] = None

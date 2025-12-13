from typing import Optional

from app.schemas.base import APIModel, ORMModel


class DeviceScheduleBase(APIModel):
    day_of_week: str
    start_time: str
    end_time: str
    enabled: bool = True


class DeviceScheduleCreate(DeviceScheduleBase):
    device_id: int


class DeviceScheduleUpdate(APIModel):
    day_of_week: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    enabled: Optional[bool] = None


class DeviceScheduleOut(DeviceScheduleBase, ORMModel):
    id: int
    device_id: int

# app/adapters/api/huawei/schemas.py
from dataclasses import dataclass
from typing import Any


@dataclass
class HuaweiStation:
    station_code: str
    name: str | None = None


@dataclass
class HuaweiDevice:
    device_id: str
    device_type: str | None = None


@dataclass
class HuaweiProduction:
    active_power_w: float
    raw: dict[str, Any]

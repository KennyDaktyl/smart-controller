from typing import List, Optional

from app.schemas.base import APIModel, ORMModel
from app.schemas.inverter_schema import InverterOut


class InstallationLite(ORMModel):
    id: int
    name: str


class InstallationBase(APIModel):
    name: str
    station_code: str
    station_addr: Optional[str] = None


class InstallationCreate(InstallationBase):
    pass


class InstallationUpdate(APIModel):
    name: Optional[str] = None
    station_addr: Optional[str] = None


class InstallationOut(InstallationBase, ORMModel):
    id: int
    user_id: int
    inverters: List[InverterOut] = []

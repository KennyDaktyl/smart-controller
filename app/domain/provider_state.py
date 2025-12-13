from dataclasses import dataclass
from datetime import datetime


@dataclass
class ProviderState:
    value: float | None
    unit: str
    measured_at: datetime
    raw: dict | None = None

# app/scheduler/task.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(order=True)
class ProviderTask:
    next_run: float
    provider_id: int = field(compare=False)
    provider: Any = field(compare=False)
    adapter: Any = field(compare=False)
    force_poll: bool = field(default=False, compare=False)

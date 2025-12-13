import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Type, Optional

from pydantic import BaseModel, Field, ValidationError

from .client import nats_client
from .publisher import publisher

logger = logging.getLogger(__name__)


# ============================================================
# 1) BASE EVENT SCHEMA
# ============================================================

class Event(BaseModel):
    """
    Standardowa struktura eventów platformy Smart Energy.
    Każdy event ma:
      - event_type  → np. INVERTER_UPDATE, HEARTBEAT
      - timestamp   → ISO time UTC
      - payload     → dowolny Pydantic model
      - meta        → dodatkowe dane (opcjonalne)
    """

    event_type: str = Field(..., description="Type identifier of the event")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Dict[str, Any] = Field(default_factory=dict)
    meta: Optional[Dict[str, Any]] = Field(default=None)

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat(),
        }

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


# ============================================================
# 2) SPECYFICZNE EVENTY PLATFORMY
# ============================================================

class InverterUpdatePayload(BaseModel):
    inverter_id: int
    serial_number: str
    active_power: float | None
    status: str
    error_message: str | None = None
    timestamp: datetime


class InverterUpdateEvent(Event):
    event_type: str = "INVERTER_UPDATE"
    payload: InverterUpdatePayload


class HeartbeatPayload(BaseModel):
    uuid: str
    status: str = "online"
    temperature: float | None = None
    uptime: float | None = None
    ip: str | None = None


class HeartbeatEvent(Event):
    event_type: str = "HEARTBEAT"
    payload: HeartbeatPayload


class DeviceEventPayload(BaseModel):
    device_id: int
    installation_id: int
    value: float | None
    status: str
    timestamp: datetime


class DeviceEvent(Event):
    event_type: str = "DEVICE_EVENT"
    payload: DeviceEventPayload


# ============================================================
# 3) REJESTR EVENTÓW – DO AUTOMATYCZNEGO DEKODOWANIA
# ============================================================

EVENT_REGISTRY: Dict[str, Type[Event]] = {
    "INVERTER_UPDATE": InverterUpdateEvent,
    "HEARTBEAT": HeartbeatEvent,
    "DEVICE_EVENT": DeviceEvent,
}


def decode_event(raw_json: bytes) -> Event:
    """
    Automatyczne odczytywanie eventów wg event_type.
    """
    try:
        data = json.loads(raw_json.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"Invalid JSON event: {e}")

    event_type = data.get("event_type")
    if not event_type:
        raise ValueError("Missing event_type")

    cls = EVENT_REGISTRY.get(event_type)
    if not cls:
        raise ValueError(f"Unknown event_type: {event_type}")

    try:
        return cls(**data)
    except ValidationError as e:
        raise ValueError(f"Event validation failed for {event_type}: {e}")


# ============================================================
# 4) WYSYŁANIE EVENTÓW — unified API
# ============================================================

async def publish_event(subject: str, event: Event):
    """
    Publikuje event jako JetStream message.

    Każdy event powinien być instancją Event lub klasy dziedziczącej.

    Przykład:
        event = InverterUpdateEvent(payload=InverterUpdatePayload(...))
        await publish_event("device_communication.inverter.123.update", event)
    """
    try:
        await nats_client.js_publish(subject, event.model_dump(mode="json"))
        logger.info(f"[EVENT] Published event_type={event.event_type} subject={subject}")
    except Exception as e:
        logger.error(f"[EVENT] Failed to publish event: {e}")
        raise e


# ============================================================
# 5) Helpry do szybkiego tworzenia eventów
# ============================================================

def make_inverter_update(**data) -> InverterUpdateEvent:
    return InverterUpdateEvent(payload=InverterUpdatePayload(**data))


def make_heartbeat(**data) -> HeartbeatEvent:
    return HeartbeatEvent(payload=HeartbeatPayload(**data))


def make_device_event(**data) -> DeviceEvent:
    return DeviceEvent(payload=DeviceEventPayload(**data))

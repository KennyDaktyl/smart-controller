from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String

from app.core.db import Base


class DeviceEvent(Base):
    __tablename__ = "device_events"

    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey("devices.id", ondelete="CASCADE"))

    event_name = Column(String, default="DEVICE_STATE", nullable=False)
    state = Column(String, nullable=False)  # e.g. ON / OFF
    pin_state = Column(Boolean, nullable=False)
    trigger_reason = Column(String, nullable=True)
    power_kw = Column(Numeric(10, 2), nullable=True)

    timestamp = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    def __repr__(self):
        return (
            f"<DeviceEvent(event_name={self.event_name}, device_id={self.device_id}, "
            f"state={self.state}, pin_state={self.pin_state}, timestamp={self.timestamp})>"
        )

import uuid
from datetime import datetime, timezone

from sqlalchemy import (JSON, UUID, Boolean, Column, DateTime, Enum, ForeignKey, Integer, Numeric,
                        String)
from sqlalchemy.orm import relationship

from app.constans.device_mode import DeviceMode
from app.core.db import Base


class Device(Base):
    __tablename__ = "devices"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    uuid = Column(UUID(as_uuid=True), unique=True, default=uuid.uuid4, index=True)

    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    raspberry_id = Column(Integer, ForeignKey("raspberries.id", ondelete="CASCADE"))

    device_number = Column(Integer, nullable=False)
    rated_power_kw = Column(Numeric, nullable=True)

    # 🔧 Enum trybu pracy
    mode = Column(
        Enum(DeviceMode, name="devicemode", create_type=False),
        default=DeviceMode.MANUAL,
        nullable=False,
    )

    threshold_kw = Column(Numeric, nullable=True)
    hysteresis_w = Column(Numeric, default=100)

    schedule = Column(JSON, nullable=True)

    is_on = Column(Boolean, default=False)
    last_update = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    manual_state = Column(Boolean, nullable=True)

    raspberry = relationship("Raspberry", back_populates="devices")
    user = relationship("User", back_populates="devices")
    schedules = relationship(
        "DeviceSchedule", back_populates="device", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Device id={self.id} name={self.name} mode={self.mode}>"

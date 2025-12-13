from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.core.db import Base


class DeviceSchedule(Base):
    __tablename__ = "device_schedules"

    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey("devices.id", ondelete="CASCADE"))
    day_of_week = Column(String, nullable=False)  # mon, tue, wed, ...
    start_time = Column(String, nullable=False)  # "10:00"
    end_time = Column(String, nullable=False)  # "20:00"
    enabled = Column(Boolean, default=True)

    device = relationship("Device", back_populates="schedules")

    def __repr__(self):
        return f"<DeviceSchedule id={self.id} {self.day_of_week} {self.start_time}-{self.end_time}>"

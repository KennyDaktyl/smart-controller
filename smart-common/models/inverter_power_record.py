from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric

from app.core.db import Base


class InverterPowerRecord(Base):

    __tablename__ = "inverter_power_records"

    id = Column(Integer, primary_key=True, index=True)
    inverter_id = Column(Integer, ForeignKey("inverters.id", ondelete="CASCADE"), nullable=False)
    active_power = Column(Numeric(10, 2), nullable=True)
    timestamp = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    def __repr__(self):
        return f"<InverterPowerRecord(inverter_id={self.inverter_id}, active_power={self.active_power}, timestamp={self.timestamp})>"

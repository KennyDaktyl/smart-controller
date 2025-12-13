from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship

from app.core.db import Base


class Inverter(Base):
    __tablename__ = "inverters"

    id = Column(Integer, primary_key=True)
    installation_id = Column(
        Integer, ForeignKey("installations.id", ondelete="CASCADE"), nullable=False
    )

    serial_number = Column(String, nullable=False, unique=True)
    name = Column(String, nullable=True)
    model = Column(String, nullable=True)
    dev_type_id = Column(Integer, nullable=True)

    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    capacity_kw = Column(Numeric(10, 3), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_updated = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    installation = relationship("Installation", back_populates="inverters")
    raspberries = relationship("Raspberry", back_populates="inverter", cascade="all, delete-orphan")

    def __repr__(self):
        return (
            f"<Inverter(name={self.name}, serial={self.serial_number}, "
            f"model={self.model}, installation_id={self.installation_id})>"
        )

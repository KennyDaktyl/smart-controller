import uuid

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.core.db import Base


class Raspberry(Base):
    __tablename__ = "raspberries"

    id = Column(Integer, primary_key=True)
    uuid = Column(UUID(as_uuid=True), unique=True, default=uuid.uuid4, index=True)
    secret_key = Column(String, nullable=False)

    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    software_version = Column(String, nullable=True)

    max_devices = Column(Integer, default=1)

    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    inverter_id = Column(Integer, ForeignKey("inverters.id", ondelete="SET NULL"), nullable=True)

    user = relationship("User", back_populates="raspberries")
    inverter = relationship("Inverter", back_populates="raspberries")

    devices = relationship("Device", back_populates="raspberry", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Raspberry id={self.id} name={self.name} identifier={self.identifier}>"

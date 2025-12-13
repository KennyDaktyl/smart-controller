from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.core.db import Base


class Installation(Base):
    __tablename__ = "installations"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    station_code = Column(String(255), nullable=False, unique=True)
    station_addr = Column(String(255), nullable=True)

    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    user = relationship("User", back_populates="installations")
    inverters = relationship(
        "Inverter", back_populates="installation", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Installation(name={self.name}, station_code={self.station_code}, user_id={self.user_id})>"

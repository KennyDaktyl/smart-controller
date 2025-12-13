from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Enum, Integer, String
from sqlalchemy.orm import relationship

from app.constans.role import UserRole
from app.core.db import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    huawei_username = Column(String, nullable=True)
    huawei_password_encrypted = Column(String, nullable=True)

    role = Column(Enum(UserRole), nullable=False, default=UserRole.CLIENT)

    installations = relationship(
        "Installation", back_populates="user", cascade="all, delete-orphan"
    )
    raspberries = relationship("Raspberry", back_populates="user", cascade="all, delete-orphan")
    devices = relationship("Device", back_populates="user", cascade="all, delete-orphan")

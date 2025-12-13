from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    JSON,
    Numeric,
    String,
    Boolean,
)
from sqlalchemy.orm import relationship

from app.core.db import Base
from common.enums.provider import ProviderType, PowerUnit


class Provider(Base):
    __tablename__ = "providers"

    id = Column(Integer, primary_key=True)

    installation_id = Column(
        Integer,
        ForeignKey("installations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # ---- domena ----
    name = Column(String, nullable=False)
    provider_type = Column(Enum(ProviderType), nullable=False)

    # np: huawei, goodwe, siemens, dht22
    vendor = Column(String, nullable=False)
    model = Column(String, nullable=True)

    # ---- dane pomiarowe ----
    unit = Column(Enum(PowerUnit), nullable=False)
    last_value = Column(Numeric(12, 4), nullable=True)
    last_measurement_at = Column(DateTime(timezone=True), nullable=True)

    # ---- sterowanie controllerem ----
    polling_interval_sec = Column(Integer, nullable=False)
    enabled = Column(Boolean, default=True)

    # ---- konfiguracja adaptera ----
    # API keys, urls, serial numbers, GPIO pins, itp.
    config = Column(JSON, nullable=False)

    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # ---- relacje ----
    installation = relationship("Installation", back_populates="providers")

    def __repr__(self) -> str:
        return (
            f"<Provider id={self.id} "
            f"type={self.provider_type} "
            f"vendor={self.vendor} "
            f"unit={self.unit}>"
        )

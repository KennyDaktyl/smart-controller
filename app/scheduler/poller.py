from __future__ import annotations

import asyncio
import logging
from typing import Any

from smart_common.providers.models import NormalizedMeasurement

try:
    from smart_common.repositories.measurement_repository import MeasurementRepository
except ImportError:  # pragma: no cover - extension required in smart_common
    MeasurementRepository = None

logger = logging.getLogger(__name__)


class ProviderPoller:
    def __init__(
        self,
        *,
        publisher: Any,
        semaphore: asyncio.Semaphore,
    ) -> None:
        self._publisher = publisher
        self._semaphore = semaphore

    async def poll(self, provider: Any, adapter: Any) -> None:
        provider_id = getattr(provider, "id", None)
        async with self._semaphore:
            try:
                measurement = await _call_adapter(adapter)
                await self._save_measurement(provider, measurement)
                await self._publish_measurement(provider_id, measurement)
                logger.info(
                    "Provider poll succeeded",
                    extra={"provider_id": provider_id},
                )
            except Exception as exc:
                logger.exception(
                    "Provider poll failed",
                    extra={"provider_id": provider_id, "error": str(exc)},
                )

    async def _save_measurement(
        self,
        provider: Any,
        measurement: NormalizedMeasurement,
    ) -> None:
        if MeasurementRepository is None:
            raise RuntimeError(
                "MeasurementRepository not available in smart_common; "
                "add it as an extension to persist measurements."
            )

        await _call_repo_save(provider, measurement)

    async def _publish_measurement(
        self,
        provider_id: int | None,
        measurement: NormalizedMeasurement,
    ) -> None:
        subject = f"energy.power.{provider_id}"
        payload = {
            "provider_id": measurement.provider_id,
            "value": measurement.value,
            "unit": measurement.unit,
            "measured_at": measurement.measured_at.isoformat(),
            "metadata": measurement.metadata,
        }
        await self._publisher.publish(subject, payload)


async def _call_adapter(adapter: Any) -> NormalizedMeasurement:
    fetch = adapter.fetch_measurement
    if asyncio.iscoroutinefunction(fetch):
        return await fetch()

    # TODO: migrate BaseHttpAdapter to async (httpx.AsyncClient) to avoid threads.
    return await asyncio.to_thread(fetch)


async def _call_repo_save(provider: Any, measurement: NormalizedMeasurement) -> None:
    # TODO: switch to async repository when smart_common offers one.
    def _save() -> None:
        from smart_common.core.db import get_db

        db_gen = get_db()
        db = next(db_gen)
        try:
            repo = MeasurementRepository(db)
            repo.save_measurement(provider, measurement)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    await asyncio.to_thread(_save)

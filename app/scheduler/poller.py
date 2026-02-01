# app/scheduler/poller.py
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any

from smart_common.models.provider import NormalizedMeasurement, Provider
from smart_common.nats.event_helpers import build_event_payload, subject_for_entity

try:
    from smart_common.repositories.measurement_repository import MeasurementRepository
except ImportError:
    MeasurementRepository = None

logger = logging.getLogger(__name__)
EVENT_SOURCE = "smart-controller"
CURRENT_ENERGY_EVENT_TYPE = "CURRENT_ENERGY"


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
        vendor = getattr(provider, "vendor", None)
        vendor_label = (
            vendor.value
            if hasattr(vendor, "value")
            else str(vendor) if vendor else None
        )
        poll_id = uuid.uuid4().hex
        task_name = f"provider-{provider_id}"
        context = {
            "provider_id": provider_id,
            "vendor": vendor_label,
            "taskName": task_name,
            "poll_id": poll_id,
        }
        adapter.poll_id = poll_id
        adapter.task_name = task_name

        provider_unit_hint = _resolve_provider_unit(provider)
        async with self._semaphore:
            logger.info("Polling provider start", extra=context)
            measurement: NormalizedMeasurement
            persisted = None
            fetch_failed = False
            try:
                raw_measurement = await self._fetch_measurement(adapter)
                measurement = _attach_provider_unit(raw_measurement, provider_unit_hint)
                logger.info(
                    "Provider fetch successful",
                    extra={
                        **context,
                        "value": measurement.value,
                        "unit": measurement.unit,
                    },
                )
                # Ensure scheduling uses the latest successful fetch timestamp
                persisted = await self._persist_measurement(
                    provider,
                    measurement,
                    poll_id=poll_id,
                )
            except Exception as exc:
                fetch_failed = True
                logger.exception(
                    "Provider poll failed",
                    extra={**context, "error": str(exc)},
                )
                measurement = _build_error_measurement(
                    provider_id=provider_id,
                    poll_id=poll_id,
                    vendor=vendor_label,
                    error=exc,
                    unit_hint=provider_unit_hint,
                )
            await self._publish_measurement(provider, measurement, context)
            if fetch_failed:
                logger.info(
                    "Publishing error measurement",
                    extra={
                        **context,
                        "metadata": measurement.metadata,
                    },
                )
            else:
                logger.info(
                    "Provider poll succeeded",
                    extra={
                        **context,
                        "measurement_persisted": persisted is not None,
                    },
                )

    async def _fetch_measurement(self, adapter: Any) -> NormalizedMeasurement:
        fetch = adapter.fetch_measurement
        if asyncio.iscoroutinefunction(fetch):
            return await fetch()

        return await asyncio.to_thread(fetch)

    async def _persist_measurement(
        self,
        provider: Provider,
        measurement: NormalizedMeasurement,
        *,
        poll_id: str,
    ) -> object | None:
        provider_id = getattr(provider, "id", None)
        if MeasurementRepository is None:
            logger.debug(
                "MeasurementRepository extension unavailable; skipping persistence",
                extra={"provider_id": provider_id, "poll_id": poll_id},
            )
            return None

        entry = await _call_repo_save(provider, measurement, poll_id=poll_id)
        if entry is None:
            logger.debug(
                "Measurement data unchanged; persistence skipped",
                extra={"provider_id": provider_id, "poll_id": poll_id},
            )

        return entry

    async def _publish_measurement(
        self,
        provider: Provider,
        measurement: NormalizedMeasurement,
        context: dict[str, Any],
    ) -> None:
        entity_id = _entity_id_from_provider(provider)
        # subject = subject_for_entity(entity_id)
        subject_env = os.getenv("SUBJECT")
        subject = subject_env.replace("*", entity_id)
        payload = build_event_payload(
            event_type=CURRENT_ENERGY_EVENT_TYPE,
            entity_type="provider",
            entity_id=entity_id,
            data=_measurement_event_data(measurement),
            source=EVENT_SOURCE,
            subject=subject,
        )
        logger.info(
            "Publishing measurement to NATS",
            extra={
                **context,
                "subject": subject,
                "payload_size": len(json.dumps(payload)),
            },
        )
        await self._publisher.publish(subject, payload, context=context)


def _build_error_measurement(
    *,
    provider_id: int | None,
    poll_id: str,
    vendor: str | None,
    error: Exception,
    unit_hint: str | None = None,
) -> NormalizedMeasurement:
    metadata = {
        "error": {
            "type": error.__class__.__name__,
            "message": str(error),
            "vendor": vendor,
            "poll_id": poll_id,
        }
    }
    unit_hint_value = unit_hint or None
    if unit_hint_value is not None:
        metadata["unit_source"] = unit_hint_value
    return NormalizedMeasurement(
        provider_id=provider_id or 0,
        value=None,
        unit=unit_hint_value,
        measured_at=datetime.utcnow(),
        metadata=metadata,
    )


def _attach_provider_unit(
    measurement: NormalizedMeasurement,
    provider_unit: str | None,
) -> NormalizedMeasurement:
    resolved_unit = provider_unit or measurement.unit
    # Don't mutate adapter values; just tag provider unit so downstream consumes know the domain source.
    metadata = dict(measurement.metadata or {})
    if resolved_unit:
        metadata.setdefault("unit_source", resolved_unit)
    return NormalizedMeasurement(
        provider_id=measurement.provider_id,
        value=measurement.value,
        unit=resolved_unit,
        measured_at=measurement.measured_at,
        metadata=metadata,
    )


def _resolve_provider_unit(provider: Provider) -> str | None:
    unit = getattr(provider, "unit", None)
    if unit is None:
        return None
    return unit.value if hasattr(unit, "value") else str(unit)


def _measurement_event_data(measurement: NormalizedMeasurement) -> dict[str, Any]:
    return {
        "value": measurement.value,
        "unit": measurement.unit,
        "measured_at": measurement.measured_at.isoformat(),
        "metadata": dict(measurement.metadata or {}),
    }


def _entity_id_from_provider(provider: Provider) -> str:
    uuid_value = getattr(provider, "uuid", None)
    if uuid_value:
        return str(uuid_value)

    provider_id = getattr(provider, "id", None)
    if provider_id is not None:
        return str(provider_id)

    return "unknown-provider"


async def _call_repo_save(
    provider: Any,
    measurement: NormalizedMeasurement,
    *,
    poll_id: str,
) -> object | None:
    def _save() -> object | None:
        from smart_common.core.db import get_db

        db_gen = get_db()
        db = next(db_gen)
        try:
            repo = MeasurementRepository(db)
            entry = repo.save_measurement(
                provider,
                measurement,
                poll_id=poll_id,
            )
            db.commit()
            return entry
        except Exception:
            db.rollback()
            raise
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    return await asyncio.to_thread(_save)

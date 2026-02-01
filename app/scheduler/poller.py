from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any

from smart_common.models.provider import NormalizedMeasurement, Provider
from smart_common.nats.event_helpers import build_event_payload

try:
    from smart_common.repositories.measurement_repository import MeasurementRepository
except ImportError as e:
    raise RuntimeError("MeasurementRepository import failed") from e

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
            "taskName": task_name,
            "provider_id": provider_id,
            "vendor": vendor_label,
            "poll_id": poll_id,
        }

        logger.info(
            "POLL START",
            extra={
                **context,
                "provider_class": type(provider).__name__,
                "adapter_class": type(adapter).__name__ if adapter else None,
            },
        )

        if adapter is None:
            raise RuntimeError(f"Adapter is None for provider_id={provider_id}")

        subject_env = os.getenv("SUBJECT")
        if not subject_env:
            raise RuntimeError("SUBJECT env variable is missing")

        adapter.poll_id = poll_id
        adapter.task_name = task_name

        provider_unit_hint = _resolve_provider_unit(provider)

        async with self._semaphore:
            logger.info("FETCH START", extra=context)

            measurement: NormalizedMeasurement
            persisted = None
            fetch_failed = False

            try:
                raw_measurement = await self._fetch_measurement(adapter)

                logger.info(
                    "FETCH END",
                    extra={
                        **context,
                        "raw_value": getattr(raw_measurement, "value", None),
                        "raw_unit": getattr(raw_measurement, "unit", None),
                    },
                )

                measurement = _attach_provider_unit(raw_measurement, provider_unit_hint)

                logger.info(
                    "PROVIDER FETCH OK",
                    extra={
                        **context,
                        "value": measurement.value,
                        "unit": measurement.unit,
                    },
                )

                persisted = await self._persist_measurement(
                    provider,
                    measurement,
                    poll_id=poll_id,
                )

            except Exception as exc:
                fetch_failed = True
                logger.exception(
                    "PROVIDER POLL FAILED",
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
                    "POLL FINISHED WITH ERROR",
                    extra={
                        **context,
                        "metadata": measurement.metadata,
                    },
                )
            else:
                logger.info(
                    "POLL FINISHED OK",
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

        logger.info(
            "PERSIST ATTEMPT",
            extra={
                "provider_id": provider_id,
                "poll_id": poll_id,
                "value": measurement.value,
                "unit": measurement.unit,
            },
        )

        entry = await _call_repo_save(provider, measurement, poll_id=poll_id)

        if entry is None:
            logger.info(
                "PERSIST SKIPPED (UNCHANGED)",
                extra={"provider_id": provider_id, "poll_id": poll_id},
            )
        else:
            logger.info(
                "PERSIST OK",
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
            "NATS PUBLISH START",
            extra={
                **context,
                "subject": subject,
                "value": measurement.value,
                "unit": measurement.unit,
            },
        )

        await self._publisher.publish(subject, payload, context=context)

        logger.info(
            "NATS PUBLISH OK",
            extra={
                **context,
                "subject": subject,
            },
        )


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

    if unit_hint:
        metadata["unit_source"] = unit_hint

    return NormalizedMeasurement(
        provider_id=provider_id or 0,
        value=None,
        unit=unit_hint,
        measured_at=datetime.utcnow(),
        metadata=metadata,
    )


def _attach_provider_unit(
    measurement: NormalizedMeasurement,
    provider_unit: str | None,
) -> NormalizedMeasurement:
    resolved_unit = provider_unit or measurement.unit

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

        logger.info(
            "DB SAVE START",
            extra={
                "provider_id": getattr(provider, "id", None),
                "poll_id": poll_id,
            },
        )

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

            logger.info(
                "DB SAVE COMMIT",
                extra={
                    "provider_id": getattr(provider, "id", None),
                    "poll_id": poll_id,
                },
            )

            return entry

        except Exception:
            db.rollback()
            logger.exception(
                "DB SAVE FAILED",
                extra={
                    "provider_id": getattr(provider, "id", None),
                    "poll_id": poll_id,
                },
            )
            raise

        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    return await asyncio.to_thread(_save)

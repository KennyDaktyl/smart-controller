# app/scheduler/scheduler.py
from __future__ import annotations

import asyncio
import heapq
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from smart_common.core.db import get_db
from smart_common.nats.client import nats_client
from smart_common.nats.publisher import NatsPublisher
from smart_common.providers.adapters.factory import create_adapter_for_provider
from smart_common.providers import get_vendor_adapter_factory
from smart_common.repositories.measurement_repository import MeasurementRepository

try:
    from smart_common.repositories.provider import ProviderRepository
except ImportError:
    from smart_common.repositories.provider import ProviderRepository

from app.scheduler.poller import ProviderPoller
from app.scheduler.task import ProviderTask

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self,
        *,
        refresh_interval_sec: float,
        default_poll_interval_sec: float,
        max_concurrency: int,
    ) -> None:
        self._refresh_interval_sec = max(5.0, refresh_interval_sec)
        self._default_poll_interval_sec = max(1.0, default_poll_interval_sec)
        self._publisher = NatsPublisher(nats_client)
        self._adapter_factory = get_vendor_adapter_factory()
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._poller = ProviderPoller(
            publisher=self._publisher,
            semaphore=self._semaphore,
        )
        self._tasks: list[ProviderTask] = []
        self._task_index: dict[int, ProviderTask] = {}
        self._inflight: set[asyncio.Task[None]] = set()
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("Scheduler starting", extra={"taskName": "scheduler"})
        logger.info("Scheduler entering main loop", extra={"taskName": "scheduler"})
        await self._refresh_providers()
        next_refresh = asyncio.get_running_loop().time() + self._refresh_interval_sec

        while not self._stop_event.is_set():
            now = asyncio.get_running_loop().time()
            logger.debug(
                "Scheduler loop tick",
                extra={
                    "taskName": "scheduler",
                    "now": now,
                    "next_refresh": next_refresh,
                    "tasks_in_queue": len(self._tasks),
                },
            )

            if now >= next_refresh:
                logger.info(
                    "Scheduler refreshing providers",
                    extra={
                        "taskName": "scheduler",
                        "now": now,
                        "next_refresh": next_refresh,
                    },
                )
                await self._refresh_providers()
                next_refresh = (
                    asyncio.get_running_loop().time() + self._refresh_interval_sec
                )

            await self._dispatch_due_tasks(now)

            next_task_candidate = self._next_task_run()
            next_task_run = self._tasks[0].next_run if self._tasks else None
            sleep_reason = (
                "refresh" if next_refresh <= next_task_candidate else "task"
            )
            next_target = min(next_refresh, next_task_candidate)
            sleep_seconds = max(0.1, next_target - asyncio.get_running_loop().time())
            logger.debug(
                "Scheduler sleeping until next target",
                extra={
                    "taskName": "scheduler",
                    "sleep_until": next_target,
                    "sleep_seconds": sleep_seconds,
                    "next_due_task": next_task_run,
                    "sleep_reason": sleep_reason,
                },
            )

            await _sleep_until(next_target, self._stop_event)
            if self._stop_event.is_set():
                break

        await self._drain_running_tasks()
        logger.info("Scheduler stopped", extra={"taskName": "scheduler"})

    async def stop(self) -> None:
        self._stop_event.set()

    def _next_task_run(self) -> float:
        if not self._tasks:
            return asyncio.get_running_loop().time() + self._refresh_interval_sec
        return self._tasks[0].next_run

    async def _dispatch_due_tasks(self, now: float) -> None:
        while self._tasks and self._tasks[0].next_run <= now:
            task = heapq.heappop(self._tasks)
            if self._task_index.get(task.provider_id) is not task:
                continue

            logger.info(
                "Dispatching provider task",
                extra={
                    "provider_id": task.provider_id,
                    "taskName": "scheduler",
                    "scheduled_for": task.next_run,
                    "now": now,
                },
            )

            should_poll, ready_at, interval, reason = should_poll_provider(
                task.provider,
                task.adapter,
                now,
                self._default_poll_interval_sec,
                force_poll=task.force_poll,
            )

            if not should_poll:
                next_run = max(ready_at, now + interval)
                logger.info(
                    "Skipping provider poll",
                    extra={
                        "provider_id": task.provider_id,
                        "reason": reason,
                        "interval": interval,
                        "next_run": next_run,
                        "taskName": "scheduler",
                    },
                )
                rescheduled = ProviderTask(
                    next_run=next_run,
                    provider_id=task.provider_id,
                    provider=task.provider,
                    adapter=task.adapter,
                    force_poll=False,
                )
                self._task_index[task.provider_id] = rescheduled
                heapq.heappush(self._tasks, rescheduled)
                continue

            poll_task = asyncio.create_task(
                self._poll_provider(task),
                name=f"provider-{task.provider_id}",
            )
            self._track_task(poll_task)

            next_run = now + interval
            rescheduled = ProviderTask(
                next_run=next_run,
                provider_id=task.provider_id,
                provider=task.provider,
                adapter=task.adapter,
                force_poll=False,
            )
            self._task_index[task.provider_id] = rescheduled
            heapq.heappush(self._tasks, rescheduled)

            logger.info(
                "Provider scheduled for next poll",
                extra={
                    "provider_id": task.provider_id,
                    "next_run": next_run,
                    "interval": interval,
                    "taskName": "scheduler",
                },
            )

    async def _poll_provider(self, task: ProviderTask) -> None:
        await self._poller.poll(task.provider, task.adapter)

    async def _refresh_providers(self) -> None:
        providers = await _fetch_active_providers()
        provider_map = {getattr(p, "id"): p for p in providers}

        logger.info(
            "Refreshing providers from database",
            extra={
                "taskName": "scheduler",
                "provider_count": len(providers),
            },
        )

        for provider_id, task in list(self._task_index.items()):
            provider = provider_map.get(provider_id)
            if not provider:
                self._task_index.pop(provider_id, None)
                continue

            context = {
                **_provider_context(provider),
                "taskType": "refresh",
            }
            logger.debug("Provider state reloaded", extra=context)

            if _provider_signature(provider) != _provider_signature(task.provider):
                try:
                    adapter = create_adapter_for_provider(
                        provider,
                        factory=self._adapter_factory,
                    )
                except Exception:
                    logger.exception(
                        "Failed to rebuild adapter",
                        extra={"provider_id": provider_id},
                    )
                    continue
                refreshed = ProviderTask(
                    next_run=asyncio.get_running_loop().time(),
                    provider_id=provider_id,
                    provider=provider,
                    adapter=adapter,
                    force_poll=True,
                )
                self._task_index[provider_id] = refreshed
                heapq.heappush(self._tasks, refreshed)

        for provider in providers:
            provider_id = getattr(provider, "id")
            if provider_id in self._task_index:
                continue
            try:
                adapter = create_adapter_for_provider(
                    provider,
                    factory=self._adapter_factory,
                )
            except Exception:
                logger.exception(
                    "Failed to create adapter",
                    extra={"provider_id": provider_id},
                )
                continue
            logger.info(
                "Provider adapter created",
                extra={
                    **_provider_context(provider),
                    "taskType": "initial_schedule",
                },
            )
            task = ProviderTask(
                next_run=asyncio.get_running_loop().time(),
                provider_id=provider_id,
                provider=provider,
                adapter=adapter,
                force_poll=True,
            )
            self._task_index[provider_id] = task
            heapq.heappush(self._tasks, task)

        logger.info(
            "Provider refresh complete",
            extra={"provider_count": len(providers)},
        )

    async def _drain_running_tasks(self) -> None:
        if not self._inflight:
            return
        await asyncio.gather(*self._inflight, return_exceptions=True)

    def _track_task(self, task: asyncio.Task[None]) -> None:
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)


async def _sleep_until(target_ts: float, stop_event: asyncio.Event) -> None:
    delay = target_ts - asyncio.get_running_loop().time()
    if delay <= 0:
        return

    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay)
    except asyncio.TimeoutError:
        return


async def _fetch_active_providers() -> list[Any]:
    # TODO: ensure ProviderRepository exposes get_active_providers() in smart_common.
    db_gen = get_db()
    db = next(db_gen)
    try:
        repo = ProviderRepository(db)
        if not hasattr(repo, "get_active_providers"):
            raise RuntimeError(
                "ProviderRepository.get_active_providers() missing; "
                "add it to smart_common to list enabled providers."
            )
        result = repo.get_active_providers()
        providers = await result if asyncio.iscoroutine(result) else result

        measurement_repo = MeasurementRepository(db)
        provider_ids = [
            provider.id
            for provider in providers
            if getattr(provider, "id", None) is not None
        ]
        last_measurements = measurement_repo.get_last_measurements(provider_ids)
        for provider in providers:
            provider_id = getattr(provider, "id", None)
            last = last_measurements.get(provider_id)
            setattr(
                provider,
                "last_measurement_at",
                last.measured_at if last else None,
            )

        return providers
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass


def _resolve_poll_interval(
    provider: Any,
    adapter: Any,
    default_interval: float,
) -> float:
    provider_expected = _to_float(getattr(provider, "expected_interval_sec", None))
    interval = default_interval if provider_expected is None else provider_expected

    adapter_min = _to_float(getattr(adapter, "min_poll_interval", None))
    if adapter_min is not None:
        interval = max(interval, adapter_min)

    return max(1.0, interval)


def should_poll_provider(
    provider: Any,
    adapter: Any,
    now: float,
    default_interval: float,
    *,
    force_poll: bool = False,
) -> tuple[bool, float, float, str]:
    interval = _resolve_poll_interval(provider, adapter, default_interval)
    if not getattr(provider, "enabled", True):
        logger.info(
            "Provider disabled, skipping poll",
            extra={
                **_provider_context(provider),
                "reason": "provider_disabled",
                "taskName": "scheduler",
            },
        )
        return False, now + interval, interval, "provider_disabled"

    if force_poll:
        logger.info(
            "Provider forced initial poll",
            extra={
                **_provider_context(provider),
                "reason": "forced_initial_poll",
                "taskName": "scheduler",
            },
        )
        return True, now, interval, "forced_initial_poll"

    last_ts = _to_monotonic_timestamp(
        getattr(provider, "last_measurement_at", None)
    )
    next_allowed = now if last_ts is None else last_ts + interval

    if last_ts is not None and now < next_allowed:
        logger.info(
            "Provider polling interval not elapsed",
            extra={
                **_provider_context(provider),
                "reason": "interval_not_elapsed",
                "next_allowed": next_allowed,
                "taskName": "scheduler",
            },
        )
        return False, next_allowed, interval, "interval_not_elapsed"

    logger.info(
        "Provider ready to poll",
        extra={
            **_provider_context(provider),
            "reason": "ready_to_poll",
            "taskName": "scheduler",
        },
    )
    return True, next_allowed, interval, "ready_to_poll"


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_monotonic_timestamp(value: datetime | None) -> float | None:
    if value is None:
        return None

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    epoch_ts = value.timestamp()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return epoch_ts

    offset = time.time() - loop.time()
    return epoch_ts - offset


def _provider_signature(provider: Any) -> str:
    signature = {
        "id": getattr(provider, "id", None),
        "vendor": _provider_vendor_key(provider),
        "config": getattr(provider, "config", None),
        "expected_interval_sec": getattr(provider, "expected_interval_sec", None),
    }
    return json.dumps(signature, default=str, sort_keys=True)


def _provider_vendor_key(provider: Any) -> str | None:
    vendor = getattr(provider, "vendor", None)
    if vendor is None:
        return None
    return vendor.value if hasattr(vendor, "value") else str(vendor)


def _provider_context(provider: Any) -> dict[str, Any]:
    return {
        "provider_id": getattr(provider, "id", None),
        "vendor": _provider_vendor_key(provider),
        "enabled": getattr(provider, "enabled", True),
        "expected_interval_sec": getattr(provider, "expected_interval_sec", None),
        "last_measurement_at": getattr(provider, "last_measurement_at", None),
    }

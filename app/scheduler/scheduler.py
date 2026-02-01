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
except ImportError as e:
    raise RuntimeError("ProviderRepository import failed") from e

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

        logger.info(
            "SCHEDULER INIT",
            extra={
                "refresh_interval_sec": self._refresh_interval_sec,
                "default_poll_interval_sec": self._default_poll_interval_sec,
                "max_concurrency": max_concurrency,
            },
        )

    async def run(self) -> None:
        logger.info("SCHEDULER STARTING", extra={"taskName": "scheduler"})

        try:
            await self._refresh_providers()
        except Exception:
            logger.exception("INITIAL PROVIDER REFRESH FAILED")
            raise

        next_refresh = asyncio.get_running_loop().time() + self._refresh_interval_sec

        logger.info(
            "SCHEDULER MAIN LOOP STARTED",
            extra={"next_refresh_in_sec": self._refresh_interval_sec},
        )

        while not self._stop_event.is_set():
            now = asyncio.get_running_loop().time()

            logger.debug(
                "SCHEDULER TICK",
                extra={
                    "now": now,
                    "tasks_in_queue": len(self._tasks),
                    "inflight_tasks": len(self._inflight),
                },
            )

            if now >= next_refresh:
                logger.info("REFRESH WINDOW REACHED")
                try:
                    await self._refresh_providers()
                except Exception:
                    logger.exception("PROVIDER REFRESH FAILED")
                next_refresh = now + self._refresh_interval_sec

            await self._dispatch_due_tasks(now)

            next_task_ts = self._next_task_run()
            sleep_until = min(next_refresh, next_task_ts)
            sleep_for = max(0.1, sleep_until - asyncio.get_running_loop().time())

            logger.debug(
                "SCHEDULER SLEEP",
                extra={
                    "sleep_for_sec": sleep_for,
                    "next_task_ts": next_task_ts,
                    "next_refresh_ts": next_refresh,
                },
            )

            await _sleep_until(sleep_until, self._stop_event)

        await self._drain_running_tasks()
        logger.info("SCHEDULER STOPPED CLEANLY")

    async def stop(self) -> None:
        logger.info("SCHEDULER STOP REQUESTED")
        self._stop_event.set()

    def _next_task_run(self) -> float:
        if not self._tasks:
            return asyncio.get_running_loop().time() + self._refresh_interval_sec
        return self._tasks[0].next_run

    async def _dispatch_due_tasks(self, now: float) -> None:
        while self._tasks and self._tasks[0].next_run <= now:
            task = heapq.heappop(self._tasks)

            if self._task_index.get(task.provider_id) is not task:
                logger.debug(
                    "STALE TASK DROPPED",
                    extra={"provider_id": task.provider_id},
                )
                continue

            logger.info(
                "TASK DISPATCH",
                extra={
                    "provider_id": task.provider_id,
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
                    "TASK SKIPPED",
                    extra={
                        "provider_id": task.provider_id,
                        "reason": reason,
                        "next_run": next_run,
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
                "TASK SCHEDULED",
                extra={
                    "provider_id": task.provider_id,
                    "next_run": next_run,
                    "interval": interval,
                },
            )

    async def _poll_provider(self, task: ProviderTask) -> None:
        logger.info(
            "POLL TASK START",
            extra={"provider_id": task.provider_id},
        )
        await self._poller.poll(task.provider, task.adapter)
        logger.info(
            "POLL TASK END",
            extra={"provider_id": task.provider_id},
        )

    async def _refresh_providers(self) -> None:
        logger.info("REFRESH PROVIDERS START")

        providers = await _fetch_active_providers()
        provider_map = {
            p.id: p for p in providers if getattr(p, "id", None) is not None
        }

        logger.info(
            "PROVIDERS LOADED",
            extra={"count": len(providers)},
        )

        for provider_id, task in list(self._task_index.items()):
            provider = provider_map.get(provider_id)
            if not provider:
                logger.info(
                    "PROVIDER REMOVED",
                    extra={"provider_id": provider_id},
                )
                self._task_index.pop(provider_id, None)
                continue

            if _provider_signature(provider) != _provider_signature(task.provider):
                logger.info(
                    "PROVIDER CONFIG CHANGED",
                    extra={"provider_id": provider_id},
                )
                try:
                    adapter = create_adapter_for_provider(
                        provider,
                        factory=self._adapter_factory,
                    )
                except Exception:
                    logger.exception(
                        "ADAPTER REBUILD FAILED",
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
            provider_id = provider.id
            if provider_id in self._task_index:
                continue

            logger.info(
                "NEW PROVIDER DETECTED",
                extra={"provider_id": provider_id},
            )

            try:
                adapter = create_adapter_for_provider(
                    provider,
                    factory=self._adapter_factory,
                )
            except Exception:
                logger.exception(
                    "ADAPTER CREATE FAILED",
                    extra={"provider_id": provider_id},
                )
                continue

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
            "REFRESH PROVIDERS DONE",
            extra={
                "providers_total": len(providers),
                "tasks_total": len(self._tasks),
            },
        )

    async def _drain_running_tasks(self) -> None:
        if not self._inflight:
            return

        logger.info(
            "DRAINING INFLIGHT TASKS",
            extra={"count": len(self._inflight)},
        )

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
    logger.info("DB FETCH PROVIDERS START")

    db_gen = get_db()
    db = next(db_gen)

    try:
        repo = ProviderRepository(db)

        result = repo.get_active_providers()
        providers = await result if asyncio.iscoroutine(result) else result

        logger.info(
            "DB FETCH PROVIDERS OK",
            extra={"count": len(providers)},
        )

        measurement_repo = MeasurementRepository(db)
        provider_ids = [p.id for p in providers if getattr(p, "id", None) is not None]

        last_measurements = measurement_repo.get_last_measurements(provider_ids)

        for provider in providers:
            last = last_measurements.get(provider.id)
            provider.last_measurement_at = last.measured_at if last else None

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
        return False, now + interval, interval, "provider_disabled"

    if force_poll:
        return True, now, interval, "forced_initial_poll"

    last_ts = _to_monotonic_timestamp(getattr(provider, "last_measurement_at", None))
    next_allowed = now if last_ts is None else last_ts + interval

    if last_ts is not None and now < next_allowed:
        return False, next_allowed, interval, "interval_not_elapsed"

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
    loop = asyncio.get_running_loop()
    offset = time.time() - loop.time()
    return epoch_ts - offset


def _provider_signature(provider: Any) -> str:
    signature = {
        "id": getattr(provider, "id", None),
        "vendor": getattr(provider, "vendor", None),
        "config": getattr(provider, "config", None),
        "expected_interval_sec": getattr(provider, "expected_interval_sec", None),
    }
    return json.dumps(signature, default=str, sort_keys=True)

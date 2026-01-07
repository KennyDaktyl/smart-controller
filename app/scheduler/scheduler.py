from __future__ import annotations

import asyncio
import heapq
import json
import logging
from typing import Any

from smart_common.core.db import get_db
from smart_common.nats.client import nats_client
from smart_common.nats.publisher import NatsPublisher
from smart_common.providers import get_vendor_adapter_factory
from smart_common.providers.enums import ProviderVendor

try:
    from smart_common.repositories.provider_repository import ProviderRepository
except ImportError:  # pragma: no cover - fallback for legacy module path
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
        logger.info("Scheduler starting")
        await self._refresh_providers()
        next_refresh = asyncio.get_running_loop().time() + self._refresh_interval_sec

        while not self._stop_event.is_set():
            now = asyncio.get_running_loop().time()
            next_run = self._next_task_run()
            sleep_until = min(next_run, next_refresh)

            await _sleep_until(sleep_until, self._stop_event)
            if self._stop_event.is_set():
                break

            now = asyncio.get_running_loop().time()
            if now >= next_refresh:
                await self._refresh_providers()
                next_refresh = now + self._refresh_interval_sec

            await self._dispatch_due_tasks(now)

        await self._drain_running_tasks()
        logger.info("Scheduler stopped")

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

            poll_task = asyncio.create_task(
                self._poll_provider(task),
                name=f"provider-{task.provider_id}",
            )
            self._track_task(poll_task)

            next_run = now + _resolve_poll_interval(
                task.provider,
                task.adapter,
                self._default_poll_interval_sec,
            )
            rescheduled = ProviderTask(
                next_run=next_run,
                provider_id=task.provider_id,
                provider=task.provider,
                adapter=task.adapter,
            )
            self._task_index[task.provider_id] = rescheduled
            heapq.heappush(self._tasks, rescheduled)

    async def _poll_provider(self, task: ProviderTask) -> None:
        await self._poller.poll(task.provider, task.adapter)

    async def _refresh_providers(self) -> None:
        providers = await _fetch_active_providers()
        provider_map = {getattr(p, "id"): p for p in providers}

        for provider_id, task in list(self._task_index.items()):
            provider = provider_map.get(provider_id)
            if not provider:
                self._task_index.pop(provider_id, None)
                continue

            if _provider_signature(provider) != _provider_signature(task.provider):
                try:
                    adapter = _create_adapter(self._adapter_factory, provider)
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
                )
                self._task_index[provider_id] = refreshed
                heapq.heappush(self._tasks, refreshed)

        for provider in providers:
            provider_id = getattr(provider, "id")
            if provider_id in self._task_index:
                continue
            try:
                adapter = _create_adapter(self._adapter_factory, provider)
            except Exception:
                logger.exception(
                    "Failed to create adapter",
                    extra={"provider_id": provider_id},
                )
                continue
            task = ProviderTask(
                next_run=asyncio.get_running_loop().time(),
                provider_id=provider_id,
                provider=provider,
                adapter=adapter,
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
        return await result if asyncio.iscoroutine(result) else result
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
    adapter_min = getattr(adapter, "min_poll_interval", None)
    provider_min = getattr(provider, "poll_interval_sec", None)

    interval = default_interval
    for candidate in (adapter_min, provider_min):
        if candidate is None:
            continue
        try:
            interval = max(interval, float(candidate))
        except (TypeError, ValueError):
            continue

    return max(1.0, interval)


def _provider_signature(provider: Any) -> str:
    signature = {
        "id": getattr(provider, "id", None),
        "vendor": _provider_vendor_key(provider),
        "config": getattr(provider, "config", None),
        "poll_interval_sec": getattr(provider, "poll_interval_sec", None),
    }
    return json.dumps(signature, default=str, sort_keys=True)


def _provider_vendor_key(provider: Any) -> str | None:
    vendor = getattr(provider, "vendor", None)
    if vendor is None:
        return None
    return vendor.value if hasattr(vendor, "value") else str(vendor)


def _create_adapter(factory: Any, provider: Any) -> Any:
    adapter_key = getattr(provider, "adapter_key", None)
    vendor = getattr(provider, "vendor", None)
    config = getattr(provider, "config", {}) or {}

    if vendor is None and adapter_key:
        try:
            vendor = ProviderVendor(adapter_key)
        except ValueError:
            try:
                vendor = ProviderVendor[adapter_key]
            except KeyError:
                vendor = None

    if vendor is None:
        raise RuntimeError(
            f"Provider {getattr(provider, 'id', None)} missing vendor metadata"
        )

    credentials = _resolve_credentials(config)
    overrides = _resolve_overrides(config)
    cache_key = str(getattr(provider, "id", "unknown"))

    # Example: Huawei adapter is registered in smart_common and resolved by vendor.
    # For provider.vendor == ProviderVendor.HUAWEI, this returns HuaweiAdapter.
    return factory.create(
        vendor,
        credentials=credentials,
        cache_key=cache_key,
        overrides=overrides,
    )


def _resolve_credentials(config: dict[str, Any]) -> dict[str, Any]:
    if "credentials" in config:
        return config["credentials"]
    return {k: v for k, v in config.items() if k not in {"settings", "overrides"}}


def _resolve_overrides(config: dict[str, Any]) -> dict[str, Any] | None:
    overrides = config.get("settings") or config.get("overrides")
    if isinstance(overrides, dict):
        return overrides
    return None

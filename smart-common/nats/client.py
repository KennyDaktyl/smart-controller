import json
import logging
import nats
from nats.js import JetStreamContext

logger = logging.getLogger(__name__)


class NATSClient:
    def __init__(self):
        self.nc = None
        self.js: JetStreamContext | None = None
        self.connected_once = False

    async def connect(self, servers: list[str] | None = None, name: str = "smartenergy-service"):
        if self.nc and self.nc.is_connected:
            return

        async def disconnected_cb():
            logger.warning("[NATS] Disconnected")

        async def reconnected_cb():
            logger.warning("[NATS] Reconnected — restoring JetStream context")
            self.js = self.nc.jetstream()

        async def error_cb(e):
            logger.error(f"[NATS] Error: {e}")

        servers = servers or ["nats://localhost:4222"]

        logger.info(f"[NATS] Connecting to {servers}...")

        self.nc = await nats.connect(
            servers=servers,
            name=name,
            reconnect_time_wait=2,
            max_reconnect_attempts=99999,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
            error_cb=error_cb,
        )

        self.js = self.nc.jetstream()
        self.connected_once = True

        logger.info("[NATS] Connected")

    async def ensure_connected(self):
        if not self.nc or not self.nc.is_connected:
            await self.connect()

    async def publish(self, subject: str, payload: dict):
        """Simple fire-and-forget publish"""
        await self.ensure_connected()
        data = json.dumps(payload).encode("utf-8")
        return await self.nc.publish(subject, data)

    async def js_publish(self, subject: str, payload: dict, timeout=2.0):
        """JetStream publish with durability."""
        await self.ensure_connected()
        if not self.js:
            self.js = self.nc.jetstream()
        data = json.dumps(payload).encode("utf-8")
        ack = await self.js.publish(subject, data, timeout=timeout)
        logger.debug(f"[NATS] JS Published {subject} seq={ack.seq}")
        return ack

    async def subscribe(self, subject: str, handler):
        """Normal NATS subscribe"""
        await self.ensure_connected()
        sub = await self.nc.subscribe(subject, cb=handler)
        logger.info(f"[NATS] SUBSCRIBED: {subject}")
        return sub

    async def subscribe_js(self, subject: str, durable: str, handler):
        """JetStream durable consumer subscribe"""
        await self.ensure_connected()
        if not self.js:
            self.js = self.nc.jetstream()

        sub = await self.js.subscribe(
            subject,
            durable=durable,
            cb=handler,
            manual_ack=True,
        )
        logger.info(f"[NATS] JS SUBSCRIBED: {subject} durable={durable}")
        return sub

    async def close(self):
        if not self.nc:
            return
        try:
            logger.info("[NATS] Draining...")
            await self.nc.drain()
        except:
            pass

        try:
            await self.nc.close()
        except:
            pass

        logger.info("[NATS] Closed.")


nats_client = NATSClient()

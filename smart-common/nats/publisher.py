import logging
from smart_common.nats.client import nats_client


logger = logging.getLogger(__name__)


class Publisher:

    async def publish_event(self, subject: str, payload: dict):
        logger.debug(f"[NATS Publisher] Publishing to {subject}: {payload}")
        return await nats_client.js_publish(subject, payload)

    async def fire_and_forget(self, subject: str, payload: dict):
        logger.debug(f"[NATS Publisher] Fire and forget to {subject}: {payload}")
        return await nats_client.publish(subject, payload)


publisher = Publisher()

import logging
from .client import nats_client

logger = logging.getLogger(__name__)


class Subscriber:
    async def listen(self, subject: str, durable: str, handler):
        logger.debug(f"[NATS Subscriber] Subscribing to {subject} with durable {durable}")
        return await nats_client.subscribe_js(subject, durable, handler)


subscriber = Subscriber()

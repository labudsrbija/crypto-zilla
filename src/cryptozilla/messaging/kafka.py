import logging
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from cryptozilla.config import KafkaSettings

log = logging.getLogger(__name__)

PRODUCER_DEFAULTS: dict[str, Any] = {
    "enable.idempotence": True,
    "acks": "all",
    "compression.type": "zstd",
    "linger.ms": 20,
    "batch.size": 256 * 1024,
    "delivery.timeout.ms": 120_000,
}


class KafkaSink:
    def __init__(self, settings: KafkaSettings, overrides: dict[str, Any] | None = None) -> None:
        conf = {**settings.client_config(), **PRODUCER_DEFAULTS, **(overrides or {})}
        self._producer = Producer(conf)
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed", extra={"topic": msg.topic(), "error": str(err)})
        else:
            self.delivered += 1

    def send(self, topic: str, key: str, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                # local queue is full, let librdkafka drain it before retrying
                self._producer.poll(0.5)
        self._producer.poll(0)

    def poll(self) -> None:
        self._producer.poll(0)

    def close(self, timeout_s: float = 10.0) -> int:
        remaining: int = self._producer.flush(timeout_s)
        if remaining:
            log.warning("messages left undelivered on close", extra={"count": remaining})
        return remaining

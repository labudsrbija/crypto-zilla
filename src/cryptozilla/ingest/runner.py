import asyncio
import logging
import random
import time

from websockets.exceptions import WebSocketException

from cryptozilla.config import IngestSettings
from cryptozilla.ingest.sources import Source
from cryptozilla.messaging import Sink

log = logging.getLogger(__name__)

RETRYABLE = (WebSocketException, OSError, TimeoutError)


class Backoff:
    def __init__(self, min_s: float, max_s: float) -> None:
        self.min_s = min_s
        self.max_s = max_s
        self.attempt = 0

    def next_delay(self) -> float:
        # full jitter keeps many reconnecting clients from hitting the server in sync
        ceiling = min(self.max_s, self.min_s * 2**self.attempt)
        self.attempt += 1
        return random.uniform(self.min_s, ceiling)

    def reset(self) -> None:
        self.attempt = 0


class Ingestor:
    def __init__(self, source: Source, sink: Sink, settings: IngestSettings) -> None:
        self.source = source
        self.sink = sink
        self.settings = settings
        self.sent = 0
        self._backoff = Backoff(settings.reconnect_min_s, settings.reconnect_max_s)

    async def run(self) -> None:
        stats = asyncio.create_task(self._report_stats())
        try:
            while True:
                try:
                    await self._session()
                    log.warning("stream ended, reconnecting", extra={"source": self.source.name})
                except RETRYABLE as exc:
                    log.warning(
                        "connection lost", extra={"source": self.source.name, "error": repr(exc)}
                    )
                delay = self._backoff.next_delay()
                log.info(
                    "reconnecting", extra={"source": self.source.name, "in_s": round(delay, 1)}
                )
                await asyncio.sleep(delay)
        finally:
            stats.cancel()
            undelivered = self.sink.close()
            log.info(
                "ingestor stopped",
                extra={"source": self.source.name, "sent": self.sent, "undelivered": undelivered},
            )

    async def _session(self) -> None:
        topic = self.settings.trades_topic
        first = True
        async for trade in self.source.stream():
            if first:
                self._backoff.reset()
                first = False
            self.sink.send(topic, trade.key, trade.model_dump_json().encode())
            self.sent += 1

    async def _report_stats(self) -> None:
        last_sent, last_t = 0, time.monotonic()
        while True:
            await asyncio.sleep(self.settings.stats_interval_s)
            self.sink.poll()
            now = time.monotonic()
            rate = (self.sent - last_sent) / (now - last_t)
            log.info(
                "stats",
                extra={"source": self.source.name, "sent": self.sent, "per_s": round(rate, 1)},
            )
            last_sent, last_t = self.sent, now

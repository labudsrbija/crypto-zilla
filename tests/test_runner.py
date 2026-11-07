import asyncio
from collections.abc import AsyncIterator

import pytest

from cryptozilla.config import IngestSettings
from cryptozilla.ingest.runner import Backoff, Ingestor
from cryptozilla.ingest.sources import Source
from cryptozilla.models import Side, Trade


class MemorySink:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, bytes]] = []
        self.closed = False

    def send(self, topic: str, key: str, value: bytes) -> None:
        self.messages.append((topic, key, value))

    def poll(self) -> None:
        pass

    def close(self, timeout_s: float = 10.0) -> int:
        self.closed = True
        return 0


class FlakySource(Source):
    """Yields two trades, then drops the connection."""

    name = "fake"

    def __init__(self) -> None:
        self.sessions = 0

    @classmethod
    def from_env(cls) -> "FlakySource":
        return cls()

    async def stream(self) -> AsyncIterator[Trade]:
        self.sessions += 1
        for i in range(2):
            yield Trade(
                source="fake",
                instrument="BTC-USDT",
                symbol="BTCUSDT",
                base="BTC",
                quote="USDT",
                trade_id=f"{self.sessions}-{i}",
                price="1",
                quantity="1",
                side=Side.BUY,
                event_ts=0,
                ingest_ts=0,
            )
        raise ConnectionResetError("dropped")


async def test_reconnects_and_closes_sink() -> None:
    source, sink = FlakySource(), MemorySink()
    settings = IngestSettings(reconnect_min_s=0.01, reconnect_max_s=0.02, stats_interval_s=60)
    task = asyncio.create_task(Ingestor(source, sink, settings).run())

    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert source.sessions >= 2
    assert sink.closed
    assert all(topic == "trades.raw" and key == "fake:BTC-USDT" for topic, key, _ in sink.messages)


def test_backoff_grows_and_resets() -> None:
    b = Backoff(1, 8)
    delays = [b.next_delay() for _ in range(6)]
    assert all(1 <= d <= 8 for d in delays)
    b.reset()
    assert b.attempt == 0

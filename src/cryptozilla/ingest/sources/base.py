import json
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Iterable
from typing import Any, ClassVar

from pydantic import ValidationError
from websockets.asyncio.client import connect

from cryptozilla.models import Trade

log = logging.getLogger(__name__)

Clock = Callable[[], int]


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class Source(ABC):
    """A market data source. One call to stream() is one connection session."""

    name: ClassVar[str]

    @classmethod
    @abstractmethod
    def from_env(cls) -> "Source": ...

    @abstractmethod
    def stream(self) -> AsyncIterator[Trade]: ...


class WebSocketSource(Source):
    ping_interval_s: ClassVar[float] = 20.0

    def __init__(self, clock: Clock = now_ms) -> None:
        self._clock = clock
        self.rejected = 0

    @abstractmethod
    def url(self) -> str: ...

    def subscribe_messages(self) -> list[dict[str, Any]]:
        return []

    @abstractmethod
    def parse(self, message: dict[str, Any]) -> Iterable[Trade]: ...

    async def stream(self) -> AsyncIterator[Trade]:
        async with connect(self.url(), ping_interval=self.ping_interval_s) as ws:
            log.info("connected", extra={"source": self.name})
            for msg in self.subscribe_messages():
                await ws.send(json.dumps(msg))
            async for raw in ws:
                try:
                    trades = list(self.parse(json.loads(raw)))
                except (ValueError, KeyError, TypeError, ValidationError) as exc:
                    self.rejected += 1
                    log.warning("rejected message", extra={"source": self.name, "error": str(exc)})
                    continue
                for trade in trades:
                    yield trade

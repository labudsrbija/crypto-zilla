from typing import Protocol


class Sink(Protocol):
    def send(self, topic: str, key: str, value: bytes) -> None: ...

    def poll(self) -> None: ...

    def close(self, timeout_s: float = 10.0) -> int:
        """Flush pending messages and return how many could not be delivered."""
        ...

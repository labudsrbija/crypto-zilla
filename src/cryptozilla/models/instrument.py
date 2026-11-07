from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Instrument:
    """Exchange-independent pair, written as BASE-QUOTE (e.g. BTC-USDT)."""

    base: str
    quote: str

    @classmethod
    def parse(cls, value: str) -> "Instrument":
        base, sep, quote = value.strip().upper().partition("-")
        if not sep or not base or not quote:
            raise ValueError(f"instrument must look like BASE-QUOTE, got {value!r}")
        return cls(base, quote)

    @classmethod
    def parse_many(cls, value: str) -> list["Instrument"]:
        return [cls.parse(v) for v in value.split(",") if v.strip()]

    def __str__(self) -> str:
        return f"{self.base}-{self.quote}"

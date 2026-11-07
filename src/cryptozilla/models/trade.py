from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_serializer

SCHEMA_VERSION = 1


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class Trade(BaseModel):
    """One executed trade, normalized across sources.

    Prices and quantities are decimals and serialize to strings, so no precision
    is lost between the exchange and the lakehouse. Timestamps are epoch millis UTC.
    """

    model_config = ConfigDict(frozen=True)

    schema_version: int = SCHEMA_VERSION
    source: str
    instrument: str
    symbol: str
    base: str
    quote: str
    trade_id: str
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    side: Side | None
    event_ts: int
    ingest_ts: int

    @field_serializer("price", "quantity")
    def _fixed_point(self, value: Decimal) -> str:
        return format(value, "f")

    @property
    def key(self) -> str:
        return f"{self.source}:{self.instrument}"

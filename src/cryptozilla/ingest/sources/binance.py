from collections.abc import Iterable
from typing import Any

from pydantic_settings import SettingsConfigDict

from cryptozilla.config import Settings
from cryptozilla.ingest.sources.base import Clock, WebSocketSource, now_ms
from cryptozilla.models import Instrument, Side, Trade


class BinanceSettings(Settings):
    model_config = SettingsConfigDict(env_prefix="BINANCE_")

    ws_url: str = "wss://stream.binance.com:9443/stream"
    instruments: str = "BTC-USDT,ETH-USDT"


class BinanceSource(WebSocketSource):
    """Spot trade stream. Public, no API key needed.

    Binance names pairs without a separator (BTCUSDT), so we keep a lookup from
    the native symbol back to the instrument we asked for.
    """

    name = "binance"

    def __init__(self, ws_url: str, instruments: list[Instrument], clock: Clock = now_ms) -> None:
        super().__init__(clock)
        if not instruments:
            raise ValueError("binance needs at least one instrument")
        self._ws_url = ws_url
        self._by_symbol = {f"{i.base}{i.quote}": i for i in instruments}

    @classmethod
    def from_env(cls) -> "BinanceSource":
        s = BinanceSettings()
        return cls(s.ws_url, Instrument.parse_many(s.instruments))

    def url(self) -> str:
        streams = "/".join(f"{symbol.lower()}@trade" for symbol in self._by_symbol)
        return f"{self._ws_url}?streams={streams}"

    def parse(self, message: dict[str, Any]) -> Iterable[Trade]:
        data = message.get("data", message)
        if data.get("e") != "trade":
            return []
        symbol = data["s"]
        instrument = self._by_symbol.get(symbol)
        if instrument is None:
            return []
        # "m" means the buyer was the maker, so the aggressor was a seller
        side = Side.SELL if data["m"] else Side.BUY
        return [
            Trade(
                source=self.name,
                instrument=str(instrument),
                symbol=symbol,
                base=instrument.base,
                quote=instrument.quote,
                trade_id=str(data["t"]),
                price=data["p"],
                quantity=data["q"],
                side=side,
                event_ts=data["T"],
                ingest_ts=self._clock(),
            )
        ]

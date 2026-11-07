from cryptozilla.ingest.sources.base import Source, WebSocketSource
from cryptozilla.ingest.sources.binance import BinanceSource

REGISTRY: dict[str, type[Source]] = {
    BinanceSource.name: BinanceSource,
}

__all__ = ["REGISTRY", "BinanceSource", "Source", "WebSocketSource"]

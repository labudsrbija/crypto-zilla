import json
from decimal import Decimal

import pytest

from cryptozilla.ingest.sources.binance import BinanceSource
from cryptozilla.models import Instrument, Side


def make_source() -> BinanceSource:
    instruments = Instrument.parse_many("BTC-USDT, eth-usdt")
    return BinanceSource("wss://example.test/stream", instruments, clock=lambda: 1_700_000_000_999)


def frame(**overrides: object) -> dict[str, object]:
    data = {
        "e": "trade",
        "E": 1_700_000_000_100,
        "s": "BTCUSDT",
        "t": 123456789,
        "p": "64250.12000000",
        "q": "0.00150000",
        "T": 1_700_000_000_050,
        "m": True,
        "M": True,
    }
    data.update(overrides)
    return {"stream": "btcusdt@trade", "data": data}


def test_url_combines_streams() -> None:
    url = make_source().url()
    assert url == "wss://example.test/stream?streams=btcusdt@trade/ethusdt@trade"


def test_parse_trade() -> None:
    [trade] = make_source().parse(frame())
    assert trade.source == "binance"
    assert trade.instrument == "BTC-USDT"
    assert trade.symbol == "BTCUSDT"
    assert trade.trade_id == "123456789"
    assert trade.price == Decimal("64250.12")
    assert trade.quantity == Decimal("0.0015")
    assert trade.event_ts == 1_700_000_000_050
    assert trade.ingest_ts == 1_700_000_000_999
    assert trade.key == "binance:BTC-USDT"


@pytest.mark.parametrize(("buyer_is_maker", "side"), [(True, Side.SELL), (False, Side.BUY)])
def test_side_is_aggressor(buyer_is_maker: bool, side: Side) -> None:
    [trade] = make_source().parse(frame(m=buyer_is_maker))
    assert trade.side is side


def test_ignores_other_events_and_symbols() -> None:
    source = make_source()
    assert list(source.parse({"result": None, "id": 1})) == []
    assert list(source.parse(frame(s="DOGEUSDT"))) == []


def test_json_keeps_decimal_precision() -> None:
    [trade] = make_source().parse(frame(p="0.00000001", q="123456789.12345678"))
    payload = json.loads(trade.model_dump_json())
    assert payload["price"] == "0.00000001"
    assert payload["quantity"] == "123456789.12345678"
    assert payload["schema_version"] == 1


def test_rejects_bad_instrument() -> None:
    with pytest.raises(ValueError):
        Instrument.parse("BTCUSDT")

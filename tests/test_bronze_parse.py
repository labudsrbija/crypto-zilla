import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from cryptozilla.models import Trade

pytestmark = pytest.mark.spark

KAFKA_TIME = datetime(2026, 11, 7, 12, 0, tzinfo=UTC)


def message(**overrides: Any) -> str:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "source": "binance",
        "instrument": "BTC-USDT",
        "symbol": "BTCUSDT",
        "base": "BTC",
        "quote": "USDT",
        "trade_id": "42",
        "price": "64250.12",
        "quantity": "0.0015",
        "side": "sell",
        "event_ts": 1_700_000_000_050,
        "ingest_ts": 1_700_000_000_999,
    }
    payload.update(overrides)
    return json.dumps(payload)


def kafka_rows(spark: Any, values: list[str]) -> Any:
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        LongType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("value", BinaryType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
        ]
    )
    rows = [(v.encode(), 0, i, KAFKA_TIME) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, schema)


def parse(spark: Any, values: list[str]) -> tuple[list[Any], list[Any]]:
    from cryptozilla.jobs.bronze_trades import parse_trades

    parsed = parse_trades(kafka_rows(spark, values))
    return parsed.valid.collect(), parsed.rejected.orderBy("kafka_offset").collect()


def test_valid_trade_is_typed(spark: Any) -> None:
    valid, rejected = parse(spark, [message()])
    assert rejected == []
    [row] = valid
    assert row.instrument == "BTC-USDT"
    assert row.price == Decimal("64250.12")
    assert row.quantity == Decimal("0.0015")
    # collect() returns naive datetimes in the machine's local zone
    assert row.event_time.astimezone(UTC) == datetime(2023, 11, 14, 22, 13, 20, 50000, tzinfo=UTC)
    assert row.kafka_offset == 0
    assert json.loads(row.raw)["trade_id"] == "42"


def test_tiny_prices_keep_precision(spark: Any) -> None:
    [row], _ = parse(spark, [message(price="0.00000001", quantity="123456789.12345678")])
    assert row.price == Decimal("0.00000001")
    assert row.quantity == Decimal("123456789.12345678")


def test_bad_messages_are_rejected_with_reason(spark: Any) -> None:
    valid, rejected = parse(
        spark,
        [
            "not json",
            message(schema_version=99),
            message(price="abc"),
            message(quantity="-1"),
            message(side="long"),
            message(trade_id=None),
        ],
    )
    assert valid == []
    assert [r.reason for r in rejected] == [
        "unparseable json",
        "unsupported schema_version",
        "invalid price",
        "invalid quantity",
        "invalid side",
        "missing: trade_id",
    ]
    assert rejected[0].raw == "not json"


def test_null_side_is_allowed(spark: Any) -> None:
    valid, rejected = parse(spark, [message(side=None)])
    assert rejected == []
    assert valid[0].side is None


def test_spark_schema_matches_trade_model() -> None:
    pytest.importorskip("pyspark")
    from cryptozilla.schemas.trades import TRADE_MESSAGE

    assert [f.name for f in TRADE_MESSAGE.fields] == list(Trade.model_fields)
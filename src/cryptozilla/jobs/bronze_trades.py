"""trades.raw (Kafka) -> lake.bronze.trades (Iceberg), continuously.

Valid and rejected rows go to separate tables through two streaming queries, each with its own
checkpoint. Iceberg's streaming sink commits once per micro-batch and skips batches it has
already committed, so restarts don't duplicate rows. foreachBatch writing both tables would
lose that guarantee.
"""

import logging
import signal
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic_settings import SettingsConfigDict

from cryptozilla.config import CatalogSettings, KafkaSettings, Settings, SparkSettings
from cryptozilla.lake.tables import BRONZE_TRADES, BRONZE_TRADES_REJECTED, PRICE_TYPE, Table

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.streaming.query import StreamingQuery

log = logging.getLogger(__name__)


class BronzeSettings(Settings):
    model_config = SettingsConfigDict(env_prefix="BRONZE_")

    topic: str = "trades.raw"
    trigger: str = "1 minute"
    starting_offsets: str = "earliest"
    max_offsets_per_trigger: int = 500_000
    fail_on_data_loss: bool = True


@dataclass(frozen=True)
class Parsed:
    valid: "DataFrame"
    rejected: "DataFrame"


def _reject_reason(raw: "Column", t: "Column", price: "Column", quantity: "Column") -> "Column":
    from pyspark.sql import functions as F

    from cryptozilla.schemas.trades import SUPPORTED_SCHEMA_VERSIONS

    required = ["source", "instrument", "symbol", "base", "quote", "trade_id", "event_ts"]
    missing = F.concat_ws(",", *[F.when(t[c].isNull(), F.lit(c)) for c in required])
    # from_json turns invalid JSON into a struct of nulls rather than null, so check the raw text
    unparseable = raw.isNull() | F.get_json_object(raw, "$").isNull()
    return (
        F.when(unparseable, "unparseable json")
        .when(~t["schema_version"].isin(*SUPPORTED_SCHEMA_VERSIONS), "unsupported schema_version")
        .when(missing != "", F.concat(F.lit("missing: "), missing))
        .when(price.isNull() | (price <= 0), "invalid price")
        .when(quantity.isNull() | (quantity <= 0), "invalid quantity")
        .when(t["side"].isNotNull() & ~t["side"].isin("buy", "sell"), "invalid side")
    )


def parse_trades(kafka_df: "DataFrame") -> Parsed:
    """Split raw Kafka rows into typed trades and rejects.

    Pure DataFrame transformation (no IO), so it works on a stream or on a test batch.
    """
    from pyspark.sql import functions as F

    from cryptozilla.schemas.trades import TRADE_MESSAGE

    raw = F.col("value").cast("string")
    df = kafka_df.select(
        raw.alias("raw"),
        F.from_json(raw, TRADE_MESSAGE).alias("t"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_time"),
    )
    # try_cast turns "abc" into null instead of failing the whole batch under ANSI mode
    price = F.col("t.price").try_cast(PRICE_TYPE)
    quantity = F.col("t.quantity").try_cast(PRICE_TYPE)
    df = df.withColumn("reason", _reject_reason(F.col("raw"), F.col("t"), price, quantity))

    valid = df.where(F.col("reason").isNull()).select(
        F.col("t.source").alias("source"),
        F.col("t.instrument").alias("instrument"),
        F.col("t.symbol").alias("symbol"),
        F.col("t.base").alias("base"),
        F.col("t.quote").alias("quote"),
        F.col("t.trade_id").alias("trade_id"),
        price.alias("price"),
        quantity.alias("quantity"),
        F.col("t.side").alias("side"),
        F.timestamp_millis(F.col("t.event_ts")).alias("event_time"),
        F.timestamp_millis(F.coalesce(F.col("t.ingest_ts"), F.col("t.event_ts"))).alias(
            "ingest_time"
        ),
        F.col("t.schema_version").alias("schema_version"),
        "kafka_partition",
        "kafka_offset",
        "kafka_time",
        "raw",
    )
    rejected = df.where(F.col("reason").isNotNull()).select(
        "reason",
        "raw",
        "kafka_partition",
        "kafka_offset",
        "kafka_time",
        F.current_timestamp().alias("rejected_time"),
    )
    return Parsed(valid, rejected)


def ensure_tables(spark: "SparkSession", catalog: str, tables: list[Table]) -> None:
    for ns in sorted({t.namespace for t in tables}):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}")
    for table in tables:
        spark.sql(table.ddl(catalog))
        log.info("table ready", extra={"table": table.fqn(catalog)})


def read_trades_stream(
    spark: "SparkSession", kafka: KafkaSettings, settings: BronzeSettings
) -> "DataFrame":
    return (
        spark.readStream.format("kafka")
        .options(**kafka.spark_options())
        .option("subscribe", settings.topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .option("failOnDataLoss", str(settings.fail_on_data_loss).lower())
        .load()
    )


def start_sink(
    df: "DataFrame", table: str, checkpoint: str, trigger: str, name: str
) -> "StreamingQuery":
    return (
        df.writeStream.format("iceberg")
        .queryName(name)
        .outputMode("append")
        .trigger(processingTime=trigger)
        .option("checkpointLocation", checkpoint)
        # columns from from_json are nullable in Spark's eyes; validation already
        # guarantees the required ones are set, so let Iceberg accept them
        .option("check-nullability", "false")
        .toTable(table)
    )


def main(args: list[str] | None = None) -> None:
    from cryptozilla.spark import make_spark_session

    catalog = CatalogSettings()
    spark_settings = SparkSettings()
    settings = BronzeSettings()

    spark = make_spark_session("cz-bronze-trades", catalog=catalog)
    ensure_tables(spark, catalog.name, [BRONZE_TRADES, BRONZE_TRADES_REJECTED])

    parsed = parse_trades(read_trades_stream(spark, KafkaSettings(), settings))
    queries = [
        start_sink(
            parsed.valid,
            BRONZE_TRADES.fqn(catalog.name),
            spark_settings.checkpoint("bronze_trades"),
            settings.trigger,
            "bronze_trades",
        ),
        start_sink(
            parsed.rejected,
            BRONZE_TRADES_REJECTED.fqn(catalog.name),
            spark_settings.checkpoint("bronze_trades_rejected"),
            settings.trigger,
            "bronze_trades_rejected",
        ),
    ]
    log.info("streams started", extra={"queries": [q.name for q in queries]})

    def stop(signum: int, _frame: Any) -> None:
        log.info("stopping streams", extra={"signal": signum})
        for q in queries:
            q.stop()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        # raises if a query fails; the container restarts and resumes from the checkpoints
        spark.streams.awaitAnyTermination()
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
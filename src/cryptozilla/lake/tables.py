"""Lakehouse table definitions. Tables are created explicitly, never inferred from a write."""

from dataclasses import dataclass

PRICE_TYPE = "decimal(38,18)"

# Shared by every streaming-written table:
# - fanout lets a streaming write hit several partitions without sorting each micro-batch
# - metadata cleanup stops one-commit-per-minute from piling up thousands of metadata files
STREAMING_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.spark.fanout.enabled": "true",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


@dataclass(frozen=True)
class Table:
    namespace: str
    name: str
    columns: str
    partitioned_by: str | None = None
    properties: dict[str, str] | None = None

    def fqn(self, catalog: str) -> str:
        return f"{catalog}.{self.namespace}.{self.name}"

    def ddl(self, catalog: str) -> str:
        sql = f"CREATE TABLE IF NOT EXISTS {self.fqn(catalog)} ({self.columns}) USING iceberg"
        if self.partitioned_by:
            sql += f" PARTITIONED BY ({self.partitioned_by})"
        if self.properties:
            props = ", ".join(f"'{k}' = '{v}'" for k, v in sorted(self.properties.items()))
            sql += f" TBLPROPERTIES ({props})"
        return sql


BRONZE_TRADES = Table(
    namespace="bronze",
    name="trades",
    columns=f"""
        source STRING NOT NULL,
        instrument STRING NOT NULL,
        symbol STRING NOT NULL,
        base STRING NOT NULL,
        quote STRING NOT NULL,
        trade_id STRING NOT NULL,
        price {PRICE_TYPE} NOT NULL,
        quantity {PRICE_TYPE} NOT NULL,
        side STRING,
        event_time TIMESTAMP NOT NULL,
        ingest_time TIMESTAMP NOT NULL,
        schema_version INT NOT NULL,
        kafka_partition INT NOT NULL,
        kafka_offset BIGINT NOT NULL,
        kafka_time TIMESTAMP NOT NULL,
        raw STRING NOT NULL
    """,
    partitioned_by="days(event_time), source",
    properties=STREAMING_PROPERTIES,
)

BRONZE_TRADES_REJECTED = Table(
    namespace="bronze",
    name="trades_rejected",
    columns="""
        reason STRING NOT NULL,
        raw STRING,
        kafka_partition INT NOT NULL,
        kafka_offset BIGINT NOT NULL,
        kafka_time TIMESTAMP NOT NULL,
        rejected_time TIMESTAMP NOT NULL
    """,
    partitioned_by="days(kafka_time)",
    properties=STREAMING_PROPERTIES,
)
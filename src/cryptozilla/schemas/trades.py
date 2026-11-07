"""Spark view of the Trade message on trades.raw.

Mirrors cryptozilla.models.Trade. Prices stay strings here, exactly as they are on the wire;
casting to decimal happens in the job so a bad value can be rejected instead of becoming null.
A test keeps the field list in sync with the pydantic model.
"""

from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

SUPPORTED_SCHEMA_VERSIONS = (1,)

TRADE_MESSAGE = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("source", StringType()),
        StructField("instrument", StringType()),
        StructField("symbol", StringType()),
        StructField("base", StringType()),
        StructField("quote", StringType()),
        StructField("trade_id", StringType()),
        StructField("price", StringType()),
        StructField("quantity", StringType()),
        StructField("side", StringType()),
        StructField("event_ts", LongType()),
        StructField("ingest_ts", LongType()),
    ]
)
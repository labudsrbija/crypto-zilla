from pydantic import SecretStr

from cryptozilla.config import CatalogSettings, KafkaSettings, SparkSettings
from cryptozilla.lake.tables import BRONZE_TRADES
from cryptozilla.spark.session import catalog_conf


def test_catalog_conf_local_defaults() -> None:
    conf = catalog_conf(CatalogSettings(uri="http://lakekeeper:8181/catalog", warehouse="cz"))
    assert conf["spark.sql.catalog.lake"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf["spark.sql.catalog.lake.type"] == "rest"
    assert conf["spark.sql.catalog.lake.uri"] == "http://lakekeeper:8181/catalog"
    assert conf["spark.sql.catalog.lake.warehouse"] == "cz"
    assert conf["spark.sql.catalog.lake.header.X-Iceberg-Access-Delegation"] == (
        "vended-credentials"
    )
    assert conf["spark.sql.defaultCatalog"] == "lake"
    assert not any(k.endswith(".credential") for k in conf)


def test_catalog_conf_cloud_auth() -> None:
    conf = catalog_conf(
        CatalogSettings(
            name="gold",
            credential=SecretStr("id:secret"),
            oauth2_server_uri="https://auth.example/token",
            io_impl="org.apache.iceberg.gcp.gcs.GCSFileIO",
        )
    )
    assert conf["spark.sql.catalog.gold.credential"] == "id:secret"
    assert conf["spark.sql.catalog.gold.oauth2-server-uri"] == "https://auth.example/token"
    assert conf["spark.sql.catalog.gold.io-impl"] == "org.apache.iceberg.gcp.gcs.GCSFileIO"


def test_bronze_ddl() -> None:
    ddl = BRONZE_TRADES.ddl("lake")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS lake.bronze.trades (")
    assert "PARTITIONED BY (days(event_time), source)" in ddl
    assert "'write.spark.fanout.enabled' = 'true'" in ddl


def test_checkpoint_paths() -> None:
    s = SparkSettings(checkpoint_root="gs://bucket/checkpoints/")
    assert s.checkpoint("bronze_trades") == "gs://bucket/checkpoints/bronze_trades"


def test_kafka_spark_options_with_sasl() -> None:
    opts = KafkaSettings(
        bootstrap_servers="broker:9093",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username="cz",
        sasl_password=SecretStr("pw"),
    ).spark_options()
    assert opts["kafka.bootstrap.servers"] == "broker:9093"
    assert (
        'ScramLoginModule required username="cz" password="pw";' in opts["kafka.sasl.jaas.config"]
    )
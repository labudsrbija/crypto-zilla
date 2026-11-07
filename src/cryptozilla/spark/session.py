from typing import TYPE_CHECKING

from cryptozilla.config import CatalogSettings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def catalog_conf(catalog: CatalogSettings) -> dict[str, str]:
    """Spark properties for one Iceberg REST catalog.

    This is the single place that changes between local (Lakekeeper + MinIO) and cloud:
    everything comes from CatalogSettings, so moving means changing env, not code.
    """
    prefix = f"spark.sql.catalog.{catalog.name}"
    conf = {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": catalog.uri,
        f"{prefix}.warehouse": catalog.warehouse,
        "spark.sql.defaultCatalog": catalog.name,
    }
    if catalog.access_delegation:
        conf[f"{prefix}.header.X-Iceberg-Access-Delegation"] = catalog.access_delegation
    if catalog.credential:
        conf[f"{prefix}.credential"] = catalog.credential.get_secret_value()
    if catalog.oauth2_server_uri:
        conf[f"{prefix}.oauth2-server-uri"] = catalog.oauth2_server_uri
    if catalog.scope:
        conf[f"{prefix}.scope"] = catalog.scope
    if catalog.io_impl:
        conf[f"{prefix}.io-impl"] = catalog.io_impl
    return conf


def make_spark_session(
    app_name: str,
    *,
    catalog: CatalogSettings | None = None,
    master: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> "SparkSession":
    """Build a session wired to the lakehouse catalog.

    master is left to spark-submit unless given, so the same job runs on a local cluster,
    in local mode for tests, or on Dataproc without changes.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    conf = {**catalog_conf(catalog or CatalogSettings()), **(extra_conf or {})}
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
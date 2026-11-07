import shutil
from collections.abc import Iterator
from typing import Any

import pytest


@pytest.fixture(scope="session")
def spark() -> Iterator[Any]:
    pyspark_sql = pytest.importorskip("pyspark.sql")
    if not shutil.which("java"):
        pytest.skip("java not installed")
    session = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("cz-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
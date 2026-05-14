"""Root conftest: shared local_spark fixture for unit tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest


@pytest.fixture(scope="session")
def local_spark() -> Iterator:
    """Local-mode SparkSession for unit tests of pure DataFrame builders."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("dbxcarta-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()

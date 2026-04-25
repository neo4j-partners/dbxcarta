"""Root conftest: live-marker injection for non-unit dirs, plus shared local_spark fixture.

Phase 1 (worklog/cleanup.md): tests under `schema_graph/`, `sample_values/`, and
`integration/` require live Databricks + Neo4j infra. They are auto-marked `live`
here so they are skipped by default (see `pyproject.toml:addopts`). Phase 2 deletes
those directories entirely; this hook goes with them.
"""

from __future__ import annotations

from typing import Iterator

import pytest

_LIVE_DIRS = ("schema_graph", "sample_values", "integration")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    live = pytest.mark.live
    for item in items:
        path = str(item.path)
        if any(f"/tests/{d}/" in path for d in _LIVE_DIRS):
            item.add_marker(live)


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

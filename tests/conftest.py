"""Root conftest: shared local_spark fixture for unit tests."""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

# Pin the Spark worker interpreter to the one running the driver, set at import
# (before any SparkSession is built by any fixture). Spark otherwise spawns
# workers via the bare `python3` on PATH, which on a machine with a newer
# system Python triggers PYTHON_VERSION_MISMATCH against the venv driver and
# fails every job that materializes a Python partition. Setting it here — not
# inside a single session fixture — covers every test module that builds its
# own session (e.g. the fk_guard execution guard), regardless of which one wins
# the race to create the first session.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def _pristine_environ() -> dict[str, str]:
    """Snapshot of ``os.environ`` taken before any test mutates it.

    Instantiated lazily at the first test's setup. No collected test module
    writes ``os.environ`` at import time, so this captures the pre-pollution
    baseline.
    """
    return dict(os.environ)


@pytest.fixture(autouse=True)
def _isolate_environ(_pristine_environ: dict[str, str]) -> Iterator[None]:
    """Restore ``os.environ`` to the pristine baseline after every test.

    The production ``inject_params`` helper writes to ``os.environ`` via
    ``setdefault`` by design. Without this, those writes leak across tests
    and poison ``BaseSettings`` constructors that read unset fields from the
    environment.
    """
    yield
    if os.environ != _pristine_environ:
        os.environ.clear()
        os.environ.update(_pristine_environ)


@pytest.fixture(scope="session")
def local_spark() -> Iterator:
    """Local-mode SparkSession for unit tests of pure DataFrame builders."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("dbxcarta-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()

"""Fixtures for tests/embeddings/.

Unit tests use a local-mode SparkSession to exercise the transform without
invoking ai_query. Integration tests reuse neo4j_driver / run_summary from
the schema_graph sibling conftest via direct import.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")


@pytest.fixture(scope="session")
def local_spark() -> Iterator:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("dbxcarta-embeddings-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()

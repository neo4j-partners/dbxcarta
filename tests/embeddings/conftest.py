"""Fixtures for tests/embeddings/.

Unit tests use a local-mode SparkSession to exercise the transform without
invoking ai_query. Integration tests reuse neo4j_driver / run_summary from
the schema_graph sibling conftest via direct import. The local_spark
fixture lives in the root tests/conftest.py.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

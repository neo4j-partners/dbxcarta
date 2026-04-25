"""Fixtures for tests/embeddings/.

Unit tests use a local-mode SparkSession (local_spark from the root conftest)
to exercise the transform without invoking ai_query.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

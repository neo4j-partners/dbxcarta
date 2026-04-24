"""Semantic min-catalog-size gate.

Exercises `_should_run_semantic` independently of the heavy
`run_fk_discovery` path, so the boundary decision is testable without
standing up Spark.
"""

from __future__ import annotations

from dbxcarta.fk_discovery import _should_run_semantic
from dbxcarta.settings import Settings


_BASE = {
    "dbxcarta_catalog": "main",
    "dbxcarta_summary_volume": "/Volumes/cat/schema/vol/dbxcarta",
    "dbxcarta_summary_table": "schema.table",
}


def _semantic_on(**overrides) -> Settings:
    return Settings(
        dbxcarta_infer_semantic=True,
        dbxcarta_include_embeddings_columns=True,
        **{**_BASE, **overrides},
    )


def test_semantic_skipped_when_below_min_tables() -> None:
    s = _semantic_on(dbxcarta_semantic_min_tables=10)
    assert _should_run_semantic(s, n_tables=5) is False


def test_semantic_runs_at_min_tables() -> None:
    """The comparison is `<`, so n_tables == min is accepted."""
    s = _semantic_on(dbxcarta_semantic_min_tables=10)
    assert _should_run_semantic(s, n_tables=10) is True


def test_semantic_runs_above_min_tables() -> None:
    s = _semantic_on(dbxcarta_semantic_min_tables=10)
    assert _should_run_semantic(s, n_tables=100) is True


def test_semantic_skipped_when_flag_off() -> None:
    """Turning DBXCARTA_INFER_SEMANTIC off short-circuits even at large catalogs."""
    s = Settings(
        dbxcarta_infer_semantic=False,
        **_BASE,
    )
    assert _should_run_semantic(s, n_tables=1_000_000) is False

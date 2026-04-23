"""Ledger I/O boundary tests.

Phase 3.5 narrowed `read_ledger` (then `_read_ledger` in pipeline.py) from
bare `except Exception` to `AnalysisException`. Phase 3.6 moved it to
`dbxcarta.ledger`. A non-existent Delta path returns None without raising;
other failures are expected to propagate as signal.
"""

from __future__ import annotations

from dbxcarta.contract import NodeLabel
from dbxcarta.ledger import read_ledger


def test_read_ledger_returns_none_on_missing_path(local_spark, tmp_path) -> None:
    """A path that doesn't exist returns None. If any exception other than the
    narrowed AnalysisException fires, it propagates — test would fail loudly."""
    missing = str(tmp_path / "ledger_never_created")
    result = read_ledger(local_spark, missing, NodeLabel.TABLE)
    assert result is None

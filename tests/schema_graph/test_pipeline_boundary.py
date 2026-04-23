"""Phase 3.5 boundary-hardening tests for the ingest pipeline.

Covers two narrowed surfaces the Phase 3.5 rewrite tightened:
  1. `_IDENTIFIER_RE` — Settings now rejects backticks, dots (in catalog),
     spaces, and hyphens. Dotted summary_table splits and validates per part.
  2. `_read_ledger` — narrowed from bare `except Exception` to
     `AnalysisException`. A non-existent Delta path returns None without
     raising; other failures are expected to propagate.

Neither test hits cloud infra: identifier tests construct Settings with
explicit kwargs; ledger test uses a tmp_path against a local Spark session.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbxcarta.pipeline import Settings, _read_ledger


_BASE_SETTINGS = {
    "dbxcarta_summary_volume": "/Volumes/cat/schema/vol/dbxcarta",
    "dbxcarta_summary_table": "schema.table",
}


# --- _IDENTIFIER_RE tightening ----------------------------------------------

@pytest.mark.parametrize("bad_catalog", [
    "evil`catalog",     # backtick
    "cat.subcat",       # dot (catalog must be single identifier)
    "cat name",         # space
    "cat-name",         # hyphen
    "1cat",             # leading digit
    "",                 # empty
])
def test_settings_rejects_non_strict_catalog(bad_catalog: str) -> None:
    with pytest.raises(ValidationError, match="Invalid Databricks identifier"):
        Settings(dbxcarta_catalog=bad_catalog, **_BASE_SETTINGS)


@pytest.mark.parametrize("good_catalog", [
    "main",
    "my_catalog",
    "_internal",
])
def test_settings_accepts_strict_catalog(good_catalog: str) -> None:
    s = Settings(dbxcarta_catalog=good_catalog, **_BASE_SETTINGS)
    assert s.dbxcarta_catalog == good_catalog


@pytest.mark.parametrize("good_table", [
    "table_name",
    "schema.table",
    "cat.schema.table",
])
def test_settings_accepts_dotted_summary_table(good_table: str) -> None:
    s = Settings(
        dbxcarta_catalog="main",
        dbxcarta_summary_volume=_BASE_SETTINGS["dbxcarta_summary_volume"],
        dbxcarta_summary_table=good_table,
    )
    assert s.dbxcarta_summary_table == good_table


@pytest.mark.parametrize("bad_table", [
    "schema.`table`",       # backtick in a part
    "schema..table",        # empty middle part
    "schema.my-table",      # hyphen in a part
    "schema.1table",        # leading digit in a part
])
def test_settings_rejects_malformed_summary_table(bad_table: str) -> None:
    with pytest.raises(ValidationError, match="Invalid identifier part"):
        Settings(
            dbxcarta_catalog="main",
            dbxcarta_summary_volume=_BASE_SETTINGS["dbxcarta_summary_volume"],
            dbxcarta_summary_table=bad_table,
        )


# --- _read_ledger narrowed exception handling -------------------------------

def test_read_ledger_returns_none_on_missing_path(local_spark, tmp_path) -> None:
    """A path that doesn't exist returns None. If any exception other than the
    narrowed AnalysisException fires, it propagates — test would fail loudly."""
    missing = str(tmp_path / "ledger_never_created")
    result = _read_ledger(local_spark, missing, "Table")
    assert result is None

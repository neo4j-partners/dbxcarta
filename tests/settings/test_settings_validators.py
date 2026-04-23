"""Settings boundary validation (Phase 3.5 field-level + Phase 3.6 cross-field).

Phase 3.5 tightened `_IDENTIFIER_RE` to strict Databricks identifier shape.
Phase 3.6 added `model_validator(mode="after")` for cross-field coherence
(semantic inference requires column embeddings; value embeddings require
sample values).

All tests construct Settings with explicit kwargs — no cloud infra, no
Spark. Validates the boundary itself, not downstream behaviour.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbxcarta.settings import Settings


_BASE_SETTINGS = {
    "dbxcarta_summary_volume": "/Volumes/cat/schema/vol/dbxcarta",
    "dbxcarta_summary_table": "schema.table",
}


# --- _IDENTIFIER_RE tightening (Phase 3.5) ----------------------------------

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


# --- Phase 3.6 cross-field validation ---------------------------------------

def test_settings_rejects_semantic_without_column_embeddings() -> None:
    """DBXCARTA_INFER_SEMANTIC=true requires DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=true.

    Phase 4 needs column embeddings to compute cosine similarity; without them
    the phase has nothing to consume. The model_validator raises at Settings
    construction rather than letting the incoherence surface mid-run.
    """
    with pytest.raises(ValidationError, match="DBXCARTA_INFER_SEMANTIC"):
        Settings(
            dbxcarta_catalog="main",
            dbxcarta_infer_semantic=True,
            dbxcarta_include_embeddings_columns=False,
            **_BASE_SETTINGS,
        )


def test_settings_rejects_value_embeddings_without_sampling() -> None:
    """DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=true requires DBXCARTA_INCLUDE_VALUES=true.

    Previously warned at runtime; Phase 3.6 moves the check to the Settings
    cross-field validator so the job fails at startup instead of mid-run.
    """
    with pytest.raises(ValidationError, match="DBXCARTA_INCLUDE_EMBEDDINGS_VALUES"):
        Settings(
            dbxcarta_catalog="main",
            dbxcarta_include_values=False,
            dbxcarta_include_embeddings_values=True,
            # Semantic off so we only hit the values check.
            dbxcarta_infer_semantic=False,
            **_BASE_SETTINGS,
        )


def test_settings_accepts_semantic_with_column_embeddings() -> None:
    """The coherent configuration constructs cleanly."""
    s = Settings(
        dbxcarta_catalog="main",
        dbxcarta_infer_semantic=True,
        dbxcarta_include_embeddings_columns=True,
        **_BASE_SETTINGS,
    )
    assert s.dbxcarta_infer_semantic is True
    assert s.dbxcarta_include_embeddings_columns is True

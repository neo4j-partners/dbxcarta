"""Settings boundary validation: field-level identifier constraints and cross-field coherence.

`_IDENTIFIER_RE` enforces strict Databricks identifier shape.
`model_validator(mode="after")` enforces cross-field coherence:
semantic inference requires column embeddings; value embeddings require
sample values.

All tests construct Settings with explicit kwargs — no cloud infra, no
Spark. Validates the boundary itself, not downstream behaviour.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbxcarta.settings import Settings


_BASE_SETTINGS = {
    "dbxcarta_summary_volume": "/Volumes/cat/schema/vol/dbxcarta",
    "dbxcarta_summary_table": "cat.schema.table",
}


# --- _IDENTIFIER_RE validation -----------------------------------------------

@pytest.mark.parametrize("bad_catalog", [
    "evil`catalog",     # backtick — injection vector
    "cat.subcat",       # dot (catalog must be single identifier)
    "cat name",         # space
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
    "dbxcarta-catalog",   # hyphens are valid; require backtick-quoting in SQL
])
def test_settings_accepts_strict_catalog(good_catalog: str) -> None:
    s = Settings(dbxcarta_catalog=good_catalog, **_BASE_SETTINGS)
    assert s.dbxcarta_catalog == good_catalog


def test_settings_accepts_three_part_summary_table() -> None:
    s = Settings(
        dbxcarta_catalog="main",
        dbxcarta_summary_volume=_BASE_SETTINGS["dbxcarta_summary_volume"],
        dbxcarta_summary_table="cat.schema.table",
    )
    assert s.dbxcarta_summary_table == "cat.schema.table"


@pytest.mark.parametrize("bad_table", [
    "table_name",           # unqualified
    "schema.table",         # missing catalog
    "schema.`table`",       # backtick in a part — injection vector
    "schema..table",        # empty middle part
    "schema.1table",        # leading digit in a part
])
def test_settings_rejects_malformed_summary_table(bad_table: str) -> None:
    with pytest.raises(ValidationError, match="Invalid Databricks"):
        Settings(
            dbxcarta_catalog="main",
            dbxcarta_summary_volume=_BASE_SETTINGS["dbxcarta_summary_volume"],
            dbxcarta_summary_table=bad_table,
        )


@pytest.mark.parametrize("bad_path", [
    "/Volumes/cat/schema/vol",       # volume root, no subdir
    "/dbfs/Volumes/cat/schema/vol/runs",
    "/Volumes/cat/schema",
])
def test_settings_rejects_non_volume_summary_paths(bad_path: str) -> None:
    with pytest.raises(ValidationError, match="DBXCARTA_SUMMARY_VOLUME"):
        Settings(
            dbxcarta_catalog="main",
            dbxcarta_summary_volume=bad_path,
            dbxcarta_summary_table=_BASE_SETTINGS["dbxcarta_summary_table"],
        )


def test_settings_accepts_explicit_staging_and_ledger_volume_paths() -> None:
    s = Settings(
        dbxcarta_catalog="main",
        dbxcarta_staging_path="/Volumes/cat/schema/vol/staging/",
        dbxcarta_ledger_path="/Volumes/cat/schema/vol/ledger/",
        **_BASE_SETTINGS,
    )
    assert s.dbxcarta_staging_path == "/Volumes/cat/schema/vol/staging"
    assert s.dbxcarta_ledger_path == "/Volumes/cat/schema/vol/ledger"


def test_settings_rejects_path_traversal_in_volume_subdir() -> None:
    with pytest.raises(ValidationError, match="invalid path segment"):
        Settings(
            dbxcarta_catalog="main",
            dbxcarta_summary_volume="/Volumes/cat/schema/vol/../runs",
            dbxcarta_summary_table=_BASE_SETTINGS["dbxcarta_summary_table"],
        )


def test_settings_rejects_unsafe_embedding_endpoint() -> None:
    with pytest.raises(ValidationError, match="serving endpoint"):
        Settings(
            dbxcarta_catalog="main",
            dbxcarta_embedding_endpoint="bad'endpoint",
            **_BASE_SETTINGS,
        )


# --- Cross-field validation --------------------------------------------------

def test_settings_rejects_semantic_without_column_embeddings() -> None:
    """DBXCARTA_INFER_SEMANTIC=true requires DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=true.

    Semantic inference needs column embeddings to compute cosine similarity;
    without them there is nothing to consume. The model_validator raises at
    Settings construction rather than letting the incoherence surface mid-run.
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

    The Settings cross-field validator catches this at construction time so
    the job fails at startup instead of reaching an embedding step with no
    Value nodes to process.
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

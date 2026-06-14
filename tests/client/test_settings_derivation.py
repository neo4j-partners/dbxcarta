"""ClientSettings resolution after the client went fully local.

The client no longer derives ops-Volume sinks: the summary/questions Delta and
JSON outputs are gone, so the only ``_resolve_defaults`` work left is defaulting
the embed endpoint to the embedding endpoint. The questions path is now a plain
local file taken from the overlay as-is, with no Volume-path derivation.
"""

from __future__ import annotations

from dbxcarta.client.settings import ClientSettings

_VOLUME = "/Volumes/dbxcarta-catalog/finance_genie_ops/dbxcarta-ops"
_REQUIRED: dict[str, str] = {
    "dbxcarta_catalog": "finance",
    "databricks_warehouse_id": "wh-123",
    "databricks_secret_scope": "dbxcarta-neo4j-finance",
    "databricks_volume_path": _VOLUME,
}


def _make(**overrides: str) -> ClientSettings:
    return ClientSettings(_env_file=None, **{**_REQUIRED, **overrides})


def test_embed_endpoint_defaults_to_embedding_endpoint() -> None:
    settings = _make(dbxcarta_embedding_endpoint="custom-embed", dbxcarta_embed_endpoint="")
    assert settings.dbxcarta_embed_endpoint == "custom-embed"


def test_explicit_embed_endpoint_wins() -> None:
    settings = _make(
        dbxcarta_embedding_endpoint="custom-embed",
        dbxcarta_embed_endpoint="explicit-embed",
    )
    assert settings.dbxcarta_embed_endpoint == "explicit-embed"


def test_questions_path_is_taken_as_is() -> None:
    settings = _make(dbxcarta_client_questions="examples/finance-genie/questions.json")
    assert settings.dbxcarta_client_questions == "examples/finance-genie/questions.json"


def test_blank_questions_path_is_not_derived() -> None:
    # No Volume-path derivation remains; a blank value stays blank and is caught
    # by the run-time preflight, not silently turned into a /Volumes path.
    settings = _make(dbxcarta_client_questions="")
    assert settings.dbxcarta_client_questions == ""

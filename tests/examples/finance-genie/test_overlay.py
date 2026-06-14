from __future__ import annotations

from pathlib import Path

from dotenv import dotenv_values

# The committed overlay is the single source of per-example dbxcarta config.
_OVERLAY = (
    Path(__file__).resolve().parents[3] / "examples" / "finance-genie" / "dbxcarta-overlay.env"
)


def _overlay_env() -> dict[str, str]:
    return {k: v for k, v in dotenv_values(_OVERLAY).items() if v is not None}


def test_questions_file_bundled_at_example_root() -> None:
    questions = _OVERLAY.parent / "questions.json"
    assert questions.is_file()


def test_overlay_carries_neocarta_ingest_contract() -> None:
    """The overlay feeds neocarta's ingest wheel through NEOCARTA_DATABRICKS_* keys.

    The connector ships as a staged wheel rather than a dbxcarta dependency, so
    its SparkIngestSettings is not importable here. This asserts the committed
    overlay carries neocarta's env contract with the expected raw values; the
    connector's own tests cover parsing those values into settings.
    """
    env = _overlay_env()
    assert env["NEOCARTA_DATABRICKS_SECRET_SCOPE"] == "dbxcarta-neo4j-finance-genie"
    assert env["NEOCARTA_DATABRICKS_CATALOG"] == "graph-enriched-finance-silver"
    # Each entry is catalog or catalog:layer; the layer rides on the catalog list.
    assert env["NEOCARTA_DATABRICKS_CATALOGS"] == (
        "graph-enriched-finance-silver:silver,graph-enriched-finance-gold:gold"
    )
    assert env["NEOCARTA_DATABRICKS_SCHEMAS"] == "graph-enriched-schema"
    # Inline embedding mode: every per-label flag on, with a required staging volume.
    for label in ("TABLES", "COLUMNS", "SCHEMAS", "DATABASES"):
        assert env[f"NEOCARTA_DATABRICKS_INCLUDE_EMBEDDINGS_{label}"] == "true"
    assert env["NEOCARTA_DATABRICKS_EMBEDDING_STAGING_VOLUME"].startswith("/Volumes/")


def test_overlay_pins_known_keys() -> None:
    env = _overlay_env()
    assert env["DBXCARTA_INJECT_CRITERIA"] == "false"
    assert env["DBXCARTA_CLIENT_ARMS"] == "no_context,schema_dump,graph_rag"
    # The client runs locally, so the questions path is the committed local
    # file at the example root, not a /Volumes path.
    assert env["DBXCARTA_CLIENT_QUESTIONS"] == "examples/finance-genie/questions.json"
    # The layer is folded into the catalog lists; the separate map is gone.
    assert "DBXCARTA_LAYER_MAP" not in env
    # The old spark-ingest keys are retired now the ingest runs the neocarta wheel.
    assert "DBXCARTA_SCHEMAS" not in env
    assert "DBXCARTA_SUMMARY_TABLE" not in env
    assert "DBXCARTA_INCLUDE_EMBEDDINGS_VALUES" not in env
    # The operator/client catalog key stays on its dbxcarta name.
    assert env["DBXCARTA_CATALOGS"] == (
        "graph-enriched-finance-silver:silver,graph-enriched-finance-gold:gold"
    )

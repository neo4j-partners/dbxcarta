from __future__ import annotations

import json
from pathlib import Path

from dbxcarta.cli import _handle_finance_genie_preset
from dbxcarta.client.settings import ClientSettings
from dbxcarta.presets.finance_genie import (
    FINANCE_GENIE_BASE_TABLES,
    FINANCE_GENIE_GOLD_TABLES,
    FINANCE_GENIE_TABLES,
    finance_genie_env,
    readiness_from_table_names,
)
from dbxcarta.settings import Settings


def test_finance_genie_env_defaults_validate_against_settings() -> None:
    env = finance_genie_env()

    settings = Settings(
        dbxcarta_catalog=env["DBXCARTA_CATALOG"],
        dbxcarta_schemas=env["DBXCARTA_SCHEMAS"],
        dbxcarta_summary_volume=env["DBXCARTA_SUMMARY_VOLUME"],
        dbxcarta_summary_table=env["DBXCARTA_SUMMARY_TABLE"],
        dbxcarta_include_values=env["DBXCARTA_INCLUDE_VALUES"] == "true",
        dbxcarta_include_embeddings_tables=True,
        dbxcarta_include_embeddings_columns=True,
        dbxcarta_include_embeddings_values=True,
        dbxcarta_include_embeddings_schemas=True,
        dbxcarta_include_embeddings_databases=True,
        dbxcarta_infer_semantic=True,
        dbxcarta_embedding_endpoint=env["DBXCARTA_EMBEDDING_ENDPOINT"],
        dbxcarta_embedding_dimension=int(env["DBXCARTA_EMBEDDING_DIMENSION"]),
        dbxcarta_embedding_failure_threshold=float(
            env["DBXCARTA_EMBEDDING_FAILURE_THRESHOLD"]
        ),
    )

    assert settings.dbxcarta_catalog == "graph-enriched-lakehouse"
    assert settings.dbxcarta_schemas == "graph-enriched-schema"


def test_finance_genie_env_defaults_validate_against_client_settings() -> None:
    env = finance_genie_env()

    client_settings = ClientSettings(
        dbxcarta_catalog=env["DBXCARTA_CATALOG"],
        dbxcarta_schemas=env["DBXCARTA_SCHEMAS"],
        databricks_warehouse_id="warehouse-id",
        dbxcarta_summary_volume=env["DBXCARTA_SUMMARY_VOLUME"],
        dbxcarta_summary_table=env["DBXCARTA_SUMMARY_TABLE"],
        databricks_volume_path=env["DATABRICKS_VOLUME_PATH"],
        dbxcarta_client_questions=env["DBXCARTA_CLIENT_QUESTIONS"],
        dbxcarta_client_arms=env["DBXCARTA_CLIENT_ARMS"],
    )

    assert client_settings.schemas_list == ["graph-enriched-schema"]
    assert client_settings.arms == ["no_context", "schema_dump", "graph_rag"]
    assert env["DBXCARTA_INJECT_CRITERIA"] == "false"


def test_readiness_all_tables_present_is_ready() -> None:
    report = readiness_from_table_names(FINANCE_GENIE_TABLES)

    assert report.base_ready
    assert report.gold_ready
    assert report.ok(strict_gold=True)


def test_readiness_base_only_warns_without_strict_gold() -> None:
    report = readiness_from_table_names(FINANCE_GENIE_BASE_TABLES)

    assert report.base_ready
    assert not report.gold_ready
    assert report.ok(strict_gold=False)
    assert not report.ok(strict_gold=True)
    assert report.missing_gold_tables == FINANCE_GENIE_GOLD_TABLES


def test_readiness_missing_base_is_not_ready() -> None:
    report = readiness_from_table_names(["accounts"])

    assert not report.base_ready
    assert not report.ok()
    assert "merchants" in report.missing_base_tables


def test_cli_print_env_ignores_existing_dbxcarta_env(monkeypatch, capsys) -> None:
    monkeypatch.setenv("DBXCARTA_CATALOG", "dbxcarta-catalog")
    monkeypatch.setenv("DBXCARTA_SCHEMAS", "dbxcarta-schema")

    code = _handle_finance_genie_preset(["--print-env"])

    out = capsys.readouterr().out
    assert code == 0
    assert "DBXCARTA_CATALOG=graph-enriched-lakehouse" in out
    assert "DBXCARTA_SCHEMAS=graph-enriched-schema" in out
    assert "dbxcarta-catalog" not in out


def test_finance_genie_questions_reference_expected_tables() -> None:
    questions_path = Path("examples/finance-genie/questions.json")
    questions = json.loads(questions_path.read_text())

    assert questions
    references = "\n".join(q["reference_sql"] for q in questions)
    for table in FINANCE_GENIE_TABLES:
        assert f"`graph-enriched-schema`.{table}" in references

"""SchemaPile dbxcarta preset.

Owns the env overlay, readiness check, and question-upload helper for the
schemapile example. The list of UC schemas is sourced from the
`DBXCARTA_SCHEMAS` env var at `env()` time so the preset always reflects
the latest output of the materializer (which writes the list into
`.env.generated`).

Resolvable via:
    uv run dbxcarta preset dbxcarta_schemapile_example:preset --print-env
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dbxcarta import ReadinessReport, validate_identifier
from dbxcarta.databricks import quote_identifier

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


_DEFAULT_CATALOG = "schemapile_lakehouse"
_DEFAULT_META_SCHEMA = "_meta"
_DEFAULT_VOLUME = "schemapile_volume"
_QUESTIONS_FILENAME = "questions.json"


@dataclass(frozen=True)
class SchemaPilePreset:
    """Preset implementation for the SchemaPile example.

    Catalog, meta schema, and volume have defaults so module-level
    construction does not depend on the environment being primed. The
    UC-schema list is pulled from `DBXCARTA_SCHEMAS` inside `env()` so the
    preset always reflects the latest materialize-step output.
    """

    catalog: str = _DEFAULT_CATALOG
    meta_schema: str = _DEFAULT_META_SCHEMA
    volume: str = _DEFAULT_VOLUME
    embedding_endpoint: str = "databricks-gte-large-en"
    embedding_dimension: int = 1024
    embedding_failure_threshold: float = 0.10
    include_values: bool = True
    sample_limit: int = 10
    sample_cardinality_threshold: int = 50
    infer_semantic: bool = True
    include_embeddings_tables: bool = True
    include_embeddings_columns: bool = True
    include_embeddings_values: bool = False
    include_embeddings_schemas: bool = True
    include_embeddings_databases: bool = False
    client_arms: str = "no_context,schema_dump,graph_rag"
    inject_criteria: bool = False

    def __post_init__(self) -> None:
        validate_identifier(self.catalog, label="catalog")
        validate_identifier(self.meta_schema, label="meta schema")
        validate_identifier(self.volume, label="volume")

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.meta_schema}/{self.volume}"

    def schemas_list(self) -> tuple[str, ...]:
        """Read DBXCARTA_SCHEMAS from the environment and validate each name."""
        raw = os.environ.get("DBXCARTA_SCHEMAS", "").strip()
        names = tuple(s.strip() for s in raw.split(",") if s.strip())
        for name in names:
            validate_identifier(name, label="schema")
        return names

    def env(self) -> dict[str, str]:
        volume_path = self.volume_path
        return {
            "DBXCARTA_CATALOG": self.catalog,
            "DBXCARTA_SCHEMAS": ",".join(self.schemas_list()),
            "DATABRICKS_VOLUME_PATH": volume_path,
            "DBXCARTA_SUMMARY_VOLUME": f"{volume_path}/dbxcarta/runs",
            "DBXCARTA_SUMMARY_TABLE": (
                f"{self.catalog}.{self.meta_schema}.dbxcarta_run_summary"
            ),
            "DBXCARTA_INCLUDE_VALUES": _bool(self.include_values),
            "DBXCARTA_SAMPLE_LIMIT": str(self.sample_limit),
            "DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD": str(self.sample_cardinality_threshold),
            "DBXCARTA_INCLUDE_EMBEDDINGS_TABLES": _bool(self.include_embeddings_tables),
            "DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS": _bool(self.include_embeddings_columns),
            "DBXCARTA_INCLUDE_EMBEDDINGS_VALUES": _bool(self.include_embeddings_values),
            "DBXCARTA_INCLUDE_EMBEDDINGS_SCHEMAS": _bool(self.include_embeddings_schemas),
            "DBXCARTA_INCLUDE_EMBEDDINGS_DATABASES": _bool(self.include_embeddings_databases),
            "DBXCARTA_INFER_SEMANTIC": _bool(self.infer_semantic),
            "DBXCARTA_EMBEDDING_ENDPOINT": self.embedding_endpoint,
            "DBXCARTA_EMBEDDING_DIMENSION": str(self.embedding_dimension),
            "DBXCARTA_EMBEDDING_FAILURE_THRESHOLD": f"{self.embedding_failure_threshold}",
            "DBXCARTA_CLIENT_QUESTIONS": f"{volume_path}/dbxcarta/{_QUESTIONS_FILENAME}",
            "DBXCARTA_CLIENT_ARMS": self.client_arms,
            "DBXCARTA_INJECT_CRITERIA": _bool(self.inject_criteria),
        }

    def readiness(
        self,
        ws: "WorkspaceClient",
        warehouse_id: str,
    ) -> ReadinessReport:
        """Report whether the materialized UC schemas exist under the catalog.

        Required-vs-optional is collapsed because the schemapile example does
        not have a fixed expected table set; readiness is "did materialize
        run, and are the produced schemas visible?".
        """
        expected = self.schemas_list()
        if not expected:
            return ReadinessReport(
                catalog=self.catalog,
                schema="",
                present=(),
                missing_required=("(DBXCARTA_SCHEMAS is empty; run materialize first)",),
                missing_optional=(),
            )

        present_schemas = _fetch_schema_names(ws, warehouse_id, self.catalog)
        present_set = {n.strip() for n in present_schemas if n and n.strip()}
        present = tuple(n for n in expected if n in present_set)
        missing = tuple(n for n in expected if n not in present_set)
        return ReadinessReport(
            catalog=self.catalog,
            schema=",".join(expected),
            present=present,
            missing_required=missing,
            missing_optional=(),
        )

    def upload_questions(self, ws: "WorkspaceClient") -> None:
        dest = os.environ.get("DBXCARTA_CLIENT_QUESTIONS", "")
        if not dest:
            raise RuntimeError(
                "DBXCARTA_CLIENT_QUESTIONS is not set; cannot determine upload destination."
            )
        if not dest.startswith("/Volumes/") or not dest.endswith(".json"):
            raise ValueError(
                f"DBXCARTA_CLIENT_QUESTIONS must be a /Volumes/... .json path, got {dest!r}"
            )
        source = Path(os.environ.get("SCHEMAPILE_QUESTIONS_FILE", "questions.json"))
        if not source.is_file():
            raise FileNotFoundError(
                f"questions file not found at {source};"
                " run dbxcarta-schemapile-generate-questions first"
            )
        _ensure_parent_dir(ws, dest)
        with source.open("rb") as fh:
            ws.files.upload(file_path=dest, contents=fh, overwrite=True)


def _fetch_schema_names(
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog: str,
) -> list[str]:
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout

    statement = (
        "SELECT schema_name"
        f" FROM {quote_identifier(catalog)}.information_schema.schemata"
    )
    response = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
    )
    rows = getattr(getattr(response, "result", None), "data_array", None) or []
    return [row[0] for row in rows if row]


def _ensure_parent_dir(ws: "WorkspaceClient", dest: str) -> None:
    from databricks.sdk.errors import ResourceAlreadyExists

    parent = dest.rsplit("/", 1)[0]
    try:
        ws.files.create_directory(parent)
    except ResourceAlreadyExists:
        pass


def _bool(value: bool) -> str:
    return "true" if value else "false"


preset = SchemaPilePreset()


__all__ = ["SchemaPilePreset", "preset"]

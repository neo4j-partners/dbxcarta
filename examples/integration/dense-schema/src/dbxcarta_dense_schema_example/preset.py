"""Dense-schema dbxcarta preset.

Configures the dbxcarta ingest and client runs for a single large schema.
Schema name is read from DBXCARTA_SCHEMAS at env() time so it reflects the
value set in the root .env before submission.

Resolvable via:
    uv run dbxcarta preset dbxcarta_dense_schema_example:preset --print-env
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dbxcarta.client.questions import load_questions
from dbxcarta.spark.databricks import quote_identifier, validate_identifier
from dbxcarta.spark.presets import ReadinessReport

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


_DEFAULT_CATALOG = "schemapile_lakehouse"
_DEFAULT_META_SCHEMA = "_meta"
_DEFAULT_VOLUME = "schemapile_volume"
_QUESTIONS_FILENAME = "dense_questions.json"
_QUESTIONS_FILE = Path(__file__).resolve().parents[2] / "questions.json"


@dataclass(frozen=True)
class DenseSchemaPreset:
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
    inject_criteria: bool = True
    schema_dump_max_chars: int = 50000

    def __post_init__(self) -> None:
        validate_identifier(self.catalog, label="catalog")
        validate_identifier(self.meta_schema, label="meta schema")
        validate_identifier(self.volume, label="volume")

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.meta_schema}/{self.volume}"

    def schemas_list(self) -> tuple[str, ...]:
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
            "DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD": str(
                self.sample_cardinality_threshold
            ),
            "DBXCARTA_INCLUDE_EMBEDDINGS_TABLES": _bool(
                self.include_embeddings_tables
            ),
            "DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS": _bool(
                self.include_embeddings_columns
            ),
            "DBXCARTA_INCLUDE_EMBEDDINGS_VALUES": _bool(
                self.include_embeddings_values
            ),
            "DBXCARTA_INCLUDE_EMBEDDINGS_SCHEMAS": _bool(
                self.include_embeddings_schemas
            ),
            "DBXCARTA_INCLUDE_EMBEDDINGS_DATABASES": _bool(
                self.include_embeddings_databases
            ),
            "DBXCARTA_INFER_SEMANTIC": _bool(self.infer_semantic),
            "DBXCARTA_EMBEDDING_ENDPOINT": self.embedding_endpoint,
            "DBXCARTA_EMBEDDING_DIMENSION": str(self.embedding_dimension),
            "DBXCARTA_EMBEDDING_FAILURE_THRESHOLD": (
                f"{self.embedding_failure_threshold}"
            ),
            "DBXCARTA_CLIENT_QUESTIONS": (
                f"{volume_path}/dbxcarta/{_QUESTIONS_FILENAME}"
            ),
            "DBXCARTA_CLIENT_ARMS": self.client_arms,
            "DBXCARTA_INJECT_CRITERIA": _bool(self.inject_criteria),
            "DBXCARTA_SCHEMA_DUMP_MAX_CHARS": str(self.schema_dump_max_chars),
        }

    def readiness(
        self,
        ws: "WorkspaceClient",
        warehouse_id: str,
    ) -> ReadinessReport:
        expected = self.schemas_list()
        if not expected:
            return ReadinessReport(
                catalog=self.catalog,
                schema="",
                present=(),
                missing_required=(
                    "(DBXCARTA_SCHEMAS is empty; run materialize first)",
                ),
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
                "DBXCARTA_CLIENT_QUESTIONS is not set;"
                " cannot determine upload destination."
            )
        source = Path(os.environ.get("DENSE_QUESTIONS_FILE", str(_QUESTIONS_FILE)))
        _validate_questions_file(source)
        _ensure_parent_dir(ws, dest)
        with source.open("rb") as fh:
            ws.files.upload(file_path=dest, contents=fh, overwrite=True)


def _fetch_schema_names(
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog: str,
) -> list[str]:
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout

    response = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            "SELECT schema_name FROM"
            f" {quote_identifier(catalog)}.information_schema.schemata"
        ),
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


def _validate_questions_file(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(
            f"questions file not found at {path};"
            " run dbxcarta-dense-generate-questions first"
        )
    questions = load_questions(str(path))
    if not questions:
        raise ValueError(f"questions file must be a non-empty JSON array: {path}")


def _bool(value: bool) -> str:
    return "true" if value else "false"


preset = DenseSchemaPreset()

__all__ = ["DenseSchemaPreset", "preset"]

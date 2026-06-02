"""Dense-schema dbxcarta preset.

Per-example dbxcarta config lives in the committed
examples/dense-schema/dbxcarta-overlay.env. This preset only provides the
operational capabilities (readiness, question upload); readiness reads the
target schema names from DBXCARTA_SCHEMAS.

Resolvable via:
    uv run dbxcarta preset dbxcarta_dense_schema_example:preset --check-ready
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


_DEFAULT_CATALOG = "dense-schema_example"
_QUESTIONS_FILE = Path(__file__).resolve().parents[2] / "questions.json"


@dataclass(frozen=True)
class DenseSchemaPreset:
    catalog: str = _DEFAULT_CATALOG

    def __post_init__(self) -> None:
        validate_identifier(self.catalog, label="catalog")

    def schemas_list(self) -> tuple[str, ...]:
        raw = os.environ.get("DBXCARTA_SCHEMAS", "").strip()
        names = tuple(s.strip() for s in raw.split(",") if s.strip())
        for name in names:
            validate_identifier(name, label="schema")
        return names

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


preset = DenseSchemaPreset()

__all__ = ["DenseSchemaPreset", "preset"]

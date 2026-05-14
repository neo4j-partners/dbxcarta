"""Pipeline self-verification: pure assertion functions over a run summary.

The same functions run at the end of `run_dbxcarta` and from the
`dbxcarta verify` CLI. Inputs are a `dict` summary matching
`RunSummary.to_dict()`, a `neo4j.Driver`, and, for catalog-level checks, a
`WorkspaceClient` plus warehouse id. Outputs are aggregated into a `Report`.
Each check returns `list[Violation]`; an empty list means the check passed or
its preconditions were not met and it self-skipped.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from neo4j import Driver


@dataclass
class Violation:
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def single_value(result: Any, key: str) -> Any:
    """Return one field from a Neo4j result that must contain one record."""
    record = result.single()
    if record is None:
        raise RuntimeError(f"Neo4j query returned no rows for {key!r}")
    return record[key]


@dataclass
class Report:
    run_id: str
    violations: list[Violation] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    def format(self) -> str:
        if self.ok:
            return f"verify: run_id={self.run_id} OK ({len(self.violations)} violations)"
        lines = [f"verify: run_id={self.run_id} FAIL ({len(self.violations)} violations)"]
        for v in self.violations:
            lines.append(f"  [{v.code}] {v.message}")
            if v.details:
                lines.append(f"    {v.details}")
        return "\n".join(lines)


def verify_run(
    *,
    summary: dict[str, Any],
    neo4j_driver: "Driver",
    ws: "WorkspaceClient",
    warehouse_id: str,
    catalog: str,
    sample_limit: int,
) -> Report:
    """Run every verify check against the given run summary; return one Report."""
    from dbxcarta.core.verify import catalog as catalog_mod
    from dbxcarta.core.verify import graph as graph_mod
    from dbxcarta.core.verify import references as references_mod
    from dbxcarta.core.verify import values as values_mod

    run_id = str(summary.get("run_id", ""))
    violations: list[Violation] = []

    violations.extend(_check_summary_shape(summary))
    violations.extend(graph_mod.check(neo4j_driver, summary))
    violations.extend(references_mod.check(neo4j_driver, summary))
    violations.extend(values_mod.check(neo4j_driver, summary, sample_limit=sample_limit))
    violations.extend(catalog_mod.check(neo4j_driver, summary, ws=ws, warehouse_id=warehouse_id, catalog=catalog))

    return Report(run_id=run_id, violations=violations)


def _check_summary_shape(summary: dict[str, Any]) -> list[Violation]:
    out: list[Violation] = []
    if summary.get("status") != "success":
        out.append(Violation(
            code="summary.status_not_success",
            message=f"Run status is {summary.get('status')!r}; verify only meaningful for successful runs.",
            details={"error": summary.get("error")},
        ))
    if not summary.get("run_id"):
        out.append(Violation(code="summary.missing_run_id", message="Run summary has no run_id."))
    if not summary.get("row_counts"):
        out.append(Violation(code="summary.missing_row_counts", message="Run summary has empty row_counts."))
    return out


__all__ = ["Violation", "Report", "single_value", "verify_run"]

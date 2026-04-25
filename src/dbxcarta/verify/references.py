"""REFERENCES (FK) edge invariants and fixture-coverage check.

Ports the three live tests in tests/schema_graph/test_build_references_rel.py:
- Edge-count invariant (universal): Neo4j REFERENCES count matches summary fk_edges.
- Accounting invariant (universal): fk_skipped == fk_declared - fk_resolved.
- Fixture-exact assertion (precondition-gated): only when seeded W8 fixture schemas
  are within scope — otherwise self-skips.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbxcarta.contract import RelType
from dbxcarta.verify import Violation

if TYPE_CHECKING:
    from neo4j import Driver


_FIXTURE_SCHEMAS = frozenset({
    "dbxcarta_test_sales",
    "dbxcarta_test_inventory",
    "dbxcarta_test_hr",
    "dbxcarta_test_events",
})
_FIXTURE_EXPECTED_DECLARED = 16
_FIXTURE_EXPECTED_RESOLVED = 16
# 16 declared FKs + 1 metadata-inferred (employees.job_title_id -> job_titles.id,
# detected from the column's "FK to job_titles" comment hint).
_FIXTURE_EXPECTED_EDGES = 17


def _expected_edge_total(summary: dict[str, Any]) -> int:
    """Total REFERENCES edges the writer is expected to have produced.

    The summary's `fk_edges` counter only counts *declared* FKs from the
    catalog; metadata- and semantic-inferred FKs are tracked under separate
    `fk_inferred_*_accepted` counters but are written to Neo4j as REFERENCES
    edges alongside the declared ones. Comparing `fk_edges` to Neo4j's
    REFERENCES count without summing the inferred totals will always look
    high by `accepted` whenever inference is on (its default for metadata).
    """
    counts = summary.get("row_counts") or {}
    return (
        counts.get("fk_edges", 0)
        + counts.get("fk_inferred_metadata_accepted", 0)
        + counts.get("fk_inferred_semantic_accepted", 0)
    )


def check(driver: "Driver", summary: dict[str, Any]) -> list[Violation]:
    out: list[Violation] = []
    out.extend(_check_edge_count(driver, summary))
    out.extend(_check_accounting(summary))
    out.extend(_check_fixture_coverage(summary))
    return out


def _check_edge_count(driver: "Driver", summary: dict[str, Any]) -> list[Violation]:
    """Neo4j's REFERENCES edge count must match declared + inferred FKs from
    the summary. A mismatch implies the Spark Connector dropped rows where an
    endpoint Column node did not exist."""
    expected = _expected_edge_total(summary)
    with driver.session() as s:
        edges = s.run(
            f"MATCH ()-[r:{RelType.REFERENCES}]->() RETURN count(r) AS cnt"
        ).single()["cnt"]
    if edges != expected:
        return [Violation(
            code="references.edge_count_mismatch",
            message=f"Neo4j has {edges} REFERENCES edges; run summary reported {expected} (declared + inferred).",
            details={"neo4j": edges, "summary_total": expected},
        )]
    return []


def _check_accounting(summary: dict[str, Any]) -> list[Violation]:
    """fk_skipped must equal fk_declared - fk_resolved; resolved <= declared; skipped >= 0."""
    counts = summary.get("row_counts") or {}
    declared = counts.get("fk_declared", 0)
    resolved = counts.get("fk_resolved", 0)
    skipped = counts.get("fk_skipped", 0)
    out: list[Violation] = []
    if skipped != declared - resolved:
        out.append(Violation(
            code="references.accounting_mismatch",
            message=f"fk_skipped ({skipped}) != fk_declared - fk_resolved ({declared} - {resolved}).",
            details={"declared": declared, "resolved": resolved, "skipped": skipped},
        ))
    if resolved > declared:
        out.append(Violation(
            code="references.resolved_exceeds_declared",
            message=f"fk_resolved ({resolved}) > fk_declared ({declared}).",
            details={"declared": declared, "resolved": resolved},
        ))
    if skipped < 0:
        out.append(Violation(
            code="references.skipped_negative",
            message=f"fk_skipped ({skipped}) is negative.",
            details={"skipped": skipped},
        ))
    return out


def _check_fixture_coverage(summary: dict[str, Any]) -> list[Violation]:
    """Strict coverage check that only applies when the run's scope is a
    superset of the seeded W8 fixture schemas. Self-skips otherwise."""
    scope = set(summary.get("schemas") or [])
    if not _FIXTURE_SCHEMAS.issubset(scope):
        return []

    counts = summary.get("row_counts") or {}
    declared = counts.get("fk_declared", 0)
    resolved = counts.get("fk_resolved", 0)
    total_edges = _expected_edge_total(summary)

    out: list[Violation] = []
    if declared < _FIXTURE_EXPECTED_DECLARED:
        out.append(Violation(
            code="references.fixture_declared_below_expected",
            message=f"Fixture schemas in scope but fk_declared={declared} < {_FIXTURE_EXPECTED_DECLARED}; seeded fixtures may be missing.",
            details={"declared": declared, "expected_min": _FIXTURE_EXPECTED_DECLARED},
        ))
    if resolved < _FIXTURE_EXPECTED_RESOLVED:
        out.append(Violation(
            code="references.fixture_resolved_below_expected",
            message=f"Fixture schemas in scope but fk_resolved={resolved} < {_FIXTURE_EXPECTED_RESOLVED}.",
            details={"resolved": resolved, "expected_min": _FIXTURE_EXPECTED_RESOLVED},
        ))
    if total_edges < _FIXTURE_EXPECTED_EDGES:
        out.append(Violation(
            code="references.fixture_edges_below_expected",
            message=f"Fixture schemas in scope but declared+inferred edges={total_edges} < {_FIXTURE_EXPECTED_EDGES}.",
            details={"total_edges": total_edges, "expected_min": _FIXTURE_EXPECTED_EDGES},
        ))
    return out

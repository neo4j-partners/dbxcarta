"""Graph-structure invariants: node counts, contract version, relationship integrity.

Mirrors the historical live graph checks for node counts, contract version,
and relationship integrity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbxcarta.spark.contract import CONTRACT_VERSION, NodeLabel
from dbxcarta.spark.verify import Violation, scoped_catalog, single_value

if TYPE_CHECKING:
    from neo4j import Driver


def check(driver: "Driver", summary: dict[str, Any]) -> list[Violation]:
    out: list[Violation] = []
    out.extend(_check_node_counts(driver, summary))
    out.extend(_check_contract_version(driver))
    out.extend(_check_relationship_integrity(driver))
    return out


def _check_node_counts(driver: "Driver", summary: dict[str, Any]) -> list[Violation]:
    """Neo4j node counts match what the job reported writing.

    Queries are scoped to the current catalog so a shared Neo4j instance
    holding data from multiple catalogs does not produce false positives.
    """
    catalog, prefix = scoped_catalog(summary)
    counts = summary.get("row_counts") or {}
    expected = {
        NodeLabel.DATABASE: counts.get("databases", 1),
        NodeLabel.SCHEMA: counts.get("schemas"),
        NodeLabel.TABLE: counts.get("tables"),
        NodeLabel.COLUMN: counts.get("columns"),
    }
    _cypher: dict[NodeLabel, tuple[str, dict]] = {
        NodeLabel.DATABASE: (
            f"MATCH (n:{NodeLabel.DATABASE}) WHERE n.id = $catalog RETURN count(n) AS cnt",
            {"catalog": catalog},
        ),
        NodeLabel.SCHEMA: (
            f"MATCH (n:{NodeLabel.SCHEMA}) WHERE n.id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": prefix},
        ),
        NodeLabel.TABLE: (
            f"MATCH (n:{NodeLabel.TABLE}) WHERE n.id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": prefix},
        ),
        NodeLabel.COLUMN: (
            f"MATCH (n:{NodeLabel.COLUMN}) WHERE n.id STARTS WITH $prefix RETURN count(n) AS cnt",
            {"prefix": prefix},
        ),
    }
    out: list[Violation] = []
    with driver.session() as s:
        for label, exp in expected.items():
            if exp is None:
                continue
            cypher, params = _cypher[label]
            actual = single_value(s.run(cypher, **params), "cnt")
            if actual != exp:
                out.append(Violation(
                    code=f"graph.node_count_mismatch.{label.value}",
                    message=f"{label.value}: Neo4j has {actual}, run summary reported {exp}.",
                    details={"label": label.value, "neo4j": actual, "summary": exp},
                ))
    return out


def _check_contract_version(driver: "Driver") -> list[Violation]:
    """Every written node carries the current contract version. Catches partial
    re-runs that mix nodes from two wheel versions."""
    labels = (NodeLabel.DATABASE, NodeLabel.SCHEMA, NodeLabel.TABLE, NodeLabel.COLUMN)
    out: list[Violation] = []
    with driver.session() as s:
        for label in labels:
            wrong = single_value(s.run(
                f"MATCH (n:{label}) WHERE n.contract_version <> $v RETURN count(n) AS cnt",
                v=CONTRACT_VERSION,
            ), "cnt")
            if wrong:
                out.append(Violation(
                    code=f"graph.wrong_contract_version.{label.value}",
                    message=f"{wrong} {label.value} node(s) have contract_version != {CONTRACT_VERSION!r}.",
                    details={"label": label.value, "count": wrong, "expected": CONTRACT_VERSION},
                ))
    return out


def _check_relationship_integrity(driver: "Driver") -> list[Violation]:
    """No orphan Schema/Table/Column nodes; no Schema with multiple Database parents."""
    out: list[Violation] = []
    queries = [
        ("graph.orphan_schema",
         "Schema node(s) have no incoming HAS_SCHEMA",
         "MATCH (n:Schema) WHERE NOT (:Database)-[:HAS_SCHEMA]->(n) RETURN count(n) AS cnt"),
        ("graph.orphan_table",
         "Table node(s) have no incoming HAS_TABLE",
         "MATCH (n:Table) WHERE NOT (:Schema)-[:HAS_TABLE]->(n) RETURN count(n) AS cnt"),
        ("graph.orphan_column",
         "Column node(s) have no incoming HAS_COLUMN",
         "MATCH (n:Column) WHERE NOT (:Table)-[:HAS_COLUMN]->(n) RETURN count(n) AS cnt"),
        ("graph.schema_multi_parent",
         "Schema node(s) have more than one Database parent",
         "MATCH (n:Schema)"
         " WITH n, size([(db:Database)-[:HAS_SCHEMA]->(n) | db]) AS parents"
         " WHERE parents > 1 RETURN count(n) AS cnt"),
    ]
    with driver.session() as s:
        for code, msg, query in queries:
            cnt = single_value(s.run(query), "cnt")
            if cnt:
                out.append(Violation(code=code, message=f"{cnt} {msg}.", details={"count": cnt}))
    return out

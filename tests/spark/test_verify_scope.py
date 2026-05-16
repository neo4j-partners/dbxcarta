"""Regression tests for catalog-scoped verify checks.

The verify node/edge/value count checks scope their Cypher to the current
catalog so a shared Neo4j instance is not a source of false positives. Node
ids are normalized through ``contract.generate_id`` (lowercased; spaces and
hyphens become underscores). The scoping predicate must apply the same
normalization: a regression where it compared the raw ``summary["catalog"]``
against ``n.id`` made every scoped count read 0 whenever the catalog or schema
name contained a hyphen, so a fully correct ingest failed the verify gate.
"""

from __future__ import annotations

import re

import pytest

from dbxcarta.spark import verify
from dbxcarta.spark.contract import generate_id, generate_value_id
from dbxcarta.spark.verify import graph as graph_mod
from dbxcarta.spark.verify import references as references_mod
from dbxcarta.spark.verify import values as values_mod

_NODE_LABEL = re.compile(r"\(n:(\w+)\)")
_REL_SRC = re.compile(r"\(src:(\w+)\)-\[r:(\w+)\]")

# A catalog/schema whose names contain hyphens — the exact trigger.
_CATALOG = "graph-enriched-lakehouse"
_SCHEMA = "graph-enriched-schema"


class _Result:
    def __init__(self, cnt: int) -> None:
        self._cnt = cnt

    def single(self):
        return {"cnt": self._cnt}


class _Session:
    def __init__(self, nodes, rels) -> None:
        self._nodes = nodes
        self._rels = rels

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def run(self, cypher: str, **params):
        if "count(r)" in cypher:
            rel = _REL_SRC.search(cypher).group(2)
            prefix = params.get("prefix")
            cnt = sum(
                1
                for rt, sid in self._rels
                if rt == rel and (prefix is None or sid.startswith(prefix))
            )
            return _Result(cnt)
        label = _NODE_LABEL.search(cypher).group(1)
        if "catalog" in params:
            cnt = sum(1 for lb, nid in self._nodes if lb == label and nid == params["catalog"])
        elif "prefix" in params:
            cnt = sum(
                1 for lb, nid in self._nodes if lb == label and nid.startswith(params["prefix"])
            )
        else:
            cnt = sum(1 for lb, _ in self._nodes if lb == label)
        return _Result(cnt)


class _Driver:
    def __init__(self, nodes, rels=()) -> None:
        self._nodes = list(nodes)
        self._rels = list(rels)

    def session(self):
        return _Session(self._nodes, self._rels)


def test_scoped_catalog_normalizes_hyphens():
    catalog_id, prefix = verify.scoped_catalog({"catalog": _CATALOG})
    assert catalog_id == "graph_enriched_lakehouse"
    assert prefix == "graph_enriched_lakehouse."


def test_scoped_catalog_empty_summary():
    assert verify.scoped_catalog({}) == ("", ".")


def _graph_nodes():
    db_id = generate_id(_CATALOG)
    schema_id = generate_id(_CATALOG, _SCHEMA)
    tables = ["orders", "customers"]
    nodes = [("Database", db_id), ("Schema", schema_id)]
    for t in tables:
        nodes.append(("Table", generate_id(_CATALOG, _SCHEMA, t)))
    for col in ["id", "name", "total"]:
        nodes.append(("Column", generate_id(_CATALOG, _SCHEMA, "orders", col)))
    return nodes


def test_node_counts_pass_with_hyphenated_catalog():
    """The exact production scenario: correct graph, hyphenated catalog."""
    summary = {
        "catalog": _CATALOG,
        "row_counts": {"databases": 1, "schemas": 1, "tables": 2, "columns": 3},
    }
    driver = _Driver(_graph_nodes())
    assert graph_mod._check_node_counts(driver, summary) == []


def test_node_counts_still_flag_a_real_mismatch():
    """The fix must not blanket-pass: a genuinely missing node still fails."""
    summary = {
        "catalog": _CATALOG,
        "row_counts": {"databases": 1, "schemas": 1, "tables": 2, "columns": 3},
    }
    nodes = [n for n in _graph_nodes() if n[0] != "Table"][:1] + [
        ("Table", generate_id(_CATALOG, _SCHEMA, "orders"))
    ]
    violations = graph_mod._check_node_counts(_Driver(nodes), summary)
    codes = {v.code for v in violations}
    assert "graph.node_count_mismatch.Table" in codes


def test_references_edge_count_scoped_with_hyphenated_catalog():
    summary = {
        "catalog": _CATALOG,
        "row_counts": {"fk_edges": 1, "fk_inferred_metadata_accepted": 0,
                       "fk_inferred_semantic_accepted": 0},
    }
    src_col = generate_id(_CATALOG, _SCHEMA, "orders", "customer_id")
    driver = _Driver(nodes=(), rels=[("REFERENCES", src_col)])
    assert references_mod._check_edge_count(driver, summary) == []


def test_value_count_scoped_with_hyphenated_catalog():
    summary = {"catalog": _CATALOG, "row_counts": {"value_nodes": 2}}
    col_id = generate_id(_CATALOG, _SCHEMA, "orders", "status")
    value_nodes = [
        ("Value", generate_value_id(col_id, "open")),
        ("Value", generate_value_id(col_id, "closed")),
    ]
    assert values_mod._check_value_count(_Driver(value_nodes), summary) == []


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))

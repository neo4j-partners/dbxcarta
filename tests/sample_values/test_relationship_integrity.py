"""Assert HAS_VALUE structural integrity: no orphan Values, at most LIMIT per column."""

import os

from neo4j import Driver


def test_no_orphan_value_nodes(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        orphans = s.run(
            "MATCH (v:Value) WHERE NOT ( (:Column)-[:HAS_VALUE]->(v) ) "
            "RETURN count(v) AS cnt"
        ).single()["cnt"]
    assert orphans == 0, f"{orphans} Value nodes have no incoming HAS_VALUE from a Column"


def test_each_value_has_exactly_one_incoming_has_value(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as s:
        bad = s.run(
            "MATCH (v:Value) "
            "WITH v, size([ (c:Column)-[:HAS_VALUE]->(v) | c ]) AS in_cnt "
            "WHERE in_cnt <> 1 "
            "RETURN count(v) AS cnt"
        ).single()["cnt"]
    assert bad == 0, f"{bad} Value nodes have incoming HAS_VALUE count != 1"


def test_column_outdegree_bounded_by_limit(neo4j_driver: Driver) -> None:
    limit = int(os.environ.get("DBXCARTA_SAMPLE_LIMIT", "10"))
    with neo4j_driver.session() as s:
        over = s.run(
            "MATCH (c:Column)-[r:HAS_VALUE]->() "
            "WITH c, count(r) AS d WHERE d > $limit "
            "RETURN count(c) AS cnt",
            limit=limit,
        ).single()["cnt"]
    assert over == 0, f"{over} Column nodes exceed {limit} HAS_VALUE edges"

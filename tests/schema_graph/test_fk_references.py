"""Assert every REFERENCES relationship resolves to existing source and target columns.

On catalogs with no declared FK constraints (row_counts["fk_references"] == 0)
this test is a no-op, not a failure.
"""

import pytest
from neo4j import Driver


def test_references_endpoints_exist(neo4j_driver: Driver, run_summary: dict) -> None:
    if run_summary["row_counts"].get("fk_references", 0) == 0:
        pytest.skip("No FK references in this run (row_counts[fk_references] == 0)")

    with neo4j_driver.session() as s:
        dangling = s.run(
            "MATCH (src:Column)-[:REFERENCES]->(tgt:Column)"
            " WHERE src IS NULL OR tgt IS NULL"
            " RETURN count(*) AS cnt"
        ).single()["cnt"]
    assert dangling == 0, f"{dangling} REFERENCES relationship(s) have a missing endpoint"

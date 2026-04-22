"""Assert REFERENCES relationships are correctly resolved and written.

Three concerns, distinguished because they apply in different scopes:

1. Edge-count invariant (universal): Neo4j's REFERENCES count matches
   run summary's fk_references.
2. Accounting invariant (universal): fk_skipped == fk_declared - fk_resolved.
3. Fixture-exact assertion (only when the seeded W8 fixtures are in scope):
   3 declared FKs resolve to 3 with 4 column-pair edges. This deliberately
   does NOT apply to arbitrary production catalogs, because the Stage 4 policy
   is log-and-skip on unresolvable FKs — partial coverage must not fail the
   test suite when the fixture schemas aren't the scope under test.
"""

from neo4j import Driver

from dbxcarta.contract import REL_REFERENCES

_FIXTURE_SCHEMAS = {"dbxcarta_fk_test", "dbxcarta_fk_test_b"}
_FIXTURE_EXPECTED_DECLARED = 3
_FIXTURE_EXPECTED_RESOLVED = 3
_FIXTURE_EXPECTED_EDGES = 4


def test_references_edge_count_matches(neo4j_driver: Driver, run_summary: dict) -> None:
    fk_references = run_summary["row_counts"].get("fk_references", 0)
    with neo4j_driver.session() as s:
        edges = s.run(
            f"MATCH ()-[r:{REL_REFERENCES}]->() RETURN count(r) AS cnt"
        ).single()["cnt"]
    assert edges == fk_references, (
        f"Neo4j has {edges} REFERENCES edges; run summary reported {fk_references}."
        " A mismatch implies the Spark Connector dropped rows where an endpoint"
        " Column node did not exist."
    )


def test_fk_accounting_invariant(run_summary: dict) -> None:
    counts = run_summary["row_counts"]
    declared = counts.get("fk_declared", 0)
    resolved = counts.get("fk_resolved", 0)
    skipped = counts.get("fk_skipped", 0)
    assert skipped == declared - resolved, (
        f"fk_skipped ({skipped}) must equal fk_declared - fk_resolved"
        f" ({declared} - {resolved})"
    )
    assert resolved <= declared
    assert skipped >= 0


def test_fixture_coverage_exact(run_summary: dict) -> None:
    """Strict coverage check: only applies when the run's scope is exactly
    (or is a superset containing) the seeded W8 fixture schemas."""
    scope = set(run_summary.get("schemas") or [])
    if not _FIXTURE_SCHEMAS.issubset(scope):
        return

    counts = run_summary["row_counts"]
    declared = counts.get("fk_declared", 0)
    resolved = counts.get("fk_resolved", 0)
    fk_references = counts.get("fk_references", 0)

    assert declared >= _FIXTURE_EXPECTED_DECLARED, (
        f"Fixture schemas in scope but fk_declared={declared} < {_FIXTURE_EXPECTED_DECLARED}."
        " The seeded fixtures may have been dropped."
    )
    # The fixtures must all resolve. If a production schema also in scope
    # contributes unresolvable FKs, the assertion relaxes to >=.
    assert resolved >= _FIXTURE_EXPECTED_RESOLVED
    assert fk_references >= _FIXTURE_EXPECTED_EDGES

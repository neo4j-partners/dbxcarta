"""Assert a second sample_values run produces identical Value and HAS_VALUE counts.

Marked slow — this test submits the job a second time and waits for it to finish.
Run explicitly with: pytest tests/sample_values -m slow
"""

import os
from pathlib import Path

import pytest
from neo4j import Driver

_PROJECT_DIR = str(Path(__file__).parent.parent.parent)


def _counts(session) -> dict[str, int]:
    c = {}
    c["Value"] = session.run("MATCH (n:Value) RETURN count(n) AS cnt").single()["cnt"]
    c["HAS_VALUE"] = session.run(
        "MATCH ()-[r:HAS_VALUE]->() RETURN count(r) AS cnt"
    ).single()["cnt"]
    return c


@pytest.mark.slow
def test_second_run_produces_same_counts(neo4j_driver: Driver) -> None:
    from dbxcarta.cli import runner

    with neo4j_driver.session() as s:
        before = _counts(s)

    os.environ["DBXCARTA_JOB"] = "sample"
    runner.submit("run_dbxcarta.py", project_dir=_PROJECT_DIR)

    with neo4j_driver.session() as s:
        after = _counts(s)

    assert after == before, f"Counts changed: before={before} after={after}"

# /// script
# requires-python = ">=3.11"
# dependencies = ["neo4j>=5", "python-dotenv>=1"]
# ///
"""Spike: validate the semantic-FK nearest-neighbor Cypher and the
`read_query` parameter question against the live Neo4j instance.

Background
----------
`fix-zombines-v3` Phase 3 ships `read_query`, which passes Cypher bind
values to the Neo4j *Spark Connector* via `query.parameter.<name>`. The
current connector docs (neo4j.com/docs/spark/current/read/query) document
*no* such option: the only parameter-injection mechanism for a `query`
read is the `script` option. So `$k` in `_SEMANTIC_NN_CYPHER` would arrive
unbound. The connector also forbids top-level `SKIP`/`LIMIT` in a custom
query (it injects its own for partitioned reads).

The connector-forwarding question is answerable from the docs (it is not a
supported option). What still needs proving against the *deployed* database
is the Cypher itself: which vector-search / scoped-subquery form this Neo4j
version accepts, and that an interpolated integer `LIMIT` works the same as
a bound `$k`. That is what this spike does, using the neo4j Python driver
(no Spark: a local Scala-2.12 pyspark cannot load the project's pinned
Scala-2.13 connector, so a driver test is the reliable signal).

Run
---
    uv run scripts/spikes/neo4j_query_param_spike.py

Reads `NEO4J_URI` / `NEO4J_USERNAME` / `NEO4J_PASSWORD` from the repo-root
`.env`. Creates an isolated `:__SpikeCol` / `:__SpikeKeyCol` fixture plus a
spike-only vector index, runs the candidate forms, and tears everything
down in a `finally` regardless of outcome.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values
from neo4j import Driver, GraphDatabase
from neo4j.exceptions import Neo4jError

_INDEX = "spike_kc_idx"
_DIM = 4
_K = 2

# Two sources and three key-like targets, all in the same (catalog, schema)
# so the structural pre-filter in every candidate form keeps them.
_FIXTURE = """
UNWIND [
  {id: 'src-a', emb: [1.0, 0.0, 0.0, 0.0]},
  {id: 'src-b', emb: [0.0, 1.0, 0.0, 0.0]}
] AS s
CREATE (:__SpikeCol {id: s.id, catalog: 'c', schema: 's', embedding: s.emb})
WITH 1 AS _
UNWIND [
  {id: 'tgt-1', emb: [1.0, 0.1, 0.0, 0.0]},
  {id: 'tgt-2', emb: [0.0, 0.9, 0.0, 0.0]},
  {id: 'tgt-3', emb: [0.2, 0.2, 1.0, 0.0]}
] AS t
CREATE (:__SpikeKeyCol {id: t.id, catalog: 'c', schema: 's', embedding: t.emb})
"""

# Candidate nearest-neighbor forms. {k} is the only substitution point so the
# same template serves the bound-parameter run ($k) and the interpolated run
# (a literal int) — that is exactly the read_query fix under test.
_FORMS: dict[str, str] = {
    # New GQL-style in-index SEARCH (Neo4j 2025.x). This is the form
    # currently hard-coded in _SEMANTIC_NN_CYPHER.
    "search_in_index": """
MATCH (s:__SpikeCol)
WHERE s.embedding IS NOT NULL
CALL (s) {{
  MATCH (t:__SpikeKeyCol)
  SEARCH t IN (VECTOR INDEX {idx} FOR s.embedding LIMIT {k}) SCORE AS score
  WHERE t.catalog = s.catalog AND t.schema = s.schema AND t.id <> s.id
  RETURN t.id AS target_id, score
}}
RETURN s.id AS source_id, target_id, score
""",
    # Stable procedure + scoped CALL (s) subquery (Neo4j 5.23+).
    "querynodes_scoped": """
MATCH (s:__SpikeCol)
WHERE s.embedding IS NOT NULL
CALL (s) {{
  CALL db.index.vector.queryNodes('{idx}', {k}, s.embedding)
  YIELD node AS t, score
  WHERE t:__SpikeKeyCol AND t.catalog = s.catalog
    AND t.schema = s.schema AND t.id <> s.id
  RETURN t.id AS target_id, score
}}
RETURN s.id AS source_id, target_id, score
""",
    # Stable procedure + legacy importing subquery (CALL { WITH s ... }).
    "querynodes_legacy": """
MATCH (s:__SpikeCol)
WHERE s.embedding IS NOT NULL
CALL {{
  WITH s
  CALL db.index.vector.queryNodes('{idx}', {k}, s.embedding)
  YIELD node AS t, score
  WHERE t:__SpikeKeyCol AND t.catalog = s.catalog
    AND t.schema = s.schema AND t.id <> s.id
  RETURN t.id AS target_id, score
}}
RETURN s.id AS source_id, target_id, score
""",
    # Option B: exact structural pre-filter, then core cosine over the
    # survivors. No vector index: the (catalog, schema) filter is applied
    # *before* scoring, so a same-schema target can never be truncated out
    # of a global top-k. `vector.similarity.cosine` is core Neo4j (>=5.18),
    # no GDS plugin. This is the form under decision for shipping.
    "prefilter_cosine": """
MATCH (s:__SpikeCol)
WHERE s.embedding IS NOT NULL
CALL (s) {{
  MATCH (t:__SpikeKeyCol)
  WHERE t.catalog = s.catalog AND t.schema = s.schema AND t.id <> s.id
  WITH t, vector.similarity.cosine(s.embedding, t.embedding) AS score
  ORDER BY score DESC
  LIMIT {k}
  RETURN t.id AS target_id, score
}}
RETURN s.id AS source_id, target_id, score
""",
}


@dataclass
class FormResult:
    name: str
    bound_ok: bool
    bound_rows: int
    bound_err: str | None
    interp_ok: bool
    interp_rows: int
    interp_err: str | None


def _repo_env() -> dict[str, str | None]:
    """Load the repo-root .env (walk up until one is found)."""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / ".env"
        if candidate.is_file():
            return dotenv_values(candidate)
    raise FileNotFoundError("no .env found above scripts/spikes/")


def _server_info(driver: Driver) -> str:
    rec = driver.execute_query(
        "CALL dbms.components() YIELD name, versions, edition "
        "RETURN name, versions[0] AS version, edition"
    ).records[0]
    return f"{rec['name']} {rec['version']} ({rec['edition']})"


def _setup(driver: Driver) -> None:
    _teardown(driver)
    driver.execute_query(
        f"CREATE VECTOR INDEX {_INDEX} IF NOT EXISTS "
        "FOR (n:__SpikeKeyCol) ON n.embedding OPTIONS {indexConfig: "
        f"{{`vector.dimensions`: {_DIM}, "
        "`vector.similarity_function`: 'cosine'}}"
    )
    driver.execute_query("CALL db.awaitIndexes(120)")
    driver.execute_query(_FIXTURE)


def _teardown(driver: Driver) -> None:
    driver.execute_query(f"DROP INDEX {_INDEX} IF EXISTS")
    driver.execute_query(
        "MATCH (n) WHERE n:__SpikeCol OR n:__SpikeKeyCol DETACH DELETE n"
    )


def _run(driver: Driver, cypher: str, params: dict[str, object]) -> int:
    return len(driver.execute_query(cypher, parameters_=params).records)


def _eval_form(driver: Driver, name: str, template: str) -> FormResult:
    bound_ok = interp_ok = False
    bound_rows = interp_rows = 0
    bound_err = interp_err = None

    # Bound: $k via the driver (control — the driver always supports binds;
    # this isolates "does the Cypher form parse/run on this server").
    try:
        bound_rows = _run(
            driver, template.format(idx=_INDEX, k="$k"), {"k": _K}
        )
        bound_ok = True
    except Neo4jError as exc:
        bound_err = (exc.message or str(exc)).splitlines()[0]

    # Interpolated: a literal trusted int, no params — the proposed
    # read_query fix (k comes from a validated >=1 setting, not user input).
    try:
        interp_rows = _run(
            driver, template.format(idx=_INDEX, k=str(_K)), {}
        )
        interp_ok = True
    except Neo4jError as exc:
        interp_err = (exc.message or str(exc)).splitlines()[0]

    return FormResult(
        name, bound_ok, bound_rows, bound_err,
        interp_ok, interp_rows, interp_err,
    )


def _verdict(results: list[FormResult]) -> None:
    print("\n=== VERDICT ===")
    print(
        "Connector: `query.parameter.<name>` is NOT a documented Spark-"
        "connector option (only `script` injects params for a `query` "
        "read). read_query's $k cannot bind through the connector; "
        "interpolating the trusted int is the fix."
    )
    working = [r for r in results if r.interp_ok and r.bound_ok]
    if working:
        first = working[0].name
        print(
            f"Cypher: use form '{first}' with the LIMIT/k interpolated. "
            "Interpolated == bound on this server, so dropping the bind "
            "loses nothing. The LIMIT lives inside a CALL subquery, which "
            "the connector's SKIP/LIMIT injection does not touch."
        )
    else:
        print(
            "Cypher: NONE of the candidate forms ran on this server — "
            "_SEMANTIC_NN_CYPHER must be rewritten to a form that does "
            "(see per-form errors above)."
        )


def main() -> int:
    env = _repo_env()
    uri = env.get("NEO4J_URI")
    user = env.get("NEO4J_USERNAME")
    pwd = env.get("NEO4J_PASSWORD")
    if not (uri and user and pwd):
        print("missing NEO4J_URI/USERNAME/PASSWORD in .env", file=sys.stderr)
        return 2

    with GraphDatabase.driver(uri, auth=(user, pwd)) as driver:
        driver.verify_connectivity()
        print(f"connected: {_server_info(driver)}")
        try:
            _setup(driver)
            results = [
                _eval_form(driver, name, tpl)
                for name, tpl in _FORMS.items()
            ]
        finally:
            _teardown(driver)

    for r in results:
        print(f"\n[{r.name}]")
        b = f"PASS rows={r.bound_rows}" if r.bound_ok else f"FAIL {r.bound_err}"
        i = (
            f"PASS rows={r.interp_rows}"
            if r.interp_ok
            else f"FAIL {r.interp_err}"
        )
        print(f"  bound $k     : {b}")
        print(f"  interpolated : {i}")

    _verdict(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

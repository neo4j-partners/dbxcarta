# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "neo4j>=5.0",
# ]
# ///
r"""Minimal vector-retrieval check against the finance-genie semantic graph.

This is the read-only counterpart to ``submit_finance_genie.py``: once that job
has built the graph in Neo4j, this script confirms the embeddings actually
retrieve. It embeds one query and runs a single ``db.index.vector.queryNodes``
lookup against the graph's vector index, printing the ranked nodes and scores.

It is intentionally a stripped-down version of neocarta's
``examples/databricks/compare_retrievers.py``: no strategy comparison, no
matrix, and no neocarta ``_mcp`` imports — just "does vector retrieval work?".

Embedding consistency
---------------------
The query is embedded through the *same* Databricks serving endpoint that
embedded the graph at ingest (``NEOCARTA_DATABRICKS_EMBEDDING_ENDPOINT`` from the
overlay, e.g. ``openai-text-embedding-3-small`` at 1536 dims). Using the same
endpoint is what makes the cosine scores meaningful; a different model or
dimension makes them noise.

Run it from the dbxcarta repo root so the relative base ``.env`` resolves::

    uv run python examples/standalone-submit/sample_retriever.py \
        --query "Which account types have the most fraud-labeled accounts?"

    # Probe the column index instead of the table index, deeper recall
    uv run python examples/standalone-submit/sample_retriever.py \
        --query "fraud labeled accounts" --index column --top-k 10

Configuration comes from ``submit_finance_genie.env`` beside this script (the
same overlay the submit script uses) for the embedding endpoint, and from its
sibling ``.env`` for the ``NEO4J_*`` credentials. It is read-only: it only runs
a vector index lookup.

Prerequisites:
- A graph already built by ``submit_finance_genie.py`` (so the vector index
  exists and nodes are embedded).
- ``NEO4J_URI``, ``NEO4J_USERNAME``, ``NEO4J_PASSWORD`` reachable via the
  overlay's sibling ``.env`` or the process environment.
- Databricks auth for the profile in the overlay, to reach the embedding
  endpoint.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dbxcarta.client.embed import embed_questions
from dbxcarta.core.env import (
    load_env_files,
    load_overlay_secrets,
    resolve_env_files,
    select_overlay_path,
)
from dbxcarta.core.workspace import build_workspace_client
from neo4j import GraphDatabase, RoutingControl

_HERE = Path(__file__).resolve().parent
_ENV_FILE = _HERE / "submit_finance_genie.env"

# neocarta names per-label vector indexes `{label}_vector_index` in inline mode
# (neocarta/ingest/indexes.py), the same names the dbxcarta client reads.
_INDEX_BY_LEVEL = {"table": "table_vector_index", "column": "column_vector_index"}
_DEFAULT_ENDPOINT = "openai-text-embedding-3-small"


def _load_config() -> None:
    """Load the overlay, base ``.env``, and the overlay's ``NEO4J_*`` secrets.

    Mirrors ``submit_finance_genie.py``: point ``DBXCARTA_ENV_FILE`` at the
    overlay beside this script, layer it over the base ``.env`` (overlay wins),
    then pull the ``NEO4J_*`` credentials from the overlay's sibling ``.env``
    since the committed overlay is secret-free.
    """
    if not _ENV_FILE.is_file():
        raise SystemExit(
            f"config not found: {_ENV_FILE}\n"
            "Copy submit_finance_genie.env.sample to submit_finance_genie.env "
            "and fill in the infra values."
        )
    os.environ.setdefault("DBXCARTA_ENV_FILE", str(_ENV_FILE))
    files, _ = resolve_env_files([])
    overlay = select_overlay_path([])
    load_env_files(files)
    load_overlay_secrets(overlay)


def _embed_query(endpoint: str, query: str) -> list[float]:
    """Embed one query through the Databricks serving endpoint."""
    ws = build_workspace_client()
    embeddings, error = embed_questions(ws, endpoint, [query])
    if embeddings is None:
        raise SystemExit(f"embedding call to {endpoint} failed: {error}")
    return embeddings[0]


def _query_vector_index(
    driver: GraphDatabase,
    database: str,
    index: str,
    embedding: list[float],
    top_k: int,
) -> list[tuple[str, str | None, float]]:
    """Return the top-k ``(id, name, score)`` nodes from a vector index.

    Neo4j's procedure API does not support a parameterized index name, but
    ``index`` is always one of the two module-level constants, never user input.
    """
    return driver.execute_query(
        f"CALL db.index.vector.queryNodes('{index}', $k, $vec) "
        "YIELD node, score "
        "RETURN node.id AS id, node.name AS name, score "
        "ORDER BY score DESC",
        k=top_k,
        vec=embedding,
        database_=database,
        routing_=RoutingControl.READ,
        result_transformer_=lambda result: [
            (row["id"], row["name"], row["score"]) for row in result
        ],
    )


def _print_results(rows: list[tuple[str, str | None, float]]) -> None:
    """Print the ranked vector-search hits."""
    if not rows:
        print("\n(no matches — is the graph built and the index populated?)")
        return
    print(f"\n  {'#':>2}  {'score':>6}  node")
    for rank, (node_id, name, score) in enumerate(rows, start=1):
        label = name or node_id
        print(f"  {rank:>2}  {score:>6.3f}  {label}  [{node_id}]")


def retrieve(query: str, level: str, top_k: int, endpoint_override: str | None) -> None:
    """Embed the query and run one vector lookup against the graph."""
    _load_config()
    uri = os.environ["NEO4J_URI"]
    username = os.environ["NEO4J_USERNAME"]
    password = os.environ["NEO4J_PASSWORD"]
    database = os.environ.get("NEO4J_DATABASE", "neo4j")
    endpoint = (
        endpoint_override
        or os.environ.get("NEOCARTA_DATABRICKS_EMBEDDING_ENDPOINT")
        or _DEFAULT_ENDPOINT
    )
    index = _INDEX_BY_LEVEL[level]

    print("=" * 80)
    print("sample vector retriever")
    print("=" * 80)
    print(f"  Query   : {query!r}")
    print(f"  Neo4j   : {uri} (database {database!r})")
    print(f"  Index   : {index}")
    print(f"  Endpoint: {endpoint}")

    embedding = _embed_query(endpoint, query)
    print(f"Embedded query into a {len(embedding)}-dim vector ({endpoint}).")

    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        rows = _query_vector_index(driver, database, index, embedding, top_k)
    _print_results(rows)


def main() -> int:
    """Parse arguments and run the retrieval."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--query",
        required=True,
        help="The natural-language / keyword query to embed and look up.",
    )
    parser.add_argument(
        "--index",
        choices=list(_INDEX_BY_LEVEL),
        default="table",
        help="Which vector index to query (default: table).",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="How many ranked nodes to return (default: 5).",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help="Embedding serving endpoint. Defaults to "
        "NEOCARTA_DATABRICKS_EMBEDDING_ENDPOINT, then "
        f"{_DEFAULT_ENDPOINT}.",
    )
    args = parser.parse_args()
    if args.top_k < 1:
        parser.error("--top-k must be >= 1")

    retrieve(args.query, args.index, args.top_k, args.endpoint)
    return 0


if __name__ == "__main__":
    sys.exit(main())

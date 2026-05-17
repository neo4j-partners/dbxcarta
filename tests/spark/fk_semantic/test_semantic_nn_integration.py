"""Opt-in integration test for the real Neo4j NN `query`-option path.

Skipped by default — it requires a reachable Neo4j with the
`keycolumn_embedding` vector index populated and a Spark session carrying
the Neo4j Spark Connector jar. Run it in Phase 6 (the real dense run) by
setting `DBXCARTA_NEO4J_IT=1` plus `NEO4J_URI` / `NEO4J_USERNAME` /
`NEO4J_PASSWORD`. This is the only test that exercises
`semantic_nn_pairs` end to end against a live index and verifies the locked
`_SEMANTIC_NN_CYPHER` syntax against the deployed Neo4j version; the fast
unit suite uses the injected `(source_id, target_id, score)` seam instead.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DBXCARTA_NEO4J_IT") != "1",
    reason="opt-in: set DBXCARTA_NEO4J_IT=1 with NEO4J_* to run (Phase 6)",
)


def test_semantic_nn_pairs_against_live_index() -> None:
    from pyspark.sql import SparkSession

    from dbxcarta.spark.ingest.fk.inference import semantic_nn_pairs
    from dbxcarta.spark.ingest.load.writer import Neo4jConfig
    from dbxcarta.spark.settings import SparkIngestSettings

    spark = SparkSession.builder.getOrCreate()
    neo4j = Neo4jConfig(
        uri=os.environ["NEO4J_URI"],
        username=os.environ["NEO4J_USERNAME"],
        password=os.environ["NEO4J_PASSWORD"],
    )
    settings = SparkIngestSettings(
        dbxcarta_catalog="main",
        dbxcarta_summary_volume="/Volumes/c/s/v/dbxcarta",
        dbxcarta_summary_table="c.s.t",
    )

    nn = semantic_nn_pairs(
        spark, neo4j, columns_frame=None, settings=settings,
        k=settings.dbxcarta_semantic_k,
    )

    # The contract is the column set, not a row count: an empty graph
    # legitimately returns zero rows. Schema proves the locked Cypher's
    # RETURN shape round-trips through the connector `query` read.
    assert set(nn.columns) == {"source_id", "target_id", "score"}
    nn.limit(1).collect()

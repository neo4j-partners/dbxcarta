"""Stage 0 spike — validate ai_query against databricks-bge-large-en.

Confirms:
  1. Output column type is array<double> (compatible with Neo4j Spark
     Connector's float-array write path).
  2. Vector dimension matches the 1024 expected by bge-large-en.
  3. failOnError => false returns null for deliberately invalid input
     instead of aborting the query.
  4. The cluster's service principal has invoke permission on the
     serving endpoint (the spike call itself is the probe).

Submit:
    dbxcarta submit scripts/run_spike_ai_query.py
"""
from _cluster_bootstrap import inject_params

inject_params()

import os

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import expr

ENDPOINT = os.environ.get("DBXCARTA_EMBEDDING_ENDPOINT", "databricks-bge-large-en")


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    # Probes: normal identifier, normal sentence, empty string, oversized
    # text (exceeds bge-large-en's 512-token window), and a NULL input to
    # observe whether null passes through as null or surfaces as a struct
    # with a populated errorMessage.
    oversized = "x " * 20000
    probes = [
        Row(label="identifier", text="sales.orders"),
        Row(label="sentence", text="Customer order line items with status and fulfillment timestamps"),
        Row(label="empty", text=""),
        Row(label="oversized", text=oversized),
        Row(label="null_input", text=None),
    ]
    df = spark.createDataFrame(probes)

    result = df.withColumn(
        "embedding",
        expr(f"ai_query('{ENDPOINT}', text, failOnError => false)"),
    )

    print(f"[spike] endpoint={ENDPOINT}")
    print("[spike] output schema:")
    result.printSchema()

    rows = result.collect()
    for r in rows:
        emb = r["embedding"]
        if emb is None:
            print(f"[spike] label={r['label']!r} -> embedding=None (whole struct null)")
            continue
        vec = emb["result"]
        err = emb["errorMessage"]
        if vec is None:
            print(f"[spike] label={r['label']!r} result=None errorMessage={err!r}")
        else:
            print(
                f"[spike] label={r['label']!r} dim={len(vec)} "
                f"type={type(vec[0]).__name__} errorMessage={err!r}"
            )

    dims = {
        len(r["embedding"]["result"])
        for r in rows
        if r["embedding"] is not None and r["embedding"]["result"] is not None
    }
    failures = [
        r["label"]
        for r in rows
        if r["embedding"] is None
        or r["embedding"]["result"] is None
        or r["embedding"]["errorMessage"] is not None
    ]
    print(f"[spike] observed dims: {dims}")
    print(f"[spike] failure labels: {failures}")


if __name__ == "__main__":
    main()

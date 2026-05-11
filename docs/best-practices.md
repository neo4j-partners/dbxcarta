# DBxCarta Best Practices

Design rules the pipeline follows, with sources. Organized by the two external systems it talks to (Spark/Databricks, Neo4j) and a short section of project-level principles that cut across both.

This is a living reference. When a rule is added or changed, cite the source.

## Spark / Databricks

### 1. Submit the full dataset to `ai_query` in one query

`ai_query` is built for batch inference. Databricks handles parallelization, retries, and scaling internally. Manually splitting the DataFrame into small batches reduces throughput rather than improving it.

**How we apply it:** the pipeline enriches the whole node DataFrame with a single `ai_query` column expression. No custom batching loop.

Source: [Use `ai_query` — Best practices](https://docs.databricks.com/aws/en/large-language-models/ai-query#best-practices) (retrieved 2026-04-21).

### 2. Use `failOnError => false` for production batches

With `failOnError => false`, a per-row failure returns a struct with `result = NULL` and a populated `errorMessage`. The query itself completes. The alternative aborts the entire job on the first bad input, which is not acceptable for a metadata-enrichment pipeline where one malformed row must not sink the run.

**How we apply it:** the transform projects `.result` as `embedding` and counts nulls against a configurable per-label and aggregate failure threshold. A returned vector whose length mismatches `DBXCARTA_EMBEDDING_DIMENSION` is normalized to null and counted as a row-level failure (no separate abort path).

Source: [Use `ai_query` — Best practices](https://docs.databricks.com/aws/en/large-language-models/ai-query#best-practices).

### 3. Prefer batch-optimized Databricks-hosted embedding models

Databricks lists a specific set of batch-optimized models for production inference. Non-listed models still work but give lower throughput. For embeddings (1024-dim), the batch-optimized options are `databricks-gte-large-en` and `databricks-qwen3-embedding-0-6b`. `databricks-bge-large-en` is available but not on the batch-optimized list.

**How we apply it:** `DBXCARTA_EMBEDDING_ENDPOINT` defaults to `databricks-gte-large-en`. Swapping endpoints is a pure `.env` change because the dimension lives next to the endpoint.

Source: [Use `ai_query` — Supported models](https://docs.databricks.com/aws/en/large-language-models/ai-query#supported-models).

### 4. Materialize any DataFrame with `ai_query` before using it twice

Spark transformations are lazy. An `ai_query` column is just an expression until an action triggers evaluation. Every action against the same DataFrame evaluates the expression again, which means the endpoint is hit again, which means tokens are billed again. This is the single biggest latent cost trap in the pipeline.

Two downstream actions exist by default: the failure-rate aggregation, and the Neo4j Spark Connector write. If a ledger is added (Stage 7), that is a third.

**How we apply it:** the pipeline writes the enriched node DataFrame to a Delta staging table once, then reads it back for the failure-rate aggregation and the Neo4j write. Delta is preferred over `.cache()` / `.persist()` because inference spend dwarfs I/O and because a materialized Delta table survives executor loss without re-inferencing.

Source: derived from Spark lazy-evaluation semantics and `ai_query` billing behavior (tokens charged per invocation). No single vendor doc states this in one place; see the Spark programming guide on [RDD persistence and lazy evaluation](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence) as background.

### 5. Accumulate per-table DataFrames in Spark; never collect value rows to the driver

When building the Value node DataFrame from sampled values, each `spark.sql(sample_query)` call already returns a Spark DataFrame. Collecting these per-table results to the driver and rebuilding a new DataFrame via `spark.createDataFrame(rows)` scales driver memory with total sample size across the entire catalog. At 10 values × 10,000 STRING columns, that is 100,000 Python objects in the driver before any Node write begins.

Instead, accumulate per-table DataFrames with `unionByName` and apply the per-column top-N limit using a `row_number()` window function. The value ID (`generate_value_id` equivalent) is applied as a Spark `concat(..., md5(col))` expression, and `dropDuplicates(["id"])` is added as a defensive dedup step before returning.

**How we apply it:** `_sample_values` in `sample_values.py` returns a Spark DataFrame (or None when all queries fail). The `sample()` function constructs `value_node_df` and `has_value_df` from that DataFrame using Spark expressions. Two `.count()` actions are triggered to populate run-summary stats; the counts are the only driver-bound results.

Source: derived from Spark lazy-evaluation semantics and the driver memory constraint for large catalogs.

### 6. Cache metadata DataFrames once, read them many times

`columns_df` is referenced by the sample-values transform (for candidate discovery) and by the column-node builder (for Neo4j writes) and would be read twice against `information_schema.columns` without caching. `information_schema` reads are cheap, but DAG stability and predictability are not.

**How we apply it:** `columns_df` is `.cache()`d in the extract stage. Sample-values candidate discovery is a pure in-memory filter over this cached DataFrame rather than a fresh UC read.

## Neo4j Spark Connector

### 1. `coalesce(1)` for relationships, `repartition(N, id)` for nodes

Neo4j write lock contention is the concern the connector docs warn about. Relationships lock both endpoint nodes, and any two Spark partitions writing relationships can contend on a shared endpoint node, so the connector's guidance to serialize writes applies straightforwardly. Nodes are different: uniqueness constraints lock per node id, so partitions that are disjoint on id cannot contend. `repartition(N, "id")` produces exactly that — hash-partitioning on a uniqueness-constrained property guarantees each node lands in a single partition, and parallel writes across partitions are safe by construction.

**How we apply it:**

- **Relationship writes** — every relationship DataFrame is `.coalesce(1)` before the Neo4j write. No exceptions; relationships always lock two nodes.
- **Node writes** — every node DataFrame is `.repartition(N, "id")` before the Neo4j write, where `N` comes from `DBXCARTA_NEO4J_NODE_PARTITIONS` (default `4`). This is a standing rule, not a benchmark-gated optimization; no measurement is required to prove it's safe because the partitioning guarantees disjoint lock keys.

Tuning `N` is a throughput question, not a correctness question.

Source: [Neo4j Spark Connector — Parameter tuning, Write parallelism](https://neo4j.com/docs/spark/current/performance/tuning/#parallelism) (retrieved 2026-04-22). The "Dataset partitioning" subsection on that page sanctions the exception directly: "if your data writes are partitioned ahead of time to avoid locks, you can generally do as many write threads to Neo4j as there are cores in the server. Suppose we want to write a long list of `:Person` nodes, and we know they are distinct by the person `id`. We might stream those into Neo4j in four different partitions, as there will not be any lock contention." Our default `N=4` matches the example.

### 2. Tune `batch.size` above the default

The connector's default `batch.size` is deliberately conservative and is too low for most production workloads. The Neo4j docs suggest starting around 20,000 and raising it toward the server's heap capacity. Larger batches mean fewer transactions and lower per-write overhead; too-large batches hit heap limits and fail.

**How we apply it:** `DBXCARTA_NEO4J_BATCH_SIZE` defaults to `20000`. The setting is exposed in `.env` so it can be tuned without a code change as catalog size and node property sizes grow.

Source: [Neo4j Spark Connector — Tune the batch size](https://neo4j.com/docs/spark/current/performance/tuning/#batch-size) (retrieved 2026-04-21).

### 3. Remember that Aura writes are leader-only and scale vertically

On AuraDB and any causal cluster, only the leader accepts writes. Write throughput scales with the cores on the leader, not with the number of Spark executors. This bounds the point at which adding more Spark parallelism stops helping.

**How we apply it:** the pipeline treats Spark-side parallelism as a second-order tuning knob, not a first-order solution. Node writes use a modest default (`DBXCARTA_NEO4J_NODE_PARTITIONS=4`, see §1); raise it incrementally against measured throughput once write volume justifies it.

Source: [Neo4j Spark Connector — Parameter tuning, Write parallelism](https://neo4j.com/docs/spark/current/performance/tuning/#parallelism) (retrieved 2026-04-21).

### 4. Create vector indexes before writing embedded nodes

Vector indexes populate asynchronously for existing data, but creating the index first lets the connector populate it as rows land rather than requiring a separate reindex pass. Dimension and similarity function are fixed at index creation and cannot be altered without dropping the index.

**How we apply it:** the bootstrap step at the top of the LOAD stage reads `DBXCARTA_EMBEDDING_DIMENSION` and creates a cosine-similarity vector index per enabled label before any node write.

Source: [Neo4j vector index](https://neo4j.com/docs/cypher-manual/current/indexes/semantic-indexes/vector-indexes/) (retrieved 2026-04-21).

## Project-level principles

These are not vendor best practices; they are consequences of the two vendor rules above plus the project's correctness requirements.

### 1. Unity Catalog is the only source of truth

The pipeline reads UC, writes Neo4j, and does not read Neo4j at any point during a run. This keeps the sink decoupled from pipeline state, makes partial-graph recovery a non-issue (reruns are idempotent MERGEs), and makes candidate discovery a pure function of UC.

### 2. Pipeline-owned artifacts memo, they do not authorize

Any caching layer introduced for cost reasons (e.g. the Stage 7 Delta embedding ledger) is a memo of "what we've already spent tokens on," never authority. Dropping the ledger must produce a correct run on the next invocation; at worst, one run re-embeds everything.

### 3. Fail fast on missing permissions; never degrade silently

The preflight enumerates every grant and endpoint permission the enabled flags require, and aborts before any read or write if any are missing. There is no partial-graph mode.

### 4. Embedding failures are counted, not thrown

`ai_query` with `failOnError => false` returns nulls rather than exceptions. The pipeline counts attempts and successes per label, computes a failure rate both per-label and in aggregate, and fails the run only if either exceeds the configured threshold. This catches the case where one label silently degrades while another masks it in aggregate.

### 5. Verify the cluster runs the current source before interpreting run output

`submit` is a reference operation, not a build operation. The runner attaches the wheel last uploaded to the UC Volume and executes the script last uploaded to the Databricks workspace; it does not rebuild or re-upload either on `submit`. Running stale code against real infrastructure produces misleading signal — apparent failures may be old bugs already fixed, apparent successes may hide regressions in new code, and the run summary reflects whatever the cluster actually ran, not what the local source says.

Any time source files change, run `dbxcarta upload --wheel && dbxcarta upload --all` before `dbxcarta submit`. Treat `job_name` and `contract_version` in the JSON run summary as a quick sanity check that the expected code ran; a wrong prefix (e.g. `schema_graph_*` instead of `dbxcarta_*`) means the workspace script is stale and a re-upload is required before interpreting any results.

### 6. Make Databricks targets explicit at the configuration boundary

Pipeline-owned artifacts are Unity Catalog objects, so their configuration should name the exact UC target instead of relying on workspace defaults. `DBXCARTA_SUMMARY_TABLE` is required to be `catalog.schema.table`, and writable file locations must be `/Volumes/<catalog>/<schema>/<volume>/<subdir>`, not DBFS paths or bare volume roots.

All dynamic identifiers are validated before use and then backtick-quoted at SQL construction. Serving endpoint names are validated before interpolation into `ai_query` calls. Local tooling builds `WorkspaceClient` from `DATABRICKS_PROFILE` when present, otherwise it lets the Databricks SDK use its default authentication chain.

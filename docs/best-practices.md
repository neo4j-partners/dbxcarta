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

Source: [Use `ai_query` — Best practices](https://docs.databricks.com/aws/en/large-language-models/ai-query#best-practices); `ai_query` spike note at `worklog/spike-ai-query.md`.

### 3. Prefer batch-optimized Databricks-hosted embedding models

Databricks lists a specific set of batch-optimized models for production inference. Non-listed models still work but give lower throughput. For embeddings (1024-dim), the batch-optimized options are `databricks-gte-large-en` and `databricks-qwen3-embedding-0-6b`. `databricks-bge-large-en` is available but not on the batch-optimized list.

**How we apply it:** `DBXCARTA_EMBEDDING_ENDPOINT` defaults to `databricks-gte-large-en`. Swapping endpoints is a pure `.env` change because the dimension lives next to the endpoint.

Source: [Use `ai_query` — Supported models](https://docs.databricks.com/aws/en/large-language-models/ai-query#supported-models).

### 4. Materialize any DataFrame with `ai_query` before using it twice

Spark transformations are lazy. An `ai_query` column is just an expression until an action triggers evaluation. Every action against the same DataFrame evaluates the expression again, which means the endpoint is hit again, which means tokens are billed again. This is the single biggest latent cost trap in the pipeline.

Two downstream actions exist by default: the failure-rate aggregation, and the Neo4j Spark Connector write. If a ledger is added (Stage 7), that is a third.

**How we apply it:** the pipeline writes the enriched node DataFrame to a Delta staging table once, then reads it back for the failure-rate aggregation and the Neo4j write. Delta is preferred over `.cache()` / `.persist()` because inference spend dwarfs I/O and because a materialized Delta table survives executor loss without re-inferencing.

Source: derived from Spark lazy-evaluation semantics and `ai_query` billing behavior (tokens charged per invocation). No single vendor doc states this in one place; see the Spark programming guide on [RDD persistence and lazy evaluation](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence) as background.

### 5. Cache metadata DataFrames once, read them many times

`columns_df` is referenced by the sample-values transform (for candidate discovery) and by the column-node builder (for Neo4j writes) and would be read twice against `information_schema.columns` without caching. `information_schema` reads are cheap, but DAG stability and predictability are not.

**How we apply it:** `columns_df` is `.cache()`d in the extract stage. Sample-values candidate discovery is a pure in-memory filter over this cached DataFrame rather than a fresh UC read.

## Neo4j Spark Connector

### 1. Repartition to 1 before writing relationships

Neo4j relationship writes lock both endpoint nodes. Writing relationships from multiple Spark partitions in parallel causes lock contention on shared endpoint nodes and can raise rejection errors. The connector's own documentation recommends a single partition for most writes.

**How we apply it:** every relationship DataFrame is `.coalesce(1)` before the Neo4j write. Node writes default to 1 as well for the first green run; a later phase may tune upward for labels whose identifiers are known to be disjoint across partitions.

Source: [Neo4j Spark Connector — Parameter tuning, Write parallelism](https://neo4j.com/docs/spark/current/performance/tuning/#parallelism) (retrieved 2026-04-21).

### 2. Tune `batch.size` above the default

The connector's default `batch.size` is deliberately conservative and is too low for most production workloads. The Neo4j docs suggest starting around 20,000 and raising it toward the server's heap capacity. Larger batches mean fewer transactions and lower per-write overhead; too-large batches hit heap limits and fail.

**How we apply it:** `DBXCARTA_NEO4J_BATCH_SIZE` defaults to `20000`. The setting is exposed in `.env` so it can be tuned without a code change as catalog size and node property sizes grow.

Source: [Neo4j Spark Connector — Tune the batch size](https://neo4j.com/docs/spark/current/performance/tuning/#batch-size) (retrieved 2026-04-21).

### 3. Remember that Aura writes are leader-only and scale vertically

On AuraDB and any causal cluster, only the leader accepts writes. Write throughput scales with the cores on the leader, not with the number of Spark executors. This bounds the point at which adding more Spark parallelism stops helping.

**How we apply it:** the pipeline treats Spark-side parallelism as a second-order tuning knob, not a first-order solution. Stage 7 (endpoint throughput benchmark) measures the actual ceiling before any parallelism is added beyond `coalesce(1)`.

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

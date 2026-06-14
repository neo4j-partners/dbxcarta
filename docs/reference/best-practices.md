# DBxCarta Best Practices

Design rules for the system, with sources. Organized by the two external systems the ingest pipeline talks to (Spark/Databricks, Neo4j) and a short section of project-level principles that cut across both.

The ingest pipeline itself now lives in the external [neocarta](https://github.com/neo4j-field/neocarta) connector, not in dbxcarta. The Spark/Databricks and Neo4j Spark Connector rules below, and the ingest-side project principles, are the design contract that pipeline follows; neocarta owns the implementation, so the module-level detail and the live contract are in neocarta's own documentation. They are kept here because they remain the rules dbxcarta requires of any ingest it drives, and because the operator, client, materialize, and submit rules that dbxcarta *does* own (§5–§7, §9, §10 under project principles) are stated against the same backdrop. The ingest contract reaches the connector through `NEOCARTA_DATABRICKS_*` overlay keys.

This is a living reference. When a rule is added or changed, cite the source.

## Spark / Databricks (neocarta ingest pipeline)

### 1. Submit the full dataset to `ai_query` in one query

`ai_query` is built for batch inference. Databricks handles parallelization, retries, and scaling internally. Manually splitting the DataFrame into small batches reduces throughput rather than improving it.

**How we apply it:** the neocarta ingest pipeline enriches the whole node DataFrame with a single `ai_query` column expression. No custom batching loop.

Source: [Use `ai_query` — Best practices](https://docs.databricks.com/aws/en/large-language-models/ai-query#best-practices) (retrieved 2026-04-21).

### 2. Use `failOnError => false` for production batches

With `failOnError => false`, a per-row failure returns a struct with `result = NULL` and a populated `errorMessage`. The query itself completes. The alternative aborts the entire job on the first bad input, which is not acceptable for a metadata-enrichment pipeline where one malformed row must not sink the run.

**How we apply it:** the transform projects `.result` as `embedding` and counts nulls against a configurable per-label and aggregate failure threshold (`NEOCARTA_DATABRICKS_EMBEDDING_FAILURE_MAX`). A returned vector whose length mismatches `NEOCARTA_DATABRICKS_EMBEDDING_DIMENSION` is normalized to null and counted as a row-level failure (no separate abort path).

Source: [Use `ai_query` — Best practices](https://docs.databricks.com/aws/en/large-language-models/ai-query#best-practices).

### 3. Prefer batch-optimized Databricks-hosted embedding models

Databricks lists a specific set of batch-optimized models for production inference. Non-listed models still work but give lower throughput. For embeddings (1024-dim), the batch-optimized options are `databricks-gte-large-en` and `databricks-qwen3-embedding-0-6b`. `databricks-bge-large-en` is available but not on the batch-optimized list.

**How we apply it:** `NEOCARTA_DATABRICKS_EMBEDDING_ENDPOINT` defaults to `databricks-gte-large-en`. Swapping endpoints is a pure overlay change because the dimension (`NEOCARTA_DATABRICKS_EMBEDDING_DIMENSION`) lives next to the endpoint.

Source: [Use `ai_query` — Supported models](https://docs.databricks.com/aws/en/large-language-models/ai-query#supported-models).

### 4. Materialize any DataFrame with `ai_query` before any second action reads it

Spark transformations are lazy. An `ai_query` column is just an expression until an action triggers evaluation. Every action against the same DataFrame evaluates the expression again, which means the endpoint is hit again, which means tokens are billed again. This is the single biggest latent cost trap in the pipeline.

Two downstream actions exist by default: the failure-rate aggregation, and the Neo4j Spark Connector write. The embedding-reuse ledger adds a third.

**How we apply it:** the pipeline embeds and writes nodes in per-table-range batches. Each batch materializes its enriched node DataFrame to a short-lived per-(chunk, label) Delta table, reads that back for the failure-rate aggregation and the Neo4j write, then deletes the table immediately after that batch's Neo4j write. There is no single global staging table reused across the run; the embed-once guarantee holds per batch because the `ai_query` column is evaluated once into the transient table before any second action reads it. Delta is preferred over `.cache()` / `.persist()` because inference spend dwarfs I/O and because a materialized Delta table survives executor loss without re-inferencing.

Source: derived from Spark lazy-evaluation semantics and `ai_query` billing behavior (tokens charged per invocation). No single vendor doc states this in one place; see the Spark programming guide on [RDD persistence and lazy evaluation](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence) as background.

### 5. Never collect catalog-scale data to the driver

Any path that collects rows whose count grows with catalog size — columns, constraints, embeddings, sampled values, or FK candidate pairs — scales driver memory with the catalog and becomes the run's ceiling well before the executors are saturated. At the ~10,000-table target this is hundreds of thousands of columns and an n²-shaped candidate space; a single all-pairs `collect()` or `spark.createDataFrame(rows)` on the driver cannot hold it. Catalog-scale work stays in DataFrames end to end (joins, windows, higher-order array functions); only fixed-size scalars (a `count()` for a run-summary stat) and statically-bounded sets cross to the driver.

**How we apply it:** the neocarta connector keeps sampled values, columns, constraints, the value index, and metadata FK edges as Spark DataFrames end to end — its sample-values transform returns a DataFrame rather than rebuilding one from collected rows, its constraints frame feeds the PK-like gate as a Spark aggregation, and prior-edge suppression is a `(source_id, target_id)` anti-join rather than a collected Python set. The only sanctioned driver collect is the bounded declared-FK resolve: declared foreign keys are bounded by the *catalog-declared* count, not the n²-shaped all-pairs space, so collecting them does not threaten driver memory at the 10,000-table target. That exception is specific to declared FKs and must not be generalized to the metadata path. See neocarta's documentation for the current module map.

Source: derived from Spark lazy-evaluation semantics and the driver memory constraint for large catalogs.

### 6. Cache metadata DataFrames once, read them many times

The columns DataFrame is referenced both by the sample-values transform (for candidate discovery) and by the column-node builder (for Neo4j writes) and would be read twice against `information_schema.columns` without caching. `information_schema` reads are cheap, but DAG stability and predictability are not.

**How we apply it:** the columns frame is cached once in the extract stage. Sample-values candidate discovery is a pure in-memory filter over the cached DataFrame rather than a fresh UC read.

### 7. Express rule logic as native Spark; never as a Python UDF

Catalyst optimizes only what it can see. A Python UDF, including a vectorized `pandas_udf`, is opaque to the optimizer: it blocks predicate pushdown and whole-stage codegen, and it pays Arrow JVM-to-Python serialization on every batch. Across a large or n²-shaped join, that overhead dominates the query.

Most logic that looks like it needs a UDF is driven by small static tables: a suffix list, a type-class map, a score table, a stopword set. Those expand into native `Column` expressions and broadcast-join lookups at plan-construction time. The governing rule: Python builds the plan, Spark evaluates every row. A UDF is justified only when the per-row logic genuinely cannot be expressed with Catalyst expressions or higher-order array functions, and that bar is rarely met for metadata, string, or vector-math rules.

**How we apply it:** in the neocarta connector, FK discovery is native DataFrame joins. Name-match stem and plural rules are generated as `Column` expressions by looping the static suffix list once while the plan is built; type compatibility and the score table are broadcast-join lookups; comment-token overlap uses `split`, `filter`, and `array_intersect`. There is no `pandas_udf` anywhere in the FK path.

Source: derived from Spark Catalyst optimization semantics; see the [Spark SQL performance tuning guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html) and the PySpark guidance that Python UDFs are a black box to the optimizer.

### 8. Release every cached DataFrame through a handle, including on failure paths

A cached or persisted DataFrame that survives the run leaks executor memory for the rest of the job. The risk is not the `.cache()` call itself but its lifecycle: a cache whose handle never escapes the function that created it cannot be released by the caller after the downstream actions finish, and a cache released only on the success path leaks whenever an exception unwinds past it. The rule: whatever caches a catalog-scale DataFrame must expose an unpersist handle, and the owner must release it in a `finally` so success and failure paths both release exactly once.

**How we apply it:** the neocarta connector exposes an idempotent unpersist handle on each result object that caches a catalog-scale DataFrame the caller needs across later actions (extract, the value stage, and FK discovery), and the pipeline owner releases them in a run-level `finally` after FK discovery and the load have finished reading them, each release independently guarded so one failure still attempts the others. A cache whose lifecycle is internal to a single function is released by that function's own `finally`; the n²-shaped FK-lineage frame is persisted `MEMORY_AND_DISK` so eviction at the 10k-table target cannot cause a silent recompute.

Source: derived from Spark cache lifecycle semantics and the executor-memory constraint for large catalogs; the `MEMORY_AND_DISK` choice over `cache()`/`MEMORY_ONLY` follows the same reasoning as §4 (a silent recompute re-runs expensive work, here the n²-shaped FK lineage rather than `ai_query` spend).

## Neo4j Spark Connector (neocarta ingest pipeline)

### 1. `coalesce(1)` for relationships, `repartition(N, id)` for nodes

Neo4j write lock contention is the concern the connector docs warn about. Relationships lock both endpoint nodes, and any two Spark partitions writing relationships can contend on a shared endpoint node, so the connector's guidance to serialize writes applies straightforwardly. Nodes are different: uniqueness constraints lock per node id, so partitions that are disjoint on id cannot contend. `repartition(N, "id")` produces exactly that — hash-partitioning on a uniqueness-constrained property guarantees each node lands in a single partition, and parallel writes across partitions are safe by construction.

**How we apply it:**

- **Relationship writes** — every relationship DataFrame defaults to `.coalesce(1)` before the Neo4j write, because relationships always lock both endpoint nodes. The rel-write partition count is a neocarta connector setting (default `1`, which is exactly `coalesce(1)`); a value `> 1` repartitions for parallel writes and must only be raised with production evidence that Neo4j tolerates the added endpoint-lock contention. The single-partition default is the safe baseline and the correctness reasoning above holds for it.
- **Node writes** — every node DataFrame is `.repartition(N, "id")` before the Neo4j write, where `N` is a neocarta connector setting (default `4`). This is a standing rule, not a benchmark-gated optimization; no measurement is required to prove it's safe because the partitioning guarantees disjoint lock keys.

Tuning `N` is a throughput question, not a correctness question.

Source: [Neo4j Spark Connector — Parameter tuning, Write parallelism](https://neo4j.com/docs/spark/current/performance/tuning/#parallelism) (retrieved 2026-04-22). The "Dataset partitioning" subsection on that page sanctions the exception directly: "if your data writes are partitioned ahead of time to avoid locks, you can generally do as many write threads to Neo4j as there are cores in the server. Suppose we want to write a long list of `:Person` nodes, and we know they are distinct by the person `id`. We might stream those into Neo4j in four different partitions, as there will not be any lock contention." The default `N=4` matches the example.

### 2. Tune `batch.size` above the default

The connector's default `batch.size` is deliberately conservative and is too low for most production workloads. The Neo4j docs suggest starting around 20,000 and raising it toward the server's heap capacity. Larger batches mean fewer transactions and lower per-write overhead; too-large batches hit heap limits and fail.

**How we apply it:** the neocarta connector defaults its Neo4j batch size to `20000` and exposes it as a connector setting so it can be tuned without a code change as catalog size and node property sizes grow.

Source: [Neo4j Spark Connector — Tune the batch size](https://neo4j.com/docs/spark/current/performance/tuning/#batch-size) (retrieved 2026-04-21).

### 3. Remember that Aura writes are leader-only and scale vertically

On AuraDB and any causal cluster, only the leader accepts writes. Write throughput scales with the cores on the leader, not with the number of Spark executors. This bounds the point at which adding more Spark parallelism stops helping.

**How we apply it:** the pipeline treats Spark-side parallelism as a second-order tuning knob, not a first-order solution. Node writes use a modest default (node-partition count `4`, see §1); raise it incrementally against measured throughput once write volume justifies it.

Source: [Neo4j Spark Connector — Parameter tuning, Write parallelism](https://neo4j.com/docs/spark/current/performance/tuning/#parallelism) (retrieved 2026-04-21).

### 4. Create vector indexes before writing embedded nodes

Vector indexes populate asynchronously for existing data, but creating the index first lets the connector populate it as rows land rather than requiring a separate reindex pass. Dimension and similarity function are fixed at index creation and cannot be altered without dropping the index.

**How we apply it:** the bootstrap step at the top of the LOAD stage reads `NEOCARTA_DATABRICKS_EMBEDDING_DIMENSION` and creates a cosine-similarity vector index per enabled label before any node write.

Source: [Neo4j vector index](https://neo4j.com/docs/cypher-manual/current/indexes/semantic-indexes/vector-indexes/) (retrieved 2026-04-21).

### 5. Fail closed at the node write boundary: project to the per-label property allowlist

The connector writes every column present in the DataFrame as a node property. Helper, intermediate, and embedding-bookkeeping columns ride the node DataFrame through the transform stages; if they reach the write unfiltered they leak into the graph as silent extra properties (the defect that motivated this rule was `table_schema` ending up on every node). A denylist is fail-open — any new helper column added upstream silently becomes a property until someone notices. The boundary is therefore an allowlist that fails closed: the write projects to exactly the declared per-label property set and raises if a required property is absent, rather than writing whatever happens to be present.

**How we apply it:** neocarta's graph contract (`neocarta.connectors.databricks.contract`) defines the per-label node-property allowlist; a column is a graph property if and only if it is listed there. The load stage raises when a declared property (other than the legitimately optional `embedding`) is missing from the DataFrame, then selects exactly the allowlisted columns before every node write. Embedding bookkeeping (the embedding text, its hash, the model, the timestamp) lives in Delta staging and the ledger and is dropped at this boundary by construction, never written as a node property. Structural membership is expressed as edges, not duplicated as scalar node properties.

Source: derived from the Neo4j Spark Connector writing every DataFrame column as a node property, plus the project's graph-contract correctness requirement.

## Project-level principles

These are not vendor best practices; they are consequences of the two vendor rules above plus the project's correctness requirements. Principles §1–§4 govern the neocarta ingest pipeline; §5–§10 govern dbxcarta-owned code (the operator CLI, the client, materialize, and the submit wheel build).

### 1. Unity Catalog is the only source of truth

The ingest pipeline reads UC, writes Neo4j, and does not read Neo4j at any point during a run. This keeps the sink decoupled from pipeline state, makes partial-graph recovery a non-issue (reruns are idempotent MERGEs), and makes candidate discovery a pure function of UC.

### 2. Pipeline-owned artifacts memo, they do not authorize

Any caching layer introduced for cost reasons (e.g. the Delta embedding-reuse ledger) is a memo of "what we've already spent tokens on," never authority. Dropping the ledger must produce a correct run on the next invocation; at worst, one run re-embeds everything.

### 3. Fail fast on missing permissions; never degrade silently

The preflight enumerates every grant and endpoint permission the enabled flags require, and aborts before any read or write if any are missing. There is no partial-graph mode.

### 4. Embedding failures are counted, not thrown

`ai_query` with `failOnError => false` returns nulls rather than exceptions. The pipeline counts attempts and successes per label, computes a failure rate both per-label and in aggregate, and fails the run only if either exceeds the configured threshold. This catches the case where one label silently degrades while another masks it in aggregate.

### 5. Verify the cluster runs the current source before interpreting run output

`submit` is a reference operation, not a build operation. The runner attaches the wheel last uploaded to the UC Volume and executes the entrypoint from it; it does not rebuild or re-upload on `submit`. Running stale code against real infrastructure produces misleading signal — apparent failures may be old bugs already fixed, apparent successes may hide regressions in new code, and the run summary reflects whatever the cluster actually ran, not what the local source says.

Any time source files change, run `dbxcarta publish-wheels && dbxcarta upload --all` before `dbxcarta submit-entrypoint`. Treat `job_name` and `contract_version` in the JSON run summary as a quick sanity check that the expected code ran; a wrong prefix (e.g. `schema_graph_*` instead of `dbxcarta_*`) means the workspace artifact is stale and a re-upload is required before interpreting any results.

### 6. Make Databricks targets explicit at the configuration boundary

Pipeline-owned artifacts are Unity Catalog objects, so their configuration should name the exact UC target instead of relying on workspace defaults. `DBXCARTA_SUMMARY_TABLE` is required to be `catalog.schema.table`, and writable file locations must be `/Volumes/<catalog>/<schema>/<volume>/<subdir>`, not DBFS paths or bare volume roots.

All dynamic identifiers are validated before use and then backtick-quoted at SQL construction. Serving endpoint names are validated before interpolation into `ai_query` calls. Local tooling builds `WorkspaceClient` from `DATABRICKS_PROFILE` when present, otherwise it lets the Databricks SDK use its default authentication chain.

### 7. The node ID is the authoritative catalog/schema/table identity

Every node ID is catalog-qualified by construction, so the ID alone fully determines which catalog, schema, and table a retrieved node belongs to. The graph spans multiple catalogs (bronze, silver, and gold across the resolved catalogs), so any consumer that rebuilds a fully-qualified name by substituting a single configured catalog (e.g. `settings.dbxcarta_catalog`) produces a wrong FQN for every node outside that one catalog. Catalog/schema/table identity is recovered from the node, never from configuration.

**How we apply it:** `dbxcarta-client` reconstructs the FQN from the authoritative per-node identity. `graph_retriever.py` walks `Database → Schema → Table` and builds the FQN from each node's own `name` property; `ids.py` (`catalog_from_node_id` / `schema_from_node_id`) parses the catalog-qualified node ID rather than assuming a single configured catalog. `settings.dbxcarta_catalog` is used only as the seed anchor in eval-harness paths, never as the catalog of a retrieved node.

Source: derived from the catalog-qualified node-ID scheme and the multi-catalog graph requirement.

### 8. Inferred FK edges require type and target-key evidence; declared FKs are authoritative

A name match is a candidate, not a foreign key. Every inferred edge (metadata and semantic) must additionally clear two hard gates: the source and target types must be compatible, and the target must carry primary-key or unique evidence. A name match with neither is never emitted. Declared catalog foreign keys are authoritative: an inferred edge that duplicates one a higher-authority strategy already emitted is suppressed, not re-emitted at a different confidence. Accuracy tuning may tighten these gates but may not weaken them.

This is a rule the neocarta ingest pipeline implements — its FK inference applies the type-equality predicate and a PK-evidence inner join as hard filters, and threads authority order so declared edges suppress inferred metadata. dbxcarta states it here because it is a correctness requirement of any FK discovery dbxcarta relies on; see neocarta's documentation for the implementation. The conceptual trade-offs are in [`../explanation/fk-discovery.md`](../explanation/fk-discovery.md) and [`design-decisions.md`](design-decisions.md).

Source: derived from FK correctness requirements (a name match alone is not a foreign key) and the declared-edge authority ordering.

### 9. An ops table's preflight DDL and its writer schema must agree on every column type

The neocarta ingest pipeline pre-creates the run-summary history table in preflight, then appends to it at the end of the run with `mode("append").option("mergeSchema", "true")`. mergeSchema reconciles new columns, but a column that exists on both sides with a conflicting type is a hard merge failure. Because the table is created once and reused, a type disagreement between the `CREATE TABLE` and the writer schema stays invisible for as long as the table survives, then fails the very next run after the table is dropped or recreated. A column's declared type must also match the runtime value, not just the field's annotation: a counter annotated `float | None` but holding an `int` is a `BIGINT` column whose writer `StructField` is `LongType`, never `DOUBLE`.

**How we apply it:** the preflight-DDL pattern and its schema-agreement test moved to neocarta with the ingest pipeline. The run-summary writer that stays in dbxcarta is `dbxcarta-materialize`'s `summary.py`: it defines its writer schema once as a single `StructType` and appends with `mergeSchema=true` and no separate preflight `CREATE TABLE`, so the cross-DDL disagreement this rule guards against cannot arise there. The rule still binds the neocarta pipeline, which does pre-create its table.

Source: derived from Delta `mergeSchema` merge semantics and a production failure where the preflight DDL declared `embedding_failure_threshold DOUBLE` while the writer wrote `LongType`, surfacing only when the summary table was recreated.

### 10. The cluster bootstrap smoke check validates only shared-environment packages; wheel content is a build-time guarantee

The pinned `databricks-job-runner` bootstrap runs its post-install smoke imports before it prepends the per-run wheel target to `sys.path`. At that point only the shared driver environment is importable: the `--no-deps` pinned closure and the DBR-provided packages. A wheel module such as `dbxcarta.core` is not yet on the path, so naming it in the smoke list fails every cluster run with `No module named 'dbxcarta'`, regardless of whether the wheel actually carries it. The smoke check answers "is the shared environment intact," never "did this wheel get built correctly."

**How we apply it:** `_ENTRYPOINT_SMOKE_IMPORTS` (in `dbxcarta-submit`'s `cli.py`) names only closure packages, and `tests/submit/test_cli.py` asserts no entry is a `dbxcarta.*` module. The guarantee that each entrypoint wheel physically carries `dbxcarta/core` is enforced where it can be, at build time: `publish-wheels` calls `_assert_wheel_bundles_core` on each freshly built wheel before it is relied upon.

Source: derived from the `databricks-job-runner==0.6.2` bootstrap ordering (smoke check precedes the `sys.path` insert of the wheel target) and a production failure where `dbxcarta.core` in the smoke list blocked every ingest run.

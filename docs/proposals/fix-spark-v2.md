# Spark Pipeline Fixes V2

## Goal

Improve `packages/dbxcarta-spark` so the ingest pipeline keeps large work in
Spark longer, avoids unnecessary early actions, and fails clearly before a
driver-side step becomes too large.

This is not a rewrite proposal. The current design is workable for small and
medium catalogs. The problem is that a few stages press Spark's "run now"
button early, then move metadata or sampled values into Python driver memory.
That creates a scale ceiling and makes large multi-catalog runs more fragile.

The recommended path is:

- Clean up the easy materialization issues first.
- Add guardrails around intentional driver-side work.
- Make the expensive FK and semantic inference paths Spark-native only when the
  catalog sizes justify the larger refactor.

## Current Assessment

The embedding path is mostly correct. `ai_query` output is written to Delta and
then read back before failure-rate checks and Neo4j writes. That materialize-once
boundary prevents repeated model calls, so it is not the main problem.

The main scaling risks are:

- FK discovery collects all extracted columns and constraints to the driver.
- Metadata FK inference and semantic FK inference run nested Python pair loops.
- Sample-value planning collects table candidates to the driver, then issues
  many small per-table or per-chunk Spark jobs.
- Sample-value output is cached for reuse, but successful runs do not explicitly
  release that cache.
- Relationship writes use `coalesce(1)` everywhere, which is safe for Neo4j lock
  contention but serializes high-volume writes.

In simple terms: some stages build a Spark factory line, but then carry all the
parts back to one desk before finishing the job.

## File Map

The orchestration is in `run.py`. There is no `pipeline.py` source file; any
`ingest/__pycache__/pipeline.cpython-*.pyc` artifacts are orphans and should
be deleted (see Phase 1).

- Load step → `run.py:_load` (`run.py:272`); run ordering and the existing
  `try/finally` cleanup → `run.py:_run` (`run.py:160`, finally at
  `run.py:211-221`).
- Sampled-value cache → `ingest/transform/sample_values.py:sample`
  (`raw_df.cache()` at line 145).
- FK driver collects → `ingest/fk/discovery.py:run_fk_discovery`
  (columns collect at line 79, constraints at 201, value index at 252).
- Metadata FK loop → `ingest/fk/metadata.py:infer_fk_pairs`
  (nested loop at lines 192-193).
- Declared FK (already Spark-native, already unpersists) →
  `ingest/fk/declared.py` (lines 114-115, 141-142).
- Relationship writes (`coalesce(1)`) → `run.py:321,327,333,339,349,360,371`.

## Assumptions

- The near-term target is reliability and predictable behavior, not a full
  Spark rewrite.
- Driver-side FK inference is acceptable for bounded catalogs.
- Broad multi-catalog runs need guardrails before they need algorithm changes.
- Relationship writes are intentionally conservative because Neo4j lock
  contention is a real risk.
- Semantic FK discovery remains disabled by default unless column embeddings are
  enabled.

## Risks

- Removing or moving materialization in the embedding path could accidentally
  re-run `ai_query`. That path should remain materialized through Delta.
- Making relationship writes more parallel can improve throughput but may create
  Neo4j contention or retries.
- Guardrails can block runs that currently complete slowly. That is still better
  than driver OOM, but the default limits must be documented.
- A Spark-native FK rewrite is larger than it looks because counters, duplicate
  suppression, tie-break behavior, and confidence scoring must remain stable.

## Phase Checklist

### Phase 1: Quick Cleanup

Status: Pending

Goal: Remove avoidable actions and leaked cache state without changing pipeline
behavior.

Checklist:

- Release the sampled-value cache as an ownership/lifetime change, not a
  "call unpersist later" change. The cached `raw_df` is a local inside
  `sample_values.py:sample`; no handle escapes it today, so the caller
  cannot release it. Change `transform_sample_values` to return an
  unpersist handle (or a small context object). Its lifetime spans two
  later consumers: the semantic FK value-index build
  (`fk/discovery.py:246-253`) and the Neo4j Value/HAS_VALUE writes
  (`run.py:316-323`). It must therefore be released only AFTER both
  `run_fk_discovery` and `_load`, in the existing `try/finally` at
  `run.py:211-221`, right next to `extract_result.unpersist_cached()`, so
  failure paths are covered too. Do not unpersist immediately after
  `sample()` returns — that would break the two later readers.
- Delete the orphan `ingest/__pycache__/pipeline.cpython-*.pyc` files.
  They have no `.py` source, can shadow imports, and confuse anyone
  executing this plan.
- (Cosmetic, must not delay the cache fix) Remove the
  `database_df.count()` action used only for logging at `run.py:291`, and
  use the known resolved catalog count instead.
- Review sampled-value count actions and combine them only where the change is
  simple and behavior-preserving.
- Keep embedding Delta staging unchanged so `ai_query` still runs once per
  staged label.

Validation:

- Existing Spark unit tests pass.
- A local or mocked run confirms sampled-value DataFrames are still available
  through the Neo4j write.
- An explicit ordering check confirms the sampled-value cache is released
  ONLY after both `run_fk_discovery` and `_load` complete (mocked-Spark
  unit tests will not catch an early unpersist; assert call order).
- Logs still report node and value counts needed for operators.

Notes:

- This phase is low risk and should be done first.
- The sampled-value cache cleanup is the most important item in this phase.

### Phase 2: Driver-Side Guardrails

Status: Pending

Goal: Make intentional driver materialization explicit, bounded, and visible in
the run summary.

Checklist:

- Add configurable limits for driver-side FK discovery inputs, especially total
  columns and total sampled value rows considered for semantic corroboration.
- Place the guardrail before the FIRST and largest driver pull at
  `fk/discovery.py:79` (the all-columns collect that feeds both metadata
  and semantic), not only before `metadata.infer_fk_pairs`. Also bound the
  constraint collect (`discovery.py:201`) and value-index collect
  (`discovery.py:252`). Compare `summary.extract.columns` against the
  configured limit; it is already materialized at `extract.py:118`, so the
  check itself is cheap.
- Skip or fail clearly when a configured driver limit is exceeded. The behavior
  should be controlled by a setting so production can choose strict failure or
  degraded inference.
- Record skipped inference and limit details in the run summary.
- Add log messages that distinguish small intentional `collect()` calls from
  blocked large driver materialization.

Validation:

- Tests cover below-limit, at-limit, and above-limit behavior.
- A synthetic large catalog test shows the run fails or degrades before
  collecting all columns.
- Existing small-catalog behavior remains unchanged by default.

Notes:

- This phase does not make FK inference Spark-native. It makes the existing
  design safer.
- Recommended initial defaults should be conservative and easy to override.

### Phase 3: Configurable Relationship Write Parallelism

Status: Pending

Goal: Preserve the current safe default while allowing high-volume graph writes
to use more Spark parallelism when Neo4j can handle it.

Checklist:

- Add a setting for relationship write partition count.
- Keep the default byte-for-byte equivalent to today's `coalesce(1)` path
  via a helper: `df.coalesce(1) if n <= 1 else df.repartition(n)`.
  `repartition(1)` is NOT equivalent to `coalesce(1)` — it inserts a full
  shuffle. The default must take the `coalesce` branch.
- Apply the setting consistently to `HAS_SCHEMA`, `HAS_TABLE`, `HAS_COLUMN`,
  `HAS_VALUE`, and `REFERENCES` writes.
- Document the tradeoff: more partitions can improve throughput, but can also
  increase Neo4j lock contention.

Validation:

- Tests or writer fakes verify the configured partition behavior.
- Default behavior remains equivalent to the current `coalesce(1)` path.
- A Databricks smoke run confirms relationship writes still complete with the
  default setting.

Notes:

- This is an operational tuning improvement, not a correctness fix.
- Do not change the default until production evidence shows Neo4j handles
  parallel relationship writes safely.

### Phase 4: Reduce Sample-Value Small Jobs

Status: Pending

Goal: Reduce the number of small Spark actions in sample-value planning while
keeping sampled value rows in Spark.

Checklist:

- Keep candidate columns in a Spark DataFrame longer before converting to Python
  table candidates.
- Batch cardinality probes more aggressively by schema or table group where
  Spark SQL can still produce clear failure handling.
- Limit schema-readability probes so large schemas do not create excessive tiny
  jobs.
- Keep sampled value rows as Spark DataFrames through node and relationship
  writes.
- Preserve current behavior for unreadable tables and schemas.

Validation:

- Existing value-sampling tests pass.
- A benchmark run reports fewer Spark jobs for the same catalog.
- Sample counts and selected top values remain equivalent on representative
  fixtures.

Notes:

- This is the highest-leverage medium-sized improvement after guardrails.
- Avoid turning this into a full dynamic SQL framework unless measurements show
  it is necessary.

### Phase 5: Spark-Native Declared FK Edges

Status: Pending

Goal: Remove the easiest FK-related `collect()` by building declared
`REFERENCES` edges directly as a Spark DataFrame.

Checklist:

- Keep the existing information_schema declared FK join in Spark.
- Generate source and target IDs with the existing Spark ID expressions.
- Add the declared FK confidence, source, and criteria columns in Spark.
- Preserve existing declared counters.
- Preserve duplicate suppression against prior pairs if declared FK discovery is
  ever no longer first.

Validation:

- Declared FK unit tests pass.
- The generated DataFrame schema still matches the Neo4j `REFERENCES` writer
  expectations.
- Run-summary declared FK counters match the current implementation on fixtures.

Notes:

- This phase is a good first Spark-native FK step because declared FK data
  already starts as DataFrames.
- Set expectations: declared FK is already mostly Spark-native and already
  unpersists its caches (`fk/declared.py:114-115,141-142`). The expensive
  driver collect is the metadata column list, so the real prize is
  Phase 6, not this phase.

### Phase 6: Spark-Native Metadata FK Inference

Status: Pending, conditional on scale pressure after Phases 1-5.

Goal: Move the expensive metadata candidate generation and scoring out of Python
pair loops and into Spark joins and filters.

Checklist:

- Represent source and target columns as Spark aliases of the extracted columns
  DataFrame.
- Apply same-catalog, same-schema, non-self, type compatibility, name-match, and
  target PK-like gates in Spark.
- Reproduce the existing confidence scoring and tie-break behavior with Spark
  expressions and window functions.
- Produce counters from Spark aggregations rather than Python counters.
- Build the final metadata `REFERENCES` DataFrame without collecting all
  candidates to the driver.

Validation:

- Existing metadata inference tests pass or are ported to compare Spark output
  with the current Python implementation.
- A fixture comparison shows edge parity with the current implementation.
- Large synthetic catalog runs show driver memory stays bounded.

Notes:

- This is a behavioral refactor. It should not be bundled with scoring changes.
- Keep the Python implementation available as a reference during migration.

### Phase 7: Semantic Inference Scale Strategy

Status: Pending, conditional on semantic inference becoming a production
bottleneck.

Goal: Avoid all-pairs Python cosine comparisons for broad catalogs.

Checklist:

- Filter semantic candidates to source-like columns and PK-like targets before
  any vector comparison.
- Evaluate whether Spark vector scoring is sufficient after candidate filtering.
- If candidate volume is still high, evaluate Databricks Vector Search or another
  approximate nearest-neighbor path.
- Keep value-overlap corroboration optional and bounded.
- Preserve the existing semantic threshold, confidence cap, and prior-pair
  suppression semantics.

Validation:

- Semantic unit tests pass.
- A benchmark captures candidate count before and after filtering.
- Semantic output is compared against the current implementation on a small
  fixture before replacing the Python path.

Notes:

- This is the last phase because semantic inference is disabled by default and
  depends on column embeddings.
- Do not optimize this before guardrails show it is the bottleneck.

## Completion Criteria

The proposal is complete when:

- Easy cleanup items are implemented and validated.
- Driver-side materialization has explicit limits and clear summary reporting.
- Relationship write parallelism is configurable with the current safe default.
- Sample-value planning creates fewer small Spark jobs without changing sampled
  output.
- Declared FK edge construction no longer requires collecting all declared FK
  rows to Python.
- Metadata and semantic FK rewrites are either implemented or explicitly
  deferred with benchmark evidence.

## Recommended Order

Do Phases 1 and 2 first. They give the most safety for the least change.

Do Phase 3 if graph writes are slow.

Do Phase 4 if value sampling creates too many Spark jobs.

Do Phases 5-7 only when catalog size or runtime evidence shows the driver-side
FK paths are the actual bottleneck.

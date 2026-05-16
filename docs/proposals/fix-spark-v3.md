# Spark Pipeline Fixes V3

## Goal

Make `packages/dbxcarta-spark` scale to the ~500-table target by removing
driver-side foreign-key work and keeping FK discovery in Spark. This is a
clean rewrite of the FK path, not a parity-preserving refactor. Nothing
downstream depends on the exact edges the current Python implementation
emits, so the new implementation is judged on correct FK semantics, not on
byte-for-byte equality with the old output.

V3 supersedes the ordering in `fix-spark-v2.md`. V2 is kept as history. V2
treated the sampled-value cache leak as the top issue and deferred the
driver-collect fix to a conditional late phase. The real scale ceiling is
the FK driver work, so V3 fixes that first and demotes the rest.

## Current Assessment

The scaling ceiling is the FK discovery stage:

- `fk/discovery.py:79` collects every extracted column to the driver.
- `fk/discovery.py:201` collects every PK and UNIQUE constraint to the
  driver.
- `fk/discovery.py:252` collects the entire value index to the driver when
  semantic inference runs.
- `fk/metadata.py:192-193` compares every column against every other
  column in a single-threaded Python double loop.
- `fk/semantic.py` repeats the same all-pairs double loop with a
  hand-rolled Python cosine.

At ~500 tables this is tens of thousands of columns and billions of
comparisons on one node. That is both a memory ceiling and a runtime
ceiling.

Lower-severity issues:

- `sample_values.py:145` caches `raw_df` and never releases it on the
  success path. No handle escapes the function, so the caller cannot
  release it.
- `run.py:_load` forces `coalesce(1)` on every relationship write.
- `run.py:291` runs a logging-only `database_df.count()`.
- Orphan `ingest/__pycache__/pipeline.cpython-*.pyc` files exist with no
  `.py` source.

`fk/declared.py` is already Spark-native and already unpersists its caches,
so it needs interface alignment only.

## Assumptions

- The target is reliability and scale at ~500 tables, not preservation of
  the current Python edge output.
- No migration, shim, or dual-run period is required. The Python FK path is
  replaced, not kept alongside.
- Correctness is verified by rule-based fixtures, not by diffing against
  the old implementation.
- Semantic inference stays disabled by default and depends on column
  embeddings.
- Cross-catalog FK pairing stays blocked, matching current behavior.

## Risks

- Name-match rules (stem and plural handling) and type compatibility are
  nontrivial. Expressing them as raw Spark SQL is error-prone. The
  recommended path is a vectorized `pandas_udf` so the rule logic stays
  readable and runs on executors without a driver collect.
- A new attenuation and tie-break curve changes which low-confidence edges
  survive. This is acceptable because parity is not a goal, but the new
  curve must be documented and covered by fixtures.
- Releasing the sampled-value cache too early breaks the semantic
  value-index build and the HAS_VALUE write. The release must happen after
  both `run_fk_discovery` and `_load`.
- Making relationship writes parallel can increase Neo4j lock contention.
  The default stays single-partition.

## Phase Checklist

### Phase 0: Cache Hygiene and Cleanup

Status: Pending

Goal: Remove the sampled-value cache leak and stray cruft before the
refactor, with no behavior change to FK output.

Checklist:

- Change `transform_sample_values` to return an unpersist handle or small
  context object for the cached sampled-value DataFrame.
- Release that handle in the existing `run.py:211-221` `finally`, after
  both `run_fk_discovery` and `_load`, next to
  `extract_result.unpersist_cached()`. Do not release immediately after
  `sample()` returns.
- Delete the orphan `ingest/__pycache__/pipeline.cpython-*.pyc` files.
- Remove `database_df.count()` at `run.py:291`. Log the resolved-catalog
  count instead.

Validation:

- `uv run pytest tests/spark -q` passes.
- An ordering check confirms the cache is released only after both
  `run_fk_discovery` and `_load`, including failure paths.
- Operator logs still report node and value counts.

Notes:

- Cheap, isolated, and independent of the FK rewrite. It runs first so
  memory pressure is removed before the larger change lands.

### Phase 1: Spark-Native Declared and Metadata FK

Status: Pending

Goal: Replace the metadata Python loop and its driver collects with Spark
DataFrame joins.

Checklist:

- Self-join `extract.columns_df` aliased as `src` and `tgt`. Join
  predicate: same catalog and schema, not the same column.
- Port the matching rules, not the loop. Reuse the logic in `_name_match`,
  `_stem_matches_table`, `types_compatible`, and `_SCORE_TABLE` from
  `fk/metadata.py` and `fk/common.py` inside a vectorized `pandas_udf`
  applied to the joined DataFrame.
- Apply the PK-like target gate by joining against the constraints
  DataFrame. Keep the `discovery.py:201` constraints query as a DataFrame
  and do not collect it. Express the id-column heuristic as a join.
- Compute attenuation and tie-break with `Window.partitionBy(src_id)` and a
  score expression. The curve is new and clean. It is not required to match
  `math.sqrt(n-1)`.
- Filter by threshold. Anti-join the declared `prior_pairs` DataFrame to
  suppress duplicates.
- Produce counters from Spark aggregations into `summary.fk_metadata`.
- Feed the result straight into `schema_graph.build_references_rel`.
- Remove the `discovery.py:79` and `discovery.py:201` collects.

Validation:

- `tests/spark/fk_metadata` and `tests/spark/fk_declared` rewritten to
  DataFrame input and output. They assert correct edges, confidences,
  rejection reasons, and prior-pair suppression on known fixtures.
- A synthetic large-catalog fixture confirms no driver-side `collect()` on
  the metadata FK path.

Notes:

- This is the core fix and the reason for V3. Declared FK is already
  Spark-native, so the work here is the metadata path and the shared
  constraints DataFrame.

### Phase 2: Spark-Native Semantic FK

Status: Pending

Goal: Remove the semantic Python all-pairs loop and the value-index
collect.

Checklist:

- Pre-filter candidates by target PK-like, type compatibility, and
  prior-pair anti-join before any vector math.
- Compute cosine similarity in Spark on the reduced candidate set. Drop the
  Python `_cosine` and the all-pairs loop.
- Apply value-overlap corroboration as a join on the Value and HAS_VALUE
  DataFrames. Remove the `discovery.py:252` collect.
- Keep the existing enable and min-tables gate in `_should_run_semantic`
  unchanged.

Validation:

- `tests/spark/fk_semantic` rewritten to DataFrame input and output,
  asserting threshold, confidence clamp, value corroboration, and
  prior-pair suppression on fixtures.
- A benchmark records candidate count before and after pre-filtering.

Notes:

- Semantic is disabled by default, so this lands after the metadata fix.
  The pattern is the same: pre-filter, then score in Spark.

### Phase 3: Configurable Relationship Write Parallelism

Status: Pending

Goal: Keep the safe single-partition default while allowing tuned
parallel relationship writes.

Checklist:

- Add a relationship write partition setting.
- Add a helper: `df.coalesce(1) if n <= 1 else df.repartition(n)`. The
  default of 1 keeps writes byte-for-byte identical to today.
  `repartition(1)` is not equivalent to `coalesce(1)` and must not be the
  default branch.
- Apply the helper to every `write_rel` call in `run.py:_load`.

Validation:

- Writer fakes verify the configured partition behavior.
- Default behavior stays equivalent to the current `coalesce(1)` path.

Notes:

- Operational tuning, not a correctness or scale fix. The default does not
  change until production evidence shows Neo4j handles parallel writes
  safely.

### Phase 4: Thin Guardrail and Documentation

Status: Pending

Goal: Add a small optional safety net and document the new settings.

Checklist:

- Add one optional setting to cap or skip FK discovery when column count is
  absurd. This replaces V2's heavy guardrail phase, which is unnecessary
  once Phase 1 removes the driver collect.
- Record any skip in the run summary.
- Document the new settings and the write-parallelism tradeoff.

Validation:

- Tests cover below-limit and above-limit behavior.
- Default small-catalog behavior is unchanged.

Notes:

- The guardrail is a backstop, not a substitute for the Phase 1 fix.

## Completion Criteria

The proposal is complete when:

- The sampled-value cache is released deterministically on success and
  failure paths.
- Metadata FK discovery runs entirely in Spark with no driver collect of
  columns or constraints.
- Semantic FK discovery runs in Spark with pre-filtering and no value-index
  collect.
- Relationship write parallelism is configurable with the current safe
  default.
- FK tests assert correct rules and edges on fixtures, with no diff against
  the old Python implementation.
- A synthetic large-catalog run completes with bounded driver memory.

## Recommended Order

Phase 0 first. It is cheap, isolated, and removes memory pressure.

Phase 1 next. It is the core fix and the reason for V3.

Phase 2 after Phase 1, since semantic is off by default.

Phases 3 and 4 last. They are tuning and backstops.

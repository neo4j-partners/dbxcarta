# Spark Pipeline Fixes V3

## Goal

Make `packages/dbxcarta-spark` scale to the ~10,000-table target by removing
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

The scaling ceiling is the FK discovery stage. Line numbers below are
indicative only: these files are mid-refactor, so the implementer must
re-derive each location from the current source rather than trust the
references here.

- `fk/discovery.py` collects every extracted column to the driver
  (`run_fk_discovery`, the `ColumnMeta.from_row` comprehension).
- `fk/discovery.py` collects every PK and UNIQUE constraint to the driver
  (`_build_pk_index`).
- `fk/discovery.py` collects every column embedding to the driver
  (`_collect_column_embeddings`). At ~10k columns × 1024 floats this is the
  single largest driver-memory item — larger than the column and
  constraint collects — yet it was missing from earlier assessments. It is
  behind the default-off semantic gate, so it is not the live ceiling for a
  default run; it becomes the dominant item the moment semantic is enabled.
- `fk/discovery.py` collects the entire value index to the driver when
  semantic inference runs (`_build_value_index`).
- `fk/metadata.py` compares every column against every other column in a
  single-threaded Python double loop (`infer_fk_pairs`).
- `fk/semantic.py` repeats the same all-pairs double loop with a
  hand-rolled Python cosine.

At ~10,000 tables this is hundreds of thousands of columns and tens of
billions of comparisons on one node. That is both a memory ceiling and a
runtime ceiling.

Lower-severity issues:

- `sample_values.py` caches `raw_df` and, on the success path, never
  unpersists it (it does unpersist on the zero-rows early return). No
  handle escapes the function, so the caller cannot release the cache after
  the downstream writes finish.
- `run.py:_load` forces `coalesce(1)` on every relationship write.
- `run.py:291` runs a logging-only `database_df.count()`.
- Orphan `ingest/__pycache__/pipeline.cpython-*.pyc` files exist with no
  `.py` source.

`fk/declared.py` is already Spark-native and already unpersists its caches,
so it needs interface alignment only. Its `collect()` of resolved FK pairs
is intentionally retained: it is bounded by the number of catalog-declared
foreign keys, not by the n²-shaped all-pairs comparison, so it does not
threaten driver memory at ~10,000 tables. Do not "fix" it.

## Assumptions

- The target is reliability and scale at ~10,000 tables, not preservation of
  the current Python edge output.
- No migration, shim, or dual-run period is required. The Python FK path is
  replaced, not kept alongside.
- Correctness is verified by rule-based fixtures, not by diffing against
  the old implementation.
- Semantic inference stays disabled by default and depends on column
  embeddings.
- Cross-catalog FK pairing stays blocked, matching current behavior.

## Risks

- Name-match rules (stem and plural handling), type compatibility, and the
  score table look dynamic but are driven by tiny static tables. They must
  be expressed as native Spark `Column` expressions and broadcast lookups,
  built once at plan-construction time. No `pandas_udf` or Python UDF: a
  UDF is opaque to Catalyst, blocks predicate pushdown and whole-stage
  codegen, and pays Arrow JVM-to-Python cost on every batch, which is
  multiplied across an n²-shaped join.
- The attenuation and tie-break curve is reproduced as-is, not changed.
  The Spark expression `score / greatest(1.0, sqrt(cnt - 1))` is
  mathematically identical to the current Python
  `score / max(1.0, sqrt(max(0, n - 1)))` for every n, so this is a faithful
  port, not a new curve.
- Single-schema skew: if the catalog is one schema (the common prototype
  case), an equi-join keyed only on `(catalog, schema)` collapses to a
  single M² partition on one node — the same ceiling, relocated from the
  driver to one executor. Phase 1 must therefore push the name-match rule
  into the join key itself (see Phase 1), so the join output is bounded by
  actual name matches, not by schema size.
- Releasing the sampled-value cache too early breaks the semantic
  value-index build and the HAS_VALUE write. The release must happen after
  both `run_fk_discovery` and `_load`.
- Making relationship writes parallel can increase Neo4j lock contention.
  The default stays single-partition.

## Phase Checklist

### Phase 0: Cache Hygiene and Cleanup

Status: Complete

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
- Done (2026-05-16): `sample()` returns a 4th `cache_handle` element;
  `ValueResult` gained `cache_handle` + `unpersist_cached()` mirroring
  `ExtractResult`. `run.py:_run` initializes `values = None` and releases it
  in the existing `finally` (per-snapshot guarded so a failure path logs
  rather than masks the original error). `database_df.count()` replaced with
  `len(settings.resolved_catalogs())` (`_load` now takes `settings`). Orphan
  `pipeline.cpython-{313,314}.pyc` deleted. `uv run pytest tests/spark -q`:
  155 passed, 1 skipped.

### Execution decision (2026-05-16): Phases 1 and 2 are executed together

Phases 1 and 2 share the same two driver collects (`column_metas`,
`pk_index` in `discovery.py`) and the same Spark scaffolding (the
constraints DataFrame, `canonicalize_expr` type compatibility, the PK-like
target gate, and the prior-pair anti-join). Executing Phase 1 alone forces
one of two bad outcomes: scope the shared collects into the semantic-only
branch as throwaway scaffolding that Phase 2 immediately deletes, or delete
them and ship a functionally broken (default-off) semantic path between
phases for no scale benefit (the embedding collect, the largest driver
item, would remain until Phase 2 anyway).

Neither is "doing it properly." The two phases are therefore implemented as
one coherent FK-path rewrite: build the shared Spark scaffolding once, use
it for both the metadata and semantic strategies, and remove all four
driver collects (columns, constraints, embeddings, value-index) in a single
pass. Phase 1 and Phase 2 checklists below remain the specification of
*what* each strategy must do; they are validated together. The recommended
order's "Phase 2 cannot precede Phase 1" still holds — they are not
reordered, they are merged.

### Phase 1: Spark-Native Declared and Metadata FK

Status: Complete (merged with Phase 2 — see Execution decision above)

Goal: Replace the metadata Python loop and its driver collects with Spark
DataFrame joins.

Checklist:

- Push the name-match rule into the join key, not a post-join filter. A
  plain equi-join on `(catalog, schema)` plus a name predicate evaluated
  after the join still materializes the full per-schema cartesian and
  collapses to one M² partition when the catalog is a single schema. Avoid
  this by deriving a `link_key` on each side from the static name rule and
  equi-joining on `(catalog, schema, link_key)` so non-matching pairs are
  never produced:
  - `src` side emits, per column, the set of keys it could match on:
    `lower(column)` for the exact branch, and for each suffix in the static
    `_STEM_SUFFIXES` the stripped stem (when the column ends with that
    suffix and is longer than it) for the suffix branch. Build this with
    an `array(...)` of the candidate keys, then `explode` it once at
    plan-construction time. The suffix loop is unrolled in Python while
    building the plan; no UDF.
  - `tgt` side emits the keys it can be matched against: `lower(column)`
    for the exact branch, and for the suffix branch the singular/plural
    forms of `lower(table)` (`t`, `t` with trailing `s`/`es` stripped)
    gated on `lower(column) = 'id'`.
  - The join output is then bounded by real name matches, not by schema
    size, which removes the single-schema skew. Add a not-same-column
    condition and the same-`(table, column)` self-reference guard as
    residual predicates.
- Collapse the explode to one row per `(src_id, tgt_id)` before anything
  downstream. A pair can match on more than one emitted key (the exact key
  plus a stem, or several tgt table-forms — singular, `-s`, `-es` — that
  collide on a degenerate table name), producing duplicate join rows with
  different `_SCORE_TABLE` scores. Dedup with an explicit kind-priority
  tiebreak: `EXACT` before `SUFFIX`, so each pair carries exactly one
  `NameMatchKind`. The dedup must run before the attenuation window so the
  window `cnt` is the per-source candidate count.
- No UDF anywhere. Python builds the plan, Spark evaluates every row.
  Expand the remaining static rules into native expressions at
  plan-construction time:
  - Type compatibility: `types_compatible` is **not** a static pair lookup
    — it is `canonicalize(a) == canonicalize(b)`, and `canonicalize`
    normalizes before any map lookup (regex parse of `DECIMAL|NUMERIC(p,s)`
    keeping scale and discarding precision, regex strip of
    `STRING|VARCHAR|CHAR(n)` to `STRING`, `trim`/`upper`, then the
    `_TYPE_EQUIV` integer-family map). Port `canonicalize` as a native
    `canonicalize_expr(col)` built from `upper`/`trim`/`regexp_extract`/
    `when`, producing one comparable token (family plus decimal scale). The
    predicate is then the derived-column equality
    `canonicalize_expr(src.data_type) == canonicalize_expr(tgt.data_type)`
    — no broadcast join. `_TYPE_EQUIV` is small enough to inline as a
    chained `when`; a broadcast map is not needed.
  - `_SCORE_TABLE`: materialize this static map as a small DataFrame keyed
    by `(name_kind, pk_evidence, comment_present)` and `broadcast`-join it
    in.
  - Comment-token overlap: native higher-order array functions
    (`split`, `filter` with length and stopword predicates,
    `array_intersect`, `size`); stopwords as a broadcast literal array.
- Apply the PK-like target gate by joining against the constraints
  DataFrame. Keep the `_build_pk_index` constraints query as a DataFrame
  and do not collect it. The id-column heuristic is its own sub-task, not a
  one-line join: `pk_kind`'s `{table}_id` branch fires only when that is
  the *sole* `_id`-suffixed column in the table, so it needs a per-table
  aggregation (`count` of `_id` columns grouped by `table_key`) feeding a
  conditional join. Budget for it explicitly. `PKIndex`'s composite-PK
  count must also be reproduced as a Spark aggregation, not a Python loop.
- Attenuation window and filter order (affects which edges survive; encode
  exactly this):
  - Window set = rows surviving name dedup, the type filter, the PK gate,
    and the `_SCORE_TABLE >= threshold` filter; **before** the prior-pair
    anti-join and **before** the attenuated-`>=`-threshold filter.
  - Compute `cnt = count() over Window.partitionBy(src_id)` on that frame,
    then `attenuated = score / greatest(1.0, sqrt(cnt - 1))`. This
    reproduces the current Python curve exactly (identical for every n) — a
    faithful port, not a new curve.
  - Then filter `attenuated >= threshold`.
  - Then left-anti-join the **declared-only** edges DataFrame. This is the
    DataFrame form of the `prior_pairs` the metadata stage actually
    receives: in `discovery.py`, when `infer_fk_pairs` runs, `prior_pairs`
    is declared edges only — metadata edges are folded into `prior_pairs`
    *after* this stage, so metadata must not anti-join against its own
    output. Separately, once this stage completes, emit the union
    `declared ∪ metadata` as a distinct DataFrame for Phase 2's semantic
    anti-join to consume; it is *not* the DataFrame used here.
- Produce summary counts from Spark aggregations into `summary.fk_metadata`
  (accepted edges and a coarse rejected total). Exact per-reason
  attribution and the counter invariant are not reproduced.
- Feed the result straight into `schema_graph.build_references_rel`.
- Remove the column and constraint driver collects in `fk/discovery.py`.

Validation:

- `tests/spark/fk_metadata` and `tests/spark/fk_declared` rewritten to
  DataFrame input and output. They assert correct edges, confidences, and
  prior-pair suppression on known fixtures.
- No-driver-collect is verified structurally, not by asserting absence of a
  call: inspect the metadata FK path's `explain()` plan for the expected
  joins/windows, and run a synthetic large-catalog fixture under a bounded
  driver-memory ceiling that would OOM if a collect were reintroduced.
  **Deferred (2026-05-16):** the synthetic large-catalog
  bounded-driver-memory fixture is not implemented and is deferred until
  later. The structural `explain()`/`grep` verification of no-driver-collect
  is in place; the memory-ceiling fixture is the outstanding part of this
  criterion.

Notes:

- This is the core fix and the reason for V3. Declared FK is already
  Spark-native, so the work here is the metadata path and the shared
  constraints DataFrame.
- Done (2026-05-16): new `fk/inference.py` is the single Spark-native
  source for both inferred strategies. `build_columns_frame` (per-column
  working frame + optional embedding left-join), `build_pk_gate`
  (PKIndex + pk_kind as Spark aggregations/joins, composite-PK count),
  `canonicalize_expr` (native type-compat token), `_comment_tokens_expr`,
  `_score_table_df` (broadcast `_SCORE_TABLE`), `infer_metadata_edges`
  (link_key explode → name dedup window → type → PK gate → broadcast
  score → attenuation window → declared-only anti-join). `discovery.py`
  rewritten: builds the constraints DataFrame (no collect), threads
  declared-only then declared∪metadata as `(source_id,target_id)` frames;
  all four driver collects removed (columns, constraints, embeddings,
  value-index). The dead Python loop in `metadata.py` was deleted, leaving
  only the static rule tables it owns; `semantic.py` deleted. `summary.py`
  now carries `CoarseFKCounts` for `fk_metadata`/`fk_semantic`.
- `tests/spark/fk_metadata`, `fk_semantic`, and `fk_discovery` rewritten to
  DataFrame input/output asserting edges, confidences, attenuation
  drop/preserve, prior-pair suppression, and the asymmetric overlap
  divisor. `fk_declared`/`fk_common`/`fk_discovery` gate tests unchanged and
  still green. No-driver-collect verified structurally: `grep` shows zero
  `.collect()` in `discovery.py`/`inference.py`, and the metadata
  `explain()` plan shows the link_key join, suffix-array explode, dedup
  row_number window, PK-gate join, broadcast `_SCORE_TABLE` join, and the
  `count() over partitionBy(source_id)` attenuation window. `discover_declared`'s
  bounded resolved-FK collect is intentionally retained per Assessment.
  `uv run pytest`: 412 passed, 1 skipped, 3 deselected.

### Phase 2: Spark-Native Semantic FK

Status: Complete (merged with Phase 1 — see Execution decision above)

Goal: Remove the semantic Python all-pairs loop, the embedding collect, and
the value-index collect.

Checklist:

- Embeddings are never collected to the driver, not collected in batches.
  Batch-streaming the collect would only cap peak memory; keeping the
  embedding column in the DataFrame removes the driver step entirely and is
  strictly better. The column node DataFrame's `embedding` array stays a
  DataFrame column end to end.
- Pre-filter candidates by target PK-like, type compatibility, and the
  prior-pair anti-join (the `declared ∪ metadata` DataFrame emitted by
  Phase 1) before any vector math. The relative order of these pre-filters
  does not change the surviving candidate set, so exact rejection-reason
  precedence is not reproduced.
- Produce summary counts from Spark aggregations (accepted edges and a
  coarse rejected total). The counter invariant and exact per-reason
  attribution are not reproduced.
- Compute cosine in-join; do not normalize or mutate the persisted vector.
  The stored `Column.embedding` feeds the Neo4j vector index and the
  semantic-search retriever; normalizing it at write time has a wide blast
  radius and is only safe if every consumer uses a scale-invariant metric,
  which is not assumed. The current `_cosine` does not assume normalized
  vectors — it divides by `a.norm * b.norm`. Port it faithfully: on the
  reduced candidate set, compute the dot product and both norms inline with
  native higher-order array functions (`aggregate` over the arrays for
  `sum(x*y)`, `sum(x*x)`, `sum(y*y)`), then `dot / (sqrt(na) * sqrt(nb))`.
  No `pandas_udf` or Python `_cosine`, no normalization step, persisted
  vectors unchanged, and the all-pairs loop is dropped.
- Apply value-overlap corroboration as a join on the Value and HAS_VALUE
  DataFrames. Reproduce `overlap_ratio` exactly: it is **asymmetric** —
  `|src_values ∩ tgt_values| / |src_values|`, the denominator being the
  *source* column's sampled-distinct count, not the target's and not the
  union. Corroboration fires when `ratio >= 0.5`; on a hit, add the `0.05`
  bonus then re-clamp confidence to `[0.80, 0.90]` (the bonus cannot push
  confidence past the cap). Remove the value-index driver collect in
  `fk/discovery.py`.
- Keep the existing enable and min-tables gate in `_should_run_semantic`
  unchanged.

Validation:

- `tests/spark/fk_semantic` rewritten to DataFrame input and output,
  asserting threshold, confidence clamp, value corroboration, and
  prior-pair suppression on fixtures. A fixture pins the asymmetric
  `overlap_ratio` (distinct source/target value sets chosen so dividing by
  the source count vs target count vs union give different results, and
  only the source-count divisor passes).
- A benchmark records candidate count before and after pre-filtering, with
  a stated upper bound (candidate set must be a small fraction of the
  Cartesian product) as the pass/fail gate.
- The semantic path's `explain()` plan shows no embedding collect; the
  large-catalog fixture runs under the same bounded driver-memory ceiling
  as Phase 1. **Deferred (2026-05-16):** same as Phase 1 — the bounded
  driver-memory fixture is deferred until later; the `explain()`-based
  no-collect check is in place.

Notes:

- Semantic is disabled by default, so this lands after the metadata fix.
  The pattern is the same: pre-filter, then score in Spark.
- Done (2026-05-16): `infer_semantic_edges` pre-filters by same
  (catalog, schema), embeddings present on both sides (left-join null →
  dropped), target PK-like, `canonicalize_expr` type-compat, and the
  declared∪metadata anti-join before any vector math. Cosine is computed
  in-join with `zip_with`/`aggregate`/`transform` (`dot/(sqrt(na)*sqrt(nb))`,
  zero-norm guarded); the persisted embedding is never normalized or
  mutated. `build_value_overlap` reproduces the asymmetric ratio
  `|src∩tgt|/|src|` as Value/HAS_VALUE joins; corroboration adds 0.05 then
  re-clamps to [0.80, 0.90]. The `_should_run_semantic` enable/min-tables
  gate is unchanged. `tests/spark/fk_semantic` rewritten to DataFrame I/O;
  a dedicated fixture pins the asymmetric divisor (src-count passes where
  target-count and union would not). The embedding collect is gone — the
  `embedding` array stays a DataFrame column end to end. Follow-up hardening
  suppresses generic `id` → `id` inferred relationships in both inferred
  paths and releases cached inferred edge frames after the load step.

### Phase 3: Configurable Relationship Write Parallelism

Status: Complete

Goal: Keep the safe single-partition default while allowing tuned
parallel relationship writes.

Checklist:

- Add `dbxcarta_rel_write_partitions: int = 1` to `SparkIngestSettings`,
  following the existing `dbxcarta_*` convention, with a `field_validator`
  that rejects values `< 1` (0 or negative silently breaks the helper's
  `n <= 1` branch and would shuffle to `repartition(0)`).
- Add a `_rel_partition(df, n)` helper next to `_project` in `run.py`:
  `df.coalesce(1) if n <= 1 else df.repartition(n)`. The default of 1
  keeps writes byte-for-byte identical to today. `repartition(1)` is not
  equivalent to `coalesce(1)` (it forces a full shuffle) and must not be
  the default branch.
- Apply the helper to every `write_rel` call in `run.py:_load` (seven call
  sites: HAS_VALUE, HAS_SCHEMA, HAS_TABLE, HAS_COLUMN, and the declared,
  metadata, and semantic REFERENCES writes). A single global setting
  governs all relationship writes; per-relationship tuning is out of scope.
- Update the `_load` docstring, which currently asserts "Relationship
  writes keep coalesce(1) to reduce lock contention." That invariant no
  longer holds; the docstring must describe the configurable behavior or
  the code ships self-contradicting.

Validation:

- Writer fakes assert the path taken, not just the outcome: `n = 1` (and
  the default) takes the `coalesce` path and never `repartition`; `n > 1`
  takes `repartition(n)`. A test pins the default at 1.
- Default behavior stays equivalent to the current `coalesce(1)` path.

Notes:

- Operational tuning, not a correctness or scale fix. The default does not
  change until production evidence shows Neo4j handles parallel writes
  safely.
- Phase 0 already threaded `settings` into `_load`, so this is a purely
  local change with no new plumbing.
- Done (2026-05-16): `dbxcarta_rel_write_partitions: int = 1` added with a
  `_validate_rel_write_partitions` field validator rejecting `< 1`.
  `_rel_partition(df, n)` added next to `_project` in `run.py`
  (`coalesce(1)` for `n <= 1`, else `repartition(n)`). Applied to all
  seven relationship `write_rel` call sites in `_load`: HAS_VALUE,
  HAS_SCHEMA, HAS_TABLE, HAS_COLUMN, and the declared / metadata / semantic
  REFERENCES writes (the checklist parenthetical originally miscounted this
  as six; corrected in this revision). The `_load` docstring was updated to
  describe the configurable behavior.
  `tests/spark/test_rel_partition.py` asserts the helper's path with a spy
  DataFrame (`n <= 1` → `coalesce(1)` only, never `repartition`; `n > 1` →
  `repartition(n)` only);
  `tests/spark/test_load_rel_partition_wiring.py` asserts `_load` routes
  every one of the seven relationship writes through `_rel_partition` with
  the configured count and never writes one that bypassed it;
  `tests/spark/settings/test_partition_and_guardrail_settings.py` pins the
  default at 1 and the `< 1` rejection.

### Phase 4: Thin Guardrail and Documentation

Status: Complete

Goal: Add a small optional safety net and document the new settings.

Checklist:

- Add `dbxcarta_fk_max_columns: int = 0` to `SparkIngestSettings`, where 0
  means unlimited (guardrail disabled). The default being the disabled
  sentinel makes "default small-catalog behavior unchanged" true by
  construction, and keeps the backstop opt-in. This replaces V2's heavy
  guardrail phase, which is unnecessary once Phase 1 removes the driver
  collect.
- The behavior is **skip, not cap**. When the column count exceeds the
  limit, FK discovery does not run at all; extract and load still proceed.
  Capping by truncating the column set was considered and rejected: it
  produces silently partial and incorrect FK edges, a correctness hazard
  worse than emitting none.
- Obtain the column count from the already-materialized extract summary
  (`summary.extract.columns`). `extract()` sets this unconditionally via
  `columns_df.count()` and always runs before `run_fk_discovery`, so the
  count is always populated and no fallback `columns_df.count()` path is
  needed (an earlier draft floated one; it is unreachable and was dropped).
  Even a scalar `count()` would not violate the no-driver-collect rule
  (CLAUDE.md / best-practices §5), which forbids collecting catalog-scale
  rows, not reading one aggregate; state this so the guardrail is not
  misread as a Phase 1 regression.
- Gate inside `run_fk_discovery` (or immediately before its call in
  `run.py`): on trip, early-return an all-`None` `FKDiscoveryResult`,
  reusing the existing contract that the load step skips writes whose
  DataFrame is `None`. Do not invent a separate skip path.
- Record the skip in the run summary as a cohesive counter group on
  `RunSummary` (matching `ExtractCounts` / `SampleValueCounts`), serialized
  through `to_dict` / `_build_row_counts` alongside the FK counters.
  Logging alone (as `_should_run_semantic` does today) is not sufficient
  for this item. The serialized `row_counts` keys must be namespaced
  (e.g. `fk_discovery_skipped*`) and must not reuse the bare `fk_skipped`
  key, which belongs to declared-FK accounting and is invariant-checked by
  `verify.references._check_accounting`.
- Document the new settings inline in `settings.py` docstrings (consistent
  with the existing comment style there), document the Phase 3
  write-parallelism / Neo4j lock-contention tradeoff next to
  `dbxcarta_rel_write_partitions`, and add a cited rule to
  `docs/reference/best-practices.md` if one is established.

Validation:

- Tests cover below-limit (FK runs normally), above-limit (FK skipped,
  extract and load still write, summary records the skip), and the default
  (0 / disabled, behavior unchanged).
- The Phase 1/2 large-catalog bounded-driver-memory fixture runs with the
  guardrail disabled, so the guardrail cannot mask that OOM regression test.
  **Deferred (2026-05-16):** this depends on the Phase 1/2 fixture, which is
  deferred until later. The guardrail defaults to disabled (`0`), so it
  cannot mask such a test once that fixture exists; the dependency is
  recorded here so the criterion is not read as satisfied.

Notes:

- The guardrail is a backstop, not a substitute for the Phase 1 fix.
- Done (2026-05-16): `dbxcarta_fk_max_columns: int = 0` added with a
  `_validate_fk_max_columns` validator rejecting negatives (0 = unlimited /
  disabled). `_fk_guardrail_tripped(settings, summary)` and
  `_skipped_result()` added to `discovery.py`; `run_fk_discovery`
  early-returns the all-`None` `FKDiscoveryResult` before any FK work when
  the cap is exceeded, reusing the existing load-step None-edge contract.
  The column count is read from `summary.extract.columns`, already
  materialized by the extract stage (verified: `extract.py` sets it via
  `columns_df.count()` before `run_fk_discovery` is called), so no new
  driver action is introduced. The skip is recorded as an `FKSkipCounts`
  dataclass on `RunSummary.fk_skip` (`None` = did not fire) with an
  `as_row_counts()` flattened uniformly in `_build_row_counts`, structurally
  matching `ExtractCounts` / `SampleValueCounts` rather than loose fields
  (no `summary_io` schema change needed, since `row_counts` is an existing
  `MapType(String, Long)`). The serialized keys are namespaced
  `fk_discovery_skipped*`, **not** the bare `fk_skipped`: that key is owned
  by declared-FK accounting (`DeclaredCounters`, `fk_declared -
  fk_resolved`) and is invariant-checked by
  `verify.references._check_accounting`. A post-implementation review caught
  that the first cut reused `fk_skipped`, which made every guardrail trip
  raise a false `references.accounting_mismatch`; the namespaced keys fix
  it and a regression test pins it. Settings are documented inline in
  `settings.py` docstrings (including the Phase 3 write-parallelism / Neo4j
  lock-contention tradeoff), in `.env.sample`, in
  `docs/reference/best-practices.md`, and in `README.md`. No new rule was
  added to `best-practices.md`: this is operational tuning / a backstop,
  not a new pipeline design rule. `tests/spark/fk_guard/test_fk_column_guardrail.py`
  covers disabled-default, below / at / above limit, the summary record,
  the namespaced `row_counts` surfacing, non-collision with declared-FK
  `fk_skipped`, the verifier accounting regression, and the all-`None`
  result honoring the load contract. Open item. **Deferred
  (2026-05-16):** the Phase 1/2 synthetic large-catalog
  bounded-driver-memory fixture this validation references does not yet
  exist and is deferred until later; the guardrail's disabled-by-default
  (`0`) means it cannot mask such a test once written, but the fixture
  itself is an outstanding Phase 1/2 deliverable (see Phase 1/2
  validation).

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
  **Deferred (2026-05-16):** the synthetic large-catalog
  bounded-driver-memory fixture is deferred until later; this criterion
  is not yet satisfied and is tracked as an outstanding Phase 1/2
  deliverable.

## Recommended Order

Phase 0 first. It is cheap, isolated, and removes memory pressure.

Phase 1 next. It is the core fix and the reason for V3.

Phase 2 after Phase 1, not reordered ahead of it despite fixing the
largest driver-memory item. That item (the embedding collect) is behind
the default-off semantic gate, so it is not the ceiling for a default run,
and Phase 2 structurally depends on Phase 1's edges and shared
constraints/type-compat DataFrames. It cannot precede Phase 1.

Phases 3 and 4 last. They are tuning and backstops.

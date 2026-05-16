# Fixes: fix-spark-v3 Phases 1 & 2 (merged FK-path rewrite)

Scope: `fk/inference.py`, `fk/discovery.py`, plus one new guard test module.

Goal of this document: fix the issues that determine whether this is a solid
Spark pipeline at the ~10k-table target, and skip everything that is only an
argument about fidelity to the deleted Python. The proposal's own Goal says
output is judged on FK semantics and rule fixtures, not byte parity, so
round-mode, counter-wording, and "faithful port" critiques are out of scope.

Three changes, in implementation order:

1. Single-execution persistence lifecycle (core scale fix).
2. id/id handling: metadata removes the redundant blunt filter; semantic
   gates id↔id on value overlap instead of a hard name drop.
3. `candidates` becomes free off the persisted frame; correct its docstring.

Then one recommended addition: a regression guard test so 1 and 2 cannot
silently come back.

---

## 1. (Core) The heavy join graph executes 5 to 7 times; it must execute once

### Finding

`columns_frame` and `pk_gate` are built once in `run_fk_discovery` and
consumed by three things: `build_pk_gate`, `infer_metadata_edges`, and
`infer_semantic_edges`. Nothing is persisted anywhere, so every standalone
action re-runs the full lineage beneath it. The recompute fan-out at the 10k
target:

- `build_pk_gate`: `composite_pk_count = pk_grouped…count()` forces a full
  constraints aggregation pass.
- `infer_metadata_edges`: `above.count()` runs columns_frame + pk_gate +
  explode/join/dedup/type/PK/score. The attenuation branch rebuilds `above`
  (same pipeline again). `edges.cache().count()` runs it a third time.
- `infer_semantic_edges`: `cand.count()` builds the candidate join. The cosine
  step rebuilds `cand`. `edges.cache().count()` again. Then
  `scored.filter(F.col("_corr")).count()` at the end recomputes cosine plus
  the value-overlap join a fourth time, after `edges` is already cached.

`columns_frame` and `pk_gate` are rebuilt underneath every one of those. This
is the exact n²-shaped cost the rewrite exists to remove, paid five to seven
times to produce two telemetry numbers.

Only `accepted` is load-bearing: `discovery.py` does
`metadata_out = metadata_edges_df if metadata_counts.accepted else None` and
the same for semantic, and `accepted` comes off `edges.cache().count()`, which
materializes the frame about to be written to Neo4j anyway. Everything else
that triggers a pass is avoidable.

### Fix: one deliberate persistence lifecycle, not scattered `.cache()`

`run_fk_discovery` owns the shared-frame lifecycle. The returned edge caches
stay owned by `FKDiscoveryResult.unpersist_cached`; do not merge the two.

In `run_fk_discovery`:

- `columns_frame.persist(StorageLevel.MEMORY_AND_DISK)` right after
  `build_columns_frame`. Three consumers, potentially large at 10k tables, so
  disk fallback (not plain `.cache()` / MEMORY_ONLY) so eviction cannot cause
  a silent recompute.
- `pk_gate.cache()` after `build_pk_gate`. Two consumers, one row per PK-like
  column, small enough for memory.
- After both `infer_metadata_edges` and `infer_semantic_edges` have returned
  (semantic may be gated off), `columns_frame.unpersist()` and
  `pk_gate.unpersist()`. These are internal intermediates; their lifecycle
  ends inside `run_fk_discovery`, separate from the returned edge frames.

In `infer_metadata_edges`:

- `above.persist(StorageLevel.MEMORY_AND_DISK)`.
- `candidates = above.count()` now fills the cache in one pass.
- Attenuation reads the persisted `above`, no upstream recompute.
- `edges` is built, `accepted = edges.cache().count()`.
- `above.unpersist()` after the `accepted` count. Metadata has no later action
  that depends on `above`, so this is safe here.

In `infer_semantic_edges` the order matters because `value_corroborated`
depends on `scored`, which is downstream of `cand` and computed after
`edges.cache().count()`:

- `cand.persist(StorageLevel.MEMORY_AND_DISK)`; `candidates = cand.count()`
  fills it in one pass; the cosine step reads the persisted `cand`.
- Build `scored` (cosine, threshold, base conf, value-overlap join,
  `_corr`/`_conf`), then `scored.persist(StorageLevel.MEMORY_AND_DISK)`.
- `cand.unpersist()` once `scored` is materialized.
- `edges` is built from `scored`; `accepted = edges.cache().count()`.
- `value_corroborated = scored.filter(F.col("_corr")).count()` reads the
  persisted `scored`, free, no cosine/overlap recompute.
- `scored.unpersist()` after `value_corroborated`.

Result: the heavy graph runs once per strategy. `columns_frame`/`pk_gate`
compute once and feed all three consumers from cache.

Lower-priority follow-on (note, not blocking): `composite_pk_count` still
forces a small constraints pass inside `build_pk_gate`. It is cheap relative
to the n² graph; leave it unless profiling shows it matters, in which case
persist the `pk_grouped` aggregation and derive both the single-PK set and the
composite count from it.

---

## 2. id/id handling: split metadata and semantic

### Finding

`~((lower(s_column)=='id') & (lower(t_column)=='id'))` is applied to `matched`
in metadata and folded into the `cand` join in semantic. It drops every pair
where both columns are literally `id`, i.e. all `A.id → B.id`, including the
legitimate shared-primary-key / 1:1 extension pattern
(`user.id → user_profile.id`). The two paths need different handling because
they have different machinery.

Metadata is name-only. Source `A.id` emits link key `E|id`, which matches
every `id` column in the schema, so its attenuation `cnt` is on the order of
the table count in the schema regardless of which target is real. Attenuation
therefore collapses every id→id score below threshold whether the blunt filter
is present or not. The filter is redundant in metadata: it suppresses noise
attenuation already suppresses, and by name alone metadata cannot distinguish
a real shared-PK FK from a generic `id` collision anyway. Real shared-PK FKs
in this catalog are declared, so the declared path already captures them; the
dedicated value-containment rule is explicitly not being built.

Semantic has no attenuation curve. Two columns both named `id` have
near-identical embeddings, so cosine fires on every id↔id pair catalog-wide
with nothing to damp the fan-out. Semantic genuinely needs a suppressor, but a
hard name drop also erases the real 1:1 case there, and the signal that
actually separates the two is value containment, which semantic already
computes in `build_value_overlap`.

### Fix

Metadata (`infer_metadata_edges`): remove the `~(s_column=='id' &
t_column=='id')` predicate from the `src.join(tgt, …)` condition. Remove the
`s_column`/`t_column` selects that exist only to feed it once they have no
other use. Attenuation remains the sole arbiter of id→id, which is correct:
generic `id` fans out wide and is attenuated away; genuine shared-PK FKs are
caught by the declared path.

Semantic (`infer_semantic_edges`): remove the same blunt predicate from the
`cand` join. Instead, require value-overlap corroboration for id↔id pairs
specifically. `build_value_overlap` already produces the asymmetric
`|src∩tgt|/|src|` ratio; gate id↔id candidates on `ratio >= overlap_threshold`
rather than dropping them by name. Non-id pairs are unaffected. This keeps
semantic from spraying generic id↔id garbage (unrelated id columns have low
containment) while letting a true 1:1 split (near-total containment) survive.
This reuses the existing overlap computation; it is not the separate
value-containment strategy that was declined.

### Fixtures

- Metadata: a schema with many tables each having an `id` column produces no
  id→id edges (attenuation), and the canonical `orders.customer_id →
  customers.id` still produces its edge.
- Semantic: two unrelated `id` columns with low value overlap produce no edge;
  a `user.id` / `user_profile.id` pair with high asymmetric containment
  produces one.

---

## 3. `candidates`: free off the persisted frame, and fix its docstring

### Finding

`candidates` and `rejected` flow only into `summary.fk_*` and one
`logger.info`. No control flow reads them. They are pre-filter-effectiveness
telemetry: the signal that proves the pipeline did not regress to n². They do
not justify a separate materialization. Separately, the `CoarseFKCounts`
docstring says metadata `candidates` is "post name-match/dedup"; the code sets
it post type/PK/score-threshold.

### Fix

Keep `candidates` in the summary. With fix 1 it is taken off the persisted
`above` / `cand`, so it costs no extra pass: the frame is materialized for
attenuation/cosine regardless, and counting rows already in cache is free.

Correct the `CoarseFKCounts` docstring to state precisely what metadata
`candidates` is: "post type/PK/score-threshold, pre-attenuation" for metadata,
"post pre-filter" for semantic. Note in one line that this intentionally
differs from the old Python `candidates` so a later reader does not revert it.
This is a docstring edit folded into the file already being changed; no
behavior change.

---

## 4. (Recommended) Regression guard so 1 and 2 cannot silently return

Without a guard, a future edit reintroduces the multi-pass recompute or a
driver collect with no test failure, and the foundation rots. The proposal's
Completion Criteria already require "a synthetic large-catalog run completes
with bounded driver memory"; this satisfies it as a repeatable artifact rather
than a manual `grep`.

One new test module, two deterministic checks (no reliance on real OOM):

Structural plan guard. Assert on a few stable operator names, not the full
plan string (which is version-brittle):

```python
plan = metadata_edges_df._jdf.queryExecution().toString()
for op in ("Generate", "Window", "BroadcastHashJoin"):
    assert op in plan, f"{op} missing — FK plan regressed"
```

`Generate` proves the suffix-array explode survived, `Window` the
dedup/attenuation, `BroadcastHashJoin` the `_SCORE_TABLE` broadcast.

Bounded-driver guard. Use `spark.driver.maxResultSize` as the tripwire, not
heap exhaustion. A reintroduced catalog-scale `.collect()` exceeds
`maxResultSize` deterministically and fast; real OOM is flaky and slow:

```python
spark = SparkSession.builder ... \
    .config("spark.driver.maxResultSize", "10m").getOrCreate()
# fabricate ~5000 synthetic columns via spark.range(...).selectExpr(...)
edges, counts, _ = infer_metadata_edges(spark, cf, pk_gate, None)
assert counts.accepted >= 0   # liveness: completed without a driver transfer
```

The guard is the absence of an exception; the assertion is a liveness touch.
`candidates` from fix 3 doubles as the pre-filter-effectiveness check here:
assert it is a small fraction of the synthetic Cartesian, which is the Phase 2
validation gate. Mark the module slow and keep it separate.

---

## Explicitly out of scope

- Round vs `bround`. Affects only a 4th-decimal display value at exact
  `.00005` boundaries and only matters because a fixture over-asserts. Not a
  pipeline-quality issue. If a fixture is brittle, loosen the fixture
  tolerance; do not treat it as a code defect.
- Faithful-port / byte-parity arguments. The proposal Goal disowns output
  parity; correctness is the rule fixtures. Not pursued.
- The dedicated embedding-independent value-containment strategy for id→id.
  Considered and declined: real shared-PK FKs are declared and covered by the
  declared path, and semantic handles the inferred remainder.

## Confirmed correct (no action)

- All four catalog-scale driver collects (columns, constraints, embeddings,
  value-index) are gone; `discover_declared`'s bounded resolved-FK collect is
  correctly retained per the Assessment.
- `canonicalize_expr` type-token order and decimal-scale extraction match
  `common.canonicalize`.
- Attenuation `score / greatest(1.0, sqrt(cnt-1))` over the post
  dedup/type/pk/score window is the intended curve and is computed before the
  anti-join.
- Name-match EXACT-over-SUFFIX dedup, the `S|`/`E|` key-prefix isolation, and
  the tgt table-form generation correctly implement the spec'd rule.
- Prior-pair threading: metadata anti-joins declared-only; semantic anti-joins
  declared ∪ metadata via `_union_pairs`.
- Asymmetric overlap `inter / src_size` with `src_size` the distinct
  source-column value count reproduces the intended `overlap_ratio`.
- In-join cosine: zero-norm guarded, `dot / (sqrt(na)*sqrt(nb))`, persisted
  embeddings never normalized or mutated.

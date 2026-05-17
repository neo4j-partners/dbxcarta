# Fixes: fix-spark-v3 Phases 1 & 2 (refined FK-path rewrite)

Scope: `fk/inference.py`, `fk/discovery.py`, `summary.py` (telemetry surface
only), updates to existing tests that assert the dropped counters
(`tests/spark/fk_metadata/test_fk_metadata.py`,
`tests/spark/fk_semantic/test_infer_semantic_pairs.py`), plus one new guard
test module.

Telemetry contract decision (settled): dropping `fk_inferred_*_candidates`,
`*_rejected`, and `*_value_corroborated` from the summary dict is an
intentional contract change. In-repo the only consumed key is
`fk_inferred_*_accepted` (`verify/references.py:31-32`); nothing reads the
dropped keys, so removal is functionally safe here. Any external dashboard or
doc that references the old keys must be updated out-of-band; that is accepted,
not a blocker.

Goal of this document: fix the issues that determine whether this is a solid
Spark pipeline at the ~10k-table target, and skip everything that is only an
argument about fidelity to the deleted Python. Output is judged on FK
semantics and rule fixtures, not byte parity, so round-mode, counter-wording,
and "faithful port" critiques are out of scope.

This is the refined plan. It supersedes an earlier draft that (a) persisted
three large per-strategy intermediates to make non-load-bearing telemetry
cheap, and (b) removed cheap early `id↔id` prunes and re-derived the result
downstream. Both traded away the scale property this project exists to
protect. The decisions below were settled first:

- **Telemetry (`candidates`, `rejected`, `value_corroborated`) is
  guard-only.** It is not computed on the production path. This is what makes
  §1 simple: with those counts gone, each strategy takes exactly one Spark
  action, so the heavy graph runs once with no need to persist intermediates.
- **Cheap early prunes stay.** On an explicitly n²-sensitive pipeline you do
  not delete a pushdown-able predicate and re-sort the garbage at the exit.

Order: 1 (single-execution) → 2 (id/id) → 3 (folded into 1) → 4 (guard).

---

## 1. (Core) The heavy join graph must execute once per strategy

### Finding

`columns_frame` and `pk_gate` are built once in `run_fk_discovery`
(`discovery.py:80,83`) and consumed across three jobs: `build_pk_gate`,
`infer_metadata_edges`, `infer_semantic_edges`. Nothing is persisted, so each
standalone action re-runs the lineage beneath it.

Within each strategy the only **load-bearing** action is
`edges.cache().count()` (`inference.py:474`, `:663`): `discovery.py` does
`metadata_out = metadata_edges_df if metadata_counts.accepted else None` and
the equivalent for semantic, and that materializes the frame about to be
written to Neo4j anyway.

Every other action is non-load-bearing telemetry:

- Metadata: `candidates = above.count()` (`:443`). `above` is then reused by
  attenuation, forcing a recompute of the explode/join/dedup/type/PK/score
  pipeline.
- Semantic: `candidates = cand.count()` (`:618`) before the cosine step
  rebuilds `cand`; `value_corroborated = scored.filter(F.col("_corr")).count()`
  (`:667`) recomputes `scored` after `edges` is already cached.

With the telemetry decision settled (guard-only), the fix is not to persist
`above`/`cand`/`scored` so the extra counts are cheap. It is to **remove the
extra counts**, so there are no extra actions to be cheap about.

### Fix

**Remove the non-load-bearing telemetry from the production path.** Concrete,
enumerated touch points so a later reader sees this was deliberate, not a
silent regression:

- `inference.py` `CoarseFKCounts`: drop the `candidates`, `rejected`, and
  `value_corroborated` fields. Keep `accepted` (load-bearing) and
  `composite_pk_skipped`. The latter is *not* free — its source
  `composite_pk_count = pk_grouped…count()` (`:236`) is a `.count()` action —
  but it is one small constraints aggregation, not the n² graph, and it
  already runs today inside `build_pk_gate`; keeping the field adds no new
  action.
- `inference.py` `CoarseFKCounts.as_summary_dict` (`:69`): drop the
  `{prefix}_candidates`, `{prefix}_rejected`, `{prefix}_value_corroborated`
  entries. `summary.py:216,218` call this and shrink automatically; no change
  there.
- `inference.py` `infer_metadata_edges`: delete `candidates = above.count()`
  (`:443`) and the `candidates=`/`rejected=` args to the `CoarseFKCounts(...)`
  construction (`:477`).
- `inference.py` `infer_semantic_edges`: delete `candidates = cand.count()`
  (`:618`) and `value_corroborated = scored.filter(...).count()` (`:667`) and
  the corresponding `CoarseFKCounts(...)` args (`:669`).
- `discovery.py`: the two `logger.info` lines (`:101-105`, `:124-129`) print
  `metadata_counts.candidates` / `semantic_counts.candidates` /
  `.value_corroborated`. Reduce them to `accepted` (and
  `composite_pks_skipped`, which stays).
- Existing tests assert the dropped fields and will fail until updated:
  `tests/spark/fk_metadata/test_fk_metadata.py:134` (`counts.rejected ==
  max(0, counts.candidates - counts.accepted)`), `:201`
  (`counts.candidates >= 3`); `tests/spark/fk_semantic/
  test_infer_semantic_pairs.py:171` (same rejected invariant), `:207`, `:223`,
  `:242` (`counts.value_corroborated …`). Rewrite these to assert on
  `accepted` (and, where the test's intent is pre-filter effectiveness or
  value corroboration, move that assertion into the new §4 guard, which
  recomputes it from the synthetic input). This is part of the change, not a
  follow-up.

Result: each strategy now has exactly one action (`edges.cache().count()`).
`above`, `cand`, and `scored` are each computed once inside that single job.
No `.persist()` on any per-strategy intermediate. This is less code and puts
nothing n²-sized in memory or on disk at the 10k target.

**Persist only the two genuinely shared frames.** `columns_frame` and
`pk_gate` still feed up to three separate jobs (`build_pk_gate`, then the two
strategy actions). Persist them once in `run_fk_discovery`:

- `columns_frame.persist(StorageLevel.MEMORY_AND_DISK)` immediately after
  `build_columns_frame` (`discovery.py:80`). Disk fallback, not `.cache()` /
  MEMORY_ONLY, so eviction at 10k tables cannot cause a silent recompute.
  Note: marking it persisted does not materialize it. `build_pk_gate`'s only
  action is `composite_pk_count = pk_grouped…count()` (`inference.py:236`),
  which operates on `constraints_df`/`pk_grouped`, **not** on `columns_frame`
  or the returned `pk_gate`, so it does not warm the columns cache. The
  columns cache fills lazily on the **first inference action**
  (`edges.cache().count()` in metadata); every later consumer reads from
  cache. If a warm cache before the first strategy is ever wanted, add an
  explicit `columns_frame.count()` right after the persist — not proposed
  here, since the first inference action would materialize it anyway and the
  extra count is itself an action.
- `pk_gate.cache()` after `build_pk_gate` (`:83`). One row per PK-like
  column, small enough for memory. Same lazy-fill caveat: it materializes on
  first use, not at the `.cache()` call.
- After both strategies have returned (or after metadata if semantic is gated
  off), `columns_frame.unpersist()` and `pk_gate.unpersist()`. These are
  internal intermediates; their lifecycle ends inside `run_fk_discovery`.
  **Wrap the body in `try` / `finally`** so the two `unpersist()` calls run on
  the failure path too; otherwise a failed FK discovery leaks both cached
  frames into the session for the rest of the job. Do **not** fold this into
  `FKDiscoveryResult.unpersist_cached`, which owns the returned edge caches
  and is a separate lifecycle. The existing
  `metadata_edges_df.unpersist()` / `semantic_edges_df.unpersist()`
  drop-on-empty paths are unaffected.

Lower-priority follow-on (note, not blocking): `composite_pk_count` still
forces a small constraints aggregation inside `build_pk_gate`. It is cheap
relative to the n² graph; leave it unless profiling says otherwise, in which
case persist the `pk_grouped` aggregation and derive both the single-PK set
and the composite count from it.

---

## 2. id/id handling: keep the cheap prune, rescue the real pair

### Finding

`~((lower(s_column)=='id') & (lower(t_column)=='id'))` is applied in the
metadata `src.join(tgt, …)` condition (`inference.py:403-406`) and folded into
the semantic `cand` join (`:601-602`). It drops every pair where both columns
are literally `id`, including the legitimate shared-primary-key / 1:1
extension pattern (`user.id → user_profile.id`). The two paths differ.

**Metadata is name-only and self-corrects.** Source `A.id` emits link key
`E|id`, matching every `id` column in the schema. Attenuation is
`score / greatest(1.0, sqrt(cnt-1))` over the post-dedup/type/PK/score window;
with `cnt` on the order of the id-column count, a ~0.9 exact-name score over
thousands of id columns collapses far below threshold. So the blunt filter is
**redundant for correctness** in metadata: attenuation already annihilates the
fanout, and a genuine shared-PK FK is declared and caught by the declared
path. The earlier draft concluded "redundant, therefore remove it." That is
backwards. The predicate is a cheap, pushdown-able prune of an
id-count × id-count cartesian *before* the join/window/dedup. Removing it adds
a large useless intermediate to the heaviest region of the pipeline for zero
correctness gain.

**Semantic has no attenuation curve.** Two columns both named `id` have
near-identical embeddings, so cosine fires on every id↔id pair catalog-wide
with nothing to damp the fan-out. Semantic genuinely needs a suppressor; a
hard name drop also erases the real 1:1 case; the signal that separates them
is value containment, which semantic already computes in
`build_value_overlap`.

### Fix

**Metadata (`infer_metadata_edges`): keep the predicate.** Leave the
`~(s_column=='id' & t_column=='id')` term in the `src.join(tgt, …)` condition
exactly as is. Add a one-line comment that this is a scale prune, not a
correctness gate (attenuation would also suppress id→id; genuine shared-PK FKs
are caught by the declared path), so a later reader does not "simplify" it
away. No `s_column`/`t_column` select removal; they feed this predicate.

**Semantic (`infer_semantic_edges`): keep the early prune, add a narrow
rescue.** Do not remove the blunt term from the `cand` join; it keeps the
id↔id cartesian out of the cosine and value-overlap joins. Instead,
re-admit *only* the id↔id subset that passes value containment:

- Build the small id↔id candidate set separately (both columns lower = `id`,
  same `(catalog, schema)`, target in `pk_gate`, prior anti-join applied).
- Call `build_value_overlap` on **that id↔id set only**, giving the
  asymmetric `|src∩tgt|/|src|` ratio; keep rows with
  `ratio >= overlap_threshold`.
- Union the survivors back into `cand` before cosine.

Two scoped overlap computations, chosen deliberately. This reuses the
`build_value_overlap` *function*, not the existing computation: there are now
two invocations — the new one on the small pre-cosine id↔id set, and the
unchanged existing one on post-cosine `scored` (`inference.py:641`). Do **not**
merge them into one shared pre-cosine overlap frame. A single shared frame
would have to compute overlap over the *full pre-cosine candidate set*, which
is materially larger and more expensive than today's post-cosine
`build_value_overlap(scored, …)` over the already-thresholded set. The two
scoped computations keep total cost at roughly today's post-cosine overlap
plus a small bounded id-only extra; the wider single-frame variant is
explicitly rejected on cost. Each invocation is its own scoped subgraph, so
there is no shared sub-DAG to recompute and no persist needed.

Value-gated, by design. `build_value_overlap` returns `None` when
`value_node_df`/`has_value_df` are absent (no sampled values —
`inference.py:502-503`). When it returns `None` the id↔id rescue cannot fire,
so a genuine `user.id → user_profile.id` 1:1 split is silently dropped from
the semantic path. This degradation is accepted: without sampled values there
is no containment signal to distinguish it from generic id↔id noise, and a
declared shared-PK FK is still caught by the declared path. State it so the
fixtures below are read as value-present guarantees, not unconditional ones.

This sprays no generic id↔id garbage (unrelated id columns have low
containment) while letting a true 1:1 split survive. It is not the separate
value-containment strategy that was declined. The rescue set is **not** the
full column Cartesian, but it is still quadratic in id columns: roughly
`id_source_count × id_target_pk_count` per `(catalog, schema)`. That is far
below the full cross-product and is the bounded extra cost noted above; it
does not reintroduce the n² over all columns that §1 removes.

### Fixtures

- Metadata: a schema with many `id`-bearing tables produces no id→id edges
  (the predicate prunes them; attenuation is the backstop), and the canonical
  `orders.customer_id → customers.id` still produces its edge.
- Semantic, direction pinned: `build_value_overlap` is asymmetric
  (`|src∩tgt|/|src|`). For a true 1:1 split, candidate
  `user_profile.id` (src) → `user.id` (tgt, in `pk_gate`) has ratio ≈ 1.0 and
  is rescued; the reverse-direction candidate `user.id` (src) →
  `user_profile.id` has a low ratio (not every user has a profile) and is
  correctly *not* rescued. Two unrelated `id` columns with low overlap produce
  no edge in either direction. Assert the surviving edge is the child→parent
  direction specifically, so the fixture cannot pass for the wrong reason.

---

## 3. `candidates` docstring — folded into §1

In the earlier draft this was a standalone docstring correction
("post type/PK/score-threshold, pre-attenuation"). With the §1 telemetry
decision, `candidates` / `rejected` / `value_corroborated` no longer exist on
the production path, so there is no field left to document. The residual work
is to purge the now-stale design narrative from **all three** places that
describe the dropped counters, not just one:

- The module docstring (`inference.py:23-25`): "Counters are coarse by
  design… the counter invariant" — rewrite to describe only `accepted` /
  `composite_pk_skipped`.
- The `CoarseFKCounts` class docstring (`:55-61`): currently defines
  `candidates` and the `rejected = candidates - accepted` invariant — replace
  with an accurate description of the *remaining* fields (`accepted`:
  post-attenuation, post-anti-join, the written set; `composite_pk_skipped`:
  the cheap constraints aggregation from `build_pk_gate`).
- One line in the class docstring stating the dropped counters moved to the
  §4 guard, so a later reader does not reinstate them on the hot path.

No separate work item; this is part of the §1 edit to the same file.

---

## 4. (Recommended) Regression guard so 1 and 2 cannot silently return

Without a guard, a future edit reintroduces the multi-pass recompute or a
driver collect with no test failure. One new test module, marked slow, kept
separate. Three deterministic checks, no reliance on real OOM.

**Single-execution guard (the one that actually locks §1).** The
operator-presence and driver-size checks below do *not* enforce "executes once
per strategy" — that invariant is the whole point of §1 and nothing else here
guards it. Assert the Spark job count directly. Wrap each strategy call and
diff `spark.sparkContext.statusTracker().getJobIdsForGroup()` (set a job group
around the call), or register a lightweight `SparkListener`/
`QueryExecutionListener` and count `onJobStart`. The metadata strategy must
trigger exactly one job (the single `edges.cache().count()`); a reintroduced
standalone `.count()` on `above`/`cand`/`scored`, or a lost `columns_frame`
persist that forces a rebuild, pushes the count above one and fails the test:

```python
before = set(sc.statusTracker().getJobIdsForGroup("fk_meta"))
sc.setJobGroup("fk_meta", "metadata strategy")
infer_metadata_edges(spark, cf, pk_gate, None)
jobs = set(sc.statusTracker().getJobIdsForGroup("fk_meta")) - before
assert len(jobs) == 1, f"metadata ran {len(jobs)} jobs — §1 regressed"
```

(Exact count is implementation-defined; pin it to the value observed on the
correct implementation and treat any increase as a regression.) This is the
check that makes §1's "executes once" claim enforced rather than asserted.

**Structural plan guard.** Assert only on operators that are stable across
Spark versions and AQE. Drop any join-strategy assertion: AQE on Databricks
defaults can demote a broadcast or rewrite to shuffled hash join, so
`BroadcastHashJoin in plan` is a false-alarm generator. Keep the two
structurally stable operators:

```python
plan = metadata_edges_df._jdf.queryExecution().toString()
for op in ("Generate", "Window"):
    assert op in plan, f"{op} missing — FK plan regressed"
```

`Generate` proves the suffix-array explode survived; `Window` proves the
dedup/attenuation window survived. These are logical-plan operators AQE does
not rewrite.

**Bounded-driver guard.** Use `spark.driver.maxResultSize` as the tripwire,
not heap exhaustion. A reintroduced catalog-scale `.collect()` exceeds
`maxResultSize` deterministically and fast; real OOM is flaky and slow:

```python
spark = SparkSession.builder ... \
    .config("spark.driver.maxResultSize", "10m").getOrCreate()
# fabricate synthetic columns via spark.range(...).selectExpr(...)
edges, counts = infer_metadata_edges(spark, cf, pk_gate, None)
assert counts.accepted >= 0   # liveness: completed without a driver transfer
```

Size the synthetic catalog so that an accidental `.collect()` of even just the
id columns would exceed 10m; a few thousand columns of bare ids may not, so
scale the fabricated rows (wider rows or more of them) until a deliberately
reintroduced collect trips it. The guard is the absence of an exception; the
assertion is a liveness touch.

**Pre-filter-effectiveness check (replaces the removed `candidates`
telemetry).** Since `candidates` is no longer returned, the guard computes its
own pre-filter set size directly from the synthetic input (one extra action in
a test is fine and is the point) and asserts it is a small fraction of the
synthetic Cartesian. This is the Phase 2 validation gate, now living where it
belongs rather than on every production run.

---

## Explicitly out of scope

- Round vs `bround`. Affects only a 4th-decimal display value at exact
  `.00005` boundaries and only matters because a fixture over-asserts. If a
  fixture is brittle, loosen the fixture tolerance; not a code defect.
- Faithful-port / byte-parity arguments. The proposal Goal disowns output
  parity; correctness is the rule fixtures. Not pursued.
- The dedicated embedding-independent value-containment strategy for id→id.
  Declined: real shared-PK FKs are declared and covered by the declared path,
  and semantic's narrow value-overlap rescue (§2) handles the inferred
  remainder.
- Persisting `above` / `cand` / `scored`. Considered and rejected: it only
  buys cheap telemetry, and §1 removes that telemetry from the hot path
  instead, which is strictly cheaper.

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
- The `to_references_rel` / `references_schema` centralization already in the
  working tree is orthogonal to this plan and compatible with it; the explicit
  per-column cast it adds reinforces declared/inferred schema conformance.

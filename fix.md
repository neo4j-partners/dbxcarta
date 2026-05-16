# Review: fix-spark-v3.md Phases 1 & 2 (merged FK-path rewrite)

Scope: `fk/inference.py`, `fk/discovery.py`, `fk/declared.py`, `fk/metadata.py`,
`fk/common.py`, `summary.py`, and the rewritten `tests/spark/fk_*`.

Verdict: the port is largely faithful. The name-match-into-join-key design,
`canonicalize_expr`, the PK gate, the attenuation curve, the asymmetric value
overlap, and the in-join cosine all match the deleted Python implementations.
The items below are the gaps worth acting on. Nothing here was changed in code —
this is the proposed-fix list for discussion.

---

## 1. (High) The n²-shaped join is materialized two-to-three times

`infer_metadata_edges`:

```python
above = scored.filter(F.col("score") >= F.lit(threshold))
candidates = above.count()          # full explode→join→dedup→type→pk→score pass
att_w = Window.partitionBy("source_id")
attenuated = above.withColumn("_cnt", ...) ...   # rebuilds `above` (uncached)
...
accepted = edges.cache().count()    # rebuilds most of it again
```

`above` is never cached, so `above.count()` runs the entire
explode → link-key join → dedup window → type filter → PK join → broadcast
score join, and then the attenuation branch runs that same pipeline a second
time. `infer_semantic_edges` has the identical shape: `candidates =
cand.count()` materializes the candidate join, then the cosine/overlap branch
recomputes `cand`.

This is the exact cost the proposal exists to remove (the n²-shaped FK work),
now paid 2× at the ~10k-table target purely to produce a coarse counter.

Proposed fix: persist the working frame before counting, e.g.

```python
above = scored.filter(F.col("score") >= F.lit(threshold)).cache()
candidates = above.count()
...   # attenuation reuses the cached frame
above.unpersist()   # after `edges` is materialized
```

and likewise `cand = cand.cache()` before `cand.count()` in
`infer_semantic_edges`, unpersisted after `edges.cache().count()`. (Alternative:
drop the standalone candidate count and derive a coarse "candidates" from a
frame already being materialized — the proposal explicitly permits coarse
counts and no invariant.)

## 2. (Decision needed) The `id`/`id` exclusion deviates from the ported rule

The working tree adds, in both `infer_metadata_edges` and
`infer_semantic_edges`:

```python
matched = matched.filter(~((lower(s_column) == 'id') & (lower(t_column) == 'id')))
```

The deleted Python `_name_match` returns `EXACT` for any `src_l == tgt_l`,
including `id == id`, and `pk_kind` classifies a target column named `id` as
`UNIQUE_OR_HEUR`. So the Python implementation **did** emit `A.id → B.id`
edges for every same-schema table pair with an `id` column (heavily attenuated,
but emitted). This new residual predicate removes that whole class of edges.

That is a real behavior change against a port the proposal repeatedly says is
"reproduced as-is, not changed." It may well be a desirable correctness
improvement (the `id↔id` fan-out is mostly noise), but it is a deliberate
deviation, not a faithful port, and it is currently an undocumented inline
filter.

This needs a user decision before any code change — do not silently keep or
silently revert:
- Option A: revert the filter, stay byte-faithful to the Python rule.
- Option B: keep it, and record it explicitly in fix-spark-v3.md as an
  intentional, justified exception to the "faithful port" rule (with the same
  rigor as the attenuation-curve and overlap-divisor notes), so the deviation
  is auditable.

## 3. (Medium) Phase 1/2 validation artifacts are missing from the suite

Both phases' Validation sections and the Completion Criteria require:
- a structural `explain()`-plan assertion (link-key join, suffix-array explode,
  dedup row_number window, PK-gate join, broadcast `_SCORE_TABLE` join,
  `count() over partitionBy(source_id)` window); and
- a synthetic large-catalog fixture run under a bounded driver-memory ceiling
  "that would OOM if a collect were reintroduced."

Neither exists in `tests/spark/fk_metadata`, `fk_semantic`, or `fk_discovery`.
The proposal's own "Done" notes concede this was verified by manual `grep` and
ad-hoc `explain()` inspection, not an automated test. The Completion Criteria
clause "A synthetic large-catalog run completes with bounded driver memory" is
therefore unmet as a repeatable artifact.

Proposed fix: add (a) one test asserting the expected operators appear in
`metadata_edges_df._jdf.queryExecution().toString()` (or `df.explain(True)`
captured), and (b) a generated wide-catalog fixture (e.g. a few thousand
synthetic columns) run with a deliberately low `spark.driver.maxResultSize` /
driver heap so a reintroduced `.collect()` fails the test. Until then, "no
driver collect" is asserted only by inspection.

## 4. (Low) `CoarseFKCounts.candidates` does not match its own docstring

The docstring says metadata `candidates` is "post name-match/dedup". The code
sets `candidates = above.count()` — that is post type filter, PK gate, and
`score >= threshold`, not post-dedup. (Semantic's `cand.count()` does match its
"post pre-filter" description.) Either correct the docstring or move the count
to the dedup frame. Cosmetic, but it is the kind of drift that misleads later
readers since the counter is explicitly the only bookkeeping that survived.

## 5. (Low / informational) round() mode

Python `round(x, 4)` is banker's rounding; Spark `F.round` is HALF_UP.
Confidences can differ by one ulp at exact `.00005` boundaries. The proposal
does not require byte-for-byte equality, so this is acceptable — noting it only
so it is a known, accepted divergence rather than a surprise when fixture
expectations are tightened.

---

## Checked and confirmed correct (no action)

- Self-reference / same-`(table, column)` guard: `generate_id` lowercases each
  part, so case-variant duplicate columns collapse to an equal `col_id`;
  `source_id != target_id` therefore subsumes both the Python `src is tgt`
  skip and the `table_key + column.lower()` guard. Faithful.
- `canonicalize_expr`: string/decimal/integer-family order and the decimal
  scale extraction (regex group 4 → `"0"` when absent) match `canonicalize`.
- Attenuation: `score / greatest(1.0, sqrt(cnt-1))` is identical to
  `score / max(1.0, sqrt(max(0, n-1)))` for every n (verified n=1,2,3,5),
  computed over the correct window set (post dedup/type/pk/score, pre
  anti-join).
- Name-match EXACT-over-SUFFIX dedup correctly reproduces the Python
  `_name_match` short-circuit; the `S|`/`E|` key prefixes prevent cross-branch
  joins; suffix table-form generation (`t`, `t[:-1]` on `s`, `t[:-2]` on `es`)
  correctly inverts `_stem_matches_table`.
- Prior-pair threading: metadata anti-joins declared-only; semantic anti-joins
  `declared ∪ metadata` via `_union_pairs`. Matches the proposal.
- Asymmetric overlap: `inter / src_size` with `src_size` the distinct
  source-column value count reproduces `ValueIndex.overlap_ratio` exactly,
  including the empty-set → no-corroboration behavior.
- In-join cosine: zero-norm guard plus `dot / (sqrt(na)*sqrt(nb))` matches
  `_cosine`; persisted embeddings are never normalized or mutated.
- All four driver collects (columns, constraints, embeddings, value-index) are
  gone; `discover_declared`'s bounded resolved-FK collect is correctly
  retained per the Assessment.

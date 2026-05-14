# dense_500 Tuning Plan

> **No grader workarounds.** When the model produces correct SQL that the
> grader scores as wrong, fix the grader. Do not add prompt instructions
> designed to make the model conform to grader limitations. Grader workarounds
> produce misleading benchmark numbers and restrict model output for the wrong
> reasons. If a prompt instruction exists to satisfy the grader rather than
> to produce better SQL, remove it and fix the grader instead.

**Status: Phases A–D complete. Grader fixed. Prompt audited. Next: regenerate
question set per Phase D spec, upload wheel, rerun client, compare results.**

The Phase G dense_500 benchmark run (2026-05-13) produced clear arm separation
at n=59 but graph_rag trailed schema_dump on correctness: 55.9% (33/59) versus
69.8% (37/53). graph_rag executes every query (100% exec rate), so the failure
is not schema selection. This plan works through the root causes in
diagnosis-first order.

---

## Baseline numbers

| Metric | no\_context | schema\_dump | graph\_rag |
|---|---|---|---|
| Attempted | 59 | 59 | 59 |
| Executed | 2 | 53 | 59 |
| Correct (of executed) | 2 | 37 | 33 |
| Exec rate | 0.034 | 0.898 | 1.000 |
| Correct rate | 1.000 | 0.698 | 0.559 |

Gap to close: graph_rag needs roughly 5 more correct answers (38/59) to match
schema_dump's 0.698.

---

## Changes Applied

### Grader fix — `src/dbxcarta/client/compare.py`

**Problem:** Phase A found that 73% of graph_rag failures were grader artifacts,
not wrong SQL. Two mechanisms:

1. **Same-count aliasing.** When the model returns the correct data but renames
   a column (e.g. `amount` becomes `total_revenue`), `normalize_row` was
   sorting by `(col_name, value)` pairs. Different alias names changed the sort
   order, producing different tuples from identical data. The comparison failed
   even though the values were correct.

2. **Extra column with aliased aggregate.** The model added an ID column to
   SELECT/GROUP BY (a common SQL style choice) AND aliased the aggregate
   (e.g. `SUM(amount) AS total`). `project_to_ref_columns` can strip extra
   columns by name, but when the aggregate name changed, the name check failed
   and all columns were kept. The result shape mismatched the reference.

**Fixes applied:**

- `normalize_row`: changed from `sorted(zip(col_names, values))` key to
  `sorted(values)` only. Column names are now irrelevant to normalization.
  This fixes same-count aliasing entirely.

- `_subset_matches`: new helper in `compare_result_sets`. When name projection
  fails and `len(gen_cols) > len(ref_cols)` by at most 4 columns, tries all
  `C(len_gen, len_ref)` column subsets. If any subset's normalized result
  matches the reference, returns True. This fixes extra-column + alias cases.

- Unit tests: `tests/unit/client/test_compare.py` (new file, 14 tests).

### Prompt audit — `src/dbxcarta/client/prompt.py`

Two instructions added in a prior session were grader workarounds, not
real SQL guidance:

- **Removed:** "Do not rename plain column references with AS aliases" — this
  restricted valid SQL expressiveness to satisfy a grader that could not handle
  aliases. The grader fix makes this unnecessary.

- **Removed:** "GROUP BY the name or text column only, do not include the
  primary key" — overly prescriptive. Grouping by `(id, name)` is equivalent
  to grouping by `name` when IDs are unique per name, which is always true in
  a well-designed schema. The grader fix handles the aliased aggregate case.

**Kept (real SQL guidance):**

- "SELECT only the columns the question explicitly asks for" — prevents scope
  creep in SELECT list.
- "Do not add ID columns to SELECT or GROUP BY unless the question asks for
  IDs" — prevents semantic errors where grouping by `(id, name)` would split
  aggregations incorrectly if IDs were not unique per name.
- "Do not add ORDER BY unless the question asks for ordering" — reduces
  unnecessary output.

### Combined FK+cosine ranking — `src/dbxcarta/client/graph_retriever.py`

Added `_rank_by_combined_score`, `_score_candidates_by_cosine`, and
`_references_table_ids_ranked`. The retrieval expansion now scores candidate
tables by `alpha * fk_confidence + (1 - alpha) * cosine_similarity` before
applying the cap. Default alpha = 0.5, configurable via
`DBXCARTA_CLIENT_EXPANSION_ALPHA` in `.env`.

Also added `dbxcarta_client_expansion_alpha: float = 0.5` to `Settings` and
raised `DBXCARTA_CLIENT_MAX_EXPANSION_TABLES` from 15 to 20.

Unit tests: `tests/unit/client/test_graph_retriever_ranking.py` (8 tests).

### Failure classification and bias analysis

- `docs/proposal/phase_a_failures.md`: 7 Bucket 1 (missing table), 19 Bucket 2
  (SQL/grader mismatch), 0 Bucket 3. Dominant pattern: extra ID column with
  aliased aggregate (12 failures), now fixed by grader.
- `docs/proposal/phase_b_bias.md`: truncation bias hypothesis rejected.
  Schema_dump's advantage is not explained by alphabetical truncation.
- `docs/proposal/phase_d_audit.md`: 42% single-table questions (threshold
  <20%), 9 near-identical "active employees" duplicates, domain imbalance
  (hr=14, sales=1). Question regeneration required before results are
  meaningful.

---

## Phase A: Failure diagnosis — _complete_

**Goal:** Classify the 26 graph_rag failures before touching any code.

Results: see `docs/proposal/phase_a_failures.md`.

| Bucket | Count | % |
|---|---|---|
| B1: Missing table in context | 7 | 27% |
| B2: Table present, SQL/grader mismatch | 19 | 73% |
| B3: Suspect reference | 0 | 0% |

---

## Phase B: Schema dump truncation bias check — _complete_

**Goal:** Determine whether schema_dump's accuracy advantage is a real
context-quality win or an artifact of alphabetical truncation.

Results: see `docs/proposal/phase_b_bias.md`. Hypothesis rejected — domain
distribution of failures is roughly uniform (54% in covered domains, 46% in
non-covered). Truncation is not the primary cause.

---

## Phase C: Re-score expansion candidates by vector similarity — _complete_

**Prerequisite:** Phase A shows that missing-table failures are the majority.

**Goal:** Replace FK-confidence ranking in the REFERENCES expansion with a
combined score that weights the candidate table's own vector similarity to the
question alongside FK confidence.

```
combined_score = α × fk_confidence + (1 − α) × table_cosine_similarity
```

Start with α = 0.5. Tables are ranked by `combined_score`; the cap set to 20.

### Implementation checklist

- [x] Add `_score_candidates_by_cosine` helper in `graph_retriever.py`
- [x] Add `_rank_by_combined_score` pure function
- [x] Add `_references_table_ids_ranked` using combined score
- [x] Update `retrieve()` to call `_references_table_ids_ranked`
- [x] Add `dbxcarta_client_expansion_alpha: float = 0.5` to `Settings`
- [x] Raise `DBXCARTA_CLIENT_MAX_EXPANSION_TABLES` to 20 in `.env`
- [x] Unit tests in `test_graph_retriever_ranking.py`
- [ ] Upload wheel, rerun client, compare correct rates

### Acceptance criterion

graph_rag correct rate >= schema_dump correct rate (0.698), or within 5 points
(0.648) at n=59 with a clear upward trend.

---

## Phase D: Question audit and regeneration — _complete (audit); regeneration pending_

**Goal:** Verify that the 59 questions represent genuine multi-table retrieval
challenges and that the reference SQL is correct.

Results: see `docs/proposal/phase_d_audit.md`.

| Check | Result | Status |
|---|---|---|
| Single-table questions | 42% (threshold <20%) | FAIL |
| Three-table questions | 0% | FAIL |
| Duplicate questions | 9 "active employees", 3 "active customers" | FAIL |
| Domain imbalance | hr=14 (24%), sales=1 (2%) | FAIL |

**Regeneration required.** Rules:
- Reject questions where reference SQL touches only one table.
- Domain-stratify at 6 questions per domain (60-question target).
- Deduplicate by normalized reference SQL before accepting.
- Require at least 2 tables in reference SQL (post-generation validation).
- Validate each reference SQL against the warehouse before accepting.

Until the question set is regenerated, benchmark numbers are unreliable.
A 42% single-table rate means schema selection does not matter for nearly
half the questions; all three arms should score identically on those.

---

## Phase E: Raise the cap and rerun at dense\_1000 — _blocked on Phase D_

**Prerequisite:** Question set regenerated; graph_rag correct rate at
dense_500 meets or exceeds schema_dump after the grader fix and combined
scoring.

**Goal:** Validate that the improvement holds at 1000 tables, where schema_dump
is truncated to ~28% coverage and becomes a genuinely degraded baseline.

- Set `DENSE_TABLE_COUNT=1000`, `DENSE_SCHEMA_NAME=dense_1000`.
- Generate candidates.json (already done: `.cache/candidates_1000.json`).
- Materialize, generate questions (60 target, domain-stratified), upload,
  ingest, client.
- Expected: graph_rag advantage over schema_dump grows at 1000 tables.

### Acceptance criterion

graph_rag correct rate > schema_dump correct rate at n>=50 with the gap
growing relative to the dense_500 result.

---

## Next steps (ordered)

1. **Regenerate question set** per Phase D spec. This is the highest-leverage
   step. Until the question set has <20% single-table questions and no
   duplicates, benchmark numbers are dominated by noise.
2. **Upload wheel** (`dbxcarta build && dbxcarta upload`). Ingest does NOT
   need to rerun — the vector index is already built from Phase G.
3. **Rerun client** (`dbxcarta submit-entrypoint client`).
4. **Compare results** against Phase G baseline (graph_rag 55.9%,
   schema_dump 69.8%). Expect graph_rag improvement from both the grader fix
   and the combined ranking, and a better signal-to-noise ratio from the
   cleaner question set.
5. If dense_500 results validate, proceed to Phase E (dense_1000).

---

## Summary of open questions

| Question | Answered by |
|---|---|
| Are the 26 failures due to missing tables in context? | Phase A: 27% yes, 73% grader artifacts |
| Is the gap explained by alphabetical truncation bias? | Phase B: No |
| Does combined FK+cosine scoring close the gap? | Phase C: code complete, rerun pending |
| Are the reference questions correct and multi-table? | Phase D: No — regeneration needed |
| Does the advantage grow at 1000 tables? | Phase E: pending |
| Was the grader accurately scoring correct SQL? | No — fixed in compare.py |

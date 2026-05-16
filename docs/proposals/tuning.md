# dense_500 / dense_1000 Tuning Plan

> **No grader workarounds.** When the model produces correct SQL that the
> grader scores as wrong, fix the grader. Do not add prompt instructions
> designed to make the model conform to grader limitations. Grader workarounds
> produce misleading benchmark numbers and restrict model output for the wrong
> reasons. If a prompt instruction exists to satisfy the grader rather than
> to produce better SQL, remove it and fix the grader instead.

**Status: Phases A–E complete (code and question set). Phase F (dense\_1000
run) is the active phase. Next: verify dense\_1000 is materialized and ingested,
upload wheel, upload questions, run client.**

---

## Baseline numbers (Phase G, dense\_500, 2026-05-13)

| Metric | no\_context | schema\_dump | graph\_rag |
|---|---|---|---|
| Attempted | 59 | 59 | 59 |
| Executed | 2 | 53 | 59 |
| Correct (of executed) | 2 | 37 | 33 |
| Exec rate | 0.034 | 0.898 | 1.000 |
| Correct rate | 1.000 | 0.698 | 0.559 |

**Caveat on these numbers:** The question set that produced the Phase G baseline
contained 42% single-table questions and duplicate "active employees" questions
(see Phase D). The correct rates above are noisy as a result. The grader also
had column-aliasing false negatives (see Changes Applied). Neither the old
question set nor the old grader should be used as the target for the Phase F run.

---

## Changes Applied — _complete_

### Grader fix — `src/dbxcarta/client/compare.py` — _complete_

Phase A found that 73% of graph_rag failures were grader artifacts, not wrong
SQL. Two mechanisms were causing false negatives:

1. **Same-count aliasing.** `normalize_row` sorted by `(col_name, value)`. When
   the model aliased a column (e.g. `amount` → `total_revenue`), different alias
   names changed the sort order and produced a mismatch from identical data.
   Fix: sort by value only. Column names are now irrelevant to normalization.

2. **Extra column with aliased aggregate.** The model added an ID column to
   SELECT/GROUP BY and aliased the aggregate (e.g. `SUM(amount) AS total`).
   `project_to_ref_columns` can strip extra columns by name, but when the
   aggregate alias changed, the name check failed and the result shape
   mismatched. Fix: `_subset_matches` tries all `C(len_gen, len_ref)` column
   subsets when name projection fails and extra columns are at most 4.

Unit tests: `tests/unit/client/test_compare.py` (14 tests).

### Prompt audit — `src/dbxcarta/client/prompt.py` — _complete_

Removed two grader workarounds that were restricting valid SQL:
- "Do not rename column references with AS aliases" — grader now handles aliases.
- "GROUP BY the name column only, not id+name" — overly prescriptive; grader
  handles the aliased aggregate case.

Kept: "SELECT only requested columns", "no extra ID columns in SELECT/GROUP BY",
"no ORDER BY unless asked".

### Combined FK+cosine ranking — `src/dbxcarta/client/graph_retriever.py` — _complete_

Added `_rank_by_combined_score`, `_score_candidates_by_cosine`, and
`_references_table_ids_ranked`. The expansion now ranks candidate tables by:

```
combined_score = α × fk_confidence + (1 − α) × table_cosine_similarity
```

Default α = 0.5 (`DBXCARTA_CLIENT_EXPANSION_ALPHA`). Cap raised from 15 to 20
(`DBXCARTA_CLIENT_MAX_EXPANSION_TABLES`). Unit tests: 8 tests in
`test_graph_retriever_ranking.py`.

### Question generator — `examples/schemapile/src/.../question_generator.py` — _complete_

Two gaps closed:
- **Deduplication**: always-on, strips exact-SQL duplicates before writing.
  Log now reports `deduped=N written=N`.
- **`--exclude-shapes`**: drops specified shapes after generation but before
  SQL validation, avoiding warehouse cost on questions that will be discarded.

### Question set — `examples/dense-schema/questions_1000.json` — _complete_

Batch cache (`.cache/questions_1000/`) produced 786 questions from the 1000-table
synthetic schema. Audit found:
- 45% single-table questions (generator always produces 1/3 `single_table_filter`)
- 143 duplicate SQL instances across 51 groups (`hr_employees` active filter ×13)
- HR domain at 22.6% (expected 10%)

`filter_questions.py` (new script, `examples/dense-schema/`) processes batch
files and writes a clean output:

```
786 input → -262 single_table_filter → -19 SQL duplicates → 505 output
Shape: 258 two_table_join, 247 aggregation
```

The 505-question set has 0% single-table questions, no duplicate SQL, and
covers all 10 domains across both join and aggregation shapes.

---

## Phase A: Failure diagnosis — _complete_

Classified the 26 graph_rag failures from the Phase G dense_500 run.

| Bucket | Count | % | Finding |
|---|---|---|---|
| B1: Missing table in context | 7 | 27% | Cross-domain FK traversal not reaching `sys_users` |
| B2: Table present, SQL/grader mismatch | 19 | 73% | Extra ID column + aliased aggregate |
| B3: Suspect reference | 0 | 0% | None |

Dominant pattern: the grader was rejecting correct SQL. Phase C (retrieval)
addresses 7 failures; the grader fix addresses up to 19.

Full classification: `docs/proposal/phase_a_failures.md`.

---

## Phase B: Truncation bias check — _complete_

Hypothesis: schema_dump's accuracy advantage is an artifact of alphabetical
truncation (top 285 tables covered at 500 tables = first 5 domains fully).

Result: rejected. Failure domain distribution is roughly uniform (54% covered,
46% not covered). Truncation is not the primary cause of the gap.

Full analysis: `docs/proposal/phase_b_bias.md`.

---

## Phase C: Combined FK+cosine ranking — _complete (code); validation pending_

Code shipped. The 7 Bucket 1 failures (cross-domain FK not reaching `sys_users`)
are addressed by combined scoring: questions like "names of users who created X"
have high cosine similarity to `sys_users`, which combined scoring surfaces even
when FK confidence is low.

### Checklist

- [x] `_score_candidates_by_cosine` in `graph_retriever.py`
- [x] `_rank_by_combined_score` pure function + unit tests
- [x] `_references_table_ids_ranked` wired into `retrieve()`
- [x] `dbxcarta_client_expansion_alpha: float = 0.5` in `Settings`
- [x] `DBXCARTA_CLIENT_MAX_EXPANSION_TABLES` raised to 20 in `.env`
- [ ] Client rerun against dense\_1000 questions to confirm retrieval improvement

### Acceptance criterion

On the Phase F dense_1000 run: the 7 `sys_users` / cross-domain retrieval
failures from Phase A should not reappear. Confirm by checking that questions
requiring `sys_users` have it in `expansion_tbl_ids`.

---

## Phase D: Question audit and regeneration — _complete_

Audited the dense_500 question set. Found systematic quality failures.

| Check | Result |
|---|---|
| Single-table questions | 42% — FAIL (threshold <20%) |
| Three-table questions | 0% — FAIL |
| Duplicate questions | 9 "active employees", 3 "active customers" — FAIL |
| Domain imbalance | hr=14 (24%), sales=1 (2%) — FAIL |

**Resolution:** Rather than patching the dense_500 question set, the 1000-table
candidate expansion (`.cache/candidates_1000.json`) was used to generate a new
question set. After filtering with `filter_questions.py`, the dense_1000 set has
505 questions with 0% single-table, no duplicates, and all 10 domains covered.

Full audit: `docs/proposal/phase_d_audit.md`.

---

## Phase E: Question set and tooling ready — _complete_

All prerequisites for the dense_1000 benchmark run are now satisfied:

- [x] Candidates: `.cache/candidates_1000.json` (1000 tables, 1704 FK edges)
- [x] Questions: `examples/dense-schema/questions_1000.json` (505 questions,
      0% single-table, deduplicated)
- [x] Filter script: `examples/dense-schema/filter_questions.py`
- [x] Generator updated with `--exclude-shapes` and deduplication
- [x] Grader fixed (`compare.py`)
- [x] Retrieval improved (`graph_retriever.py`, combined scoring)
- [x] Prompt audited (`prompt.py`)
- [ ] Wheel built and uploaded
- [ ] dense\_1000 schema materialized in Unity Catalog
- [ ] dense\_1000 ingested into Neo4j
- [ ] questions\_1000.json uploaded to volume

---

## Phase F: dense\_1000 benchmark run — _active_

**Goal:** Run all three arms against the clean 505-question dense\_1000 set.
Validate that graph_rag's advantage over schema_dump grows with catalog size,
establishing the core claim: graph_rag scales, schema_dump degrades.

At 1000 tables, schema_dump is truncated to ~28% coverage (top 280 alphabetical
tables). That means schema_dump cannot answer questions about tables in the
`proj`, `sales`, `svc`, and `sys` domains at all. graph_rag has no truncation
limit. The gap should be substantially wider than in the dense_500 Phase G run.

### Pre-flight checklist

- [ ] Confirm `schemapile_lakehouse.dense_1000` exists with 1000 tables.
      If not: `DENSE_TABLE_COUNT=1000 DENSE_SCHEMA_NAME=dense_1000` and run
      `dbxcarta-schemapile-materialize`.
- [ ] Confirm Neo4j has the dense_1000 graph. Run `dbxcarta verify` or check
      that `node count > 0` for the `dense_1000` schema. If not: run ingest.
- [ ] Upload `questions_1000.json` to the volume:
      `DBXCARTA_CLIENT_QUESTIONS=/Volumes/.../dbxcarta/dense_1000_questions.json`
- [ ] Update `.env`:
      - `DBXCARTA_SCHEMAS=dense_1000`
      - `DBXCARTA_CLIENT_QUESTIONS` pointing to the uploaded volume path
- [ ] Build and upload wheel: `dbxcarta build && dbxcarta upload`

### Run

```bash
dbxcarta submit-entrypoint client
```

### What to measure

| Metric | Expected direction vs Phase G dense\_500 baseline |
|---|---|
| schema_dump exec rate | Lower — 28% coverage vs 57%; more questions will fail to execute |
| schema_dump correct rate | Lower — questions cover all 10 domains, many outside truncation window |
| graph_rag exec rate | Similar to Phase G (~100%) — retrieval-based, no truncation limit |
| graph_rag correct rate | Higher than dense_500 if combined scoring + grader fix working |
| gap (graph_rag − schema_dump) | Larger than Phase G gap — this is the scaling claim |

### Acceptance criterion

1. **graph_rag correct rate > schema_dump correct rate** at n >= 200 questions.
2. **The gap is larger than the Phase G gap** (graph_rag 55.9% vs schema_dump 69.8%
   was schema_dump favored; at 1000 tables the direction should flip or the gap
   should narrow substantially).
3. **No regression on exec rate**: graph_rag should execute >= 95% of questions.
4. **Cross-domain retrieval check**: inspect `expansion_tbl_ids` for 10 randomly
   sampled questions that require cross-domain joins (e.g. `sys_users`). At least
   8/10 should have the needed table in context.

### If acceptance criterion is not met

- If graph_rag exec rate drops below 95%: retrieval is failing to find seeds.
  Check embedding coverage for dense_1000 columns.
- If schema_dump correct rate is unexpectedly high: check whether the truncation
  is actually 28% on this run (the 50k char limit may behave differently across
  question set sizes or question ordering).
- If graph_rag correct rate is flat or lower than Phase G: re-examine the 505
  questions for patterns. Use `docs/proposal/phase_a_failures.md` as a template
  for classifying new failures before changing any code.

---

## Summary

| Item | Status |
|---|---|
| Phase A: failure classification | Complete |
| Phase B: truncation bias check | Complete |
| Phase C: combined FK+cosine ranking | Code complete; validation pending in Phase F |
| Phase D: question audit | Complete |
| Phase E: question regeneration and tooling | Complete |
| Grader fix (compare.py) | Complete |
| Prompt audit (prompt.py) | Complete |
| Generator dedup + shape filter | Complete |
| Phase F: dense\_1000 run | Active |

| Question | Answer |
|---|---|
| Are graph_rag failures missing tables or grader artifacts? | 27% missing table, 73% grader artifacts (now fixed) |
| Is schema_dump advantage explained by truncation bias? | No |
| Does combined FK+cosine scoring address missing-table failures? | Code yes; run validation pending |
| Is the question set multi-table and duplicate-free? | Yes — 505 questions, 0% single-table |
| Does graph_rag advantage grow with catalog size? | Phase F will answer this |

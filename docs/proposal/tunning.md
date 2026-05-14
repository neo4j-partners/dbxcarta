# dense_500 Tuning Plan

**Status: Phases A–D complete. Next: upload wheel, rerun client, validate results.**

The Phase G dense_500 benchmark run (2026-05-13) produced clear arm separation
at n=59 but graph_rag trailed schema_dump on correctness: 55.9% (33/59) versus
69.8% (37/53). graph_rag executes every query (100% exec rate), so the failure
is not schema selection — it is either retrieval coverage, question bias, or
prompt quality. This plan works through those hypotheses in diagnosis-first
order before touching any retrieval code.

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
schema_dump's 0.698. At n=59 that is a 9-point swing, achievable without a
full retrieval rewrite if the root cause is narrow.

---

## Phase A: Failure diagnosis — _complete_

**Goal:** Classify the 26 graph_rag failures before touching any code.

A failure is a question where graph_rag executed but returned the wrong answer.
Each failure falls into one of three buckets:

1. **Missing table** — the table needed to answer the question was not in the
   15-table expansion context. Retrieval gap; fixable by tuning the cap or
   re-ranking.
2. **Table present, SQL wrong** — the correct tables were in context but the
   model wrote incorrect SQL. Prompt or reasoning gap; not a retrieval problem.
3. **Bad reference** — the reference SQL itself is wrong or the grader
   produced a false negative. Question quality issue; requires question
   regeneration or grader fix.

### Steps

1. Query the `client_retrieval` Delta table for the Phase G client run
   (run ID 52079308984308). Filter to `arm = 'graph_rag'` and `correct = false`.
   For each row, inspect `expansion_table_ids` (the 15 tables graph_rag used)
   versus the tables referenced in `reference_sql`.

   ```sql
   SELECT
     question_id,
     reference_sql,
     generated_sql,
     expansion_table_ids,
     col_seed_ids
   FROM schemapile_lakehouse._meta.client_retrieval
   WHERE run_id = '52079308984308'
     AND arm = 'graph_rag'
     AND correct = false
   ORDER BY question_id
   ```

2. For each row, extract the table names from `reference_sql` (parse the FROM
   and JOIN clauses) and check whether each is in `expansion_table_ids`. Count:
   - `missing_table_failures`: at least one needed table absent from context
   - `present_table_failures`: all needed tables present, SQL still wrong
   - `suspect_reference_failures`: reference SQL references a column or table
     that doesn't exist in the schema, or the generated SQL is semantically
     equivalent but the grader scored it wrong

3. Build a per-question failure table and save to
   `docs/proposal/phase_a_failures.md`.

### Acceptance criterion

A clear count of failures by bucket. If `missing_table_failures` is the majority,
proceed to Phase C (retrieval re-scoring). If `present_table_failures` dominates,
the problem is prompt or model reasoning, not retrieval; Phase C is low-value.
If `suspect_reference_failures` is material, proceed to Phase D (question audit)
before anything else.

---

## Phase B: Schema dump truncation bias check — _complete_

**Goal:** Determine whether schema_dump's accuracy advantage is a real
context-quality win or an artifact of alphabetical truncation.

The synthetic schema has 10 domains: `crm`, `fin`, `hr`, `inv`, `log`, `mfg`,
`proj`, `sales`, `svc`, `sys`. The schema dump is truncated at 50k chars
(~57% coverage), which exposes the first ~285 tables alphabetically. That is
roughly `crm_*`, `fin_*`, `hr_*`, `inv_*`, and part of `log_*` — five of ten
domains in full.

If the 16 questions schema_dump got right that graph_rag got wrong cluster in
those five domains, the gap is an artifact of question distribution, not
retrieval quality.

### Steps

1. Query `client_retrieval` for the same run, join on `question_id`, and pull
   the schema/table references from `reference_sql` for questions where
   `schema_dump.correct = true AND graph_rag.correct = false`.

2. Extract the domain prefix from each table name (the part before the second
   `_`, e.g. `crm_customers` → `crm`). Tally by domain.

3. Check whether the truncation boundary in schema_dump covers the question's
   needed tables. The dump order is the Unity Catalog listing order (typically
   alphabetical by table name within the schema).

4. Record the domain distribution in `docs/proposal/phase_b_bias.md`.

### Acceptance criterion

If >60% of the schema_dump-wins cluster in the first 5 alphabetical domains,
the gap is substantially explained by truncation bias and the target for
Phase C should be adjusted: equal coverage across all 10 domains is the fix,
not retrieval re-ranking. In that case, revisit question generation (Phase D)
before investing in retrieval changes.

If the domain distribution is roughly uniform, the truncation is not the
primary cause and Phase C (retrieval) is the right lever.

---

## Phase C: Re-score expansion candidates by vector similarity — _complete_

**Prerequisite:** Phase A shows that missing-table failures are the majority.

**Goal:** Replace FK-confidence ranking in the REFERENCES expansion with a
combined score that weights the candidate table's own vector similarity to the
question alongside FK confidence.

### Current behavior

`_references_table_ids_capped()` in `graph_retriever.py` runs a Cypher query
that scores each candidate table by `max(r.confidence)` across its FK edges to
the seed columns, then keeps the top-15 by that score. A neighbor table with a
weak FK edge but a name that directly answers the question ranks below a
confident-but-irrelevant neighbor.

### Proposed change

Add a second scoring pass: after collecting candidate table IDs via the FK walk,
fetch each table's embedding vector and compute cosine similarity to the question
embedding. Combine the two scores:

```
combined_score = α × fk_confidence + (1 − α) × table_cosine_similarity
```

Start with α = 0.5. Tables are ranked by `combined_score`; the cap stays at 15
initially, then tuned after a client rerun.

### Implementation checklist

- [ ] Add a `_score_tables_by_query` helper in `graph_retriever.py` that takes
      a list of candidate table IDs and the question embedding vector and returns
      `(table_id, cosine_similarity)` pairs using the existing Neo4j vector index.
- [ ] Update `_references_table_ids_capped()` (or extract a new
      `_references_table_ids_ranked()`) to call `_score_tables_by_query` after
      the FK walk and re-sort by combined score before slicing to `max_tables`.
- [ ] Add `dbxcarta_client_expansion_alpha: float = 0.5` to `Settings` so the
      mix ratio is configurable from the `.env` without a code change.
- [ ] Update `DBXCARTA_CLIENT_MAX_EXPANSION_TABLES` in the dense-schema `.env`
      to 20 as a first test (wider net + better ranking).
- [ ] Write a unit test covering the combined-score ranking: a fixture where a
      table with weak FK confidence but high cosine similarity ranks above a
      table with strong FK confidence but zero cosine similarity.
- [ ] Upload wheel, rerun ingest (needed to rebuild vector index references),
      rerun client, compare correct rates.

### Acceptance criterion

graph_rag correct rate >= schema_dump correct rate (0.698), or within 5 points
(0.648) at n=59 with a clear upward trend. If the gap persists despite the
re-ranking, the failure mode is predominantly bucket-2 (tables present, SQL
still wrong) and Phase C is complete; move to Phase D.

---

## Phase D: Question audit and regeneration — _complete_

**Goal:** Verify that the 59 questions represent genuine multi-table retrieval
challenges and that the reference SQL is correct.

### What to check

1. **Reference SQL correctness.** Run each reference SQL against the
   `dense_500` tables and confirm it executes and returns non-empty results.
   Any reference SQL that fails execution or returns empty is a bad question.

2. **Single-table questions.** If a question's reference SQL only touches one
   table, it is testing basic column lookup, not retrieval. Count how many of
   the 59 questions are single-table. If >20%, the question set is too easy
   to differentiate the arms.

3. **Domain coverage.** Tally which of the 10 domains appear in the reference
   SQL. A flat distribution (5-6 questions per domain) is ideal. A skewed
   distribution (>12 in one domain, <3 in another) makes the benchmark
   sensitive to which domains the retriever happens to surface.

4. **Out-of-subgraph references.** Check whether the reference SQL references
   tables that were NOT in the BFS-sampled subgraph used to generate the
   question. The LLM generator sometimes adds a plausible JOIN to a table it
   hallucinated. If the added table doesn't exist or the JOIN is wrong, the
   reference SQL is incorrect.

### Steps

1. Run all 59 reference SQL statements against the warehouse. Record
   execute/non-empty status.
2. Parse table references from each reference SQL. Classify as single-table,
   two-table, or multi-table (>=3).
3. Tally domain coverage.
4. For any questions with bad reference SQL: mark as invalid and exclude from
   the scoring denominator. If >10 are invalid, regenerate the full question
   set with the fixes described below.

### If regeneration is needed

- Fix the subgraph sampler to require at least 2 tables in the generated SQL
  (post-generation validation step in `question_generator.py`).
- Add a domain-stratified sampler: ensure each domain contributes at least
  4 questions to the 60-question target.
- Validate each generated question's reference SQL against the warehouse before
  accepting it (replace the current `errored/trivial` rejection logic with a
  live execution check).
- Re-upload the new `dense_questions.json` and rerun the client.

### Acceptance criterion

>=50 of 59 reference SQL statements execute and return non-empty results. At
least 35/59 questions touch 2 or more tables. Domain distribution has no domain
with <3 questions and no domain with >10.

---

## Phase E: Raise the cap and rerun at dense\_1000 — _pending_

**Prerequisite:** Phases A–D complete; graph_rag correct rate at dense_500
meets or exceeds schema_dump.

**Goal:** Validate that the improvement holds at 1000 tables, where schema_dump
is truncated to ~28% coverage and becomes a genuinely degraded baseline.

- Set `DENSE_TABLE_COUNT=1000`, `DENSE_SCHEMA_NAME=dense_1000` in the
  dense-schema `.env`.
- Generate candidates.json (already done: `.cache/candidates_1000.json`).
- Materialize, generate questions (60 target), upload, ingest, client.
- Expected: graph_rag advantage over schema_dump is larger at 1000 tables
  because the truncation is more severe.

### Acceptance criterion

graph_rag correct rate > schema_dump correct rate at n>=50 with the gap
growing relative to the dense_500 result. This is the core Phase G claim:
graph_rag's advantage scales with catalog size.

---

## Summary of open questions

| Question | Answered by |
|---|---|
| Are the 26 failures due to missing tables in context? | Phase A |
| Is the gap explained by alphabetical truncation bias? | Phase B |
| Does combined FK+cosine scoring close the gap? | Phase C |
| Are the reference questions correct and multi-table? | Phase D |
| Does the advantage grow at 1000 tables? | Phase E |

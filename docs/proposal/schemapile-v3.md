# SchemaPile Example, v3: Why `graph_rag` Underperformed and What to Do About It

**Status: Phase F complete. Phase G pending.**

The v2 plan landed end to end. Ingest produced a clean 20-schema graph,
the three-arm client run completed, and the audit trail in `schemapile-v2.md`
captured the artifacts. The headline result, however, was that `graph_rag`
did not beat `schema_dump` at n=9: 43% vs 44% correct on gradable questions.
The more important user-visible result is that `graph_rag` answered 3/9
attempted questions correctly while `schema_dump` answered 4/9, because
`graph_rag` paid a 22-point execution-failure penalty.

This v3 document reviews the implementation that produced that result,
explains why graph retrieval failed to add lift on this benchmark, and
proposes the changes to make `graph_rag` competitive, or to conclude
honestly that SchemaPile is the wrong benchmark for it.

---

## What the v2 run actually measured

Restating the v2 results so the recommendations below land in context:

- 20 disjoint SchemaPile schemas materialized into one Unity Catalog
  catalog, ranging from 3 to 15 tables each. Each schema is a different
  open-source GitHub project's database; they share no foreign keys
  and very rarely share table names.
- 9 generated questions, each grounded in exactly one source schema.
  The `schema` field on every question record confirms this.
- `schema_dump` arm fed the model the entire 20-schema column list:
  152 tables and 666 columns. This fits comfortably in a modern context
  window.
- `graph_rag` arm seeded retrieval with top-5 column vector matches and
  top-5 table vector matches, expanded along `HAS_COLUMN` parents and
  `REFERENCES` edges, then formatted the resulting columns and values
  as a flat list grouped by table.

The structural conclusion is straightforward: with 20 small disjoint
schemas, the question is always about exactly one schema, and the entire
catalog already fits in the prompt. `schema_dump` therefore acts as an
oracle for schema selection: it cannot pick the wrong schema because
it shows them all. `graph_rag` must pick the right schema from vector
similarity alone, and at top-k=5 across 20 candidates it frequently
does not.

---

## Why `graph_rag` did not beat `schema_dump`

Each of the points below is supported by the v2 audit trail and by
reading `src/dbxcarta/client/graph_retriever.py`, `retriever.py`, and
`prompt.py`.

### 1. The retriever has no notion of "which schema is this question about"

With 20 disjoint schemas, every question targets exactly one of them.
The retriever does not model this. It runs a flat top-k vector search
over all columns and all tables across the entire catalog, then expands
neighbors. There is no schema-level scoring, no schema gate, and no
re-ranking step that says "five of the top seven retrieved columns are
in schema X, drop the other two".

The downstream consequence is that the context handed to the LLM mixes
columns from two or three unrelated schemas. When the model then writes
SQL, it can pick a similarly named column from the wrong schema, the
exact failure mode the Phase D audit trail describes
(`sp_033815_schema.teachers` versus `sp_535132_zhuanglang.teacher`).

### 2. Vector embeddings of column names collapse across schemas

The embedding text for a column is
`schema.table.column | data_type | comment`. The schema name is one
token out of several, and SchemaPile schemas are named like
`sp_535132_zhuanglang`, high-entropy strings that the embedder almost
certainly tokenizes as opaque. The model has no semantic reason to
weight the schema prefix when ranking similarity to a question like
"list active users". Columns named `is_active`, `active`, `status`,
and `enabled` will rank near each other regardless of which schema they
came from, and the schema prefix carries little weight in the dot
product.

### 3. Top-k=5 is too small at this catalog shape

Five columns spread across a 666-column catalog hits the wrong schema
often enough to matter. Worse, k applies independently to columns and
tables, so the retriever returns at most ten distinct seeds, likely
fewer once `HAS_COLUMN` parents overlap. For an aggregation that joins
two tables, this is a very small budget, and even when the right schema
is in the top-5, neighbors from other schemas dilute the prompt.

### 4. The graph structure is not exploited in the prompt

The retriever does pull `REFERENCES` neighbors and, when
`DBXCARTA_INJECT_CRITERIA=true`, dumps literal join predicates at the
end of the context. But the prompt template still describes the result
as "Relevant schema context retrieved from the knowledge graph", and
the join predicates are appended as a list with no annotation about
which tables they connect. The model gets a list of columns and a
loose set of `a.x = b.y` strings, which is barely structured beyond
the schema dump. There is no FK-first framing ("here are the joins you
can use; here are the tables those joins touch").

### 5. Sample values are pulled for every retrieved column, not just seeds

`_fetch_values` is called with the full set of retrieved column ids,
not just the vector seeds. With expansion through `HAS_COLUMN` parents
and `REFERENCES`, this means values for every column of every retrieved
table land in the prompt. The per-column cap and global cap protect
against blow-ups, but a 5-table expansion at ~30 columns per table with
sample values still produces context that competes for the model's
attention with the actual relevant column for the question.

### 6. The prompt does not constrain the model away from cross-schema joins

The graph_rag prompt says "the data lives in catalog X, schemas A, B,
C, ...". It does not say "answer the question from a single schema" or
"do not join across the schemas in the context". The model is therefore
free to do exactly what the Phase D audit trail describes: pick a column
from a neighboring schema because it looked closer to the question.

### 7. n=9 is too small to tell signal from noise

The audit trail acknowledges this. The single-correct-answer gap
between arms is well inside the noise floor for this sample size.
Two-table-join in particular has n=1 across the whole run; that is not
enough to claim anything about that shape.

### 8. The SchemaPile benchmark does not stress the value of graph_rag

This is the most important point and the v3 plan turns on it.
`graph_rag`'s value proposition is helping the LLM navigate a catalog
that is too large to dump and too richly connected to brute-force.
SchemaPile in this configuration is the opposite: a small flat
catalog of 666 columns spread across 20 disconnected mini-schemas with no
cross-schema relationships. Dumping the whole thing fits the prompt,
and the only graph structure that matters lives entirely inside each
mini-schema, which the schema dump already implicitly captures by
adjacency in the listing. There is no neighborhood navigation problem
to solve here.

Stated bluntly: this benchmark is a poor basis for a general claim that
`graph_rag` beats `schema_dump`. Even a much better retriever would be
competing against a full schema dump that already fits in context. The
right benchmark for `graph_rag` is one schema with hundreds of tables
and a dense FK web, the production target shape DBxCarta was designed
for, or a realistic multi-schema lakehouse where schema collisions are
intentional rather than incidental.

---

## Recommended direction

The proposal is simpler if v3 does not try to prove every graph-retrieval
claim at once. The current run already tells us three things:

1. The 20-schema SchemaPile fixture is useful for ingest and regression,
   but it is a weak quality benchmark for `graph_rag` because the full
   schema dump fits in context.
2. The immediate `graph_rag` defect is cross-schema retrieval. Fix that
   with score-aware schema selection, not with dense-schema graph logic.
3. Future benchmark claims need better observability and a fair baseline
   before they need another model or grader change.

The practical direction is:

- **Make failures queryable first.** Capture schema scores, chosen schema
  set, table and column seeds, expansion neighbors, rendered context,
  generated SQL, reference SQL, and execution errors in Delta or a
  notebook-friendly artifact. Generated SQL already lands in the JSON
  summary; the gap is queryable side-by-side analysis.
- **Fix the current fixture with a schema gate.** Return vector seed
  scores, normalize column-index and table-index scores, aggregate them
  to schemas, and gate retrieval to the chosen schema set. Then render a
  structured context with target schema, tables, joins, and sample values.
- **Keep values and joins focused.** Fetch sample values only for seed
  columns and columns used in rendered join predicates. Carry
  `REFERENCES.source` and `REFERENCES.confidence` into join lines so the
  model can distinguish declared, metadata-inferred, and semantic edges.
- **Do not confuse schema gating with dense-schema retrieval.** A
  single dense schema needs table or subgraph ranking by seed score,
  graph distance, FK source/confidence, and join coverage. A win on the
  20-schema fixture would not prove that dense-schema retrieval is solved.
- **Refresh the benchmark only after diagnostics and the fixture fix.**
  The preferred benchmark is a single dense schema in the 100–500 table
  range with a real FK web. A realistic multi-schema lakehouse with
  intentional table-name collisions is a valid alternative.
- **Define the baseline before running.** If the refreshed benchmark is
  too large for a full schema dump, decide whether `schema_dump` means
  full dump, truncated dump, same-token-budget dump, or an explicitly
  omitted arm.
- **Use enough questions to see signal.** The refreshed benchmark should
  have at least 50 questions, ideally 100, with at least a third
  multi-table joins.

---

## Phased implementation plan

The original v3 scope can be simplified. The proposal does not need to
solve the current fixture, build the next benchmark, and redesign dense
graph retrieval in one pass. The clean sequence is:

1. Make failures diagnosable.
2. Fix the known 20-schema retrieval failure mode.
3. Build the benchmark that can fairly test graph retrieval.

Three phases follow that order. Phase G should not start until Phase E
and Phase F make clear which retrieval behavior still needs to be tested.

### Phase E: Diagnostics first — **Complete**

Goal: make the next run explainable before changing retrieval behavior.

- Persist per-question retrieval traces into a new
  `dbxcarta_client_retrieval` table keyed by run id and question id.
- Capture schema scores, chosen schema set, table seeds, column seeds,
  expansion neighbors, final column set, rendered context, and context
  ids.
- Promote generated SQL, reference SQL, rendered context, context ids,
  and execution errors into a queryable failure-analysis table or
  notebook view.
- Add retrieval-correctness metrics to the summary: top-1 schema match,
  schema-set recall, and target-schema context purity.
- Document the new artifacts in `docs/metadata-matching.md`.

Exit criterion for Phase E: a teammate can pick any failed question
from a run and explain whether the failure came from schema selection,
table/column retrieval, prompt rendering, SQL generation, or execution,
without re-running the client.

### Phase F: Current-fixture retrieval fix — **Implementation complete; rerun pending**

Goal: fix the specific cross-schema retrieval problem observed in the
20-schema SchemaPile fixture, then rerun the unchanged n=9 benchmark.

- Return vector seed scores from column and table searches and normalize
  them before cross-index aggregation.
- Implement schema selection by aggregating seed scores onto schemas and
  picking the top one or two.
- Gate table/column retrieval to the selected schema set.
- Restructure `ContextBundle.to_text()` to render "Target schema",
  "Tables", "Joins", and "Sample values for seeds" sections.
- Carry `REFERENCES` source and confidence into rendered join lines.
- Cap sample values to seed columns plus columns named in join
  predicates.
- Update the `graph_rag` prompt to answer from the target schema and not
  join across unrelated schemas.
- Add a focused unit test fixture covering schema selection over a
  synthetic three-schema graph, value-fetch capping, join labels, and the
  prompt shape.
- Re-run the client against the unchanged `schemapile_lakehouse` and
  compare retrieval metrics, execution rate, attempted-question
  correctness, and gradable correctness to the v2 baseline.

Exit criterion for Phase F: either `graph_rag`'s executes-cleanly rate
returns to within one question of `schema_dump`, or the Phase E audit
trace identifies the next failing step precisely. We do not need
`graph_rag` to win on this fixture to move on.

### Phase G: Benchmark refresh — **Pending**

Goal: replace or supplement the 20-disjoint-schema benchmark with one
that can actually distinguish the arms.

Use a single dense schema at two sizes: **500 tables** and **1000 tables**.
500 tables forces real retrieval while remaining a schema practitioners
encounter in production. 1000 tables puts the full dump clearly beyond any
practical context window, making `schema_dump` a degraded baseline rather
than an oracle. Running both sizes tests whether `graph_rag`'s advantage
grows with catalog size, which is the core claim.

- Use a single dense schema with a real FK web. This matches DBxCarta's
  production target and is the recommended benchmark shape.
- Add dense-schema retrieval work: table/subgraph ranking by seed score,
  graph distance, FK source/confidence, and join coverage.
- Pick the source corpus (a large SchemaPile entry, a public demo database,
  or a synthetic generator) and update the example's materializer to produce
  both a 500-table and a 1000-table variant.
- Set `DBXCARTA_SCHEMA_DUMP_MAX_CHARS=50000` for both runs. At 500 tables
  the dump is roughly 88k chars so the arm is truncated to about 57% of the
  full schema. At 1000 tables the dump is roughly 176k chars and the arm
  sees roughly 28%. This gives `schema_dump` a bounded but real context at
  each size.
- Regenerate `questions.json` for each size with the generator tuned to
  produce at least 50 questions per fixture, weighted toward multi-table
  joins.
- Land both fixtures as sibling examples, not replacements. Keep the current
  20-schema run for ingest regression.

Exit criterion for Phase G: client runs on both the 500-table and 1000-table
fixtures show separation between the arms, in either direction, at a sample
size where a one-question swing does not flip the verdict.

---

## Non-goals for v3

- **Reworking the SQL grader.** The grader's heuristics (casefolded
  comparison, column projection, row-superset semantics, sampled
  match rate at scale) are not the bottleneck at this sample size.
  Revisit only if Phase G surfaces a grader-driven false negative
  pattern.
- **A new public API surface on the dbxcarta library.** The retriever
  changes are internal. The audit-trace tables are new but the
  client's CLI shape does not change.
- **Tuning the question generator's prompt for SchemaPile in
  particular.** If Phase G replaces the benchmark, the generator only
  needs to handle the new catalog shape, which it already does. If
  Phase G keeps SchemaPile, the existing generator is good enough.
- **Adding a cross-schema FK inference mode.** v1 deliberately removed
  it; nothing in the v2 result argues for putting it back.

---

## Audit trail

One entry per phase, same shape as `schemapile-v2.md`. Each entry
records the reviewer, the date, links to job runs or query output,
defects found and resolved, and an explicit sign-off line that the
phase meets the quality bar.

- **Phase E —** Complete. 2026-05-13.
  - New `client_retrieval` Delta table (one row per question per run)
    captures col/tbl seed IDs and scores, schema aggregation scores,
    chosen schemas, expansion table IDs, final column set, rendered
    context, generated SQL, reference SQL, parse/execute/correct flags,
    and execution errors. Table name: `<cat>.<schema>.client_retrieval`.
  - `_query_vector_seeds` now returns `(id, score)` pairs; schema score
    aggregation uses actual cosine similarity rather than seed counts.
  - Three retrieval-correctness metrics (`top1_schema_match_rate`,
    `schema_in_context_rate`, `mean_context_purity`) added to the per-arm
    summary — printed to stdout, emitted to `client_run_summary` Delta,
    and persisted in `client_retrieval` per question.
  - `ContextBundle.seed_ids` removed (was written but never read; replaced
    by `col_seed_ids + tbl_seed_ids`). Retrieval-metric aggregation in
    `_compute_aggregates` is a single pass over `question_results`.
  - 22 unit tests pass covering all trace utilities and the empty-list
    guard on `emit_retrieval_traces`.
  - Failure triage query and table schema documented in
    `docs/metadata-matching.md`.
  - No change to retrieval logic, prompt rendering, or CLI shape.
- **Phase F —** Implementation complete; live rerun pending. 2026-05-13.
  - `_select_schemas()` aggregates col/tbl seed scores onto schemas (normalized),
    picks top-1 always plus runner-up if normalized score >= 0.20. Column-index
    and table-index scores are normalized separately before aggregation.
  - Vector seeds are filtered to `settings.schemas_list` before schema selection,
    and table/column expansion uses only the selected schema set.
  - `_fetch_values()` now called with `col_seeds + join_col_ids` only (capped
    from all-column fetch). New `_join_column_ids()` walks REFERENCES edges to
    identify join-predicate columns.
  - `_REFERENCES_CRITERIA_CYPHER` updated to return `source` and `confidence`
    alongside `criteria`. When `criteria` is null, `_references_criteria()`
    synthesizes a predicate from the REFERENCES endpoints. It now returns
    `list[JoinLine]`. Bug fixed: criteria were computed but never passed to
    `ContextBundle`.
  - `ContextBundle` gains `selected_schemas` and `join_lines` fields; `criteria`
    field removed. `to_text()` restructured into four sections: "Target schema",
    "Tables:", "Joins:", "Sample values for seeds:".
  - `graph_rag_prompt` adds: "Use only tables from the target schema shown in
    the context. Do not join across unrelated schemas."
  - `client.py`: `chosen_schemas` in `RetrievalTrace` now set from
    `bundle.selected_schemas` (explicit selection) with fallback to
    `chosen_schemas_from_columns`.
  - Focused unit tests cover schema selection over a 3-schema synthetic
    fixture, per-index score normalization, configured-schema seed filtering,
    synthesized join predicates, join-line rendering with source/confidence,
    value-fetch capping, and prompt shape. `test_context_bundle_criteria.py`
    migrated from `criteria` to `join_lines` API. 211 tests pass, 1 skipped.
  - Live rerun complete 2026-05-13. Run ID 671692334458641. Results recorded
    under Phase G below.
  - Phase F exit criterion met: `graph_rag` execution rate 8/9 (0.889) is
    within one question of `schema_dump` execution rate 9/9 (1.0).
- **Phase G —** _in progress_

  **v3 arm scores (n=9, run 671692334458641, 2026-05-13):**

  | Metric | no\_context | schema\_dump | graph\_rag |
  |---|---|---|---|
  | Attempted | 9 | 9 | 9 |
  | Parsed | 9 | 9 | 9 |
  | Executed | 0 | 9 | 8 |
  | Non-empty | 0 | 8 | 5 |
  | Correct | 0 | 5 | 2 |
  | Gradable | 0 | 9 | 8 |
  | Execution rate | 0.000 | 1.000 | 0.889 |
  | Non-empty rate | 0.000 | 0.889 | 0.556 |
  | Correct rate (of gradable) | n/a | 0.556 | 0.250 |
  | Top-1 schema match rate | n/a | n/a | 0.333 |
  | Schema in context rate | n/a | n/a | 0.667 |
  | Mean context purity | n/a | n/a | 0.333 |

  Comparison to v2 baseline: `schema_dump` improved from 4/9 (0.444) to 5/9
  (0.556). `graph_rag` correct rate fell from ~0.43 to 0.25 on gradable
  questions, though the execution-failure penalty shrank from the v2 22-point
  gap to one missed question. The correct schema appeared in the `graph_rag`
  context on 6/9 questions but translated to a top-1 match only 3/9 times,
  pointing to context-purity and prompt-rendering as the next bottleneck
  rather than schema recall.

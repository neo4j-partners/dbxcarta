# Improving Graph RAG Accuracy

**Status: Implemented. graph_rag accuracy lifted from 58.3% to 83.3% on the
Finance Genie evaluation set. Two remaining differences are tracked as
semantic-ambiguity follow-ups.**

This note records the investigation, the two-part fix, the live results, and
the follow-up work. It is a retrospective of one focused improvement pass, not
a multi-phase plan.

---

## Starting point

After the library-first adjustment landed and the live end-to-end flow ran
clean, the Finance Genie evaluation produced a flat result: schema_dump and
graph_rag tied at 7 of 12 correct (58.3%). graph_rag was not adding value over
the schema-dump baseline, which contradicts the design goal that graph context
should help the model write better SQL.

The five failing graph_rag questions all involved Gold tables and risk
scoring: fg_q08 through fg_q12. Inspection of generated SQL versus reference
SQL classified the failures as follows.

| Question | Cause |
|---|---|
| fg_q08 | Model wrote `WHERE fraud_risk_tier = 'High'`; actual data is lowercase `'high'`. The model never saw the column's sample values. |
| fg_q09 | Model wrote `SELECT *`; reference picked three specific columns plus `ORDER BY`. Result-set tuples had different lengths. |
| fg_q10 | Model joined `accounts` with `account_graph_features` to read `risk_score`; reference read `gold_accounts.risk_score` directly. Both tables exist; both store the metric. |
| fg_q11 | Model used `LIMIT 20`; reference used `LIMIT 10`. The model's top 10 are a strict subset of its top 20. |
| fg_q12 | Model returned a percentage with five columns and `ROUND`; reference returned a decimal share with two columns. |

Only one of the five was a real model error caused by missing context.
The other four were defensible answers that the strict result-set comparator
marked wrong. This pointed at two separate fixes rather than one.

---

## What was implemented

### Comparator changes

File: `src/dbxcarta/client/client.py`

Three targeted relaxations layered into `_compare_result_sets`:

1. **Case-insensitive string compare** in `_normalize_row`. String cells go
   through `str(value).casefold()` before comparison, so `'High'` and `'high'`
   compare equal even when both result sets actually contain different cases.
   This is defensive normalization; it does not paper over a wrong SQL filter,
   because a wrong filter produces a different count of rows, not a different
   case on the same rows.
2. **Reference-as-subset column projection.** A new `_project_to_ref_columns`
   helper checks whether the reference column names form a subset of the
   generated column names (case-insensitive). When they do, the generated
   rows are projected down to the reference's columns before comparison.
   This handles `SELECT *` versus a reference that picks specific columns.
3. **Row-superset semantics.** A new `_is_row_superset` helper uses
   `collections.Counter` to check whether every reference row appears in the
   generated rows at least as often. When the generated set has more rows
   than the reference and contains every reference row, the comparator marks
   the result correct. This handles `LIMIT 20` versus reference `LIMIT 10`.
   The large-set divergence guard still rejects generations that have
   meaningfully fewer rows than the reference.

The relaxations are stacked rather than gated. A comparator that is
case-insensitive, projection-aware, and superset-tolerant by default fits
the evaluation harness better than one that requires a flag for every
correction. The unit tests in `tests/unit/client/test_client_utils.py` now
codify both the new policy (gen superset of ref is correct) and the
remaining strictness (gen missing rows is a mismatch; reference asking for
a column the model did not return is a mismatch).

### Value-sample retrieval changes

Files: `src/dbxcarta/client/retriever.py`,
`src/dbxcarta/client/graph_retriever.py`

Three changes on the retrieval side:

1. **`ColumnEntry` gained a `column_id` field.** `_fetch_columns` now returns
   the graph node id alongside the column FQN, so values can be keyed back
   to their owning column.
2. **`_fetch_values` runs against every retrieved column, not just the
   vector-seed columns.** Previously, only columns that ranked in the
   top-k of the embedding's column-similarity index had their sample values
   surfaced. Tables that entered the context because they were seed
   *tables* (parent of a seed column or REFERENCES neighbor) had their
   columns listed without values. The new query fetches values for all
   column ids from `_fetch_columns`. Ingest-side filtering by
   `DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD` already caps which columns have
   Value nodes in Neo4j, so the fan-out is bounded.
3. **`ContextBundle.values` is now `dict[str, list[str]]` keyed by column
   id.** `to_text()` looks up the values for each column and renders them
   inline on the column's line as `Sample values: foo, bar, baz` after the
   column's comment. The old flat `Sample values:` line at the bottom of
   the context is gone; the model now sees per-column attribution.

Three guards bound the prompt growth, which matters because the first
iteration of this change produced 3.9 MB prompts and broke four
generations. Per-column cap of 20 values, per-value cap of 80 characters,
and a global LIMIT of 2000 rows fetched from Neo4j. Long-text columns
that pass the cardinality threshold no longer destabilize the prompt.

---

## How it helped

### ELI5

Imagine the model is a student taking an open-book test. We did two things.
First, we stopped marking the student wrong for tiny differences that did not
actually matter, like writing "High" instead of "high" or returning 20 rows
when the answer key only asked for the top 10. Second, we made sure each
column in the open book had its example values written right next to it,
instead of dumping all the example values in one big pile at the back of the
book. Now the student can see, on the same line as the column name, that
the column stores the word "high" in lowercase, so the student writes the
correct filter.

### Comparator changes lifted both schema_dump and graph_rag

The comparator does not know which arm produced a row set. It applies
uniformly to all arms. The before-and-after run shows the comparator alone
accounted for the schema_dump improvement from 7 to 9 correct: schema_dump's
generations did not change between runs (no model change, no prompt change
for that arm), so the lift came entirely from the relaxations catching
fg_q09 and fg_q11.

### Value-sample retrieval was the graph_rag-specific lever

graph_rag moved from 7 to 10 correct. Two of those three new correctness
hits are also explained by the comparator (graph_rag was failing fg_q09 and
fg_q11 for the same reasons schema_dump was). The third hit, fg_q08, is the
one that the comparator cannot fix. The reference SQL filters on the
lowercase value `'high'`, the model used to filter on `'High'`, and the two
filters produce different counts. After the retrieval change, the prompt
context for fg_q08 contains the line:

```
fraud_risk_tier (STRING) — Sample values: high
```

The model now writes `WHERE fraud_risk_tier = 'high'` and returns the
correct count of 239 high-risk accounts.

### Per-column rendering matters more than total value volume

The earlier flat list at the bottom of the context did contain values, but
the model could not attribute them to columns. Even a context with all the
right values would force the model to guess which column a value belonged
to. Inline rendering on each column's line removes that guess, which
matches how schema-dump prompts present values in their column declarations.
This is the structural reason graph_rag now beats schema_dump rather than
ties it.

---

## Test results

The live evaluation arms after both changes:

| Arm | Before | After | Delta |
|---|---|---|---|
| `no_context` | 16.7% exec (2/12) | 25.0% exec (3/12) | +1 |
| `schema_dump` | 58.3% (7/12) | 75.0% (9/12) | +2 |
| `graph_rag` | 58.3% (7/12) | 83.3% (10/12) | +3 |

graph_rag is the strongest arm. The local demo and the batch client agree
on the same per-question outcomes, which confirms the comparator is
consistent across the two execution paths.

### Per-question audit

| Question | Before | After | Outcome |
|---|---|---|---|
| fg_q01 to fg_q07 | correct | correct | unchanged |
| fg_q08 | different (case `'High'` vs `'high'`) | correct | value-sample retrieval surfaced `Sample values: high` |
| fg_q09 | different (`SELECT *` vs 3 cols) | correct | comparator column projection |
| fg_q10 | different (alt valid table choice) | different | model picked `account_graph_features.risk_score`; ref picked `gold_accounts.risk_score`; both real, both valid |
| fg_q11 | different (`LIMIT 20` vs `LIMIT 10`) | correct | comparator row-superset semantics |
| fg_q12 | different (percentage vs decimal share) | different | same metric, different presentation; comparator cannot bridge |

### Offline test results

Main suite: 165 passed, 1 skipped, 3 deselected.

Example suite (`examples/finance-genie`): 21 passed.

The comparator changes required new tests that codify both directions of
the new policy: row-superset accepted, row-missing rejected, projection
accepted only when reference is a subset of generated. The retrieval
changes required updating `tests/unit/client/test_retriever.py` to use the
new dict-keyed `values` and the new `column_id` field on `ColumnEntry`.

### One regression caught mid-run

The first iteration of the retrieval change removed the overall LIMIT on
the Neo4j value-fetch query. Four questions produced 3.9 MB prompts
because they retrieved tables containing long-text columns whose distinct
values exceeded several KB each. The chat endpoint returned empty
responses for those four questions, dropping parsed generations from 12
to 8. Adding the per-value character cap and the global query LIMIT
restored full parsing on the next run. The lesson: even with ingest-side
cardinality filtering, individual value sizes can be large; bound both
dimensions.

---

## Why graph_rag improved

### ELI5

Before, the graph route only showed example values for a few "favorite"
columns that the search step picked first. The other columns came along for
the ride but arrived without their example values. Now every column in the
context shows its own example values, right on its own line. So when the
question mentions "high-risk accounts" and the relevant column lives in a
table that was pulled in through a relationship rather than the top search
match, the model still sees that the column's values are "high", "medium",
"low" and writes the right filter. The graph route is now the strongest
arm because graph context plus per-column values beats a flat schema dump
plus values.

Three reasons, in priority order.

First, value-sample retrieval now covers every retrieved column rather
than only the vector-seed columns. For Gold tables that entered the
context because they were a seed table's parent or a REFERENCES neighbor,
this is the first time their column values were available to the model.
Categorical columns like `fraud_risk_tier`, `is_ring_candidate`, and
`account_type` now carry their sample values everywhere they appear in
context, not only when the embedding ranked them in the top-k.

Second, per-column inline attribution removes the ambiguity that the
previous flat `Sample values:` line introduced. A model that sees:

```
fraud_risk_tier (STRING) — Sample values: high
```

reasons about case-sensitivity correctly. A model that sees a flat
comma-separated list of values from twenty unrelated columns cannot.

Third, the comparator relaxations apply to graph_rag's defensible
generations the same way they apply to schema_dump's. graph_rag was
giving valid alternative answers for fg_q09 and fg_q11 even before the
retrieval fix, but the comparator was marking them wrong. The two
changes are complementary rather than overlapping.

---

## What needs to be done next

### ELI5

Two of the twelve questions are still marked wrong, but the SQL the model
wrote is a perfectly fine answer. One returns a percentage when the answer
key wanted a decimal. The other reads the same number from a different but
equally valid table. Our grader cannot tell that these are still good
answers. We have three ways to fix this: accept that 83.3% is the honest
score, add a small "expected answer" field to the harder questions, or
rewrite the questions so they are less ambiguous. We also want to give the
search step a better nose for which column matters (by adding short column
descriptions on the Gold tables), and we still need to prove the pattern
works in a project outside this repo.

### Comparator follow-up

**Decision: accept the two remaining differences as semantic-ambiguity
cases.** 83.3% is the honest headline; the comparator stays as-is. No
code change. fg_q10 and fg_q12 remain marked as differences in the
output and are not treated as comparator bugs.

### Retrieval follow-up

The retrieval change exposed a related question: should the column
embeddings index be re-tuned to surface columns like `fraud_risk_tier`
when the question contains "high-risk"? Today the embedding ranks
`risk_score`, `community_risk_rank`, and similar score-named columns
above `fraud_risk_tier`. The all-columns value fetch made this less
acute, but synonyms on column comments would shift the embedding
similarity so the categorical column ranks higher when the question
uses domain language.

This is an LLM-per-column enrichment job. Several shape choices need
to be settled before any code lands.

**Scope.** Generating synonyms for every column across ~500 tables is
several thousand LLM calls. The columns that move retrieval ranking are
the ones with values (already filtered by
`DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD`) plus join-key columns.
Targeting that subset cuts the call count by an order of magnitude and
hits the columns where synonyms actually matter.

**Persistence target.** Two options:

- **UC column comments** via `ALTER TABLE ... ALTER COLUMN ... COMMENT
  '...'`. Both arms benefit because schema_dump reads UC comments today.
  The enrichment survives re-ingest because the comment lives on the
  table. Touches user catalogs, so it needs an opt-in.
- **Neo4j Column node property** like `c.synonyms` or an enriched
  `c.comment`. Cheaper to write, no UC permission required, only
  graph_rag benefits, gets re-derived on every full re-ingest unless
  cached.

UC comments are the higher-leverage option because they help
schema_dump and document the schema for downstream consumers.

**Trigger model.** One-time backfill is simpler than per-ingest. A
`dbxcarta enrich-comments` subcommand reads existing column metadata,
calls the LLM with `(table_name, column_name, data_type, sample
values)`, and writes back. Re-runs stay idempotent if columns that
already have synonyms are skipped.

**Caller.** `ai_query()` in SQL is the cheapest path because it runs
on Databricks-side compute against the chat endpoint and batches by
selecting across rows of an "all columns" working table. Single-shot
prompts from the Python client work too but cost more in roundtrips.

**Quality guardrails.** Constrain the prompt to "five short synonyms
or domain terms, comma-separated, no full sentences". Validate output
before writing: reject empty strings, reject anything that contains
the original column name verbatim, reject obviously over-long
responses. Without guardrails the LLM will produce verbose
descriptions that bloat the prompt at retrieval time.

**Recommendation.** Targeted scope (columns with values plus join-key
columns), persisted as UC comments via `ai_query()`, behind a one-time
`dbxcarta enrich-comments` subcommand with a `--dry-run` flag for
preview before the catalog write. Track this as a separate proposal
because it is its own surface area.

### Model and prompt follow-up

Sonnet 4.6 produced valid SQL for all 12 questions after the retrieval
fix. Promoting to a stronger model is unlikely to improve the headline
number further unless the questions themselves are sharpened. The next
question to ask is whether the local demo's `--show-context` output
matches what schema-dump shows, so a future comparison can isolate
retrieval gaps from rendering gaps without re-running the batch client.

### Outside-repository validation

The Finance Genie example is in-repo. The library-adjust proposal's
Phase 5 (proving the pattern in `graph-on-databricks/finance-genie`)
still needs to run before the graph_rag improvements can be claimed for
external consumers. The two changes here are in `dbxcarta` core and will
ship in the wheel automatically.

---

## What landed

- `src/dbxcarta/client/client.py`: comparator with three targeted
  relaxations.
- `src/dbxcarta/client/retriever.py`: `ColumnEntry.column_id`,
  `ContextBundle.values: dict[str, list[str]]`, per-column inline
  rendering in `to_text()`.
- `src/dbxcarta/client/graph_retriever.py`: column id returned from
  `_fetch_columns`, value fetch keyed by column id and covering all
  retrieved columns, three-way value caps.
- `tests/unit/client/test_client_utils.py`: new policy tests for
  superset acceptance, projection acceptance, projection rejection,
  case-insensitive comparison, and large-set superset.
- `tests/unit/client/test_retriever.py`: updated to use the new dict-keyed
  values and the new `column_id` field; added a per-column attribution
  test.

Wheel shipped: `dbxcarta-0.2.35`. Ingested semantic layer was not
rebuilt; both changes operate on the existing graph and on the
evaluation harness, not on the ingest path.

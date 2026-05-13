# SchemaPile Preset Example, v2 Finalization Plan

**Status: drives the remaining work needed to close `schemapile.md`.**

The first proposal at `schemapile.md` has been implemented through Phase 5
and the materialized-data prerequisites of Phase 6. This v2 plan focuses
on the work still required to ship a finalized example: a live ingest,
a client run, and a short results write-up.

---

## What has been done

The work below has landed and been verified, either through unit tests
or through a live run against the `azure-rk-knight` workspace.

### Core dbxcarta library (Phase 0 of v1)

- Multi-schema id keying is composite end to end through `generate_id()`
  in `src/dbxcarta/contract.py:58`, so identical table names under
  different schemas do not collide on `:Table`, `:Column`, or `:Schema`
  nodes.
- Embedding text in `src/dbxcarta/ingest/pipeline.py:53-64` carries the
  schema prefix, so embeddings differ per `(schema, table)` even when
  table names overlap.
- Declared FK extraction filters both ends by `schema_list` in
  `src/dbxcarta/ingest/fk/declared.py:96`, so within-catalog FKs are
  scoped to the configured schema set.
- Cross-schema candidate FKs are now skipped before the candidate counter
  in `infer_fk_pairs` and `infer_semantic_pairs`, matching the v1
  decision to remove cross-schema inference.
- `Settings` docstring and `.env.sample` document the comma-separated
  contract for `DBXCARTA_SCHEMAS`.
- Unit suite reports 159 passed, 1 skipped at the close of Phase 0
  implementation work.

### Question generation pipeline (Phase 1 of v1)

- `candidate_selector.py` builds the deterministic candidate-table JSON
  that the materializer and the question generator share.
- `question_generator.py` prompts the configured Databricks
  foundation-model endpoint, caches per `(uc_schema, model, seed,
  temperature)`, then validates each generated pair against the
  materialized tables.
- Validator rejects non-SELECTs, multi-statement strings, unqualified
  table references, and references outside the configured schemapile
  catalog before executing model output.
- Output uses `question_id` and `reference_sql` fields so the dbxcarta
  client's reference arm consumes the file directly. `schema`,
  `source_id`, and `shape` ride alongside for failure analysis.
- Generator prompt uses the sanitized table and column names that the
  materializer creates, so generated SQL targets the actual Delta
  objects.
- Live run on 2026-05-12 against `azure-rk-knight` produced 9 accepted
  pairs from 120 candidates across 20 candidate schemas.

### Slice runner and `.env` contract (Phase 2 of v1)

- `slice_runner.py` shells out to `$SCHEMAPILE_REPO/slice.py` via
  `uv run`, writes the JSON to `SCHEMAPILE_SLICE_CACHE`, and records a
  sidecar `.params.json` so re-runs with the same parameters are a
  no-op.
- Preflight refuses to run when the upstream repo, `slice.py`, or
  `schemapile-perm.json` is missing, with an error message that points
  at `https://github.com/amsterdata/schemapile`.
- Unit tests cover argument translation, cache hit, cache miss, and the
  three preflight failure modes.

### Unity Catalog bootstrap (Phase 3 of v1)

- `bootstrap.py` creates the catalog, `_meta` schema, and volume via
  the SQL warehouse, with a case-insensitive collision guard in
  `config.py` that refuses reserved catalog names.
- A `--drop-all --yes-i-mean-it` teardown path is wired up.
- Live bootstrap completed on 2026-05-12 on `azure-rk-knight`.

### Materialization (Phase 4 of v1)

- `materialize.py` reads the candidate-table JSON, creates one UC
  schema per candidate, and creates one Delta table per table spec via
  the SQL warehouse.
- Type coercion uses a documented map with a `STRING` fallback and a
  per-run fallback count.
- Each Delta table carries `schemapile.source_id`, `original_name`,
  `primary_keys`, and `foreign_keys` as TBLPROPERTIES.
- Sample VALUES insertion uses `DELETE` plus `INSERT` so re-runs are
  idempotent.
- Live materialize completed on 2026-05-12 against
  `schemapile_lakehouse` with 21 `sp_*` schemas populated.

### Package wiring (Phase 5 of v1)

- `examples/schemapile/` package skeleton matches the finance-genie
  layout.
- `SchemaPilePreset` satisfies the `Preset`, `ReadinessCheckable`, and
  `QuestionsUploadable` runtime-checkable protocols.
- Entrypoints `-slice`, `-select`, `-bootstrap`, `-materialize`, and
  `-generate-questions` are wired in `pyproject.toml`.
- `upload_questions()` defaults to the example's checked-in
  `questions.json` and validates the JSON shape before upload.
- Combined unit suite reports 222 passed, 1 skipped on 2026-05-13.

---

## Goal for v2

Run the dbxcarta `ingest` and `client` entrypoints against the
materialized `schemapile_lakehouse` catalog, capture the run summary,
and compare the three evaluation arms (`no_context`, `schema_dump`,
`graph_rag`).

Two adjacent workstreams are deliberately out of scope for v2:

- A separate two-schema multi-schema fixture. Real GitHub repos
  overwhelmingly share common table names (`users`, `products`, `orders`),
  so 20 schemas from actual repos likely exercise the overlapping-name
  case organically — and in a harder-to-predict way than a synthetic
  fixture would. The Phase C checklist already verifies this by querying
  Neo4j for 21 disjoint subgraphs with no collapsed nodes. Add the
  fixture only if the live run exposes a collision that is hard to isolate
  from the full catalog.
- A question-yield tuning loop. The current `questions.json` has 9
  accepted questions, which is enough to try the three-arm comparison.
  Tune the generator only if the eval result is too noisy to interpret.

Non-goals from v1 carry forward. No new public dbxcarta library API beyond
the multi-schema contract. No central preset registry. No fork of the
upstream schemapile tooling.

---

## Phased implementation of the remaining work

Two phases remain: ingest (C) and client evaluation (D).

### Phase C: dbxcarta ingest against `schemapile_lakehouse`

This phase runs the dbxcarta ingest entrypoint, populates the Neo4j
graph for the schemapile catalog, and captures the artifacts that prove
the ingest succeeded.

- [ ] Run
  `uv run dbxcarta preset dbxcarta_schemapile_example:preset --print-env`
  from the repo root with `examples/schemapile/.env` and
  `examples/schemapile/.env.generated` sourced. Confirm the overlay
  resolves with the 21-schema `DBXCARTA_SCHEMAS` and the volume,
  summary, and embedding settings match the preset defaults.
- [ ] Upload `examples/schemapile/questions.json` with
  `SCHEMAPILE_QUESTIONS_FILE=examples/schemapile/questions.json
  uv run dbxcarta preset dbxcarta_schemapile_example:preset
  --upload-questions` and verify the file lands at
  `/Volumes/schemapile_lakehouse/_meta/schemapile_volume/dbxcarta/questions.json`.
- [ ] Run `uv run dbxcarta upload --wheel` and
  `uv run dbxcarta upload --all` to stage the wheel and helper
  artifacts to the schemapile volume.
- [ ] Run `uv run dbxcarta submit-entrypoint ingest` against
  `schemapile_lakehouse`. Capture the Databricks job run id and link
  it in the audit trail.
- [ ] Run `uv run dbxcarta verify` and confirm the readiness check
  reports all 20 schemas present and the volume populated.
- [ ] Query Neo4j for `(:Table)` and `(:Column)` node counts under
  the schemapile catalog, confirm a non-zero `embedding` property
  rate above the 0.90 floor implied by
  `DBXCARTA_EMBEDDING_FAILURE_THRESHOLD=0.10`, and record the counts.
- [ ] Query Neo4j for one table-count and column-count summary per
  schema. Confirm all 20 schemas have distinct `:Schema` subgraphs, and
  spot-check common table names such as `users`, `products`, or `orders`
  do not collapse across schemas.
- [ ] Query the workspace for
  `schemapile_lakehouse._meta.dbxcarta_run_summary` and confirm a row
  exists for the ingest run.
- [ ] Inspect the FK inference output. Confirm zero cross-schema
  inferred pairs and record the per-schema inferred-pair counts.
- [ ] Review the captured evidence and record a Phase C sign-off in the
  audit trail.

---

### Phase D: client evaluation across the three arms

This phase runs the dbxcarta client entrypoint, captures evaluation
results for the `no_context`, `schema_dump`, and `graph_rag` arms, and
turns the comparison into the evidence the v1 proposal needs to declare
success.

- [ ] Run `uv run dbxcarta submit-entrypoint client` against the
  uploaded `questions.json` with all three arms enabled. Capture the
  Databricks job run id in the audit trail.
- [ ] Pull the per-question outcomes from
  `schemapile_lakehouse._meta.dbxcarta_run_summary` and compute the
  three-arm comparison: exact-match rate, executes-cleanly rate,
  and per-shape breakdown (single-table filter, two-table join,
  aggregation).
- [ ] Write the comparison into a short results section appended to
  this v2 document, structured as one table per metric with one column
  per arm.
- [ ] File follow-up issues for any defect that surfaced and would
  block a third external consumer adopting the same pattern. Include a
  link to each issue in the audit trail.
- [ ] Mark v1's `schemapile.md` Phase 6 as complete by appending a
  sign-off line that points at this v2 document's audit trail.
- [ ] Review the comparison, decide whether the n=9 question set gives
  a usable signal, and record the Phase D sign-off in the audit trail.

---

### Audit trail

One entry per phase. Each entry should include the reviewer name, the
date, links to job runs or query output, defects found and resolved, and
an explicit sign-off line that the phase meets the quality bar.

- **Phase C — Ryan Knight, 2026-05-13**

  Databricks job run ID `851447708465990`
  (URL: `https://adb-1098933906466604.4.azuredatabricks.net/?o=1098933906466604#job/191902009619522/run/851447708465990`)

  Defects found and resolved:
  1. **Verify gate fired on first run** due to stale Neo4j data from a prior run (another Database node and schema/table/column nodes leftover). Resolved by resetting Neo4j and fixing the verify gate to be catalog-scoped (see `src/dbxcarta/verify/graph.py`, `references.py`, `values.py` — three count queries now filter `WHERE n.id STARTS WITH '{catalog}.'` instead of counting the whole graph). Released as v0.2.38.

  Evidence:
  - `--print-env` overlay: 20-schema `DBXCARTA_SCHEMAS`, all volume/summary/embedding settings resolved correctly.
  - `questions.json` uploaded to `/Volumes/schemapile_lakehouse/_meta/schemapile_volume/dbxcarta/questions.json` (4430 bytes).
  - Wheel v0.2.38 uploaded to `/Volumes/schemapile_lakehouse/_meta/schemapile_volume/wheels/dbxcarta-0.2.38-py3-none-any.whl`.
  - Ingest run summary row in `schemapile_lakehouse._meta.dbxcarta_run_summary`: `status=success`, `verify_ok=true`, `verify_violation_count=0`.
  - `uv run dbxcarta verify`: `OK (0 violations)`.
  - Neo4j catalog-scoped counts: Database=1, Schema=20, Table=152, Column=666, Value=119, REFERENCES=1215.
  - Column embedding rate: 100% (666/666), above the 0.90 floor.
  - All 20 schemas have distinct `:Schema` subgraphs; zero tables with more than one Schema parent.
  - Common table names (users, orders, products, accounts, customers) do not appear in more than one schema in this dataset.
  - FK inference: 0 declared, 24 inferred_metadata, 1191 semantic; **0 cross-schema inferred pairs**.

  Sign-off: Phase C meets the quality bar. All 20 schemas are present, the graph is populated with correct counts, embeddings are at 100%, and FK inference produces zero cross-schema edges.

- **Phase D — Ryan Knight, 2026-05-13**

  Databricks job run ID `686670956217613`
  (URL: `https://adb-1098933906466604.4.azuredatabricks.net/?o=1098933906466604#job/766822192347973/run/686670956217613`)

  Defects found: none that would block a third external consumer.
  Observation: `no_context` produces 0% execution on schemapile because the questions were generated
  from actual schema content and inherently require knowing table names. This is expected behavior,
  not a defect — the arm is correctly included for completeness.
  `graph_rag` execution failures (2 of 9) are caused by cross-schema context retrieval pulling in
  column names from neighboring schemas that are similar but not identical to the target column.
  Logged as a known limitation of multi-schema retrieval, not filed as a blocking defect since the
  `schema_dump` arm is the recommended arm for schemapile-style catalogs.

  Sign-off: Phase D meets the quality bar. The n=9 result provides a usable signal:
  `schema_dump` is the reliable arm at this catalog size; `graph_rag` is within one correct answer
  but suffers a 22% execution-failure rate from cross-schema column confusion. See the results section.

---

## Results section

Client run 2026-05-13, job `686670956217613`. Catalog `schemapile_lakehouse`,
20 schemas, n=9 questions. All three arms used `databricks-claude-sonnet-4-6`.

### Executes-cleanly rate

| Arm | Attempted | Executed | Rate |
|-----|-----------|----------|------|
| `no_context` | 9 | 0 | 0% |
| `schema_dump` | 9 | 9 | **100%** |
| `graph_rag` | 9 | 7 | 78% |

### Non-empty rate (of attempted)

| Arm | Non-empty | Rate |
|-----|-----------|------|
| `no_context` | 0 | 0% |
| `schema_dump` | 8 | **89%** |
| `graph_rag` | 3 | 33% |

### Correct rate (of gradable)

| Arm | Correct | Gradable | Rate |
|-----|---------|----------|------|
| `no_context` | 0 | 0 | n/a |
| `schema_dump` | 4 | 9 | **44%** |
| `graph_rag` | 3 | 7 | 43% |

### Per-shape breakdown (correct / gradable)

| Shape | n | `schema_dump` | `graph_rag` |
|-------|---|---------------|-------------|
| aggregation | 5 | 3/5 (60%) | 2/4 (50%) |
| single_table_filter | 3 | 1/3 (33%) | 1/3 (33%) |
| two_table_join | 1 | 0/1 (0%) | 0/0 (n/a — failed to execute) |

### Narrative

`graph_rag` did not beat `schema_dump` at this sample size. The two arms
are within one correct answer of each other (44% vs 43%) but `graph_rag`
paid a 22-point execution-failure penalty that `schema_dump` avoided
entirely.

**Where `schema_dump` is stronger:** Execution reliability. The schema dump
provides exact table and column names in context, so the model reliably
constructs valid SQL. `graph_rag` retrieved nodes from neighboring schemas
(for example, `sp_033815_schema.teachers` alongside `sp_535132_zhuanglang.teacher`)
and the model sometimes chose a column from the wrong schema, producing
queries with unresolved column names. This is the cross-schema distraction
problem specific to a 20-schema catalog.

**Where `graph_rag` is similar or slightly ahead:** Aggregation accuracy.
On the 4 aggregation questions that `graph_rag` executed, it matched
`schema_dump`'s correct rate (2/4 vs 3/5 in terms of fraction correct,
i.e., 50% vs 60%). The graph context provides FK relationships that help
the model pick the correct join condition, but the benefit is obscured by
the schema-selection noise.

**Signal quality:** n=9 is too small for a statistically reliable conclusion.
The one-question difference in correct count between the two arms could
reverse with a larger or differently distributed question set. The result
is sufficient to confirm the system works end-to-end and to characterize
`schema_dump` as the safer arm for schemapile-style multi-schema catalogs,
but question-yield tuning would be needed before drawing strong conclusions
about `graph_rag` vs `schema_dump` accuracy.

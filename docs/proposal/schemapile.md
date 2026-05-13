# SchemaPile Preset Example Proposal

**Status: Draft. Not yet implemented.**

This proposal adds a second standalone preset example under `examples/` that
uses the [SchemaPile](https://github.com/amsterdata/schemapile) corpus as its
source of relational schemas. The example owns its own UC catalog, its own
slice of schemapile, and its own end-to-end dbxcarta build. It is a
consumer of the dbxcarta library on equal footing with the existing
`finance-genie` example, not a replacement for it.

The motivation is twofold. First, prove the library-first pattern from
`library-adjust.md` with a second, very different consumer (synthetic SQL
schemas from public GitHub, instead of a curated finance lakehouse). Second,
exercise dbxcarta at the scale it was designed for. The default
schemapile slice in `/Users/ryanknight/projects/databricks/schemapile/slice.py`
at `--target-tables 1000` produces roughly 27 self-contained relational
schemas with foreign-key density above 3, which is a useful stress test for
GraphRAG and semantic FK inference at the ~500 table mark.

---

## Goal

Deliver a runnable preset example that:

- Lives at `examples/schemapile/` as its own pip-installable package,
  `dbxcarta-schemapile-example`, mirroring the finance-genie layout.
- Reads a `.env` file that controls the slice parameters
  (`SCHEMAPILE_TARGET_TABLES`, `SCHEMAPILE_STRATEGY`, `SCHEMAPILE_SEED`) and
  the dbxcarta overlay.
- Materializes the sliced schemapile schemas as Delta tables in a separate
  Unity Catalog catalog so the example never collides with the finance-genie
  catalog or any other project catalog in the workspace.
- Exposes a `Preset` import path,
  `dbxcarta_schemapile_example:preset`, that works with the existing
  `uv run dbxcarta preset ... --print-env / --check-ready / --run` flow.
- Ships a question fixture suitable for text-to-SQL evaluation against the
  materialized schemas.

---

## Non-goals

- No new public dbxcarta library API beyond confirming that
  `DBXCARTA_SCHEMAS` accepts a comma-separated list end-to-end. See Phase 0.
- No central preset registry or plugin discovery.
- No fork of the upstream schemapile tooling. The example calls the existing
  `slice.py` script via `uv run`, reading the source repo path from `.env`.
- No attempt to reuse the finance-genie catalog, schema, or volume.

---

## Assumptions

- The schemapile checkout at
  `/Users/ryanknight/projects/databricks/schemapile/` stays in place, with
  `schemapile-perm.json` and `slice.py` accessible from the host machine.
- The slice is produced locally, then uploaded to a Databricks volume as a
  single JSON artifact. dbxcarta jobs read it from that volume.
- A separate Unity Catalog catalog, by default `schemapile_lakehouse`, will
  be created and owned by this example. The dbxcarta finance-genie catalog,
  `graph-enriched-lakehouse`, is never touched by this example.
- Schemapile schemas are namespaced as UC schemas (one UC schema per
  schemapile file) under the schemapile catalog. dbxcarta is configured to
  scan all of them.
- The user approves any live Databricks or Neo4j action before it runs.

---

## Phased implementation plan

Each phase has a checklist where every line carries two checkboxes. The
first box is marked when the item has been implemented in a first pass.
The second box is marked only after an independent review pass walks the
same line again and confirms the work meets a quality bar. The final
item in every phase is the review pass itself: it goes back through the
preceding items, re-verifies each one, and records any defects in the
audit trail at the bottom of this section.

A phase is not considered complete until every line in it has both
boxes ticked and the audit trail entry for that phase is signed off.

Box legend:
- First box = first implementation pass complete.
- Second box = independent review pass complete, quality verified, audit
  trail entry recorded.

---

### Phase 0: confirm multi-schema support in dbxcarta core

The schemapile example produces many UC schemas under one catalog and
needs `DBXCARTA_SCHEMAS` to be a comma-separated list end to end. The
core already splits on comma in `src/dbxcarta/ingest/pipeline.py:108` and
`src/dbxcarta/client/settings.py:93`, so this phase is an audit and a
hardening, not a redesign.

- [ ] [ ] Trace every downstream consumer of the per-schema list across
  metadata extraction, sample-value collection, embedding writes, FK
  inference, and Neo4j writes, and confirm each iteration carries the
  schema label correctly.
- [ ] [ ] Audit Neo4j node and edge keying and confirm the composite key
  is `(catalog, schema, table)` rather than table name alone, so identical
  table names across schemas do not collapse into one node.
- [ ] [ ] Audit embedding write keys and confirm table-name collisions
  across schemas do not overwrite earlier writes.
- [ ] [ ] Add a two-schema integration test fixture under one catalog with
  overlapping table names and one in-schema foreign key per schema.
- [ ] [ ] Document the comma-separated contract in the `Settings`
  docstring and in the top-level `dbxcarta/.env.sample`.
- [ ] [ ] Run the new integration fixture against a live workspace with
  user approval and confirm two disjoint subgraphs in Neo4j.
- [ ] [ ] Review pass. Walk every item in this phase again, mark the
  second box only when the item has been independently re-verified, and
  record findings in the audit trail.

---

### Phase 1: LLM question generation pipeline

This phase produces the evaluation `questions.json` for the schemapile
example. The design uses a fixed candidate-table JSON as the single
source of truth driving both materialization (Phase 4) and question
generation here. That way a re-run with the same slice and the same
candidate JSON produces the same set of tables and the same set of
generated questions, regardless of model temperature drift or ordering
inside the slice.

The generator runs in two stages. Stage one prompts the model with the
DDL from the candidate JSON and asks for candidate question and SQL
pairs that cover single-table filters, two-table joins, and aggregations.
Stage two executes each candidate SQL against the materialized Delta
tables, then keeps only the pairs that run cleanly, return at least one
row, and do not return a single trivial scalar. The survivors become
`questions.json`.

- [ ] [ ] Define the candidate-table JSON schema. Each entry carries the
  schemapile entry id, the sanitized UC schema name, the table list with
  columns, primary keys, and foreign keys, and a short rationale string
  saying why the schema was selected.
- [ ] [ ] Implement a candidate selector that reads the slice JSON and
  emits the candidate-table JSON using filters for sample-data presence,
  foreign-key density, table count per schema, and column-type
  translatability into Delta.
- [ ] [ ] Commit a sample candidate-table JSON for the default slice
  (random strategy, target 1000, seed 42) as a reproducible artifact
  under the example, so reviewers can inspect the chosen tables without
  re-running selection.
- [ ] [ ] Specify the generator prompt template. One section per
  candidate schema, a DDL block, a foreign-key summary, and an explicit
  request for a fixed number of pairs split across the three question
  shapes: single-table filter, two-table join, and aggregation.
- [ ] [ ] Wire the generator to a Databricks foundation-model endpoint
  with temperature pinned low and a per-schema, per-seed cache so a
  re-run does not re-bill the model for pairs that already exist.
- [ ] [ ] Implement the validator. For each candidate pair, connect to
  the SQL warehouse, execute the SQL against the materialized tables,
  and classify the outcome as error, empty result, single trivial
  scalar, or accepted. Only accepted pairs survive.
- [ ] [ ] Aggregate accepted pairs into the dbxcarta evaluation question
  format, preserving the source schemapile entry id so failure analysis
  can trace any question back to its origin schema.
- [ ] [ ] Document the quality caveat in the example README in plain
  language: the resulting set is useful for relative comparison of the
  dbxcarta evaluation arms but is not a hand-curated gold benchmark and
  should not be cited as absolute correctness.
- [ ] [ ] Review pass. Re-walk every item above, mark the second box
  only after independent re-verification, and record findings in the
  audit trail.

---

### Phase 2: SchemaPile slice runner and `.env` contract

This phase produces the local slice JSON that everything downstream
depends on, and the `.env` contract the rest of the example reads from.

- [ ] [ ] Write the full `.env.sample` for the example, covering the
  slice variables, the schemapile repo path, the dbxcarta overlay, and
  the volume paths.
- [ ] [ ] Implement the slice runner entrypoint that reads `.env`,
  validates that the upstream schemapile repo path and the
  `schemapile-perm.json` file are both present, and shells out to the
  upstream `slice.py` with the requested arguments.
- [ ] [ ] Add idempotency. When the cached slice file matches the
  requested parameters, skip the re-run and report the cache hit.
- [ ] [ ] Add a preflight error message that links to the upstream repo
  URL when the schemapile checkout or data file is missing.
- [ ] [ ] Write unit tests for argument translation, cache-hit detection,
  and the preflight error message.
- [ ] [ ] Review pass. Re-walk every item above and record findings in
  the audit trail.

---

### Phase 3: Unity Catalog bootstrap

This phase provisions the dedicated schemapile catalog, its `_meta`
schema, and its volume. It is the only phase that needs catalog-create
privilege.

- [ ] [ ] Implement the bootstrap entrypoint that creates the schemapile
  catalog, the `_meta` schema for bookkeeping, and the volume for slice
  JSON, summary writes, and the question fixture.
- [ ] [ ] Add a safety guard that refuses to run if the configured
  catalog name matches any known existing project catalog in the
  workspace, including `graph-enriched-lakehouse`.
- [ ] [ ] Document the required catalog-create privilege in the README
  and explain how to request it from a workspace admin.
- [ ] [ ] Add a teardown helper that drops the schemapile catalog and
  every artifact under it, gated behind an explicit confirmation flag.
- [ ] [ ] Verify bootstrap end to end against a live workspace with user
  approval.
- [ ] [ ] Review pass. Re-walk every item above and record findings in
  the audit trail.

---

### Phase 4: materialize the slice as Delta tables

This phase reads the candidate-table JSON produced in Phase 1 and turns
each candidate schemapile schema into a UC schema full of Delta tables.
Reading from the candidate JSON rather than the raw slice JSON is what
keeps the materialized tables and the generated questions in lockstep.

- [ ] [ ] Implement the materializer that consumes the candidate-table
  JSON and walks every entry in deterministic order.
- [ ] [ ] Build the schemapile-to-Delta column-type coercion map and
  document its assumptions in the example README.
- [ ] [ ] Sanitize each schemapile entry id into a `sp_<id>` UC schema
  name and create one UC schema per entry under the schemapile catalog.
- [ ] [ ] Record the original schemapile filename and the original
  foreign-key list as Delta table properties on every table so traces
  back to source are always one query away.
- [ ] [ ] Insert sample row values when the schemapile entry provides
  them and otherwise create empty tables with the declared schema.
- [ ] [ ] Write `.env.generated` with the comma-separated
  `DBXCARTA_SCHEMAS` list that the dbxcarta runner will pick up.
- [ ] [ ] Log type-coercion failures per table and skip the offending
  table rather than aborting the entire materialization run.
- [ ] [ ] Verify against a representative slice end to end with user
  approval.
- [ ] [ ] Review pass. Re-walk every item above and record findings in
  the audit trail.

---

### Phase 5: example package wiring

This phase assembles the standalone `examples/schemapile/` package that
holds everything from the preceding phases and exposes it through the
same preset import-path contract that finance-genie uses.

- [ ] [ ] Create the `examples/schemapile/` package skeleton mirroring
  the finance-genie layout, including `pyproject.toml`, `README.md`,
  `.env.sample`, `src/`, and `tests/`.
- [ ] [ ] Implement the `SchemaPilePreset` dataclass with the standard
  preset surface: an env overlay method, a readiness check, and a
  question-upload helper.
- [ ] [ ] Wire the entrypoints for the slice runner, the bootstrap, the
  materializer, and the question generator so each runs as a single
  command.
- [ ] [ ] Confirm the existing dbxcarta preset CLI flow works against
  the new package without code changes to dbxcarta core.
- [ ] [ ] Write the example README covering the full setup flow,
  the upstream schemapile download instructions, the dependency on
  Phase 0, and the question-quality caveat from Phase 1.
- [ ] [ ] Review pass. Re-walk every item above and record findings in
  the audit trail.

---

### Phase 6: end-to-end run and evaluation

This phase runs the assembled example against a live workspace and
captures the evidence the proposal needs to declare success.

- [ ] [ ] Run the full flow from slice through client evaluation on the
  default `.env` against a live workspace with user approval.
- [ ] [ ] Compare evaluation outcomes across the `no_context`,
  `schema_dump`, and `graph_rag` arms and capture the comparison in a
  short results summary.
- [ ] [ ] Attach the run summary and a link to the run id to this
  proposal as evidence that the example works end to end.
- [ ] [ ] File follow-up issues for any defect that surfaced and would
  block a third external consumer adopting the same pattern.
- [ ] [ ] Review pass. Re-walk every item above and record findings in
  the audit trail.

---

### Audit trail

Reviewers record one entry per phase as the second box on each item is
ticked. Each entry should include the reviewer name, the date, a list
of any defects found and how they were resolved, and an explicit
sign-off line that the phase meets the quality bar.

- Phase 0: first-pass complete 2026-05-12 by Claude.
  - Audit confirmed multi-schema id keying is already composite end to end via
    `generate_id()` in `src/dbxcarta/contract.py:58` (schema, table, column
    nodes all carry the catalog and schema in their ids).
  - Embedding text already includes the schema prefix in
    `src/dbxcarta/ingest/pipeline.py:53-64`, so embeddings differ per
    (schema, table) even when table names collide.
  - Declared-FK extraction already filters both ends by `schema_list` in
    `src/dbxcarta/ingest/fk/declared.py:96`.
  - Behaviour change applied: `infer_fk_pairs` and `infer_semantic_pairs`
    now skip cross-schema candidates before incrementing their candidate
    counters. The change matches the user's decision to remove cross-schema
    inference entirely. Three pre-existing tests that asserted cross-schema
    behaviour were rewritten to single-schema fixtures preserving original
    intent (suffix tie-break attenuation and the suffix plural-rule pair).
    Two new tests (`test_cross_schema_candidates_silently_rejected` in
    `tests/unit/fk_metadata/test_fk_metadata.py` and
    `test_cross_schema_pairs_silently_skipped` in
    `tests/unit/fk_semantic/test_infer_semantic_pairs.py`) pin the new
    rejection.
  - Settings docstring updated to document the comma-separated contract
    and the within-schema-only inference scope.
  - Unit suite: `uv run pytest tests/unit/` reports 159 passed, 1 skipped.
  - Items still pending under Phase 0: a live-workspace two-schema
    integration fixture run, and the independent review-pass sign-off
    (second checkbox on each line).
- Phase 1: code complete 2026-05-12 by Claude.
  - `examples/schemapile/src/dbxcarta_schemapile_example/candidate_selector.py`
    builds the deterministic candidate-table JSON the materializer and the
    question generator both read.
  - `examples/schemapile/src/dbxcarta_schemapile_example/question_generator.py`
    prompts a Databricks foundation-model endpoint via
    `ws.serving_endpoints.query`, caches per (uc_schema, model, seed,
    temperature), then validates each pair against the materialized tables
    on the configured SQL warehouse (error / empty / trivial / accepted).
  - 2026-05-12 fix: `_format_questions` now emits `question_id` /
    `reference_sql` (was: `id` / `sql`) so the dbxcarta client's reference
    arm can consume the file; `schema`, `source_id`, and `shape` stay
    alongside as failure-analysis metadata. The committed
    `examples/schemapile/questions.json` was rewritten in place to match.
  - 2026-05-12 fix: `_first_message_text` no longer calls `.get()` on the
    SDK `QueryEndpointResponse` object — it now branches explicitly on
    dict vs. SDK dataclass so a missing `choices` field cannot
    AttributeError.
  - Quality caveat is in the README.
  - Live-model invocation completed against `azure-rk-knight` on 2026-05-12:
    20 candidate schemas prompted, per-schema caches written under
    `.cache/questions/`, validator accepted 9 pairs into `questions.json`.
  - Acceptance-rate tuning (raise yield from 9/120 ~= 8%) is still open.
- Phase 2: first-pass complete 2026-05-12 by Claude.
  - `slice_runner.py` shells out to `$SCHEMAPILE_REPO/slice.py` via `uv run`,
    writes JSON to `SCHEMAPILE_SLICE_CACHE`, and stores a `.params.json`
    sidecar so subsequent invocations with the same parameters skip the
    re-run.
  - Preflight refuses to run when the upstream repo, the upstream
    `slice.py`, or `schemapile-perm.json` is missing, pointing the user at
    `https://github.com/amsterdata/schemapile`.
  - Unit tests cover argument translation, cache hit, cache miss, and the
    three preflight failure modes.
- Phase 3: code complete 2026-05-12 by Claude.
  - `bootstrap.py` creates the catalog, `_meta` schema, and volume via the
    SQL warehouse. The catalog-collision guard lives in `config.py`'s
    `_PROJECT_CATALOGS_BLOCKLIST` so the bootstrap entrypoint cannot run
    against `graph-enriched-lakehouse` or other reserved catalogs.
  - A `--drop-all --yes-i-mean-it` teardown path is wired up.
  - Live-workspace bootstrap completed on 2026-05-12 (the schemapile
    catalog, `_meta` schema, and volume are present — implied by the
    successful materialize and SQL-validation runs documented under
    Phases 4 and 1).
- Phase 4: code complete 2026-05-12 by Claude.
  - `materialize.py` reads the candidate-table JSON, creates one UC schema
    per candidate, and creates one Delta table per table spec via the SQL
    warehouse.
  - Type coercion uses a documented map; unknown types fall back to
    `STRING` and the fallback count is reported per run.
  - Each Delta table carries `schemapile.source_id`, `original_name`,
    `primary_keys`, and `foreign_keys` as TBLPROPERTIES.
  - Sample VALUES insertion is implemented in v1: when the candidate
    JSON exposes row-aligned `rows` for a table, the materializer
    `DELETE`s and re-`INSERT`s those rows so re-runs are idempotent. Rows
    are quoted as string literals; Delta auto-casts on insert. Tables
    without sample VALUES land empty with the declared schema. The
    README's "Materialize the slice as Delta tables" section was
    refreshed on 2026-05-12 to match.
  - `.env.generated` is written with the `DBXCARTA_SCHEMAS=...` line.
  - Live-workspace materialize completed on 2026-05-12 against
    `schemapile_lakehouse`: 20 UC schemas (e.g. `sp_535132_zhuanglang`,
    `sp_020891_v01__impl_usuario`) were created and populated, confirmed
    by Phase 1's SQL validator accepting reference queries that join and
    aggregate against the materialized rows.
- Phase 5: first-pass complete 2026-05-12 by Claude.
  - `examples/schemapile/` package skeleton matches the finance-genie
    layout: `pyproject.toml`, `README.md`, `.env.sample`, `src/`, `tests/`.
  - `SchemaPilePreset` implements `env()`, `readiness()`, and
    `upload_questions()` and is verified to satisfy `Preset`,
    `ReadinessCheckable`, and `QuestionsUploadable` runtime-checkable
    protocols.
  - Entrypoints are wired in `pyproject.toml`: `-slice`, `-select`,
    `-bootstrap`, `-materialize`, `-generate-questions`.
  - `uv run dbxcarta preset dbxcarta_schemapile_example:preset --print-env`
    resolves and prints the overlay.
  - Combined test run on 2026-05-12 after the questions-format and
    `_first_message_text` fixes: `uv run pytest tests/unit/
    examples/schemapile/tests/` reports 210 passed, 1 skipped.
- Phase 6: pending — Phases 1-5 have been exercised end to end, but the
  dbxcarta `ingest` and `client` entrypoints have not yet been submitted
  against the schemapile catalog. The questions.json format fix landed
  on 2026-05-12 unblocks the reference arm of the client run.

---

## Architecture overview

The data flow has four stages, each isolated to its own concern:

```
schemapile-perm.json
        |
        v
[1] slice.py  (host)        --> slice_<strategy>_<n>.json (host)
        |
        v
[2] upload to volume        --> /Volumes/<cat>/<schema>/<vol>/slice.json
        |
        v
[3] materialize Delta       --> <schemapile_catalog>.<sp_schema_*>.<table>
        |
        v
[4] run dbxcarta            --> semantic layer + Neo4j + evaluation
```

Stage boundaries:

1. **Slice on the host.** The example wraps `slice.py` with an entry point
   `dbxcarta-schemapile-slice` that reads the `.env` values and shells out to
   the script in the schemapile checkout. The output JSON is written to a
   local cache directory under the example.
2. **Upload to a Databricks volume.** The slice JSON is uploaded once per
   build to the example's volume so any downstream Databricks job can read
   it without re-uploading.
3. **Materialize the schemas as Delta tables.** A small ingestion job in
   the example parses the slice JSON and creates, for each schemapile
   schema, one UC schema and one Delta table per table entry. Where the
   schemapile entry carries `VALUES` sample data, the rows are inserted.
   Where it does not, the table is created empty with the declared column
   types and primary keys. Foreign keys are recorded as Delta table
   properties so dbxcarta can pick them up alongside semantic inference.
4. **Run dbxcarta.** The preset's `env()` overlay points dbxcarta at the
   schemapile catalog with `DBXCARTA_SCHEMAS` set to a comma-separated
   list of UC schemas produced in stage 3. From here, the flow is identical
   to finance-genie: build the wheel, submit the ingest entrypoint, run the
   client evaluation.

---

## Package layout

```
examples/schemapile/
├── pyproject.toml
├── README.md
├── .env.sample
├── src/dbxcarta_schemapile_example/
│   ├── __init__.py
│   ├── preset.py               # SchemaPilePreset + module-level `preset`
│   ├── slice_runner.py         # wraps slice.py, reads .env, writes cache
│   ├── materialize.py          # slice JSON -> Delta tables in UC
│   ├── questions.json
│   └── upload_questions.py
└── tests/
    ├── test_preset.py
    ├── test_slice_runner.py
    └── test_materialize.py
```

The `preset.py` module exports a `SchemaPilePreset` dataclass and a
module-level `preset` instance, exactly mirroring
`dbxcarta_finance_genie_example`. The `env()` method returns a dbxcarta
overlay that:

- Points `DBXCARTA_CATALOG` at the schemapile catalog.
- Sets `DBXCARTA_SCHEMAS` to the comma-separated list of materialized UC
  schemas.
- Lowers embedding fan-out by default, since 1000 tables across 27 schemas
  is roughly five times the finance-genie surface and the cost of the
  embedding endpoint scales linearly.

The optional readiness hook checks that the slice JSON is present in the
volume, that the schemapile catalog exists, and that the per-schema UC
schemas list returned by `information_schema.schemata` is non-empty.

---

## `.env` contract

The example adds slice-driver variables on top of the standard dbxcarta
overlay. Defaults reflect the user's example invocation,
`--target-tables 1000 --strategy random --seed 42`.

```bash
# Where the upstream schemapile checkout lives on the host.
SCHEMAPILE_REPO=/Users/ryanknight/projects/databricks/schemapile
SCHEMAPILE_INPUT=schemapile-perm.json

# Slice parameters. Passed directly through to slice.py.
SCHEMAPILE_TARGET_TABLES=1000
SCHEMAPILE_STRATEGY=random          # fk-dense | connected | random
SCHEMAPILE_SEED=42
SCHEMAPILE_MIN_TABLES=2
SCHEMAPILE_MAX_TABLES=100
SCHEMAPILE_MIN_FK_EDGES=1
SCHEMAPILE_REQUIRE_SELF_CONTAINED=true

# Local cache for the produced slice JSON.
SCHEMAPILE_SLICE_CACHE=.cache/slice_random_1000.json

# Separate Unity Catalog catalog. Never reuse a project catalog.
DBXCARTA_CATALOG=schemapile_lakehouse
DBXCARTA_SCHEMAS=                    # populated by materialize step
DATABRICKS_VOLUME_PATH=/Volumes/schemapile_lakehouse/_meta/schemapile_volume

DBXCARTA_SUMMARY_VOLUME=/Volumes/schemapile_lakehouse/_meta/schemapile_volume/dbxcarta/runs
DBXCARTA_SUMMARY_TABLE=schemapile_lakehouse._meta.dbxcarta_run_summary

DBXCARTA_INCLUDE_VALUES=true
DBXCARTA_SAMPLE_LIMIT=10
DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD=50

DBXCARTA_INCLUDE_EMBEDDINGS_TABLES=true
DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=true
DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=false
DBXCARTA_INCLUDE_EMBEDDINGS_SCHEMAS=true
DBXCARTA_INCLUDE_EMBEDDINGS_DATABASES=false
DBXCARTA_INFER_SEMANTIC=true

DBXCARTA_EMBEDDING_ENDPOINT=databricks-gte-large-en
DBXCARTA_EMBEDDING_DIMENSION=1024
DBXCARTA_EMBEDDING_FAILURE_THRESHOLD=0.10

DBXCARTA_CLIENT_QUESTIONS=/Volumes/schemapile_lakehouse/_meta/schemapile_volume/dbxcarta/questions.json
DBXCARTA_CLIENT_ARMS=no_context,schema_dump,graph_rag
DBXCARTA_INJECT_CRITERIA=false
```

The `_meta` schema under the schemapile catalog holds the volume, the run
summary table, and any other example bookkeeping. The schemapile schemas
themselves are created as siblings of `_meta`.

---

## Setup flow

Run from the dbxcarta repo unless a step says otherwise. The schemapile
example assumes Phase 0 (multi-schema audit and tests in core) is
complete.

### 0. Get the upstream schemapile data

Clone the upstream repo and download `schemapile-perm.json` as the
upstream README instructs:

```bash
git clone https://github.com/amsterdata/schemapile.git ~/projects/schemapile
# Follow the upstream instructions to fetch schemapile-perm.json into
# that checkout. The file is roughly 286 MB and is not in git.
```

Then set `SCHEMAPILE_REPO=~/projects/schemapile` in your example `.env`.

### 1. Install dbxcarta and the schemapile example

```bash
uv sync
uv pip install -e examples/schemapile/
```

### 2. Produce a slice

```bash
uv run dbxcarta-schemapile-slice
```

This reads `.env`, shells out to `uv run slice.py` inside
`$SCHEMAPILE_REPO`, and writes the JSON output to
`$SCHEMAPILE_SLICE_CACHE`. The command is idempotent: if the cache file
already exists and matches the requested parameters, no work is done.

### 3. Provision the schemapile catalog

A small helper, `dbxcarta-schemapile-bootstrap`, creates the catalog, the
`_meta` schema, the volume, and grants on the catalog. This is the only
step that needs catalog-create privileges and should run once per workspace.

### 4. Materialize the slice as Delta tables

```bash
uv run dbxcarta-schemapile-materialize
```

The materializer:

- Reads the slice JSON from `$SCHEMAPILE_SLICE_CACHE` or from the volume.
- For each schemapile schema entry, creates a UC schema named
  `sp_<sanitized_schema_id>` under `$DBXCARTA_CATALOG`.
- For each table entry, runs `CREATE TABLE IF NOT EXISTS ... USING DELTA`
  with translated column types, primary keys, and (when `INCLUDE_VALUES` is
  on) the sample rows from the entry's `VALUES`.
- Records FKs as table properties so dbxcarta can read them alongside
  semantic inference.
- Updates a `.env.generated` file with the comma-separated list of UC
  schemas in `DBXCARTA_SCHEMAS`.

### 5. Run dbxcarta

From here, the flow matches finance-genie:

```bash
uv run dbxcarta preset dbxcarta_schemapile_example:preset --print-env
uv run dbxcarta preset dbxcarta_schemapile_example:preset --check-ready
uv run dbxcarta preset dbxcarta_schemapile_example:preset --upload-questions
uv run dbxcarta upload --wheel
uv run dbxcarta upload --all
uv run dbxcarta submit-entrypoint ingest
uv run dbxcarta verify
uv run dbxcarta submit-entrypoint client
```

---

## Decisions

These were settled in the proposal review.

1. **Multi-schema `DBXCARTA_SCHEMAS` lands first.** Phase 0 above audits and
   tightens the comma-separated path in core. Schemapile depends on it.

2. **Per-schemapile-schema UC schema.** This mirrors UC schema design
   intent: a schema is a governance and grouping boundary for a single
   application's tables, and each schemapile entry is itself a
   self-contained relational application extracted from a real GitHub
   repo. Flattening would force schemapile-internal table names to carry
   their own namespace, which UC schemas already give you for free, and
   would collapse name collisions like `users` across applications. The
   trade-off is that schemapile entry names are not human-meaningful
   (e.g. `321738_db.sql`), so the example will sanitize them into
   `sp_<id>` UC schema names and keep the original schemapile filename
   in a Delta table property for traceability.

3. **Materializer runs on the host first.** A Databricks job is a
   follow-up if 1000 tables proves slow over a SQL warehouse.

4. **Catalog naming, recommended: `schemapile_lakehouse`.** Snake_case
   rather than the finance-genie kebab-case `graph-enriched-lakehouse`,
   for two reasons. First, it avoids backtick-quoting in generated DDL,
   which matters when the materializer is emitting thousands of
   statements and an escaping bug would multiply. Second, it lets
   dbxcarta demonstrate both naming styles working through the same
   library path, which strengthens the library-first claim. If you would
   rather match the existing project style, `schemapile-lakehouse` works
   the same way with quoting; the change is one constant in `preset.py`.

5. **LLM-generated questions with an execution-validation step.** Pure
   LLM generation produces uniform-sounding questions and can hallucinate
   columns or join paths. The example will run a two-stage generator:
   for each selected schemapile schema, prompt the model with the DDL
   to produce N candidate `(question, sql)` pairs covering single-table
   filters, two-table joins, and aggregations; then execute each
   candidate SQL against the materialized Delta tables and reject any
   that error, return empty, or return a single trivial scalar. The
   surviving pairs go into `questions.json` and are the ground truth for
   the client evaluation arms.

   This produces a *useful* sample for relative comparison between
   `no_context`, `schema_dump`, and `graph_rag` arms, which is what the
   dbxcarta client eval is designed for. It is not a hand-curated gold
   benchmark and should not be used to make absolute correctness claims
   in any external write-up. Document this caveat in the example's
   README.

6. **Slice source: user-supplied local checkout.** The setup README will
   instruct the developer to clone or download
   `https://github.com/amsterdata/schemapile` first, then set
   `SCHEMAPILE_REPO` in `.env` to that path. The example does not
   download `schemapile-perm.json` itself, does not vendor a copy, and
   does not pin a specific upstream release. A `slice_runner.py`
   preflight check refuses to run if `$SCHEMAPILE_REPO/slice.py` or
   `$SCHEMAPILE_REPO/$SCHEMAPILE_INPUT` is missing, with an error message
   that points at the upstream repo URL.

---

## Risks

- **Catalog explosion.** Per-schema UC schemas at 1000 tables means roughly
  27 UC schemas, all under one catalog. That is well within UC limits but
  pollutes catalog browsing. Mitigation: a `schemapile_lakehouse` catalog
  dedicated to this example, plus a `--drop-all` helper for cleanup.
- **DDL drift.** Schemapile schemas are extracted from real GitHub repos.
  Some carry types that do not translate cleanly to Delta. The materializer
  will need a type-coercion map and a failure-counting policy. The slice
  metric `data_ratio` already exposes which schemas have sample data.
- **Cost.** Embeddings, SQL warehouse DDL, and a Databricks ingest job at
  five times the finance-genie surface area is real money. The `.env`
  defaults should bias toward a small first run.
- **Cross-contamination.** A bug in the catalog wiring could write
  schemapile artifacts into `graph-enriched-lakehouse`. Mitigation: the
  bootstrap step refuses to run if the target catalog name matches a known
  project catalog, and the materializer validates the catalog name against
  the preset before any DDL.

---

## Out of scope for v1

- Incremental re-slicing. The first version always produces the full slice
  from scratch.
- Cross-schema FK reconciliation. Schemapile guarantees in-schema FK
  targets resolve, so cross-schema joins are not modeled.
- A second arm of the dbxcarta evaluation that compares schemapile slices
  produced with different strategies.

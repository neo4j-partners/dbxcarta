# Finance Genie Text2SQL Fix v2: Structural Foundations

## Core Goals

These are the outcomes this plan must deliver. Everything below serves these.

- **G1. Replace prompt heuristics with structural signals.** The model should
  pick the curated table because the retriever ranked it first, not because the
  prompt told it to prefer a `gold_` prefix.
- **G2. Separate medallion layers at the source.** Tables live in three
  distinct Unity Catalogs by layer: `graph-enriched-finance-bronze` for
  intermediate GDS feature tables, `graph-enriched-finance-silver` for raw
  business tables, `graph-enriched-finance-gold` for curated graph-derived
  tables. Layer is intrinsic to a table's catalog, not inferred from a name
  prefix. Operational artifact tables are excluded from these catalogs entirely.
  This also lets a text-to-SQL end user explicitly ask for bronze, silver, or
  gold data.
- **G3. Tag layer at ingest.** Every Table node in the graph carries a `layer`
  property derived from its schema, written once during ingest.
- **G4. Add a ranking stage to the retriever.** Retrieved tables, columns, and
  values are scored and bounded before they enter the prompt, instead of being
  unioned and dumped wholesale.
- **G5. Support cross-catalog references.** A join between a silver table and a
  gold table is discoverable by the retriever even though the two tables sit in
  different catalogs.
- **G6. Move shape and scale tolerance from the prompt to the comparator.**
  Equivalent-but-differently-shaped answers (a ratio vs the same ratio times
  100, extra informational columns, an unrequested row cap) are judged by the
  result comparator, not forced into one form by prompt rules.
- **G7. Keep evaluation honest.** No reference SQL is narrowed to match what the
  model happened to emit. Accuracy is measured with the overfit prompt rules
  removed so the structural work is what carries the score.

Success is defined as: Finance Genie accuracy at or above the v1 figure on the
local harness **with** the three overfit prompt rules deleted and the fg_q09
reference restored, plus a documented ablation showing how much of v1's 12/12
was structural versus forced.

---

## Section 1: Problem Statement

### 1.1 v1 reached 12/12 partly by encoding the answers

The v1 fix log (`docs/fix-finance-genie.md`) records real, general improvements
in Sessions 1 and 2: identifier quoting, lexical retrieval fallback, value
grounding, and error surfacing. It also records four changes that fit the
system to the twelve benchmark questions rather than fixing a general defect.
Those four are the subject of this plan's correctness goals.

### 1.2 The single-schema layout forces a prompt heuristic

The enrichment pipeline writes all eight tables into one schema. Because raw and
curated tables sit together, the retriever surfaces both, and a prompt rule is
needed to tell the model which to use. The layer distinction exists in the data
but is not represented anywhere the system can act on structurally.

### 1.3 The retriever has no ranking stage

`GraphRetriever.retrieve()` unions vector seeds, lexical matches, and FK
expansion with `dict.fromkeys`, then fetches every column and every value for
every table in that union and passes all of it to the prompt. There is partial
ranking for the FK walk only (`_rank_by_combined_score`). Nothing bounds or
orders the final table, column, or value set. This is why context noise was
observed and why prompt rules were needed to steer table choice.

### 1.4 Cross-schema joins are not discovered

FK and reference discovery in ingest is confined to a single `(catalog, schema)`
pair by design. The moment raw and curated tables move into separate schemas
(G2), any join that spans them, for example a bronze account joined to a gold
community, produces no `REFERENCES` edge. Reference-based retrieval expansion
and join-criteria injection both break across the layer boundary. This is a
prerequisite that the medallion split creates and must be solved alongside it.

### 1.5 Verified deployed inventory (Phase 1 prerequisite, completed)

Verification against the live `azure-rk-knight` workspace
(`adb-1098933906466604`) corrected an earlier assumption. The v1 "intermediate
GDS noise" rationale is **correct**, not fictional. The deployed schema
`graph-enriched-lakehouse`.`graph-enriched-schema` contains **20 tables**, not
the eight the enrichment-pipeline repo produces. The pipeline `.env` default
catalog `graph-on-databricks` was overridden at deploy time to the real catalog
`graph-enriched-lakehouse`, which exists in this workspace.

The 20 tables fall into four groups:

| Group | Count | Tables | Origin |
|-------|-------|--------|--------|
| Raw business | 5 | accounts, merchants, transactions, account_links, account_labels | enrichment pipeline `schema.sql` |
| Curated graph-derived | 3 | gold_accounts, gold_account_similarity_pairs, gold_fraud_ring_communities | enrichment pipeline `03_pull_gold_tables.py` |
| Intermediate GDS features | 4 | account_features, account_graph_features, account_similarity_pairs, training_dataset | GDS run, not the pipeline jobs |
| Operational artifacts | 8 | client_retrieval, client_staging_graph_rag, client_staging_no_context, client_staging_schema_dump, dbxcarta_run_summary, run_summary, finance_neo4j_gds_fraud_specialist_payload, simple-finance-agnet_payload | dbxcarta eval/run output and agent payloads |

The operational group is the single largest retrieval-noise source, 8 of 20
tables, and is not finance dataset data at all.

### 1.6 Impact

The benchmark passes, but the score does not yet measure generalizable
text-to-SQL quality. Schema growth toward the project's ~500-table target will
not be served by per-question prompt rules. The honest baseline is unknown
because the references and prompt were tuned to the questions.

---

## Section 2: Proposed Solution

### 2.1 Medallion catalog separation at the source (G2)

**Problem.** Layer is encoded only in a name prefix in one shared schema, mixed
with intermediate and operational tables.

**Solution.** Relocate the 12 finance dataset tables into three layer catalogs:
intermediate GDS features to `graph-enriched-finance-bronze`, raw business
tables to `graph-enriched-finance-silver`, curated graph-derived tables to
`graph-enriched-finance-gold`, each with the `graph-enriched-schema` schema. The
8 operational artifact tables are excluded entirely; dbxcarta's run summary and
client staging tables are redirected to a dedicated ops location
(`dbxcarta-catalog.finance_genie_ops`) and the agent payload tables are not
recreated. The enrichment pipeline config is updated so future runs target the
split catalogs.

**Benefit.** Layer becomes intrinsic to catalog, queryable, enforceable with
grants, and explicitly selectable by a text-to-SQL end user. The largest
retrieval-noise source is removed.

### 2.2 Layer tagging at ingest (G3)

**Problem.** Nothing in the graph tells the retriever a table's layer.

**Solution.** Add a `layer` property to Table nodes in `build_table_nodes`,
derived from the table's schema name through a configurable schema-to-layer
mapping, not a name-prefix regex. Bump the contract version and re-ingest.

**Benefit.** One structural property, written once, that the retriever can rank
and filter on. The mapping is config, not code, so it does not become a new
hardcode.

### 2.3 Cross-catalog reference support (G5)

**Problem.** Separating layers into catalogs breaks FK and join discovery
across them. The per-schema confinement is now also a per-catalog confinement.

**Solution.** Extend reference discovery so declared and inferred relationships
can cross catalogs within the configured catalog set, with the confinement made
an explicit setting rather than a hard assumption.

**Benefit.** Silver-to-gold joins (for example `gold_accounts.account_id` to a
silver `accounts.account_id`) remain discoverable, so the medallion split does
not regress join-criteria injection or reference-based expansion.

### 2.4 Retriever ranking stage (G4, G1)

**Problem.** All retrieved tables, columns, and values reach the prompt
unordered and unbounded.

**Solution.** Add a scoring stage after column fetch that ranks tables and
columns by a blend of vector seed score, lexical token-match count, FK
confidence, and layer preference, then bounds the set to a configurable budget.
The layer-preference term replaces the prompt's `gold_` rule. Token-match-count
scoring replaces the unranked lexical union called out in v1 Next Step 4.

**Benefit.** The right table arrives first because it scored highest, removing
the need for the prompt to name a prefix. Context noise drops, which was the
underlying cause cited for several v1 prompt rules.

### 2.5 Move shape and scale tolerance to the comparator (G6, G7)

**Problem.** The prompt forces one answer shape (default `LIMIT 10`, no `* 100`,
exact projection) so generated SQL byte-matches the reference. The comparator
is lenient on projection but strict on scale, an asymmetry that pushed
correctness rules into the prompt.

**Solution.** Make the result comparator order-insensitive where the question
does not specify order, projection-tolerant where extra columns are
informational, and scale-aware for ratio and percentage answers. Once the
comparator accepts equivalent forms, delete the three overfit prompt rules.

**Benefit.** Accuracy then reflects whether the model produces a correct
answer, not whether it guessed the reference's arbitrary shape. This is the
real test of generalization.

### 2.6 Honest evaluation (G7)

**Problem.** The fg_q09 reference was narrowed to match model output.

**Solution.** Restore the fg_q09 reference to the analytically correct columns
and let the projection-tolerant comparator handle the difference. Re-measure
accuracy with overfit rules removed. Produce an ablation table attributing the
score to each structural change.

**Benefit.** A defensible accuracy number and a clear record of what was
structural versus forced.

---

## Section 3: Requirements

Each is a testable statement.

- R1. The deployed catalog, schemas, and full table inventory are verified
  against a live workspace and recorded before any layer logic is written.
- R2. The 12 finance dataset tables live in the bronze, silver, and gold
  catalogs per the inventory grouping; the 8 operational tables are absent from
  them; the enrichment pipeline config targets the split catalogs.
- R3. `questions.json` references the silver and gold catalogs (base tables in
  silver, `gold_` tables in gold), with no broken three-part names.
- R4. Every Table node carries a `layer` property whose value comes from a
  configurable catalog-to-layer map (bronze/silver/gold).
- R5. The contract version is bumped and the change is reflected in ingest and
  any contract-version assertions.
- R6. A reference between two tables in different catalogs is discovered and
  written as a `REFERENCES` edge when both catalogs are in scope.
- R7. The retriever applies a documented scoring function to tables and columns
  and bounds the final set to a configurable budget.
- R8. The retriever's layer-preference term makes curated tables outrank raw
  tables for graph-derived-metric questions without any prompt instruction.
- R9. The three overfit prompt rules (gold-prefix preference, default
  `LIMIT 10`, share/ratio exact-projection) are removed from `graph_rag_prompt`.
- R10. The result comparator treats order-free, projection-extra, and
  scale-equivalent answers as matches, with unit tests for each case.
- R11. The fg_q09 reference SQL is restored to its analytically correct form.
- R12. The local accuracy harness is run with R9 and R11 in effect and the
  result, plus a per-change ablation, is recorded.

---

## Overfitting Inventory

This is the explicit answer to "what else is overfitting," each with its
structural replacement in this plan.

| # | Overfit artifact | Why it is forced | Replaced by |
|---|------------------|------------------|-------------|
| O1 | `prompt.py` gold-prefix preference rule | Encodes this schema's naming convention to steer table choice | G2 catalog separation + G4 layer-ranking term (R8) |
| O2 | `prompt.py` default `LIMIT 10` for unbounded superlatives | `10` exists only to match fg_q11's arbitrary reference cap | G6 comparator: top-set comparison when the question names no count (R10) |
| O3 | `prompt.py` share/ratio exact-projection, no `* 100` rule | Reverse-engineered from fg_q12's reference shape | G6 comparator: scale-aware and projection-tolerant numeric compare (R10) |
| O4 | `questions.json` fg_q09 reference narrowed to one column | Benchmark moved to match model output | G7 restore reference + projection-tolerant comparator (R11) |
| O5 | Unranked lexical union (v1 Next Step 4) | Any single-token match unioned in, no scoring, creates noise | G4 token-match-count scoring (R7) |
| O6 | Comparator asymmetry: lenient on projection, strict on scale | Forced correctness rules into the prompt to compensate | G6 symmetric tolerance in the comparator (R10) |

Items O1 through O4 are the four v1 changes that fit the system to the
benchmark. O5 and O6 are structural weaknesses that made the forcing necessary.

---

## Assumptions

- A1. The enrichment pipeline repository at the path in 1.5 can be modified;
  same author, same workspace.
- A2. The target catalog supports creating additional schemas and the operator
  has grants to do so.
- A3. Re-running ingest against the re-organized schemas is acceptable; the
  graph is rebuildable.
- A4. The local `graph_rag` harness remains the accuracy gate for this work; the
  Databricks client job is run only for confirmation.
- A5. The result comparator lives in the client `eval` package; its exact
  location and current tolerance behavior are mapped at the start of Phase 5,
  before any comparator edit.

---

## Risks

- Risk1. Catalog or schema name mismatch between the pipeline and
  `questions.json` indicates more than one deployment. Mitigation: the Phase 1
  verification prerequisites resolve this against the live workspace before any
  pipeline change is made, and are a hard gate.
- Risk2. Cross-schema reference support is a behavior change in shared ingest
  code that affects every preset, not just Finance Genie. Mitigation: gate it
  behind an explicit setting, default preserving current per-schema behavior.
- Risk3. Removing the prompt rules before the comparator is tolerant will
  regress the harness. Mitigation: Phase 5 lands comparator tolerance and prompt
  removal together; ablation measures each.
- Risk4. Contract version bump may break integration assertions and require a
  full re-ingest before any retrieval testing. Mitigation: sequence the bump and
  re-ingest as an explicit phase boundary with a handoff.
- Risk5. The ranking budget could drop a needed table. Mitigation: budget is
  configurable; Phase 4 includes a recall check that every gold reference table
  for the twelve questions survives ranking.

---

## Decisions (resolved)

- D1. RESOLVED. Three catalogs: `graph-enriched-finance-bronze` (intermediate
  GDS features), `graph-enriched-finance-silver` (raw business),
  `graph-enriched-finance-gold` (curated), each with schema
  `graph-enriched-schema`. Operational tables excluded entirely; dbxcarta ops
  output redirected to `dbxcarta-catalog.finance_genie_ops`.
- D2. Phase 2 (layer tagging at ingest) is resequenced to run before the single
  re-ingest in Phase 1, so the bronze/silver/gold labels are present on first
  ingest and a second expensive re-ingest is avoided. Phases 1 and 2 are
  executed together as the foundation.
- D3. RESOLVED. The dbxcarta-spark ingest engine was single-catalog
  (`dbxcarta_catalog: str`, one Database node, FK confined to one
  `(catalog, schema)`). Chosen model: **unified multi-catalog graph**. A new
  `dbxcarta_catalogs` setting (comma list, blank = historical single-catalog
  behavior, so every other preset is unaffected) drives extract to union
  `information_schema` across catalogs into one graph with one Database node
  per catalog. `dbxcarta_layer_map` (catalog:layer pairs) drives the Table
  `layer` property. FK discovery still skips cross-catalog pairs in Phase 1
  (`metadata.py:196`, `semantic.py:190`); Phase 2 relaxes exactly those lines.
- D4. CAVEAT. `run.py` run-summary and `_verify` still key off the single
  `dbxcarta_catalog`. The preset points that anchor at the silver catalog;
  verify is warn-only by default (`dbxcarta_verify_gate=False`), so
  catalog-vs-graph checks for bronze/gold are not gated in Phase 1. Widening
  verify scope to all ingested catalogs is deferred (not required for the
  Phase 1 goal); revisit if the verify gate is ever turned on.

---

## Phases

Execute one phase at a time. Do not start a phase while the prior phase has
open non-deferred items.

**The enrichment pipeline is fixed first.** Phase 1 reorganizes the source
tables into the three medallion catalogs and tags layer at ingest. Nothing
downstream (ranking, evaluation) can be correct until the tables physically
live in the right catalogs and the graph carries the layer property. Layer
tagging (old Phase 2) is merged into Phase 1 so the single re-ingest already
carries labels. Verification of the deployed inventory is complete and recorded
in Section 1.5. Mapping the result comparator is deferred to Phase 4, where it
is first used.

### Phase 1: Medallion catalog reorganization, layer tagging, and re-ingest

**Goal.** The 12 finance dataset tables live in the bronze/silver/gold catalogs,
every Table node in the graph carries a config-derived `layer` property, and the
operational tables are gone from the dataset. This is the foundation; every
later phase depends on it.

Catalog and data relocation:
- [x] Create catalogs `graph-enriched-finance-bronze`,
      `graph-enriched-finance-silver`, `graph-enriched-finance-gold`, each with
      schema `graph-enriched-schema`, in `azure-rk-knight`.
- [x] Create `dbxcarta-catalog.finance_genie_ops` for dbxcarta ops output.
- [x] Recreate the 5 raw tables in silver and the 3 curated tables in gold from
      the existing deployed tables via Delta `DEEP CLONE` (preserves data,
      column comments, and table properties).
- [x] Recreate the 4 intermediate GDS tables in bronze from the existing
      deployed tables via Delta `DEEP CLONE`.
- [x] Verify row counts match the source for all 12 tables. All 12 match
      (accounts/merchants/transactions/account_links/account_labels/
      account_features/account_graph_features/training_dataset 25000 each
      except merchants 7500, transactions 250000, account_links 300000,
      account_similarity_pairs 7, gold_accounts 239,
      gold_account_similarity_pairs 2000, gold_fraud_ring_communities 2).

Pipeline config (reproducibility):
- [ ] Parameterize the enrichment pipeline so `schema.sql` /
      `upload_and_create_tables.sh` target the silver catalog and
      `gold_schema.sql` / `jobs/03_pull_gold_tables.py` target the gold catalog.
      (Reproducibility only — does not block ingest; data already relocated
      via DEEP CLONE. Pending user confirmation: cross-repo edit.)

Layer tagging at ingest:
- [x] Add a configurable catalog-to-layer mapping to ingest settings
      (`dbxcarta_catalogs`, `dbxcarta_layer_map`, `resolved_catalogs()`,
      `layer_map()` in `settings.py`).
- [x] Add a `layer` column in `build_table_nodes` derived from that mapping;
      `build_database_nodes`/`build_has_schema_rel` made multi-catalog;
      `extract.py` unions `information_schema` across catalogs; preflight and
      declared/PK FK reads widened per catalog (cross-catalog pairs still
      blocked).
- [x] Bump `CONTRACT_VERSION` 1.0 -> 1.1; no assertions pinned the literal.
      Full suite green (372 passed, 1 skipped).

Questions and preset:
- [x] Update `questions.json` three-part names: base tables to the silver
      catalog, `gold_` tables to the gold catalog (fg_q09 still narrowed;
      restored in Phase 5).
- [x] Update the Finance Genie preset to ingest the three finance catalogs
      (`DBXCARTA_CATALOGS`, `DBXCARTA_LAYER_MAP`), exclude operational tables,
      and redirect ops to `dbxcarta-catalog.finance_genie_ops`
      (`DBXCARTA_SUMMARY_TABLE`, summary volume, questions path). Preset tests
      updated; green.

Re-ingest:
- [ ] Run the dbxcarta ingest end to end against the three catalogs.
      (Cost-bearing cloud job; pending user go-ahead and environment check.)
- [ ] Confirm Table nodes carry the expected `layer` values and no operational
      table is present in the graph.

Completion criteria: the workspace shows the 12 tables in the correct catalogs
with matching row counts; the graph has all 12 tables tagged with the correct
`layer` and zero operational tables; `questions.json` names resolve;
contract-version tests pass.

### Phase 2: Cross-catalog reference support

**Goal.** Silver-to-gold joins are discovered when both catalogs are in scope.

Checklist:
- [ ] Add an explicit setting controlling cross-catalog reference discovery,
      defaulting to current per-schema/catalog behavior.
- [ ] Allow declared and inferred reference discovery to span catalogs within
      the configured catalog set when the setting is enabled.
- [ ] Enable the setting for the Finance Genie preset.
- [ ] Re-ingest; confirm at least one silver-to-gold `REFERENCES` edge exists.

Completion criteria: a known cross-layer join appears as a `REFERENCES` edge;
default behavior for other presets is unchanged by test.

### Phase 3: Retriever ranking stage

**Goal.** Tables and columns are scored, layer-aware, and bounded before prompt
assembly.

Checklist:
- [ ] Add a scoring function blending vector seed score, lexical token-match
      count, FK confidence, and a layer-preference term.
- [ ] Apply it after `_fetch_columns`; bound the table and column set to a
      configurable budget.
- [ ] Surface `layer` through the retrieval path so the ranker can read it.
- [ ] Recall check: every gold reference table for the twelve questions survives
      ranking at the chosen budget.

Completion criteria: ranking unit tests pass; the recall check passes; curated
tables outrank raw tables for graph-metric questions with no prompt rule.

### Phase 4: Remove prompt overfitting and add comparator tolerance

**Goal.** Correctness tolerance lives in the comparator; the three prompt rules
are gone.

Checklist:
- [ ] Locate the result comparator in the client `eval` package; document its
      current order, projection, and scale behavior with file:line refs.
- [ ] Add order-insensitive comparison when the question specifies no order.
- [ ] Add top-set comparison when the question names no row count.
- [ ] Add scale-aware and projection-tolerant numeric comparison.
- [ ] Delete the gold-prefix, default-`LIMIT 10`, and share/ratio rules from
      `graph_rag_prompt`; keep the general grounding instructions.
- [ ] Update prompt-wording assertions in the retriever tests.

Completion criteria: comparator unit tests for order, projection, and scale
pass; the three rules are absent from the prompt; grounding instructions remain.

### Phase 5: Restore honest references

**Goal.** No reference is fitted to model output.

Checklist:
- [ ] Restore the fg_q09 reference SQL to its analytically correct columns.
- [ ] Re-upload the question set (Phase 1 changed catalog names).

Completion criteria: fg_q09 reference reflects the question's correct answer and
passes via comparator tolerance, not via a narrowed reference.

### Phase 6: Unit and contract testing

**Goal.** The new structural code is covered without a live Neo4j.

Checklist:
- [ ] Catalog-to-layer mapping tests.
- [ ] `build_table_nodes` `layer`-column tests.
- [ ] Cross-catalog reference discovery tests, both setting states.
- [ ] Ranking-function tests including the layer-preference term and budget cap.
- [ ] Comparator tolerance tests from Phase 4.
- [ ] Full suite green; contract-version assertions updated.

Completion criteria: `uv` test suite passes with no skips beyond the pre-existing
one.

### Phase 7: Accuracy evaluation and ablation

**Goal.** A defensible accuracy number and an attribution of v1's score.

Checklist:
- [ ] Run the local `graph_rag` harness on all twelve questions with overfit
      rules removed and fg_q09 restored, with backoff pacing.
- [ ] Measure the forced delta: run the twelve questions with O1 to O4 reverted
      against the Phase 1 baseline to quantify how much of v1's 12/12 was forced.
- [ ] Produce an ablation: baseline, +catalog separation, +layer ranking,
      +comparator tolerance, with the per-step accuracy delta.
- [ ] Re-run the heuristic-sensitive questions a second time to check drift.
- [ ] Record results and the structural-versus-forced attribution in a v2 fix
      log section.

Completion criteria: accuracy at or above the v1 figure with overfit rules
removed, or a written explanation of any gap with the responsible question and
root cause.

---

## Files Anticipated to Change

| Area | File | Change |
|------|------|--------|
| Pipeline | enrichment-pipeline `.env`, `sql/schema.sql`, `sql/gold_schema.sql`, `jobs/03_pull_gold_tables.py` | Silver/gold catalog split |
| Questions | `examples/integration/finance-genie/.../questions.json` | Silver/gold catalogs; fg_q09 reference restored |
| Ingest | `packages/dbxcarta-spark/.../settings.py` | Catalog-to-layer map; cross-catalog reference setting |
| Ingest | `packages/dbxcarta-spark/.../ingest/schema_graph.py` | `layer` column in `build_table_nodes` |
| Ingest | `packages/dbxcarta-spark/.../contract.py` | Contract version bump |
| Ingest | `packages/dbxcarta-spark/.../ingest/fk/*` | Cross-catalog reference discovery |
| Preset | `examples/integration/finance-genie/.../finance_genie.py` | Three-catalog env, ops redirect, cross-catalog setting |
| Retriever | `packages/dbxcarta-client/.../graph_retriever.py` | Ranking stage, layer surfacing |
| Prompt | `packages/dbxcarta-client/.../prompt.py` | Remove O1, O2, O3 rules |
| Eval | `packages/dbxcarta-client/.../eval/` comparator | Order, projection, scale tolerance |
| Tests | `tests/spark/*`, `tests/client/*` | Coverage for all of the above |

---

## Status

| Phase | Status |
|-------|--------|
| 1 Medallion catalog reorg, layer tagging, re-ingest | In progress |
| 2 Cross-catalog reference support | Pending |
| 3 Retriever ranking stage | Pending |
| 4 Remove prompt overfit + comparator tolerance | Pending |
| 5 Restore honest references | Pending |
| 6 Unit and contract testing | Pending |
| 7 Accuracy evaluation and ablation | Pending |

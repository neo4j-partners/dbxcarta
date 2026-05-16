# Finance Genie Text2SQL accuracy issues

## Scope of this document

This records what was tested while validating the `harden-deploy-v2`
runner update against the `examples/integration/finance-genie` pipeline,
the deployment results, and a separate Text2SQL accuracy problem the
local demo exposed.

The deployment work is sound and is not the subject of this document.
The runner 0.6 update, the bootstrap model, the connector preflight, the
verify gate, and both submit entrypoints all passed end to end. The
accuracy problem is a pre-existing dbxcarta semantic-layer and GraphRAG
retrieval issue, outside the harden-deploy scope. It is documented here
because the end-to-end run is the first time it was measured on this
workspace.

## What was tested and run

Workspace `azure-rk-knight`, catalog `graph-enriched-lakehouse`, schema
`graph-enriched-schema`, cluster `0515-141455-wb8qxgo2`, against the
published runner `databricks-job-runner==0.6`.

1. Quality review of the runner 0.6 change set (committed, tagged
   `v0.6`, published to PyPI):
   - Removal of the dead versioned-wheel auto-attach path
     (`Runner.find_wheel`, the `wheel_path` parameter across
     `submit_job`, `submit_bootstrap_job`, `Compute.decorate_task`,
     `Compute.environments`, `ClassicCluster.attach_libraries`, the
     unused `Library` import).
   - Bootstrap path (`publish_wheel_stable`, `submit_bootstrap`) left
     intact; new `tests/test_compute.py` regression coverage added.
   - dbxcarta consumer compatibility: zero references to any removed
     runner symbol; pin bumped consistently to `==0.6` in
     `packages/dbxcarta-spark/pyproject.toml`, the
     `_INGEST_PINNED_CLOSURE` in
     `packages/dbxcarta-spark/src/dbxcarta/spark/cli.py`, and `uv.lock`.
   - Full consumer unit suite: 366 passed, 1 skipped, 3 deselected.

2. Full finance-genie pipeline end to end, following the example
   `README.md` setup flow: preset install, readiness check, Neo4j
   secret refresh, question upload, `uv build --all-packages`,
   `dbxcarta upload --wheel`, `dbxcarta upload --all`,
   `dbxcarta submit-entrypoint ingest`, `dbxcarta verify`,
   `dbxcarta submit-entrypoint client`.

3. Local read-only demo accuracy harness: all 12 questions in
   `examples/integration/finance-genie/src/dbxcarta_finance_genie_example/questions.json`
   run through `local_demo ask --question-id <id>`, which generates SQL
   via GraphRAG retrieval and the chat endpoint, executes it on the SQL
   warehouse, executes the fixture `reference_sql`, and compares the two
   result sets.

## Deployment results (all green)

| Stage | Outcome |
| --- | --- |
| Readiness (8 Finance Genie tables) | ready (5 base + 3 gold, strict-optional) |
| Stable wheel publish on runner 0.6 | `dbxcarta_spark-stable.whl` and `dbxcarta_client-stable.whl` published via `publish_wheel_stable` |
| Connector preflight, cold cluster, maven `pending` | failed fast, no run created (correct safety behavior) |
| Connector preflight, after maven `INSTALLED` | `[ok] neo4j connector` |
| Ingest, run `654632037008202` | SUCCESS; bootstrap pip-installed `databricks-job-runner==0.6`; `smoke_check: "ok (6)"` |
| `dbxcarta verify` | OK, 0 violations |
| Client eval, run `384499397758436` | SUCCESS (no-preflight path, confirms Phase V2 preflight scoping) |

The cluster was TERMINATED at the start, so the first ingest cold-started
it and the connector preflight correctly failed fast while the Neo4j
maven library was still `pending`. After the maven library reached
`INSTALLED`, the rerun succeeded with no further restart.

## What is wrong: Text2SQL accuracy is 2/12 (16%)

Only the two trivial single-base-table questions passed. The other ten
generated SQL that failed to execute, so they never reached a result-set
comparison. None reached the `different` state; all ten are hard
generation failures.

| Q | Question | Reference truth | Generated SQL fault |
| --- | --- | --- | --- |
| fg_q01 | How many accounts are there? | `accounts` count | correct |
| fg_q02 | What account types exist? | `accounts.account_type` distinct | correct |
| fg_q03 | What merchant categories exist? | `merchants.category` | invented column `merchant_category` |
| fg_q04 | Total transaction amount by merchant category | `transactions` join `merchants` on `merchant_id`, `m.category` | invented column `merchant_category` |
| fg_q05 | Transactions per account type | `transactions` join `accounts`, `a.account_type` | put `account_type` on `transactions` (wrong table grounding) |
| fg_q06 | Total transfer amount sent by account type | `account_links` join `accounts`, `l.amount`, `l.src_account_id` | unresolved column |
| fg_q07 | Confirmed fraud ring members | `account_labels.is_fraud = true` | invented column `is_fraud_ring_member` |
| fg_q08 | High-risk accounts in gold accounts | `gold_accounts.fraud_risk_tier = 'high'` | invented column `risk_level` |
| fg_q09 | Fraud ring communities that are ring candidates | `gold_fraud_ring_communities.is_ring_candidate` | table not found |
| fg_q10 | Average graph risk score by account region | `gold_accounts.region`, `risk_score` | emitted `graph_enriched_accounts` instead of `gold_accounts` |
| fg_q11 | Account pairs with highest merchant similarity | `gold_account_similarity_pairs.similarity_score` | table not found |
| fg_q12 | Share of accounts in ring-candidate communities by region | `gold_accounts` join `gold_fraud_ring_communities` on `community_id` | invented column `community_label` |

### Failure taxonomy

The ten failures split into two distinct patterns.

1. Base-table column hallucination (fg_q03, fg_q04, fg_q05, fg_q06,
   fg_q07). The model produces a plausible but nonexistent column name
   instead of the real one: `merchant_category` for `category`,
   `is_fraud_ring_member` for `is_fraud`, and `account_type` attached to
   the wrong table. The base tables exist and are in scope, so the
   retrieved context is not surfacing exact column names with enough
   strength for the model to prefer them over guesses.

2. Gold-table absence (fg_q08, fg_q09, fg_q10, fg_q11, fg_q12). The
   model either reports the gold table as not found or fabricates a name
   from the catalog string. fg_q10 is the clearest case: it emitted
   `graph-enriched-lakehouse.graph-enriched-schema.graph_enriched_accounts`,
   a name pattern-built from the catalog identifier, rather than the
   real `gold_accounts`. This indicates the gold-table identifiers and
   columns are not present in the retrieved GraphRAG context at all.

### What this is not

`dbxcarta verify` returned 0 violations, so the ingest did write a
consistent semantic layer to Neo4j and the node and edge counts match
the run summary. The accuracy problem is therefore downstream of a
correct ingest: it is in what the retriever surfaces for a question and
how the prompt grounds the model, not in whether the graph was written.

## Suspected root causes, ranked

1. GraphRAG retrieval coverage. The retriever (`GraphRetriever` in the
   client package, driven by question embeddings against the Neo4j
   vector indexes) appears to seed on table-level similarity and is not
   pulling the correct column nodes for base tables, and is not pulling
   gold-table nodes at all. The gold tables are semantically distant
   from the question text (a question says "high-risk accounts", the
   table is `gold_accounts` with `fraud_risk_tier`), so pure embedding
   similarity misses them.

2. Prompt schema grounding. `graph_rag_prompt` builds the schema block
   from the retrieved context. Preflight reported `graph_schema_lines=200`.
   If the schema block is capped or if it lists table names without
   authoritative column lists, the model is free to invent columns. The
   base-table hallucinations are consistent with a context that names
   tables but underspecifies columns.

3. Semantic-layer content for gold tables. Less likely given verify
   passed, but not ruled out: the ingest may have written gold-table
   nodes without column-level detail or without the embeddings the
   retriever queries, which would explain total gold-table absence in
   retrieval even though the graph is internally consistent.

## Next steps to fix

These are investigation and fix steps in priority order. They are
proposed, not yet executed. Per the project working agreement, the
specific fix should be discussed before any code change.

1. Capture the retrieved context for representative failures. Add a path
   to print `context_ids` and `context_text` even when the generated SQL
   fails to execute (today `local_demo` raises before the
   `--show-context` print, so the context is never seen on failure).
   Run it for fg_q03 (base-column miss) and fg_q10 (gold-table miss) and
   confirm directly whether the real columns and the gold tables appear
   in the context.

2. Inspect the semantic layer in Neo4j for the gold tables. Confirm that
   `gold_accounts`, `gold_fraud_ring_communities`, and
   `gold_account_similarity_pairs` have table nodes, column nodes with
   real names (`fraud_risk_tier`, `region`, `risk_score`,
   `is_ring_candidate`, `similarity_score`), and the vector embeddings
   the retriever queries. This separates a retrieval problem from an
   ingest-content problem.

3. If gold tables are present in the graph but absent from retrieval,
   fix retrieval. Options to evaluate: raise the retrieval breadth or
   top-k, add table-name and column-name lexical matching alongside
   vector similarity, or expand the seed set so gold tables are
   reachable from base-table seeds through `REFERENCES` and similarity
   edges.

4. If columns are present in the graph but weak in the prompt, fix
   grounding. Ensure `graph_rag_prompt` emits an authoritative,
   uncapped column list for every retrieved table and instructs the
   model to use only listed columns and tables, with the exact catalog
   and schema quoting the reference SQL uses
   (`` `graph-enriched-lakehouse`.`graph-enriched-schema`.<table> ``).
   The fg_q10 `graph_enriched_accounts` fabrication shows the model is
   not being held to real identifiers.

5. Add a regression harness. Promote the 12-question accuracy loop into
   a repeatable scored check (correct, different, error counts) so
   accuracy is tracked across changes rather than measured once. The
   client evaluation entrypoint already scores questions on the cluster;
   a local equivalent keeps the loop fast during iteration.

6. Re-measure after each change against the same 12 questions and record
   the accuracy delta in this document.

## Reproduction

```bash
# deployment path (already validated, green)
uv run dbxcarta submit-entrypoint ingest
uv run dbxcarta verify
uv run dbxcarta submit-entrypoint client

# accuracy harness (the failing measurement)
cd examples/integration/finance-genie
for q in fg_q01 fg_q02 fg_q03 fg_q04 fg_q05 fg_q06 \
         fg_q07 fg_q08 fg_q09 fg_q10 fg_q11 fg_q12; do
  uv run python -m dbxcarta_finance_genie_example.local_demo \
    ask --question-id "$q"
done
```

## Work log

### 2026-05-16 Step 1 (capture retrieved context) and Step 2 (inspect Neo4j): blocked

Investigated: built a read-only diagnostic that mirrors
`run_graph_rag_question` up to context assembly. It embeds the question,
runs `GraphRetriever.retrieve`, and prints the full bundle
(`col_seed_ids` with scores, `tbl_seed_ids` with scores,
`expansion_tbl_ids`, `selected_schemas`, the per-table column list, the
`to_text()` context, and the final `graph_rag_prompt`) without executing
SQL. The same script also dumps the Neo4j semantic layer: vector index
state, every `Schema -> Table` with column counts and table-embedding
presence, and the column-level detail for `gold_accounts`,
`gold_fraud_ring_communities`, and `gold_account_similarity_pairs`.

Result: blocked by an external infrastructure failure, not a code
defect. The Neo4j Aura host in `.env`,
`4b2239bb.databases.neo4j.io`, returns `NXDOMAIN`, while
`databases.neo4j.io` resolves normally. For Aura this means the instance
auto-paused or was removed since the prior session, when the same host
was reachable and the 12-question harness ran. There is no Aura
provisioning automation in this repo; `setup_secrets.sh` only mirrors
existing credentials into the Databricks secret scope. Steps 1, 2, 5,
and 6 all require a reachable Neo4j and cannot run until the instance is
resumed or recreated and `.env` plus the `dbxcarta-neo4j` secret scope
point at it. The deployment pipeline re-run also depends on this, since
ingest writes the graph and verify and client read it.

### 2026-05-16 Static root-cause analysis (no Neo4j required)

Investigated: read `graph_retriever.py`, `retriever.py`, `prompt.py`,
and `settings.py` to confirm the two suspected root causes directly from
code rather than from live retrieval.

Result: two concrete code-level causes confirmed.

1. Retrieval coverage. Seeds come only from the `column_embedding` and
   `table_embedding` vector indexes at `dbxcarta_client_top_k = 5`, with
   no lexical or name-match fallback anywhere in `GraphRetriever`.
   Expansion is limited to parent tables of seed columns and the
   `REFERENCES` FK walk. Gold tables are derived enrichment tables that
   carry no `REFERENCES` edges to base columns and are not parents of
   base seed columns, so the only path into context is a direct
   table-level or column-level vector hit. With top_k 5 and gold tables
   phrased differently from the questions (the question says "high-risk
   accounts", the table is `gold_accounts` with `fraud_risk_tier`), gold
   tables fall outside the top-5 and never enter context. This matches
   the observed total gold-table absence and the fg_q10 fabrication.

2. Identifier grounding. `ContextBundle.to_text()` renders tables as
   `Table: graph-enriched-lakehouse.graph-enriched-schema.<table>` with
   no backtick quoting, and `graph_rag_prompt` never instructs the model
   to quote identifiers or to use only the listed tables and columns.
   The catalog and schema contain hyphens, so an unquoted three-part
   name is invalid Spark SQL. The reference SQL uses
   `` `graph-enriched-lakehouse`.`graph-enriched-schema`.<table> ``. A
   model that copies the context name verbatim still produces SQL that
   fails to execute, independent of retrieval quality. This is a
   sufficient cause for execution failures on its own and explains why
   ten of twelve questions never reached a result-set comparison.

### Proposed fixes (to discuss before code changes)

These follow from the static analysis and are scoped to the client
package. Per the project working agreement they are proposed, not
applied, and need a reachable Neo4j to validate.

1. `prompt.py`: add explicit grounding rules to `_SQL_INSTRUCTION` or
   `graph_rag_prompt`. Require backtick-quoted three-part names exactly
   as shown in the context, and restrict the model to the tables and
   columns listed in the context with no invention.
2. `retriever.py`: make `ContextBundle.to_text()` emit backtick-quoted
   fully qualified names so the model has a correct identifier to copy.
3. `graph_retriever.py`: add a lexical fallback that matches question
   tokens against table and column names and unions those seeds with
   the vector seeds, and raise `dbxcarta_client_top_k` for this fixture,
   so gold tables become reachable.

### Blocker for the user

Resume or recreate the Neo4j Aura instance and point `.env`
(`NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`) and the
`dbxcarta-neo4j` Databricks secret scope at it. The current host
`4b2239bb.databases.neo4j.io` no longer resolves. Once Neo4j is
reachable, steps 1, 2, 5, and 6 and the full pipeline re-run can
proceed.

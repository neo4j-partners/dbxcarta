# DBxCarta Example Client — Proposal

## Motivation

The server side of DBxCarta builds a semantic layer from Unity Catalog metadata and writes it to Neo4j as a graph of `Database`, `Schema`, `Table`, `Column`, and `Value` nodes, with descriptions and embeddings attached. That graph is the product. The open question is whether it actually improves downstream query generation, and specifically whether a Text2SQL agent that retrieves from the graph produces more accurate SQL than an agent that works from schema dumps alone.

There is no client in the repository today. Nothing exercises the graph end to end, and there is no evidence, good or bad, that the semantic layer earns its keep. This proposal adds a sample client under `examples/` whose only job is to answer that question.

The framing is deliberate: the semantic layer is treated as unstructured content ingested into a graph, and GraphRAG is how the client retrieves against it. Vector search finds the likely-relevant columns and tables, and a structural pass expands the seed through graph relationships to pick up joinable tables, referenced columns, and literal values. The retrieved bundle becomes context for the LLM that writes the SQL.

## Goal

Ship a batch evaluation job that takes a list of natural-language questions, generates SQL for each question under several retrieval strategies, executes the SQL against a Databricks SQL warehouse, and records which strategies produced queries that ran and returned results. Earlier phases grade on execution only. Later phases add ground-truth comparison.

The client is not a product surface. It is a test harness whose output tells the team whether Phase 1 through Phase 3 of the server produced something useful.

## Shape

The client is a Spark job structured exactly like the server jobs. It is submitted through the existing `databricks-job-runner` CLI, dispatched through `scripts/run_dbxcarta.py` as a new phase, reads configuration from environment variables, and emits a typed run summary to stdout, a JSON artifact in the UC summary volume, and a row in the Delta run-summary table. Catalog, schemas, warehouse id, summary volume, and summary table all come from the same variables the server jobs already use.

The retriever is a pluggable interface with one concrete hybrid implementation for v1. The interface lives inside `examples/client/` for the first cut so it does not expand the public library surface of `src/dbxcarta/`. If later work shows other clients want to reuse it, it graduates to `src/`.

The generation step calls a Databricks Model Serving chat endpoint. The endpoint name is parameterized as `DBXCARTA_CHAT_ENDPOINT` with no default, so the client fails loudly if the environment is not configured.

## Prerequisites

Phase 3 of the server (`src/dbxcarta/embeddings.py`) is currently a stub. The client cannot do vector retrieval until Phase 3 is implemented, embeddings are written to `Column` and `Table` nodes in Neo4j, and a vector index exists. The proposal assumes Phase 3 lands first and creates a vector index on `Column.embedding` and `Table.embedding` at write time; the client-side preflight verifies that both indexes exist and fails fast if they do not.

If Phase 3 lands without creating the indexes, the client adds a one-shot preflight step that creates them before the first run. Client code does not compute cosine similarity in Python; that path is rejected because it will not scale to a real catalog and will hide index misconfiguration.

## Retrieval strategy

Three retrieval arms are specified. The client runs each question through each arm in a single job so results are directly comparable on the same input.

**Arm 0, no context.** The question goes to the LLM with no schema information at all. This is the floor. If the model already knows enough about the catalog from its training data or generic SQL knowledge to write working queries, that is signal the task is too easy to distinguish the other arms.

**Arm 1, schema dump.** The client pulls the full column list for the catalog (or a schema-scoped subset) from Neo4j and pastes it into the prompt in a structured form. This is the baseline any team would try first before standing up a graph. It is the arm that has to be beaten.

**Arm 2, GraphRAG hybrid.** Vector seed, then structural expansion. The vector seed runs similarity search against `Column.embedding` and `Table.embedding` using an embedding of the question, and returns the top matches. The structural pass walks the graph from each seed: up to the parent `Table` and `Schema`, out to sibling columns on the same table, across `REFERENCES` edges to joinable columns, and down to `HAS_VALUE` literals that ground WHERE-clause values. The expanded bundle is rendered into the prompt.

All three arms share the same generation step and the same execution step. Only the context block differs.

## Grading

Early phases grade on execution. A question is considered answered if the generated SQL parses, runs against the configured warehouse, and returns at least one row within a timeout. Queries that fail to parse, raise a warehouse error, or return an empty result set are marked unanswered. The run summary reports counts and rates per arm.

Execution-only grading is deliberately weak. It catches hallucinated table names and syntactic failures, and nothing else. A query that runs and returns wrong rows looks identical to a correct one. This is acceptable as the first signal because hallucinated identifiers are the dominant failure mode for schema-blind agents, and because the harness is useful even when the grading rubric is blunt.

Ground-truth grading is added in a later phase. It compares generated SQL to a reference SQL by executing both and comparing result sets, either as set equality for small results or as a row-count and column-wise sampled comparison for large ones. Reference SQL lives alongside the question in the input file.

## Phased plan

Each phase is a separate, reviewable piece of work. Each ends with a live submit against a real catalog, not just a local test run.

**Phase C1, scaffolding and the grading loop.** Add `examples/client/` with a questions file checked into the repo, a Settings class modeled on `schema_graph.py`, a dispatcher hook in `scripts/run_dbxcarta.py`, a preflight that checks warehouse connectivity and the summary volume, and a run summary type. Wire the execute-and-check-non-empty step against the warehouse. No retrieval, no LLM. Feed hand-written SQL through the loop to prove the grading path works end to end.

**Phase C2, Arm 0 working.** Add the Model Serving chat client and the prompt template. Run Arm 0 over the questions file. Record pass rates. This validates the LLM wiring without any graph dependency.

**Phase C3, Arm 1 schema dump.** Add a Neo4j read that pulls columns for the configured catalog or schema allowlist, render them into the prompt, and run Arm 1. Compare pass rates against Arm 0. At this point the harness can already produce a useful result even without Phase 3 of the server.

**Phase C4, retriever interface and Arm 2 vector-only.** Define the retriever interface (question in, context bundle out) and ship a vector-only implementation that returns top-k columns and tables by embedding similarity. Run Arm 2 in vector-only mode. This phase depends on server Phase 3 being complete.

**Phase C5, Arm 2 hybrid structural expansion.** Extend the v1 retriever to walk from vector seeds to parent tables, sibling columns, `REFERENCES` joins, and `HAS_VALUE` literals. Emit the expanded bundle. Compare against Arm 1 and vector-only Arm 2.

**Phase C6, ground-truth grading.** Extend the questions file schema to carry reference SQL. Add a result-set comparison step. Re-grade prior runs. Upgrade the run summary to carry both execution and correctness metrics. Promote the input store from a checked-in file to a Delta table so historical runs can be queried through SQL.

**Phase C7, optional follow-up.** Once v1 is stable, evaluate whether the retriever interface should move from `examples/client/` into `src/dbxcarta/` for reuse by a future MCP server or notebook client. No code movement until there is a second consumer.

## Inputs and outputs

The input for C1 through C5 is a JSON file checked into `examples/client/questions/` with an array of objects, each carrying a question id, a natural-language question, and optional notes. C6 adds a reference-sql field to the same schema, and the file is promoted to a Delta table in a later phase.

The output for every run is the standard server-job trio: stdout run summary, a timestamped JSON artifact in the UC volume `DBXCARTA_SUMMARY_VOLUME`, and a Delta row in `DBXCARTA_SUMMARY_TABLE`. The run summary carries per-arm counts of attempted, parsed, executed, non-empty, and (from C6) correct. For each question the summary also records the generated SQL and the retrieved context ids so failures can be inspected without replaying the run.

## Configuration

New environment variables introduced by the client:

- `DBXCARTA_CHAT_ENDPOINT`: Databricks Model Serving endpoint name for the generation step. No default.
- `DBXCARTA_EMBED_ENDPOINT`: Model Serving endpoint for embedding the incoming question. Defaults to whatever Phase 3 uses so the question and node embeddings live in the same space.
- `DBXCARTA_CLIENT_QUESTIONS`: path to the questions file, relative to the project root or a UC volume path.
- `DBXCARTA_CLIENT_ARMS`: comma-separated list of arms to run, defaulting to all three. Useful for iterating on one arm without paying the cost of the others.
- `DBXCARTA_CLIENT_TOP_K`: vector seed size for Arm 2. Small default, tunable.
- `DBXCARTA_CLIENT_TIMEOUT_SEC`: warehouse execution timeout per question.

Existing server variables are reused without change: catalog, schemas, warehouse id, secret scope, summary volume, and summary table.

## Risks and open questions

**The questions file is where all the signal lives.** Whatever questions are chosen for v1 will shape the conclusion. If they are too easy, every arm passes and the harness produces no signal. If they are too hard, every arm fails and the harness produces no signal. An explicit curation step is needed before C2 lands, not after. A mix of easy lookups, multi-table joins, filter-with-literal questions, and questions that require a REFERENCES walk is the target distribution.

**Arm 1 is a moving target.** A full schema dump will not fit in the context window for a realistic catalog. The client has to decide whether Arm 1 means the whole catalog (feasible only for tiny catalogs), the schema allowlist (which is closer to what a thoughtful human would paste), or a token-budgeted slice. The proposal picks schema allowlist for v1 and notes that the choice biases the comparison.

**Execution-only grading will over-reward Arm 1.** A schema dump gives the model every identifier, so syntactic pass rates climb even when the chosen join or filter is wrong. The gap between Arm 1 and Arm 2 will look smaller under execution-only grading than it is. This is an argument for moving to ground-truth grading sooner rather than later, and for reporting both metrics side by side in C6.

**Warehouse cost.** Every question executes SQL against the configured warehouse for every arm, for every run. For a 100-question file and three arms, that is 300 warehouse queries per run. A per-run cap and a warehouse-size guardrail in the preflight are cheap insurance.

**Model Serving latency and rate limits.** Three arms per question means three chat calls plus one embedding call. The job should parallelize across questions with a bounded concurrency, not issue calls sequentially, and should surface rate-limit errors as run-summary warnings rather than failing the whole run.

**Index creation responsibility.** Whether vector indexes are created by server Phase 3 or by the client preflight is not yet decided. Preference is server Phase 3 because the indexes are part of the graph contract. If that slips, the client owns it temporarily and hands it back later.

## Success criteria

The client is successful when all of the following hold:

- One command submits a client run against a configured catalog and returns a run id.
- The run summary shows per-arm execution rates on a curated questions file of at least twenty items spanning easy lookups, joins, filter-with-literal, and REFERENCES-walk questions.
- Arm 2 beats Arm 1 by a non-trivial margin on execution rate, or a report explains why it does not and what that implies for the server-side semantic layer.
- The retriever interface has one working implementation and a second mock implementation used in a unit test, demonstrating the interface is real and not load-bearing on the implementation.
- Ground-truth grading, once added, is reproducible across two consecutive runs on the same input.

Failure to beat Arm 1 is a valid outcome. The point of the harness is to produce that answer honestly, not to confirm a prior.

# Clean Boundaries v3 — Requirements

This document captures *what* the v3 architecture must deliver, in plain English,
without prescribing interfaces or code. The design doc (`clean-boundaries-v3.md`)
will be reworked once these requirements are agreed.

The companion v2 doc (`clean-boundaries-v2.md`) is treated as superseded by this
work; the v1 doc (`clean-boundaries.md`) is still authoritative for packaging
and boundary-test discipline. Cross-project context (the wider pattern that
v3 instantiates) is recorded in `clean-boundaries-future.md` and is out of
v3 scope per Goal 7.

---

## Current overview

DBxCarta is a custom text-to-SQL agent. The semantic layer is a Neo4j graph
built from a relational catalog (today Unity Catalog) that the agent queries to
ground SQL generation. The repo is already split into four published
distributions under the `dbxcarta` PEP 420 namespace:

- **dbxcarta-core**: shared, backend-neutral primitives (settings, contract
  enums, identifier helpers, verify rules).
- **dbxcarta-spark**: the production ingest pipeline. Reads Unity Catalog via
  Spark, samples values, embeds via `ai_query`, and writes to Neo4j through the
  Neo4j Spark Connector. The only pipeline shipped today.
- **dbxcarta-client**: this repo's reference client. Provides bolt query-time
  access to the populated Neo4j graph for the agent's retrieval path, and
  ships the Text2SQL evaluation harness.
- **dbxcarta-presets**: composition root. Canonical wirings (for example
  `finance-genie`) and the `preset` CLI live here; this is the only
  distribution allowed to depend on multiple adapters.

The largest catalogs in scope produce tens of millions of `Value` nodes, each
carrying a 1024-dimension embedding. End-to-end data volume reaches the tens of
gigabytes. The pipeline must stay distributed across Spark executors from
`information_schema` read through Neo4j write; no driver-side materialization
of the full graph, value set, or embedding payload is acceptable on the main
data path.

v1 landed the packaging and boundary tests. v2 attempted to make the
core-to-adapter contract precise and got several things wrong; see below. v3 is
the next attempt.

---

## Goals

### Architectural goals

1. **The shape of the graph is owned by core.** The set of node labels, edge
   types, key fields, and per-field types is a versioned contract that adapters
   read from core. Adapters cannot deviate. A new label or field is a core
   change.

2. **Adapters keep their native data carriers.** The Spark pipeline writes
   DataFrames through the Neo4j Spark Connector with no driver-side
   materialization of the full graph, value set, or embedding payload. A
   future row-based pipeline (BigQuery + bolt driver) writes row batches
   over bolt. Both produce the same graph shape.

3. **New backends are additive.** Adding a BigQuery + bolt pipeline requires
   one new reader, one new writer, and a new mapping. Zero changes to core,
   Spark, or client code.

4. **Clients are pipeline-agnostic consumers.** A client reads the
   populated graph via whatever Neo4j surface fits (bolt driver, Spark
   Connector, MCP server, etc.). Clients conform to the graph schema by
   convention, are read-only against the graph, do not import adapter
   packages, and do not need to know which pipeline produced the data.
   `dbxcarta-client` is this repo's reference client (Databricks-flavored
   eval and retrieval for the agent); other clients may live in this
   repo or elsewhere.

5. **Boundaries are enforced where it matters most.** Shape primitives are
   imported from core, and adapters project their outputs against those
   primitives by hand. The machine-checked rule is the boundary tests for
   forbidden imports between distributions. Writer-gated conformance and a
   runtime contract sentinel are deferred to a later iteration once a second
   adapter exists; their absence is recoverable, and the cost of designing
   them with one adapter is high.

6. **Observability is not lost.** Every metric the current `RunSummary`
   captures (per-label embedding attempts/successes/ledger hits, sample-value
   stats, FK counts by source, verify outcomes, neo4j post-write counts) must
   continue to be produced and recorded in Delta with no regression.

### Process commitments

7. **A beta scope is named and frozen.** v3 commits to a one-line scope
   freeze before Phase 2 starts. The beta is the smallest cut of the
   phased plan that proves the pattern end-to-end: the Spark migration
   of the one shipped preset, the boundary tests, and the
   suggested-schema README. The BigQuery adapter, reusable conformance
   harness, runtime contract sentinel, multi-preset coverage, and any
   cross-project work are out. Phase 6 is the acceptance gate.

   **Beta acceptance checklist (Phase 6 gate):**

   1. `preset --run finance-genie` produces a Delta summary row
      schema-identical to the Phase 1 baseline.
   2. The boundary tests for forbidden imports pass.
   3. The end-to-end integration test matches the pre-v3 production
      baseline captured in Phase 1.

   Without an explicit beta line, the work expands to fill every
   requirement and the doc never leaves flux. Why this is a goal and
   not a phase activity: v3 has drifted across two prior attempts; a
   named, public beta scope is the lever that makes the freeze real.

8. **No compatibility shims.** v3 removes the partial-contract shims
   from earlier attempts instead of preserving them behind deprecated
   aliases. Core exposes graph shape, identifiers, field vocabulary,
   and declarative verify rules; adapters expose their own concrete
   entrypoints and settings. The concrete removal list lives in Phase
   4.

9. **The open-requirements list stops growing.** Items 1–10 below are
   the work. Questions that surface during Phase 1 or 1.5 are recorded
   in `clean-boundaries-future.md`, not appended here. Phase 1.5
   freezes the list. The list's growth has been the leading indicator
   of scope creep through v2 and v3 drafts; capping it is how the
   requirements stop being in flux. The future doc exists specifically
   so "but what about..." questions have somewhere to land that does
   not reopen v3.

---

## Performance constraint: DataFrame-native, end to end

The Spark pipeline today reads tens of thousands of columns from
`information_schema` on the largest customer catalogs. Per column it samples
values, generates a 1024-dimension embedding via `ai_query`, and writes
everything to Neo4j via the Spark Connector. A single run can produce tens of
millions of `Value` rows, each carrying an array of 1024 floats. Total data
volume on large catalogs is in the tens of gigabytes.

Every step of that pipeline must stay distributed. Once Spark has the data laid
out across executors, the work happens in parallel on the cluster:
`information_schema` joins, sampling, `ai_query` calls, and the Neo4j
connector's batched writes all execute on workers. The driver coordinates the
plan but never holds row data. Cluster size, not driver memory, sets
throughput.

An unbounded driver-side collect anywhere in this chain breaks the pipeline.
Pulling ten million `Value` rows with 1024-float embeddings to the driver costs
roughly 80 GB of heap to materialize as Python objects, serializes the
cluster's parallel work onto one node, forces a second pass when the writer
fans the rows back out to executors, and stretches run time from minutes to
hours. On the largest catalogs the job stops fitting on any driver size and
simply fails.

This is the principal reason v2 was wrong. A port that hands the writer a
materialized Python value of the whole graph cannot avoid collecting every node
and every edge onto the driver. That contract is incompatible with the only
production deployment that exists today.

v3 must make the bulk no-materialization property a structural guarantee, not a
convention:

- The Spark writer's input is a lazy plan, never a Python value of the rows.
- v3 has no runtime contract validation on the write path (per
  Resolved Req 9 and Resolved Req 10). Future-iteration validation
  rules are recorded in `clean-boundaries-future.md`.
- Spark actions for bounded metadata, metrics, staging, ledger maintenance,
  and summary emission are allowed when they follow Spark best practices and
  do not materialize full graph, value, or embedding payloads on the driver.
  No second pass through materialized Python graph data, no driver bottleneck,
  no perf regression versus the pre-v3 baseline.
- Catalogs ten times larger than today's must continue to scale linearly with
  cluster size.

Any future architecture change that would require an unbounded driver-side
collect, a pandas conversion of bulk graph data, or per-row Python iteration
over full graph/value/embedding payloads on the main data path is out of scope.
Such needs are expressed as new shape primitives or new auxiliary helpers; none
of those materialize bulk data.

---

## What v2 got wrong

v2 was the previous attempt to define the core-to-adapter contract. Three
concrete problems with v2 surfaced once the real use cases were articulated:

1. **A bare string source identifier is not a contract.** v2's
   `source_ref: str` carried no domain information and gave no type-level
   guarantee that the adapter and caller agreed on what the string meant.

2. **A materialized result value forces a driver collect.** v2 had the
   pipeline return a Python value describing the full semantic layer. Wrapping
   every Table, Column, Value, and embedding row in a Python object kills the
   Spark pipeline as described in the performance section above.

3. **Behavioral flags were confused with shape variants.** v2 hesitated on
   whether knobs like "include sample values" or "infer foreign keys" belonged
   on the input. They belong on neither: they are choices about *which mapping*
   the pipeline runs, not flags on a uniform input.

The architectural mistake under all three: v2 tried to make the contract a
*value-typed function signature*. The actual contract is *shape* — the graph
the pipeline produces, not the Python values that briefly exist on the way
there. A separate version-stamp layer is deferred (see
`clean-boundaries-future.md`).

---

## Per-package requirements

The four-distribution split from v1 stays. v3 sharpens what each distribution
owns.

### dbxcarta-core

Core is the contract. It is a small, pure-Python package with no backend
imports at runtime.

**Owns:**

- The set of node labels, edge types, key fields, and per-field type
  declarations that describe the universal graph shape.
- The set of relational entity declarations that every reader adapter must
  produce (databases, schemas, tables, columns, sampled values, foreign keys).
- The vocabulary of allowed field data types. The set is closed and small.
  Adding a type is a core change with synchronized adapter updates.
- Identifier validation helpers (carrier-neutral text rules already in
  `dbxcarta.core.databricks`; the subpackage name reflects the
  identifier-rule provenance, not an SDK dependency).
- The verification *rules*: the declarative checks that compare what the
  catalog says against what was written to the graph. The *execution* of
  those rules (which needs both a graph driver and a warehouse client)
  lives in adapters. Verify rules are part of the contract; running them
  is not.

**Does not own:**

- Any code that reads or writes a specific backend.
- Any import of pyspark, neo4j, google-cloud-bigquery, or databricks-sdk at
  runtime. Type-checker-only imports are acceptable.
- The decision of *how* to compute embeddings, sample values, or discover
  foreign keys. Core declares the inputs and outputs; adapters compute.

**Must guarantee:**

- The package imports cleanly with none of the forbidden backends installed.
- A boundary test enforces the no-forbidden-imports rule at the source level
  and at runtime.

### dbxcarta-spark

Spark is the production ingest adapter. It is large by necessity because it
owns every concrete pipeline step.

**Owns:**

- Reading Unity Catalog `information_schema` into the relational entity shape
  declared by core.
- Sampling distinct column values at the cardinality and limit policy the
  current pipeline implements.
- Generating embeddings via `ai_query` with the existing per-label text
  expressions, ledger-based deduplication, and failure-rate accounting.
- Discovering foreign-key relationships from three sources: Unity Catalog
  declared constraints, name/PK metadata heuristics, and semantic similarity
  on column and value embeddings.
- Writing nodes and edges to Neo4j via the Neo4j Spark Connector, in the
  ordering and batching the current pipeline uses.
- Bootstrapping Neo4j constraints before write and purging stale `Value` nodes
  after column-id resolution.
- Running the core-owned verify rules against the populated graph as the final
  pipeline step.
- Producing the per-run summary record (today's `RunSummary`) and writing it
  to the Delta summary table.

**Does not own:**

- The graph shape itself. Labels, edge types, key fields, and the
  field-type vocabulary come from `dbxcarta.core.contract`; Spark
  consumes them and does not redefine them.
- Any client-facing query surface. Spark writes the graph; the client
  reads it.
- Cross-adapter coordination. Spark runs end-to-end on its own and does
  not assume another adapter exists or doesn't.

**Must guarantee:**

- No step on the main data path performs an unbounded driver-side collect, a
  pandas conversion, or per-row Python iteration over the full graph, value
  set, or embedding payload. Bounded driver metadata and metric actions are
  acceptable when they follow Spark best practices.
- Every DataFrame written to Neo4j uses the label, edge type, and field
  names declared in `dbxcarta.core.contract`. The adapter test suite
  asserts on produced DataFrame schemas; there is no runtime gate.
- The summary record includes every metric the current `RunSummary` includes.
  No observability regression.
- The package depends on `dbxcarta-core` and nothing in the client, presets,
  or any other adapter.

### dbxcarta-client

`dbxcarta-client` is this repo's reference client: the Databricks-flavored
runtime that the dbxcarta agent uses for retrieval and the Text2SQL
evaluation harness. It is one client among many possible; other clients
(MCP servers, alternate retrieval surfaces, future Spark-based readers)
may live in this repo or elsewhere. The graph schema is the cross-client
contract; client implementation choices (driver, framework, dependencies)
are client-owned, the same principle that applies to adapters.

**Owns:**

- Querying the populated Neo4j graph over bolt for the agent's
  retrieval path.
- The Text2SQL evaluation harness (`dbxcarta.client.eval`), which runs
  as a Databricks job with Spark.
- Databricks-hosted runtime integrations the agent depends on (LLM
  calls via serving endpoints, SQL via warehouses, embeddings via
  Databricks model serving).
- Knowing the graph shape by convention via the `dbxcarta.core.contract`
  enums and the suggested-schema README.

**Does not own:**

- The graph shape. Clients consume `dbxcarta.core.contract` enums; they
  do not redefine labels, edges, or field names.
- Any write path. Read-only against the populated graph; never mutates
  it.
- Knowledge of which adapter populated the graph. Clients are
  pipeline-agnostic per Goal 4.

**Must guarantee:**

- Does not import any adapter package (`dbxcarta.spark`,
  `dbxcarta.presets`). The dependency direction stays one-way: clients
  depend on the contract, not on writers.
- Does not build, refresh, or mutate the semantic layer. Read-only
  against the graph.
- The top-level `dbxcarta.client` namespace stays narrow so a
  hypothetical lightweight consumer (for example, a retrieval-only
  caller that imports the public API without needing the eval
  harness) does not pull heavyweight dependencies transitively. The
  boundary tests at `tests/boundary/test_import_boundaries.py`
  enforce this property.

### dbxcarta-presets

Presets is the composition root. It is the only distribution allowed to
depend on multiple adapters.

**Owns:**

- Canonical compositions of reader, embedder, sampler, mapping, and writer
  for shipped scenarios (today: `finance-genie`).
- The `preset` CLI that runs a full ingest from a named composition.
- Examples and reference wirings in `examples/`.

**Does not own:**

- The ingest steps themselves. Presets composes adapter-provided
  pieces; it does not re-implement reading, sampling, embedding, or
  writing.
- The graph shape or its evolution. Adding labels or fields is a core
  change, not a presets change.

**Must guarantee:**

- Output formatting and CLI behavior remain compatible with the current
  `preset --run` contract.

### dbxcarta-bigquery (future, not built in v3)

Not in scope for v3 implementation. Its requirements are listed only to keep
v3's "additive" promise honest.

**Will own:**

- Reading BigQuery `INFORMATION_SCHEMA` into the relational entity shape
  declared by core.
- A row-batch (bolt) writer that produces the same graph shape as the Spark
  writer.
- A row-based embedder against the chosen vendor endpoint.
- Its own mapping object producing the same graph shape from BigQuery inputs.

**Will not require:**

- Any change to core, Spark, client, or presets to land its first version
  beyond the composition wiring in presets.

---

## What stays from v1

v1 is authoritative for these and v3 does not change them:

- Four-distribution packaging under the `dbxcarta` PEP 420 namespace.
- Boundary tests for import-time and source-level forbidden-import rules.
- Console scripts owned by the implementing distribution.
- `EnvOverlay` for `os.environ.setdefault`-style configuration composition.
- Identifier validation in `dbxcarta.core.databricks`.

---

## Resolved requirements

Decisions recorded for the ten items the review surfaced. Phase 1
verifies these against the inventory; Phase 1.5 freezes them.

1. **Verify execution composition.** Verify *rules* live in
   `dbxcarta-core` as declarative checks. Rule definitions in core are
   carrier-neutral: they describe what to compare (catalog claim
   against graph state), not how to query a specific store. No Cypher
   or warehouse-SQL literals are embedded in core; adapters translate
   rules to their own query languages. *Execution* lives in the
   adapter that ships the relevant clients. For v3 that means
   `dbxcarta-spark` runs verify using its existing Neo4j access and
   Databricks warehouse client. No separate verify or neo4j
   distribution is created. A future row-based adapter brings its own
   Neo4j driver dependency and runs the same rules.

2. **Summary continuity.** Today's `RunSummary` stays the canonical
   record. v3 does not introduce a parallel `BuildResult`. Goal 6
   forecloses changes to the Delta summary schema; a second
   summarization surface only creates a translation layer to evolve.
   The existing builder/config/result types are part of the Goal 8
   removal list (Phase 4), not adapted to a new summary surface.

3. **Schema conformance tolerance.** Now a unit-test question, since
   enforcement is convention only (Resolved Req 9) and there is no
   runtime harness (Resolved Req 10). A pytest assertion checks that
   an adapter-produced DataFrame's label or edge name matches a
   `dbxcarta.core.contract` enum value and that the declared field set
   with matching dtype is present. Field ordering, nullability, and
   field metadata are ignored.

4. **Field type vocabulary.** Phase 1 produces the closed set as the
   field-type inventory rather than enumerating it upfront; Phase 2
   design consumes the set as a fixed input. Adding a type thereafter
   is a core change with synchronized adapter updates. The set will
   cover every type the current pipeline actually produces (including
   `int`, `date`, `decimal`, and any others surfaced by inventory).

5. **Row-side protocols.** Deferred. v3 ships Spark-only. Row-based
   protocols are not defined until the BigQuery + bolt pipeline starts.
   Recorded in `clean-boundaries-future.md`.

6. **Contract sentinel.** Removed from v3. The client does not check a
   contract version stamp on startup, and adapters do not write one.
   Goal 4's pipeline-agnostic property holds by convention rather than
   by runtime check: the client talks bolt to whatever populated the
   graph (so connectivity is unaffected by which adapter ran), and the
   team is responsible for keeping client queries aligned with the
   schema adapters produce (no machine check enforces this). The
   `CONTRACT_VERSION` constant stays in core as a marker; the sentinel
   design is recorded in `clean-boundaries-future.md` for the next
   iteration.

7. **Mapping shape.** No contract. Each adapter sequences its own work.
   Spark's existing phased structure under `ingest/` stays unchanged.
   Core declares only inputs and outputs; the orchestration shape is
   adapter-owned, same principle as Resolved Req 8.

8. **Per-adapter configuration flow.** Adapter-owned. Each adapter's
   settings live in the adapter (Spark's `settings.py` is the pattern).
   No string-keyed bag on the build spec; no source-reference extension
   on the contract. Presets composes a run by importing the adapter's
   settings type.

9. **Enforcement strictness.** Convention only. Adapters import the
   core-declared primitives and project their outputs against them by
   hand. The only machine-checked rule is the boundary tests for
   forbidden imports between distributions. Stricter enforcement
   (writer-gated conformance, runtime contract sentinels) is deferred
   until a second adapter exists.

10. **Test-infrastructure commitments.** No dedicated conformance
    harness in v3 beta. Plain pytest assertions in the adapter test
    suite check produced DataFrame schemas against the
    `dbxcarta.core.contract` enums. A reusable conformance harness
    and a round-trip-against-real-graph heavy tier are both deferred
    (see `clean-boundaries-future.md`).

Cross-project concerns and other deferred work live in
`clean-boundaries-future.md`. New questions that surface during
Phase 1 or 1.5 go to that doc, not back into this list (see Goal 9).

---

## Phased implementation plan

The work splits into discovery, design, and execution. Phases are
sequential — each unblocks the next — but each is a mergeable unit on its
own and can be paused without invalidating the previous one.

### Phase 1 — Pre-flight discovery

**Goal.** Produce the complete picture of what the current pipeline does and
resolve every open requirement, so the design doc rewrite has every input it
needs. No code changes. No design decisions about the new architecture beyond
what the open requirements force.

**Activities.**

- **Inventory dbxcarta-spark.** Walk every file under `ingest/` plus
  `run.py` and `settings.py`. For each named phase capture: what it
  consumes, what it produces, what side-effects it has, which configuration
  knobs control it, which metrics it records, which other phases it depends
  on. Resolve ambiguity by reading the code, not memory.
- **Inventory dbxcarta-core.** Identify what is already there, what is
  clean (no backend imports), and what currently violates the
  no-backend-imports rule (verify is the known case; surface anything
  else).
- **Inventory dbxcarta-client.** Identify exactly what the client reads
  from Neo4j today and what queries it issues.
- **Inventory dbxcarta-presets.** Capture what the `preset --run` CLI
  takes, what it passes through, and what the wheel entrypoint relies on.
  This bounds what the cutover must preserve.
- **Field-type inventory.** Enumerate every distinct data type the current
  pipeline reads from `information_schema` and every type it writes to
  Neo4j (including embeddings and the awkward cases — decimal precision,
  arrays of structs in `data_type` strings, ints vs longs).
- **Observability inventory.** Enumerate every field on today's
  `RunSummary` and every consumer that reads it (Delta table schema,
  downstream analytics, the verify report).
- **Capture pre-v3 baseline.** Record the current end-to-end
  integration test output and a representative Delta summary row from
  `preset --run finance-genie` against today's main branch. This is
  the artifact Goal 7 acceptance criterion 3 matches against; without
  it the criterion is unverifiable.
- **Cross-check resolved requirements.** Walk items 1–10 in the
  resolved-requirements section against the inventory findings. Flag
  any code reality that contradicts a recorded decision; no contradiction
  is the expected case.

**Outputs (added to this requirements doc).**

- **Pipeline obligations checklist.** Three lists: named phases that must
  exist somewhere; ordering and dependency constraints between phases;
  cross-cutting concerns (ledger, fail-rate gate, idempotency, per-label
  flags, summary). Each item is a requirement: the design must place it
  and identify its owner. Omission is not allowed.
- **Inventory cross-check note.** A short report listing any inventory
  finding that does not align with the resolved-requirements section
  (expected: none).
- **Field-type vocabulary.** The closed set of dtypes the design will use,
  derived from the inventory rather than guessed.
- **Observability contract.** The set of metrics and the destinations
  (Delta summary table, verify report, run logs) that the design must
  preserve.
- **Baseline artifact reference.** Pointer to the captured pre-v3
  integration test output and reference summary row that Goal 7
  acceptance criterion 3 validates against.

**Done when.** Every item in the obligations checklist has a named
owner-candidate (even if the final placement is deferred to design),
the inventory cross-check has surfaced no conflicts with the
resolved-requirements section, the field-type and observability
inventories are complete, and the pre-v3 baseline is captured. No
code written.

### Phase 1.5 — Requirements lockdown

**Goal.** Stop iterating on requirements. Take the doc as it stands
after Phase 1, fix any remaining inconsistencies, and freeze it. From
this point through Phase 6, requirement changes are out of scope unless
the design or implementation surfaces a problem that cannot be resolved
within the existing requirements.

**Activities.**

- Fold Phase 1 outputs (obligations checklist, inventory cross-check
  note, field-type vocabulary, observability contract, baseline
  artifact reference) into this requirements doc.
- Re-read the requirements doc top to bottom.
- Confirm every resolved requirement (items 1–10) survived the Phase
  1 inventory cross-check unchanged.
- Confirm the beta scope line from Goal 7 is final: what's in, what's
  out, and the three-item acceptance checklist.
- Time-box: one or two sessions, not multiple weeks. Default decision
  rule for any contested item is "pick the option that ships fastest
  and is reversible."

**Done when.** Phase 1 outputs are folded in, the requirements doc
reads end-to-end without unresolved questions, and there is explicit
agreement that no more requirement edits and no beta-scope edits land
before Phase 2 starts. Future amendments go to a v4 doc, not back into
v3.

### Phase 2 — Design rewrite

**Goal.** Rewrite `clean-boundaries-v3.md` against the locked
requirements. The design must explicitly place each obligation, cite each
resolved requirement, and show its work where a previous draft hand-waved.
No code yet.

**Done when.** The design doc has a placement for every obligation, a
concrete answer that cites each resolved requirement, and the merge
gate is specified in terms the integration test can check.

### Phase 3 — Core contract cleanup

**Goal.** Land the carrier-neutral graph shape primitives, field vocabulary,
identifier helpers, and declarative verify rule definitions in
`dbxcarta-core`. Keep the phase mergeable by avoiding adapter-breaking
deletions until the Spark cutover in Phase 4. Boundary tests prove the new core
surface loads without backend imports and identify the legacy exports that
Phase 4 must remove.

### Phase 4 — Spark adapter migration

**Goal.** Move the existing Spark pipeline behind the new shape
primitives, port-by-port. The obligations checklist drives the
migration: each named phase moves to its placement, the observability
contract is maintained, verify execution moves fully into
`dbxcarta-spark`, the runtime `neo4j` dependency is removed from
`dbxcarta-core`, and the end-to-end integration test matches the
pre-v3 production baseline.

**Removal list (per Goal 8 — no compatibility shims):**

- `SemanticLayerConfig`, `SemanticLayerBuilder`, and
  `SemanticLayerResult` exports from `dbxcarta-core`.
- The `run_dbxcarta(..., semantic_config=...)` entry branch.
- The `SparkSemanticLayerBuilder` shim in `dbxcarta-spark`.

### Phase 5 — Presets and examples cutover

**Goal.** Update `preset --run`, the wheel entrypoint, and the examples
(including the external `sql-semantics` programmatic demo) to the new
composition. The CLI contract is preserved.

### Phase 6 — Boundary tests, docs, beta acceptance

**Goal.** Tighten boundary tests to include the new adapter rules. Add
plain pytest assertions on adapter-produced DataFrame schemas against
the `dbxcarta.core.contract` enums (per Resolved Req 10).
Publish the suggested-schema README that documents the graph shape in
prose. Cross-reference v1 sections that remain authoritative, mark v2
superseded, refresh the architecture diagrams and per-package READMEs.

**Done when (Goal 7 acceptance gate).** All three criteria from Goal
7's beta acceptance checklist hold simultaneously:

1. `preset --run finance-genie` produces a Delta summary row
   schema-identical to the Phase 1 baseline.
2. The boundary tests for forbidden imports pass (the v1 tests plus
   any tightening added in this phase).
3. The end-to-end integration test matches the pre-v3 production
   baseline captured in Phase 1.

Phases 3–6 will be expanded once Phase 1 outputs land and Phase 2
rewrites the design doc against them. Their requirements are intentionally
sketch-level here.

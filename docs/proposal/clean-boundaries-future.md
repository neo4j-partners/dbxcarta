# Clean Boundaries — Beyond v3

This document holds work and questions that have been scoped *out* of
the v3 beta (see `clean-boundaries-requirements-v3.md`). It exists so
that "but what about..." thoughts have a home that does not expand v3.

Nothing here is committed. Items are recorded so they are not lost; the
sequencing, prioritization, and final shape are decided when the v3
beta ships and the second project's experience informs the next
iteration.

How to use this doc:

- If you have a new question or concern about the pattern and v3 does
  not address it, add it here, not to v3 (see v3 Goal 9).
- Phase 1.5 of v3 explicitly forbids adding new items to v3's
  open-requirements list. New items go here.
- When v3 beta ships, this doc becomes the starting point for v4 (or
  whatever the next iteration is called).

---

## Cross-project vision

v3 is the first concrete instance of a cross-project pattern. The same
ports-and-adapters split v3 lands for dbxcarta is also the pattern
under which a second project,
[`neocarta`](https://github.com/neo4j-field/neocarta), will be rebuilt
once dbxcarta v3 stabilizes. Cross-project work is out of v3 scope (see
v3 Goal 7); this section records the wider context for why the
per-package contract is shaped the way it is.

This is a prototype-stage commitment. The goal is to get close to the
right shape with dbxcarta, not to lock the pattern down before two
working implementations exist. Cross-repo governance, breaking-change
policy, contract package relocation, and the second project's carrier
choices are deliberately deferred. v3 ships a beta of the pattern;
adjustments are expected once neocarta migrates.

### Shared scope and reference implementation

The contract covers the relational sub-shape only: Database / Schema /
Table / Column / Value with `HAS_*` and `REFERENCES`. dbxcarta produces
this. Neocarta's RDBMS connectors (BigQuery, CSV, Dataplex, query log)
will produce this once migrated. Other neocarta sources (LPG metadata,
vector stores, anything else) stay project-local and outside the v3
contract.

dbxcarta is the reference implementation. `dbxcarta-spark` proves the
pattern with Unity Catalog to Neo4j on a DataFrame-native carrier. The
contract lives in `dbxcarta-core` during v3; neocarta will depend on it
once migration starts. The "additive backends" promise (v3 Goal 3) is
read at project scope: a new pipeline in a different repository is the
same shape of change as a new pipeline inside dbxcarta.
Project-specific surfaces above the contract (neocarta's MCP server,
its agent, its evaluation harness) stay project-owned and do not
redefine the contract.

### Client surfaces are project-tailored

Each project ships the client(s) its consumption pattern requires.
dbxcarta's agent uses a Databricks-flavored client for retrieval and
eval. Neocarta's MCP server may use the Neo4j Python driver directly.
A future analytics client may use the Neo4j Spark Connector. The
cross-project contract is the graph schema, not a shared client API.
Shared client helpers (bolt session lifecycle, common query patterns)
may emerge in a future iteration if duplication becomes visible; the
v3 beta does not commit to them.

### Cross-project invariants

Every constraint in the v3 requirements still holds at cross-project
scope. The no-driver-collect property — no unbounded driver-side
materialization of bulk graph, value, or embedding payload — is part
of the contract for any bulk-volume pipeline regardless of carrier
(DataFrame, row-batch, or other). Core stays backend-neutral;
cross-project portability does not introduce neocarta's dependencies
(pandas, Pydantic-row carriers, Google clients) into core any more
than it introduces Spark today.

---

## Cross-project concerns

These were considered for v3's open-requirements list and pulled out as
deferred. They become decisions when neocarta migration starts.

### Contract package home

The contract lives in `dbxcarta-core` for the v3 beta. Open question
for the next iteration: keep that name (neocarta consumes a package
called `dbxcarta-core`), rename it to a project-neutral distribution,
or extract it into a third repo. Each option has different consequences
for branding, release cadence, and the dependency graph neocarta
inherits.

### Shared client packaging

The v3 beta ships `dbxcarta-client`. Open question for the next
iteration: whether the cross-project client is `dbxcarta-client`
evolving into a shared role, a renamed or relocated client
distribution, or a new distribution that supersedes it. Where the
contract-version check, bolt session lifecycle, and shared query
helpers live. How neocarta's MCP server consumes the client (direct
dependency vs. wrapper).

### Neocarta carrier model

Neocarta's connectors today are Pydantic row models, not
DataFrame-native. Open question for the next iteration: whether the
cross-project port supports a row-based bolt carrier as a first-class
adapter category (which would also pave the way for the BigQuery +
bolt pipeline listed below), or whether neocarta's connectors are
rebuilt onto an existing carrier during their migration.

### Cross-repo governance

If neocarta depends on `dbxcarta-core`, then `CONTRACT_VERSION` bumps,
label additions, type-vocabulary changes, and deprecation policy have
an external consumer. Open questions: who controls bumps; how
breaking changes are coordinated; what neocarta's veto looks like;
release cadence and stability commitments.

---

## Shape extensions

### LPG sub-shape

Neocarta has a second metadata shape today: Database / Schema / Node /
Relationship / Property, used to describe the metadata of a graph
database itself (separate from the relational sub-shape v3 covers).
The v3 contract intentionally does not cover this.

Open questions when LPG is added to the contract:

- Whether both sub-shapes share `NodeLabel` / `RelType` enums or use
  sibling enum families.
- How an adapter declares which sub-shape it emits.
- How the client routes queries against the right sub-shape.
- Whether the contract version is one number covering both or
  independent versions per sub-shape.
- Whether the two sub-shapes coexist in one Neo4j graph or are
  populated into separate graphs.

### Vector-store sources

Neocarta has a Pinecone path under consideration. Not in scope for the
v3 contract. If a future iteration extends the contract to vector-store
metadata, it lands here first.

---

## Beyond-beta v3 work

Scoped out of the v3 beta by Goal 7. These items remain inside the
broader v3 plan (the existing `clean-boundaries-requirements-v3.md`
phases anticipate them) but are not part of the first shipped cut.

- **BigQuery + bolt adapter (`dbxcarta-bigquery`).** Already sketched
  in v3 under "dbxcarta-bigquery (future, not built in v3)". Builds on
  the row-based bolt carrier question above and on the row-side
  protocols deferral below.
- **Row-side protocols.** Not defined in v3. v3 ships Spark-only, so
  the protocol shape a row-based bolt writer would consume is left
  open until the BigQuery + bolt pipeline starts. The two travel
  together.
- **Contract version sentinel.** Removed from the v3 beta. The pattern,
  for the next iteration:
  - Every adapter writes a `CONTRACT_VERSION` string onto the graph as
    part of its run.
  - The client reads it on startup and refuses to serve against a
    missing or unrecognized version.
  - The stamp is per-graph state; the constant and the check logic are
    shared code.

  Reasons to defer: v3 has only one adapter, so cross-pipeline drift
  detection is moot. The sentinel introduces atomicity questions
  (write order, half-write window, behavior on mismatch with freshly
  written data) that are easier to answer once a second adapter exists.
  The `CONTRACT_VERSION` constant stays in `dbxcarta-core` as a marker
  so the eventual sentinel does not need a new home for the value.
- **Stricter shape enforcement.** v3 ships convention plus boundary
  tests for forbidden imports (per v3 Resolved Req 9). Writer-gated
  conformance — a mandatory validation step the writer cannot bypass
  — and lint rules over carrier outputs are deferred to the same
  iteration that introduces a second adapter. Should any future
  iteration reintroduce runtime contract validation on the write
  path, it must operate against planner metadata, not row contents;
  triggering a Spark action to validate is forbidden, since the
  no-driver-collect invariant binds even validation work.
- **Reusable conformance harness.** v3 beta ships only plain pytest
  assertions on adapter-produced DataFrame schemas, no shared harness.
  A reusable harness (importable by future adapters to assert their
  outputs match the core-declared shape) and a
  round-trip-against-a-real-Neo4j tier are both deferred (per v3
  Resolved Req 10).
- **Multi-preset coverage.** The beta covers the one shipped preset
  (`finance-genie`). Additional presets land after the beta proves the
  pattern.

---

## How items move between docs

- A new item surfaces during v3 work → add it here.
- An item here becomes relevant for an upcoming iteration → promote it
  to that iteration's requirements doc (v4, not back to v3).
- An item here turns out to be wrong or no longer relevant → delete
  it.

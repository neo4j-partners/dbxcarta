# Proposal: A Standalone OWL/SHACL Policy Layer for dbxcarta

Status: proposal, not yet implemented.

## Goal

One vendor-neutral semantic policy artifact, authored once, consulted at two
enforcement points:

1. **Ingest gate.** Every ingestion pipeline that writes to the metadata graph
   validates its output against a shared OWL/SHACL contract before the write.
   The contract covers structural well-formedness and a data policy: every
   retrievable resource must carry a classification.
2. **Query gate.** The dbxcarta retriever applies a single access rule of the
   form **"role X may see resources classified Y"** before any schema metadata
   reaches the LLM.

The policy lives in the repository as RDF/Turtle, not in Neo4j config or
Databricks. It is the same artifact regardless of which pipeline produced the
graph or which platform runs retrieval.

## Non-goals and the boundary that must stay explicit

This layer governs **metadata visibility and SQL routing**. It is **not** a
replacement for Unity Catalog data-plane security. When generated SQL executes,
UC grants still enforce the actual data read. What this layer adds is closing
the Neo4j metadata side-channel (table names, column names, descriptions,
sampled values) and enforcing the classification rule consistently across
ingestion pipelines that UC does not see.

Identity is handled out of band: access to the retrieval tool is gated by
identity, which establishes the caller's **role**. This proposal does not
resolve per-user UC grants. It consumes the already-established role and
applies the classification rule. This is a deliberate simplification the user
chose, and it is what makes Option B tractable.

## Why Option B (standalone policy both sides consult)

Option A (mirror UC grants into the graph) was rejected because there will be
multiple ingestion pipelines, not all of them UC-grant-aware, and because the
goal is a classification-based rule, not UC grant replication. A standalone
policy authored once and consulted by every pipeline and by the retriever is
the right shape for that. The accepted cost is drift: the classification policy
is separate from UC grants and must be maintained on its own. That is
acceptable because classification, not UC grants, is the contract here.

## Architecture

```
        ┌──────────────────────────────────────────────┐
        │  Policy artifact (repo, Turtle/OWL + SHACL)   │
        │  - vocabulary: Classification, Role           │
        │  - access matrix: roleMayAccess               │
        │  - SHACL shapes: graph contract + "must be    │
        │    classified"                                │
        └───────────────┬───────────────┬──────────────┘
                         │ loaded by     │ loaded by
            ┌────────────▼───────┐  ┌────▼─────────────────┐
            │  Ingest PEP        │  │  Query PEP            │
            │  pyshacl validate  │  │  PDP.allowed(role,c)  │
            │  before graph write│  │  filter ContextBundle │
            └────────┬───────────┘  └────┬─────────────────┘
                     │                    │
            pipelines (dbxcarta      GraphRetriever.retrieve
            Spark + others)          (client package)
```

A single small module, proposed as `dbxcarta.policy`, owns:

- Loading the Turtle policy graph (rdflib).
- The ingest validator (pyshacl) used by any pipeline.
- The Policy Decision Point: `allowed(role: str, classification: str) -> bool`,
  an in-memory lookup over the access matrix, not a per-request pyshacl run.

This module must be importable by both the Spark ingest package and the client
package without dragging Spark or Databricks into the client. It should be a
leaf dependency: rdflib + pyshacl only.

## Semantic model

OWL defines the vocabulary so the rule and the labels are one portable artifact:

- Classes: `Database`, `Schema`, `Table`, `Column`, `Value` (mirroring the
  existing graph contract), plus `Classification` and `Role`.
- Classification instances, for example `Public`, `Internal`, `Restricted`,
  `PII`. Start with a small closed set.
- Role instances, for example `Analyst`, `PiiReader`, `Admin`.
- Properties: `hasClassification` (resource to Classification),
  `roleMayAccess` (Role to Classification).

The access matrix is data in the same graph:

```turtle
:Analyst   :roleMayAccess :Public, :Internal .
:PiiReader :roleMayAccess :Public, :Internal, :Restricted, :PII .
:Admin     :roleMayAccess :Public, :Internal, :Restricted, :PII .
```

"Role X may see resources classified Y" is exactly `roleMayAccess`. Decisions
are default-deny: a classification not listed for a role is denied, and a
resource with no resolvable classification is treated as the most restrictive
class.

## Ingest gate design

The contract is one SHACL file shared by every pipeline. It declares:

- Structural shapes that restate the graph contract: id pattern
  `catalog.schema.table[.column]`, every `Column` has exactly one parent
  `Table`, every `REFERENCES` target is a `Column`, required `description` and
  `embedding` where the current contract requires them.
- A data-policy shape: every `Table` and `Column` has exactly one
  `hasClassification` drawn from the allowed Classification set. This is the
  rule that guarantees the query gate always has something to decide on.

### Why not pyshacl in the pipeline

The original design said each pipeline lifts its nodes to RDF and runs
`pyshacl` before the write. That does not survive a well-built Spark pipeline.
`pyshacl` operates on an in-memory `rdflib` graph, so running it over the
dataset forces a driver-side `collect`/materialization, which the dbxcarta
ingest pipeline is deliberately designed never to do. The question is not "run
pyshacl in the pipeline" but "where does validation run so the pipeline stays
distributed." The answer is two tiers, with the one `.ttl` as the single source
of truth.

### Tier 1: in-pipeline, distributed (fast path)

Validate inside the Spark pipeline with no `collect`, using a Spark-native
data-quality engine: PyDeequ constraint verification, or, on Databricks,
Lakeflow/DLT `EXPECT ... ON VIOLATION DROP/FAIL`. Either flags or quarantines
violating rows and fails or counts in aggregate, fully distributed. This
extends the distributed graph-contract verification the Spark package already
has rather than introducing a new driver-side step. It covers the structural
shapes and the must-be-classified rule, the hot-path subset.

### Tier 2: authoritative gate at the Neo4j boundary (full fidelity)

Spark writes to a **staging** graph or label exactly as today, distributed and
uncollected. After the write, `n10s.validation.shacl.validate()` runs inside
Neo4j against the property graph directly (no Spark-side RDF, no
materialization). Clean staging is promoted to live; a failing batch never
becomes visible. This is the full-SHACL check and the authority for shapes
Tier 1 does not cover. It is also the Going Meta tooling (sessions 3 and 11 use
n10s for SHACL), keeping this consistent with the alignment section below.

The `.ttl` is the single artifact: Tier 1 is its fast projection for the
pipeline, Tier 2 is its exact evaluation at the boundary.

### Build vs reuse, and effort

There is no production-grade, maintained "SHACL-on-Spark" library to install;
every conformant engine (pyshacl, Jena/TopBraid, RDF4J) is in-memory JVM or
Python, and the "semantics on Spark" lineage (S2RDF, SANSA, SHACL2SPARQL) is
research-grade and not a safe dependency. Tier 1 is therefore a small bounded
build, not an install:

- **Reuse the shape parser.** Load the `.ttl` with `rdflib` (already a pyshacl
  transitive dependency) and query the shapes graph. Do not hand-parse Turtle.
  The `rdfshapes/shacl-sparql` repo is a useful mapping reference, not a
  dependency.
- **Reuse the enforcement engine.** PyDeequ or DLT expectations already do
  distributed row flagging, violation metrics, quarantine, and fail
  thresholds. Do not rebuild these.
- **Reuse the full engine for Tier 2.** n10s validates the property graph in
  Neo4j directly; no SHACL engine to write.
- **Net-new code is only the translation map**: ~8 to 10 SHACL components
  (`sh:targetClass`, simple `sh:path`, `sh:datatype`, `sh:minCount`/`maxCount`,
  `sh:pattern`, `sh:in`, `sh:class`/`sh:node` referential via anti-join, the
  composite must-be-classified rule) to PyDeequ checks. Property paths,
  `sh:sparql`, nested `sh:and/or/not/xone`, recursion, and
  `sh:qualifiedValueShape` are out of scope for Tier 1 and handled by Tier 2.

Effort for the `.ttl`-to-Deequ translator over that subset, with tests: a few
days to roughly one to two weeks for one focused engineer. A cheaper start is
to hand-author the Deequ/expectation checks for the current contract and add a
unit test asserting every SHACL shape has a corresponding check, deferring the
generated translator until the contract grows enough to need it. Tier 2 (n10s
on the staged graph) remains the authoritative gate in both cases, so fidelity
is not lost by starting hand-authored.

Classification source, in priority order:

1. Unity Catalog tags or classifications propagated onto nodes at ingest, where
   the pipeline has UC access.
2. Pipeline-supplied classification for non-UC sources.
3. No default that silently passes. Absent classification fails the ingest
   gate by design, so coverage gaps are loud, not silent leaks.

## Query gate design

The enforcement point is `GraphRetriever.retrieve(question, embedding)` in
`packages/dbxcarta-client/src/dbxcarta/client/graph_retriever.py`, which
returns a `ContextBundle` of table, column, and value nodes
(`retriever.py:39`). The PEP filters that bundle before it is rendered for the
LLM.

- `retrieve` gains the caller role from request context (the same identity
  decision that gated tool access supplies it). Signature change is on the
  concrete retriever and the `Retriever` ABC at `retriever.py:110`.
- After candidate nodes are gathered, each node's `hasClassification` is read
  and `PDP.allowed(role, classification)` is checked. Denied nodes are dropped
  before subgraph expansion is rendered, so a denied table cannot leak through
  its columns or sampled values.
- Default-deny: a node whose classification cannot be resolved is dropped, and
  if the role is unknown the bundle is empty.
- Optionally, generated SQL is checked: every table the SQL references must map
  to a node the role may access, as a second PEP in front of the executor.
  Recommended as a fast follow, not phase one.

The decision is an in-memory dictionary lookup loaded once at process start,
so per-query cost is negligible. pyshacl is used only at ingest, never per
query.

## Honest caveats

- **Drift.** The classification policy is not UC grants. If UC tags change and
  the policy graph does not, the two disagree. Mitigation: derive
  classification from UC tags at ingest where possible, and treat the policy
  graph as the authority for everything else.
- **Coverage is the whole game.** The query gate is only as good as
  classification coverage. The ingest gate's "must be classified" shape is what
  makes coverage enforceable rather than aspirational. Do not weaken it with a
  permissive default.
- **Every retrieval path must go through the PEP.** If any code path returns
  graph nodes without passing the filter, the side-channel reopens. The PEP
  belongs at the single `retrieve` boundary, and alternate retrievers must
  inherit it.
- **This is not data security.** Repeating the boundary: UC still enforces the
  data read. This layer enforces what metadata is visible and what SQL is
  allowed to be routed.

## Alignment with Going Meta (Jesús Barrasa)

This approach is consistent with the position Jesús Barrasa develops across the
Going Meta series. The reading below is inferred from the series content, not a
direct review of this proposal.

**SHACL as the shape and quality contract: directly endorsed.** Two sessions
are devoted to exactly the ingest-gate pattern: session 3, "Controlling the
shape of your graph with SHACL," and session 11, "Graph data quality with graph
expectations." Validating every pipeline's output against a shared SHACL
contract before the write is the practice these sessions teach.

**Define once, push logic into the data: his core thesis.** Session 15: "let's
remove part of the business logic from the application and let's put it closer
to the data ... by doing that we make it explicit, we understand more about our
data and we make it reusable," and "the whole idea of adding a semantic layer is
that you do it once and every consumer can benefit from that." Session 34: a
semantic layer is "a way of capturing declaratively some meaning and then making
use of it dynamically." The proposal's premise, one declarative artifact
authored once and consumed by every ingestion pipeline and the retriever, is
this argument applied to policy.

**Ontology actively driving construction across pipelines: his model.**
Session 31 demonstrates a pipeline that is "entirely domain-agnostic": the code
reads the ontology and uses it to drive graph construction, so multiple
pipelines conforming to one shared contract is the intended design, not a
deviation.

**Keep OWL and SHACL distinct: his precision, which this proposal follows.**
Going Meta separates OWL for formal meaning and inference (session 4, "Ontology
based reasoning 101") from SHACL for closed-world validation (sessions 3 and
11). Strict schema enforcement is SHACL's job, not OWL's, and neither is an
authorization engine. This proposal already respects that split: OWL for
vocabulary only, SHACL at ingest only, and a plain in-memory lookup for the
runtime access decision.

**The drift caveat is his objection to a parallel policy.** The series'
investment argument (session 26) is that an explicit semantic description "can
be used to validate the quality of your data ... to dynamically generate data
ingest pipelines ... to improve the quality of LLM responses," and that
maintaining two representations of the same knowledge risks them drifting
apart. This is exactly the Option B drift risk flagged in the caveats above. It
reinforces the recommendation to derive classification from one authoritative
source (UC tags at ingest, priority 1) rather than hand-maintaining a matrix
parallel to UC grants.

Net: he would agree with the OWL/SHACL schema enforcement at ingest without
reservation, and with the define-once, logic-in-the-data architecture. His only
friction is the two caveats this proposal already carries.

## Phased plan

1. **Vocabulary and contract.** Author the OWL vocabulary and the shared SHACL
   file (structural shapes plus "must be classified"). Add the `dbxcarta.policy`
   leaf module with the loader and PDP. No behavior change yet.
2. **Ingest gate, Tier 1 (in-pipeline).** Hand-author the PyDeequ or DLT
   `EXPECT` checks for the current contract, running distributed alongside the
   existing Spark verification with no `collect`. Add a unit test asserting
   every SHACL shape has a corresponding check. Add classification derivation
   from UC tags. Prove other pipelines can consume the same checks.
3. **Ingest gate, Tier 2 (Neo4j boundary).** Write to a staging graph or
   label, run `n10s.validation.shacl.validate()` against the staged property
   graph, and promote staging to live only on a clean result. This is the
   authoritative full-SHACL gate.
4. **Query gate.** Thread role into `Retriever.retrieve`, add the
   classification filter to `GraphRetriever`, default-deny. Tests for
   allow, deny, unclassified, unknown role.
5. **`.ttl`-to-Deequ translator (deferred).** Replace the hand-authored Tier 1
   checks with a generated translator over the supported SHACL subset, once the
   contract grows enough that hand-maintaining checks is the bottleneck.
   Estimated at a few days to roughly one to two weeks for one focused
   engineer; Tier 2 stays authoritative throughout, so this is optional.
6. **SQL routing gate (fast follow).** Validate referenced tables in generated
   SQL against the same PDP before execution.

## Open questions

1. Where does the caller role enter the client at runtime: an explicit
   parameter on the retrieve call, request-scoped context, or settings?
2. Initial Classification and Role sets, and the starting `roleMayAccess`
   matrix.
3. For non-UC pipelines, who owns assigning classification, and is there a
   review step before the ingest gate accepts a new source?
4. Should phase 3 hard-fail the whole bundle on any denied node, or silently
   drop denied nodes and answer from what remains? Default-deny per node is
   assumed above; confirm.

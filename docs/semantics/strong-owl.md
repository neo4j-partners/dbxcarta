# Proposal: A Standalone OWL/SHACL Policy Layer for dbxcarta

Status: proposal, not yet implemented.

## TLDR

How a common schema gets enforced, in plain terms:

- **One shared shape, written once.** The rules for what a valid graph looks
  like live in a single SHACL file stored as Turtle (`.ttl`). SHACL is a
  standard language for "every Table must have a parent, every node must carry a
  classification, ids must look like `catalog.schema.table`."
- **The Turtle file is the human-readable contract.** It is vendor-neutral and
  authored once. Nothing reads it at runtime in the pipeline.
- **Hand-converted to PyDeequ.** A developer (or AI, as a one-time dev step)
  translates each SHACL shape into a matching PyDeequ constraint check. PyDeequ
  is a data-quality library that runs natively inside Spark.
- **PyDeequ enforces the shape in every batch.** Each pipeline run validates its
  output against those checks, fully distributed across the cluster, with no
  `collect` to the driver. Rows that violate the shape are flagged, quarantined,
  or fail the batch.
- **A unit test keeps them in sync.** It asserts every SHACL shape has a
  matching PyDeequ check, so the contract and the enforcement cannot silently
  drift apart.

So: SHACL in Turtle is the spec, PyDeequ is the distributed enforcement of that
spec on each batch. There is no SHACL engine running in the pipeline itself.

## Goal

One vendor-neutral semantic policy artifact, authored once, consulted at two
enforcement points:

1. **Ingest gate.** Every pipeline that writes to the metadata graph validates
   its output before the write. The contract covers structural
   well-formedness and one data policy: every retrievable resource must carry
   a classification.
2. **Query gate.** The dbxcarta retriever applies a single access rule of the
   form **"role X may see resources classified Y"** before any schema metadata
   reaches the LLM.

The policy lives in the repository as RDF/Turtle. It is the same artifact
regardless of which pipeline produced the graph or which platform runs
retrieval.

## Non-goals and the boundary

This layer governs **metadata visibility and SQL routing**. It is **not** a
replacement for Unity Catalog data-plane security. When generated SQL executes,
UC grants still enforce the actual data read. What this layer adds is closing
the Neo4j metadata side-channel (table names, column names, descriptions,
sampled values) and enforcing the classification rule consistently across
ingestion pipelines that UC does not see.

Identity is handled out of band. Access to the retrieval tool is gated by
identity, which establishes the caller's **role**. This proposal consumes the
already-established role and applies the classification rule. It does not
resolve per-user UC grants. That simplification is what keeps the design
tractable.

## Why a standalone policy both sides consult

Mirroring UC grants into the graph was rejected because there will be multiple
ingestion pipelines, not all of them UC-grant-aware, and because the goal is a
classification-based rule, not UC grant replication. A standalone policy
authored once and consulted by every pipeline and by the retriever is the right
shape for that. The accepted cost is drift: the classification policy is
separate from UC grants and must be maintained on its own. That is acceptable
because classification, not UC grants, is the contract here.

## Today's basic demo

This is what gets built now. Two enforcement points, one `.ttl`, no custom
Turtle parser, no Neo4j staging machinery.

### The artifact

One Turtle file holds:

- **OWL vocabulary.** Classes `Database`, `Schema`, `Table`, `Column`, `Value`
  mirroring the existing graph contract, plus `Classification` and `Role`.
- **Instances.** Classification instances `Public`, `Internal`, `Restricted`,
  `PII`. Role instances `Analyst`, `PiiReader`, `Admin`. Start with a small
  closed set.
- **Access matrix as data**, in the same graph:

  ```turtle
  :Analyst   :roleMayAccess :Public, :Internal .
  :PiiReader :roleMayAccess :Public, :Internal, :Restricted, :PII .
  :Admin     :roleMayAccess :Public, :Internal, :Restricted, :PII .
  ```

- **SHACL shapes.** Structural shapes that restate the graph contract (id
  pattern `catalog.schema.table[.column]`, every `Column` has exactly one
  parent `Table`, every `REFERENCES` target is a `Column`, required
  `description` and `embedding` where the current contract requires them), plus
  one data-policy shape: every `Table` and `Column` has exactly one
  `hasClassification` drawn from the allowed set.

A single small leaf module, proposed as `dbxcarta.policy`, owns loading the
Turtle graph with rdflib and exposing the Policy Decision Point:
`allowed(role: str, classification: str) -> bool`. Its only dependency is
rdflib. It must be importable by both the Spark ingest package and the client
package without dragging Spark or Databricks into the client.

### Ingest gate: PyDeequ in the pipeline

The SHACL shapes are the spec. The enforcement is hand-authored (or
AI-authored as a one-time dev step) PyDeequ constraint checks committed
alongside the `.ttl`. A unit test asserts every SHACL shape has a corresponding
PyDeequ check, so the spec and the checks cannot silently diverge.

These checks run **inside the Spark pipeline, fully distributed, with no driver
`collect`**. PyDeequ flags or quarantines violating rows and fails or counts in
aggregate across the cluster. This extends the distributed graph-contract
verification the Spark package already has rather than introducing a
driver-side step. It covers the structural shapes and the must-be-classified
rule.

Classification source, in priority order:

1. Unity Catalog tags or classifications propagated onto nodes at ingest, where
   the pipeline has UC access.
2. Pipeline-supplied classification for non-UC sources.
3. No default that silently passes. Absent classification fails the ingest
   gate by design, so coverage gaps are loud, not silent leaks.

There is no SHACL engine running in the pipeline. SHACL is the human-readable
contract; PyDeequ is its distributed enforcement.

### Query gate: `.ttl` to dictionary enforcement

At process start the `dbxcarta.policy` module loads the access matrix from the
`.ttl` with rdflib into an in-memory dictionary. There is no SHACL at query
time.

The enforcement point is `GraphRetriever.retrieve(question, embedding)` in
`packages/dbxcarta-client/src/dbxcarta/client/graph_retriever.py`, which
returns a `ContextBundle` of table, column, and value nodes.

- `retrieve` gains the caller role from request context. The signature change
  is on the concrete retriever and the `Retriever` ABC at `retriever.py:110`.
- After candidate nodes are gathered, each node's `hasClassification` is read
  and `PDP.allowed(role, classification)` is checked. Denied nodes are dropped
  before subgraph expansion is rendered, so a denied table cannot leak through
  its columns or sampled values.
- Default-deny: a node whose classification cannot be resolved is dropped, and
  if the role is unknown the bundle is empty.

The decision is a dictionary lookup, so per-query cost is negligible.

## Possible future extensions

Not built for the demo. Listed so the path is known.

- **Tier 2 authoritative gate.** Spark writes to a Neo4j staging label, then
  `n10s.validation.shacl.validate()` runs inside Neo4j against the staged
  property graph, and staging is promoted to live only on a clean result. This
  is the full-fidelity SHACL check covering shapes the PyDeequ subset cannot
  (property paths, `sh:sparql`, nested `sh:and/or/not/xone`, recursion,
  `sh:qualifiedValueShape`). No SHACL engine to write; n10s is the engine.
- **Generated `.ttl`-to-PyDeequ translator.** Replace the hand-authored checks
  with a translator over the supported SHACL subset, once the contract grows
  enough that hand-maintaining checks is the bottleneck. Estimated at a few
  days to one or two weeks for one focused engineer.
- **SQL routing gate.** Validate every table referenced in generated SQL
  against the same PDP before execution, as a second query-side check in front
  of the executor.
- **Richer classification sourcing.** Derive classification from Unity Catalog
  tags at ingest as the authoritative source, reducing policy drift.

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
- **This is not data security.** UC still enforces the data read. This layer
  enforces what metadata is visible and what SQL is allowed to be routed.

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
vocabulary only, SHACL as the ingest contract, and a plain in-memory lookup for
the runtime access decision.

**The drift caveat is his objection to a parallel policy.** The series'
investment argument (session 26) is that an explicit semantic description "can
be used to validate the quality of your data ... to dynamically generate data
ingest pipelines ... to improve the quality of LLM responses," and that
maintaining two representations of the same knowledge risks them drifting
apart. This is exactly the drift risk flagged in the caveats above. It
reinforces the recommendation to derive classification from one authoritative
source rather than hand-maintaining a matrix parallel to UC grants.

Net: he would agree with the OWL/SHACL schema enforcement at ingest without
reservation, and with the define-once, logic-in-the-data architecture. His only
friction is the two caveats this proposal already carries.

## Phased plan

The plan is scoped to the basic demo. The future extensions above are out of
scope until the demo proves the shape.

1. **Vocabulary and contract.** Author the `.ttl`: OWL vocabulary, the access
   matrix, and the SHACL shapes (structural plus "must be classified"). Add the
   `dbxcarta.policy` leaf module with the rdflib loader and the in-memory PDP.
   No behavior change yet.
2. **Ingest gate.** Author the PyDeequ checks from the SHACL shapes, running
   distributed in the Spark pipeline alongside the existing verification with
   no `collect`. Add classification derivation from UC tags. Add a unit test
   asserting every SHACL shape has a corresponding PyDeequ check.
3. **Query gate.** Thread role into `Retriever.retrieve`, load the access
   matrix from the `.ttl` into the PDP dictionary, add the classification
   filter to `GraphRetriever`, default-deny. Tests for allow, deny,
   unclassified, and unknown role.

## Open questions

1. Where does the caller role enter the client at runtime: an explicit
   parameter on the retrieve call, request-scoped context, or settings?
2. Initial Classification and Role sets, and the starting `roleMayAccess`
   matrix.
3. Should the query gate hard-fail the whole bundle on any denied node, or
   silently drop denied nodes and answer from what remains? Default-deny per
   node is assumed above; confirm.

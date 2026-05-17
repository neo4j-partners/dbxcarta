# The Enterprise Ontology Race, and Where dbxcarta Fits

Summary and analysis of Pankaj Kumar, *"Google vs Microsoft vs Palantir: The
Enterprise Ontology Race, And the Layer All Three Are Missing"* (Medium, May 3,
2026).

## What the article argues

Three vendors are racing to own the semantic layer that grounds enterprise AI
agents in meaning rather than raw data:

- **Google Knowledge Catalog** (formerly Dataplex), plus the Enterprise
  Knowledge Graph and Smart Storage. Auto-generates semantics with Gemini,
  builds a context graph across BigQuery and federated SaaS, exposes context to
  agents through MCP tools and "golden queries." Its open standard is
  schema.org plus RDF/JSON-LD.
- **Microsoft Fabric IQ.** Evolves Power BI semantic models into ontology
  items with typed relationships and permitted actions. Proprietary format,
  DirectLake-bound.
- **Palantir Foundry/AIP.** A decade-old object/action-type ontology with
  mature write-back governance and approval chains. Fully proprietary, no
  OWL/SHACL/RDF export, high compute cost and lock-in.

The author scores them on ontology foundation, agent grounding, lock-in, and
standards alignment. Google is the most open on data standards and the only one
shipping an RDF/JSON-LD path. Microsoft has the strongest agent permission
model. Palantir has the deepest write-back governance.

## The missing layer

All three solve the **retrieval** side of the semantic layer: they tag
entities, map relationships, and feed context to agents at inference time. None
of them solve the **enforcement** side.

Specifically, none ship a formal constraint layer:

- schema.org has no mechanism for OWL class hierarchies or SHACL shape
  constraints. Google can describe a `ComplianceHold` but cannot formally
  forbid releasing one without a Legal approver.
- Microsoft's permitted actions are enforced at the Fabric boundary. When a
  Fabric agent delegates to a Snowflake agent over A2A, that enforcement is
  invisible to the other side.
- Palantir enforces governance well, but only inside Foundry. The governance
  chain breaks the moment a Foundry agent talks to a non-Foundry agent.

The pattern is **platform-internal enforcement, cross-platform gap**. The
missing layer is a **vendor-neutral OWL/SHACL semantic contract that sits
between platforms, not inside any one of them**: business rules expressed once
in OWL plus SHACL, validated before any platform adapter is invoked, so the
same constraint blocks an invalid entity whether it routes to Google,
Microsoft, or Palantir. The platforms become delivery adapters; the ontology is
the governance layer you own. The author's reference implementation is the
open-source OWL Portability Layer.

## How dbxcarta stacks up

dbxcarta is not an enterprise ontology platform and does not try to be. It
builds a metadata knowledge graph in Neo4j from Unity Catalog and serves it as
a schema-context layer for GraphRAG and Text2SQL: embed a question, similarity-
search the schema nodes, expand the subgraph over typed relationships, hand the
LLM the schema it needs before it writes SQL. The working scope is on the order
of 500 tables, with a query-rating feedback loop tuning retrieval quality.

Mapped onto the article's framing:

**Which layer it plays at.** dbxcarta is squarely a **retrieval / grounding**
tool, the same layer Google, Microsoft, and Palantir are racing for, but
scoped to one workload (Text2SQL schema grounding over Unity Catalog) rather
than a general enterprise context engine. It is closest in spirit to Google
Knowledge Catalog's "ground the agent in semantic context at query time," just
narrower and self-hosted.

**Ontology foundation.** dbxcarta's graph is a typed property graph
(`Database`, `Schema`, `Table`, `Column`, `Value` with `REFERENCES` edges and
embedding vectors). That is a pragmatic schema model, not a formal ontology.
Like all three vendors in the article, it has **no OWL class hierarchy and no
SHACL constraints**. It cannot express or enforce `ComplianceHold subClassOf
HoldStatus` or "a hold needs a Legal approver." It was never meant to; its job
is to retrieve the right schema, not to govern business rules.

**The missing layer is missing here too, and that is fine.** dbxcarta has the
same enforcement gap the article identifies, because it lives on the retrieval
tier by design. Its correctness mechanism is **empirical and learned** (the
query-rating feedback loop improves which schema gets retrieved), not
**formal and constraint-based** (SHACL shapes that block semantically valid but
contextually catastrophic actions). These are complementary, not competing. If
a dbxcarta-grounded agent ever needed cross-platform business-rule enforcement,
that would be a separate OWL/SHACL layer above dbxcarta, exactly the
independent layer the article argues every enterprise should own regardless of
vendor.

**Lock-in profile.** This is where dbxcarta scores best against the article's
matrix. It has no proprietary ontology product to be hostage to. The graph
contract is open and typed, storage is Neo4j, ingest is Databricks/Unity
Catalog. There is real gravity (Unity Catalog on the ingest side, Neo4j on the
storage side), comparable in character to the article's "Google lock-in is
gravitational, not structural" point, but there is no Foundry-style structural
lock-in: no Action Types or OSDK with no export format. The semantic contract
is a documented graph schema, not an opaque vendor object model.

### Where dbxcarta would land on the article's table

| Dimension | dbxcarta |
|---|---|
| Layer played | Retrieval / schema grounding (workload-scoped Text2SQL), not enterprise ontology |
| Ontology foundation | Typed Neo4j property graph + embeddings; no OWL, no SHACL |
| Agent grounding | Dynamic: similarity search + graph expansion feeds schema context at query time, exposed via MCP |
| Standards alignment | Open graph contract, but no RDF/OWL/SHACL; not a standards play |
| Lock-in | Low/structural-free; gravitational toward Unity Catalog + Neo4j |
| Correctness model | Empirical (query-rating feedback loop), not formal constraint enforcement |

### Honest verdict

dbxcarta competes well on the dimension it targets: cheap, portable,
feedback-tuned schema grounding for Text2SQL over a mid-size Unity Catalog
estate, without buying into a proprietary ontology stack. It does **not**
address the layer the article says all three giants are missing, the formal
cross-platform OWL/SHACL enforcement layer, and it should not be evaluated as
if it did. If an enterprise used dbxcarta to ground agents and also needed
contextual-rule enforcement, the article's recommendation still holds: build
that constraint layer once, above dbxcarta, and own it independently of any
platform.

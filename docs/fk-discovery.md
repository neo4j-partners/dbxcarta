# FK Discovery and Join Inference

## Overview

Databricks Genie is the platform-native path from natural language to SQL, and it works well inside its design envelope: a curated space of up to 30 tables with hand-tuned example queries, join relationships, and SQL expressions. That envelope is where this system does not belong. **DBxCarta exists for the case where the catalog has hundreds or thousands of tables and no one has decided in advance which 30 belong together.** At that scale, the Genie pattern of curating a focused space per use case does not survive first contact with the catalog, and the Unity Catalog information schema on its own does not close the gap either.

Unity Catalog tracks declared primary-key and foreign-key constraints, but those constraints are informational only and, in most production catalogs, are absent entirely. Engineers know which columns join to which, but that knowledge lives in notebooks, tribal memory, and query history, not in the metadata layer. A retrieval system that needs to answer questions across a thousand tables cannot wait for those declarations to be written.

DBxCarta closes that gap by inferring join relationships from three independent signals: column naming conventions, data type compatibility paired with primary-key evidence, and semantic similarity between column descriptions. Each inferred relationship is stored with a confidence score and a record of which method discovered it. At query time, the graph retriever traverses those relationships to expand the set of tables relevant to a question and, where available, injects the literal join predicate directly into the prompt sent to the SQL-generation model.

The result is that an LLM working against a catalog of a thousand tables can receive a focused, join-aware schema context rather than a generic dump of column names, without any manual annotation of the catalog and without pre-committing to a 30-table subset.

---

## Core Features

- **Multi-signal inference.** Foreign key relationships are discovered through three distinct methods: declared FK constraints from Unity Catalog, naming-convention analysis paired with primary-key evidence, and cosine similarity between pre-computed column embeddings. Each method runs independently and produces its own edges in the graph.

- **Confidence scoring.** Every inferred relationship carries a numeric confidence score between 0.0 and 1.0. Declared FK constraints score 1.0. Metadata-inferred relationships score between 0.78 and 0.95 depending on the quality of the evidence. Semantically inferred relationships score between 0.80 and 0.90, with a boost for pairs where actual column values overlap. At query time, a threshold filter controls which relationships are traversed.

- **Provenance tagging.** Each relationship records how it was discovered: "declared" for Unity Catalog constraints, "inferred\_metadata" for name and type-based inference, and "semantic" for embedding-based inference. This makes it possible to measure the contribution of each signal independently and to debug unexpected join suggestions.

- **Primary-key gating.** Metadata inference only proposes a join if the target column has evidence of being a primary key. That evidence can come from a declared primary key constraint, a declared unique constraint, or a naming heuristic for catalogs with no formal constraints at all. Without this gate, a column named `user_id` would match every `id` column in the catalog.

- **Tie-break attenuation.** When a source column produces multiple plausible join targets above the confidence threshold, each candidate's score is attenuated in proportion to the number of competing matches. A source column with nine high-scoring targets sees each score reduced by roughly a third, typically dropping ambiguous matches below the retrieval threshold while leaving well-constrained two-way joins intact.

- **Value-overlap corroboration.** For semantically inferred relationships, the system checks whether actual sampled values from the source column appear in the target column's sampled values. Pairs where at least half the source values are present in the target receive a confidence boost and are tagged as value-corroborated. This evidence is stronger than naming or embedding alone.

- **Literal join predicates.** Where a declared FK constraint provides the join condition explicitly, that predicate is stored on the relationship edge. At query time, the retriever can pull the exact predicate and inject it into the model prompt, so the model sees `orders.customer_id = customers.id` rather than only knowing that the two tables are related.

- **Graph-based join traversal.** The query-time retriever uses the graph to expand the schema context around a user's question. Starting from the columns most semantically similar to the question, it walks FK edges in both directions to collect related tables. Only edges above the confidence threshold are traversed. The expanded set of tables, their columns, and any relevant join predicates are assembled into the context passed to the model.

- **Scale to large catalogs.** Inference runs on the driver without needing distributed compute. Aggressive early filtering on type compatibility and PK-likeness reduces the working set from a full Cartesian product of column pairs to a small fraction of that space. A catalog with a thousand tables and ten primary-key-like columns per table produces a tractable number of candidate pairs rather than a combinatorial explosion.

- **Observable run summaries.** Each pipeline run produces a structured summary that counts candidates considered, edges accepted, and rejections broken down by reason: name mismatch, type incompatibility, no PK evidence, tie-break attenuation, and sub-threshold score. This makes it straightforward to understand why a particular join was not discovered and to tune thresholds if needed.

---

## What This Adds Beyond Genie and Unity Catalog

The right comparison is not against the Unity Catalog information schema alone. Genie already reads that schema, auto-imports declared PK/FK constraints as join relationships, and layers its own knowledge store on top. The meaningful question is what DBxCarta adds beyond a well-curated Genie space backed by the knowledge store and knowledge mining.

| Capability | UC information schema | Genie space + knowledge store | DBxCarta |
|---|---|---|---|
| FK discovery | Declared constraints only | Declared constraints auto-imported; new joins suggested from thumbs-up feedback | Declared plus three inferred signals, ranked by confidence, working at day zero |
| Scale target | N/A | 30 tables per space (hard cap), 200 knowledge-store snippets | Thousands of tables in a single graph |
| Cold start | Authoritative for what exists | Knowledge mining needs user feedback traffic to learn | Produces candidate joins before any conversations occur |
| Provenance and confidence | Not available | Not surfaced | Per-edge score and source tag, threshold-filterable |
| Literal join predicates | Not available for inferred joins | Stored when authored by a human | Stored on edge where derivable |
| Sampled column values | Not available | Entity matching stores up to 1,024 values per column for up to 120 columns | Stored and used for value-overlap corroboration on inferred edges |
| Semantic similarity across columns | Not available | Not available for join discovery | Pre-computed embeddings on all schema nodes |
| Portability | System table queries | Scoped to the Genie space | Graph is consumable by any LLM pipeline, not only Genie |
| Auditability | N/A | Author sees authored snippets; inference steps not exposed | Run summaries with candidates, acceptances, and rejection reasons |

The practical consequence is that a Genie space is the right tool when you can commit in advance to 30 tables and you have the time and expertise to curate them. DBxCarta is the right tool when you cannot.

---

## How the Inference Pipeline Works

The pipeline runs in three phases after extracting Unity Catalog metadata.

**Metadata inference** analyzes every pair of columns in the catalog where one column plausibly references another. It applies three gates in sequence. First, the column names must match either exactly across tables or through a suffix pattern where one column ends in `_id`, `_fk`, or `_ref` and the other is the corresponding identifier column for that table. Second, the types must be compatible, with normalization that treats integer variants as equivalent and decimal types as compatible if their scale matches. Third, the target column must have primary-key evidence. Only pairs that pass all three gates receive a score. A soft signal from shared vocabulary in column comments can raise a score slightly, but its absence does not disqualify a pair.

**Semantic inference** operates on column pairs that passed the type and primary-key gates but were not matched by name. It computes cosine similarity between the pre-computed embeddings of the two columns' names and descriptions. Pairs above the similarity threshold receive a baseline confidence score. That score rises if the sampled values from the two columns overlap substantially. Pairs that do not reach the final confidence threshold are discarded entirely.

**Graph storage** writes every accepted relationship into Neo4j with its confidence score, source tag, and join predicate if available. Declared FK edges are written first. Metadata-inferred and semantically inferred edges are written after, with deduplication ensuring that a relationship already covered by a declared FK is not duplicated. All writes use merge semantics, so the pipeline is safe to re-run.

At query time, the graph retriever seeds from vector similarity search, walks FK edges above the confidence threshold, collects the parent tables of all reachable columns, fetches their full column lists and sample values, and assembles that context alongside any stored join predicates before calling the SQL-generation model.

---

## Relationship to the Genie Knowledge Store

Several DBxCarta capabilities overlap with features that Genie already provides inside a space, and it is worth being precise about where the overlap is and where it is not.

**Declared FK ingestion.** Genie automatically saves primary and foreign key constraints defined in Unity Catalog as join relationships in the space. DBxCarta's "declared" provenance tag covers the same ground. Inside a Genie workflow, this part of DBxCarta is strictly redundant.

**Sampled values on columns.** Genie's entity matching stores distinct values from eligible string columns (up to 1,024 values per column, up to 120 columns per space) and uses them to reconcile user phrasing with actual data. DBxCarta's value-overlap corroboration is conceptually similar: it uses sampled values to validate inferred joins. The use cases diverge (prompt matching vs. join validation), but the underlying signal is the same.

**Proactive suggestions.** Genie's knowledge mining proposes new joins and SQL expressions by analyzing thumbs-up responses and downloaded results, and auto-suggests example queries from popular workspace query history. DBxCarta's three-signal inference targets similar outcomes but works before any user traffic exists.

## Where DBxCarta Extends the Platform

Once the overlaps are accounted for, the parts that remain are what justify the system:

- **Scale past the 30-table Genie cap.** This is the single most important differentiator. Genie spaces are intentionally small and curated. If the retrieval pattern is "pick the right 20 tables out of 1,000 per question," the platform does not provide a native answer.
- **Cold-start discovery.** Genie's knowledge mining is reactive and needs conversational signal to learn. On a new catalog with no query traffic, it produces nothing. DBxCarta works on day zero.
- **Provenance, confidence scores, and run summaries.** Genie does not surface its internal reasoning about suggested joins. DBxCarta exposes per-edge scores, source tags, and rejection reasons, which matter when the output feeds a downstream pipeline that needs auditability.
- **Portability beyond Genie.** The graph is a first-class artifact. Custom agents, multi-agent systems, external BI tools, or non-Databricks consumers can query it without being constrained by a Genie space boundary.
- **Cross-catalog and cross-schema discovery.** A Genie space is scoped to a curated asset list. There is no platform feature that mines FK relationships across an entire Unity Catalog.

## Design Complexity: What Is Load-Bearing and What Is Not

Not every piece of the pipeline is equally necessary. An honest assessment:

- **Multi-gate sequencing with tie-break attenuation.** This solves a real ambiguity problem — a column named `user_id` will naively match every `id` column in the catalog — but the current machinery is elaborate. A hard top-N cap per source column covers most of the same ground with less moving parts and is easier to explain to a reader of the run summary.
- **Semantic inference on column descriptions.** Works where descriptions exist. In most production catalogs they do not, at which point this signal degrades to embedding similarity on column names, which is a noisier cousin of the naming-convention rule running alongside it. Worth keeping, worth not overweighting.
- **Neo4j as a separate dependency.** Justifiable only if multi-hop traversal with confidence filtering is core to retrieval. If the typical query walks one or two hops, a combination of Delta tables plus Databricks Vector Search could host the same data with one fewer system to operate. The current choice is defensible but should be revisited as retrieval patterns stabilize.
- **Value-overlap corroboration.** The signal is strong, but if the catalog is already using Genie's entity matching, some of this data exists elsewhere. Worth sharing a sampling strategy across both paths rather than maintaining two independent sample stores.

The parts that are clearly load-bearing: the naming-convention plus PK-evidence gate, the confidence score per edge, the provenance tag, and the run summary. Those are the features that make the output trustworthy enough to feed an LLM prompt automatically.

## When to Use This vs. a Curated Genie Space

A rough decision rule:

- **Use a curated Genie space** when the question domain is scoped, the relevant tables number in the tens, and a data team can invest in example queries and join definitions. This is the path Databricks recommends and it will produce better answers than automated inference for any well-defined use case.
- **Use DBxCarta** when the catalog is large enough that pre-committing to 30 tables is not viable, when the consumer is an LLM pipeline outside Genie, when cold-start coverage matters more than peak accuracy on a narrow domain, or when downstream systems need confidence-scored, auditable relationship metadata.

The two are not mutually exclusive. A reasonable operating model is to use DBxCarta for catalog-wide discovery and retrieval, and to promote high-value, high-traffic subsets into curated Genie spaces once the question shape has stabilized.

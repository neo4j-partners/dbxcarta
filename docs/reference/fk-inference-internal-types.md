# FK-Inference Internal Types

Documents the typed dataclass layer that sits between Spark DataFrames and the
FK discovery pipeline, and identifies the single boundary-conversion point for
each type transition.

## Layer Map

```
Spark DataFrames (information_schema.*)
        │
        │ .collect() → Row[dict-like]
        ▼
┌────────────────────────────────────────────────────────────┐
│ ingest.extract.extract (pure UC pull)                      │
│ ingest.fk.discovery.run_fk_discovery (pipeline edge)       │
│                                                            │
│   columns_df      → [ColumnMeta.from_row(r), ...]          │
│   pk_rows_df      → [ConstraintRow.from_row(r), ...]       │
│   declared edges  → frozenset[DeclaredPair]                │
│   column_node_df  → dict[col_id, ColumnEmbedding]          │
│   value_node_df⋈has_value_df → ValueIndex                  │
│                                                            │
│ No dict-key access past this point.                        │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│ ingest.fk.metadata.infer_fk_pairs (Phase 3, pure Python)   │
│ ingest.fk.semantic.infer_semantic_pairs (Phase 4, pure)    │
│                                                            │
│   Attribute access only. Enums for name-match, PK-         │
│   evidence, rejection-reason, score bucket, edge source.   │
│   Pure-Python cosine in ingest.fk.semantic.                │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│ ingest.schema_graph.build_references_rel                   │
│                                                            │
│   list[FKEdge] → Spark DataFrame (REFERENCES schema)       │
│                                                            │
│ Single FKEdge → tuple conversion. EdgeSource.value         │
│ serialization is here. No magic strings downstream.        │
└────────────────────────────────────────────────────────────┘
        │
        ▼
Spark DataFrame → Neo4j Spark Connector
```

Run-summary counters flatten to `RunSummary.row_counts` via each counter
class's serialization method. The flattening happens inside
`RunSummary.to_dict()`, the sole point where nominal enum keys become prefixed
string keys. Delta table column `row_counts MAP<STRING, BIGINT>` forces the
string shape on storage, but nothing upstream of the write boundary touches a
stringly-typed counter.

## File Layout

```
packages/dbxcarta-core/src/dbxcarta/core/
  contract.py                NodeLabel, RelType, EdgeSource; identifier generation

packages/dbxcarta-spark/src/dbxcarta/spark/
  settings.py                SparkIngestSettings + cross-field validators
  run.py                     run_dbxcarta + _run orchestrator
  ingest/
    extract.py               ExtractResult + extract() — pure UC pull
    preflight.py             Pre-run checks (catalog, volume, endpoint)
    summary.py               RunSummary + nominal sub-counter dataclasses
    schema_graph.py          DataFrame builders (nodes and relationships)
    fk/
      common.py              ColumnMeta, ConstraintRow, DeclaredPair, FKEdge,
                             PKIndex, PKEvidence, pk_kind, types_compatible,
                             canonicalize, build_id_cols_index, _TYPE_EQUIV
      declared.py            Catalog-declared FK discovery
      metadata.py            Phase 3 metadata inference
      semantic.py            Phase 4 semantic inference
      discovery.py           Declared → metadata → semantic FK orchestrator
    transform/
      embeddings.py          ai_query transform
      sample_values.py       Distinct-value sampling
      staging.py             Path resolution + Delta staging lifecycle
      ledger.py              Re-embedding ledger I/O
    load/
      neo4j_io.py            bootstrap/purge/query/load wrappers typed with enums
      writer.py              Neo4j Spark Connector wrappers
```

## Types

| Type | Frozen | Owner module | Purpose |
|---|---|---|---|
| `NodeLabel` | StrEnum | `contract` | Neo4j node labels |
| `RelType` | StrEnum | `contract` | Neo4j relationship types |
| `EdgeSource` | StrEnum | `contract` | `REFERENCES.source` provenance tag |
| `ColumnMeta` | yes | `ingest.fk.common` | Driver-side column metadata row |
| `ConstraintRow` | yes | `ingest.fk.common` | `information_schema.key_column_usage` row |
| `DeclaredPair` | yes | `ingest.fk.common` | `(source_id, target_id)` suppression-set member |
| `FKEdge` | yes | `ingest.fk.common` | Emitted REFERENCES edge |
| `PKIndex` | yes | `ingest.fk.common` | Declared PK/UNIQUE index + composite-count |
| `PKEvidence` | enum | `ingest.fk.common` | DECLARED_PK / UNIQUE_OR_HEUR |
| `NameMatchKind` | enum | `ingest.fk.metadata` | EXACT / SUFFIX |
| `RejectionReason` | enum | `ingest.fk.metadata` | Phase 3 rejection tags |
| `ScoreBucket` | enum | `ingest.fk.metadata` | Phase 3 score buckets |
| `InferenceCounters` | no | `ingest.fk.metadata` | Phase 3 mutable counter aggregate |
| `ColumnEmbedding` | yes | `ingest.fk.semantic` | col_id + vector + cached norm |
| `ValueIndex` | yes | `ingest.fk.semantic` | col_id → frozenset[sampled value-text] |
| `SemanticRejectionReason` | enum | `ingest.fk.semantic` | Phase 4 rejection tags |
| `SemanticInferenceCounters` | no | `ingest.fk.semantic` | Phase 4 mutable counter aggregate |
| `ExtractResult` | no | `ingest.extract` | Raw UC DataFrames post-pull |
| `FKDiscoveryResult` | no | `ingest.fk.discovery` | Declared, metadata, and semantic DataFrames + counts |
| `ExtractCounts` | no | `ingest.summary` | Per-catalog row counts |
| `SampleValueCounts` | no | `ingest.summary` | Sample-values stats, flattens to row_counts |
| `EmbeddingCounts` | no | `ingest.summary` | Per-NodeLabel embedding bookkeeping |
| `RunSummary` | no | `ingest.summary` | Top-level aggregate; `to_dict` flattens to wire shape |

## Invariants

- **Counter invariants.** After Phase 3,
  `fk_metadata.candidates == fk_metadata.accepted + Σ rejections`. After
  Phase 4, `fk_semantic.considered == fk_semantic.accepted + Σ rejections`.
  `fk_semantic.value_corroborated` is an independent overlay, not a rejection.

- **Immutability.** `ColumnMeta`, `ConstraintRow`, `DeclaredPair`, `FKEdge`,
  `PKIndex`, `ColumnEmbedding`, and `ValueIndex` are all
  `frozen=True, slots=True`. Immutability is load-bearing for
  `frozenset[DeclaredPair]` hashing.

- **PK-evidence purity.** `PKIndex` holds only what the DBA declared
  (PRIMARY KEY + leftmost UNIQUE). Name-based fallback (`id`, `{table}_id`)
  lives in the free function `build_id_cols_index` consumed by `pk_kind`.

- **Boundary conversions are single-source.** Spark Row → dataclass happens
  once per type at `*.from_row` classmethods called from `ingest.extract` or
  `ingest.fk.discovery`. Dataclass → Spark tuple happens once in
  `ingest.schema_graph.build_references_rel`. Enum → string goes through
  `.value` at the serialization site.

- **Magic strings are eliminated in the FK surface.** Labels, rel types, edge
  sources, and embedding-text label keys are all `StrEnum` members.

## Settings Cross-Field Validation

`Settings.model_validator(mode="after")` rejects known-incoherent
configurations at construction:

- `DBXCARTA_INFER_SEMANTIC=true` with
  `DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=false`. Phase 4 needs column embeddings.
- `DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=true` with
  `DBXCARTA_INCLUDE_VALUES=false`. Value embeddings need Value nodes to embed.

## PEP-8 / Python-Focus Compliance

- Pydantic is used at the env-var boundary (`Settings`) only. Internal DTOs
  are `@dataclass`.
- Databricks identifiers are validated at the env-var boundary; hyphens are
  allowed only because every dynamic identifier is backtick-quoted before SQL.
- `dbxcarta_summary_table` must be an explicit `catalog.schema.table` target.
- Exception catches are narrowed across the FK-touching surface. One deliberate
  `except Exception` remains in `ingest.pipeline.run_dbxcarta`: the top-level
  catch-all records any failure into `RunSummary.error` before re-raising.

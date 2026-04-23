# FK-inference internal types

Deliverable from `worklog/fk-gap-v3-build.md` Phases 3.5 and 3.6. Documents
the nominal dataclass layout introduced by the foundation rewrite, and
pinpoints the one place each boundary conversion happens.

## Layer map

```
Spark DataFrames (information_schema.*)
        │
        │ .collect() → Row[dict-like]
        ▼
┌────────────────────────────────────────────────────────────┐
│ extract.extract (pure UC pull)                             │
│ inference.run_inferences (pipeline edge, both FK phases)   │
│                                                            │
│   columns_df      → [ColumnMeta.from_row(r), ...]          │
│   pk_rows_df      → [ConstraintRow.from_row(r), ...]       │
│   fk_pairs_df     → frozenset[DeclaredPair]                │
│   column_node_df  → dict[col_id, ColumnEmbedding]          │
│   value_node_df⋈has_value_df → ValueIndex                  │
│                                                            │
│ No dict-key access past this point.                        │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│ fk_inference.infer_fk_pairs     (Phase 3, pure Python)     │
│ fk_semantic.infer_semantic_pairs (Phase 4, pure Python)    │
│                                                            │
│   Attribute access only. Enums for name-match, PK-         │
│   evidence, rejection-reason, score bucket, edge source.   │
│   Pure-Python cosine in fk_semantic (no numpy dep).        │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│ schema_graph.build_inferred_references_rel                 │
│                                                            │
│   list[InferredRef] → Spark DataFrame (5-col schema)       │
│                                                            │
│ Single InferredRef → tuple conversion. EdgeSource.value    │
│ serialization is here — no magic strings downstream.       │
└────────────────────────────────────────────────────────────┘
        │
        ▼
Spark DataFrame → Neo4j Spark Connector
```

Run-summary counters flatten to `RunSummary.row_counts` (on the wire) via
each counter class's `as_summary_dict(prefix)`. The flattening happens
inside `RunSummary.to_dict()` — **the sole point** where nominal enum
keys become prefixed string keys. Delta table column
`row_counts MAP<STRING, BIGINT>` forces the string shape on storage, but
nothing upstream of the write boundary touches a stringly-typed counter.

## File layout (post-Phase-3.6)

```
src/dbxcarta/
  contract.py      NodeLabel, RelType, EdgeSource (StrEnum); generate_id
  fk_common.py     ColumnMeta, ConstraintRow, DeclaredPair, InferredRef,
                   PKIndex, PKEvidence, pk_kind, types_compatible,
                   canonicalize, build_id_cols_index, _TYPE_EQUIV
  fk_inference.py  Phase 3 metadata: NameMatchKind, RejectionReason,
                   ScoreBucket, _SCORE_TABLE, InferenceCounters,
                   infer_fk_pairs
  fk_semantic.py   Phase 4 semantic: ColumnEmbedding, ValueIndex,
                   SemanticRejectionReason, SemanticInferenceCounters,
                   infer_semantic_pairs
  extract.py       ExtractResult + extract() — pure UC pull
  inference.py     InferenceResult + run_inferences() — Phase 3 + Phase 4
  settings.py      Settings (pydantic) + cross-field validators
  staging.py       Path resolution + Delta staging lifecycle
  ledger.py        Re-embedding ledger I/O
  neo4j_io.py      bootstrap/purge/query/load wrappers typed with enums
  preflight.py     Pre-run checks (catalog, volume, endpoint)
  pipeline.py      run_dbxcarta + _run orchestrator (thin)
  embeddings.py    ai_query transform
  sample_values.py Distinct-value sampling
  summary.py       RunSummary + nominal sub-counter dataclasses
  schema_graph.py  DataFrame builders (nodes, rels, inferred refs)
  writer.py        Neo4j Spark Connector wrappers
  contract.py      Identifier generation + enums
```

## Types

| Type | Frozen | Owner module | Purpose |
|---|---|---|---|
| `NodeLabel` | StrEnum | `contract` | Neo4j node labels |
| `RelType` | StrEnum | `contract` | Neo4j relationship types |
| `EdgeSource` | StrEnum | `contract` | `REFERENCES.source` provenance tag |
| `ColumnMeta` | yes | `fk_common` | Driver-side column metadata row |
| `ConstraintRow` | yes | `fk_common` | `information_schema.key_column_usage` row |
| `DeclaredPair` | yes | `fk_common` | (source_id, target_id) suppression-set member |
| `InferredRef` | yes | `fk_common` | Emitted REFERENCES edge (source: EdgeSource) |
| `PKIndex` | yes | `fk_common` | Declared PK/UNIQUE index + composite-count |
| `PKEvidence` | enum | `fk_common` | DECLARED_PK / UNIQUE_OR_HEUR |
| `NameMatchKind` | enum | `fk_inference` | EXACT / SUFFIX |
| `RejectionReason` | enum | `fk_inference` | Phase 3 rejection tags |
| `ScoreBucket` | enum | `fk_inference` | Phase 3 score buckets |
| `InferenceCounters` | no | `fk_inference` | Phase 3 mutable counter aggregate |
| `ColumnEmbedding` | yes | `fk_semantic` | col_id + vector + cached norm |
| `ValueIndex` | yes | `fk_semantic` | col_id → frozenset[sampled value-text] |
| `SemanticRejectionReason` | enum | `fk_semantic` | Phase 4 rejection tags |
| `SemanticInferenceCounters` | no | `fk_semantic` | Phase 4 mutable counter aggregate |
| `ExtractResult` | no | `extract` | Raw UC DataFrames post-pull |
| `InferenceResult` | no | `inference` | Phase 3 + Phase 4 DataFrames + counts |
| `ExtractCounts` | no | `summary` | Per-catalog row counts |
| `SampleValueCounts` | no | `summary` | Sample-values stats, flattens to row_counts |
| `EmbeddingCounts` | no | `summary` | Per-NodeLabel embedding bookkeeping |
| `RunSummary` | no | `summary` | Top-level aggregate; `to_dict` flattens to wire shape |

## Invariants

- **Counter invariants.** After Phase 3,
  `fk_metadata.candidates == fk_metadata.accepted + Σ rejections`.
  After Phase 4,
  `fk_semantic.considered == fk_semantic.accepted + Σ rejections`
  (and `fk_semantic.value_corroborated` is an independent overlay,
  not a rejection). Enforced by counter-invariant tests in both test
  suites.

- **Immutability.** `ColumnMeta`, `ConstraintRow`, `DeclaredPair`,
  `InferredRef`, `PKIndex`, `ColumnEmbedding`, `ValueIndex` are all
  `frozen=True, slots=True`. Enforced by `test_*_is_frozen` cases.
  Immutability is load-bearing for `frozenset[DeclaredPair]` hashing.

- **PK-evidence purity.** `PKIndex` holds only what the DBA declared
  (PRIMARY KEY + leftmost UNIQUE). Name-based fallback (`id`,
  `{table}_id`) lives in the free function `build_id_cols_index`
  consumed by `pk_kind`. Intent: do not entangle declared evidence with
  inferred-from-column-names evidence inside a single type.

- **Boundary conversions are single-source.** Spark Row → dataclass
  happens once per type at `*.from_row` classmethods called exclusively
  from `extract.py` / `inference.py`. Dataclass → Spark tuple happens
  once in `schema_graph.build_inferred_references_rel`. Enum → string
  (for both Cypher interpolation and DataFrame literals) goes through
  `.value` at the single serialization site.

- **Magic strings are eliminated in the FK surface.** Labels, rel types,
  edge sources, and embedding-text label keys are all `StrEnum` members;
  never module-level string constants. Grep `LABEL_|REL_HAS|REL_REFERENCES`
  in `src/dbxcarta/` should return zero hits.

## Settings cross-field validation

`Settings.model_validator(mode="after")` rejects two known-incoherent
configurations at construction (not halfway through a run):

- `DBXCARTA_INFER_SEMANTIC=true` with
  `DBXCARTA_INCLUDE_EMBEDDINGS_COLUMNS=false` — Phase 4 needs column
  embeddings. Default: both flags `false`; enable together to use
  Phase 4.
- `DBXCARTA_INCLUDE_EMBEDDINGS_VALUES=true` with
  `DBXCARTA_INCLUDE_VALUES=false` — Value embeddings need Value nodes
  to embed. Previously a runtime warning; Phase 3.6 moves it to the
  boundary.

## PEP-8 / python-focus compliance

- Pydantic is used at the env-var boundary (`Settings`) only. Internal
  DTOs are `@dataclass`, per the skill's decision table.
- `_IDENTIFIER_RE` is strict (`^[a-zA-Z_][a-zA-Z0-9_]*$`); dotted
  `dbxcarta_summary_table` splits on `.` and validates per part.
- Exception catches narrowed across the FK-touching surface:
  - `AnalysisException` for Delta missing-path (`ledger.read_ledger`).
  - `AnalysisException` for Delta merge-target missing (`ledger.upsert_ledger`).
  - `Py4JJavaError` scoped to the `dbutils.fs.ls` probe in
    `staging.truncate_staging_root`.
  - `(AnalysisException, Py4JJavaError)` in `sample_values`'s three
    per-table/per-chunk resilience sites and in `preflight`'s ai_query
    probe.
  - One deliberate `except Exception` remains in `pipeline.run_dbxcarta`
    — the top-level catch-all whose job is to record any failure into
    `RunSummary.error` before re-raising. Narrowing it would silently
    lose novel failure modes. Documented in code.
- mypy-strict config covers the entire domain layer via
  `[[tool.mypy.overrides]]` in `pyproject.toml`.

# FK-inference internal types

Deliverable from worklog/fk-gap-v3-build.md Phase 3.5. Documents the nominal
dataclass layout introduced by the foundation rewrite, and pinpoints the one
place each boundary conversion happens.

## Layer map

```
Spark DataFrames (information_schema.*)
        │
        │ .collect() → Row[dict-like]
        ▼
┌───────────────────────────────────────────────────────┐
│ pipeline._infer_metadata_references (the edge)         │
│                                                       │
│   columns_df       → [ColumnMeta.from_row(r), ...]    │
│   pk_rows_df       → [ConstraintRow.from_row(r), ...] │
│   fk_pairs_df      → frozenset[DeclaredPair]          │
│                                                       │
│ No dict-key access past this point.                   │
└───────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────┐
│ fk_inference.infer_fk_pairs (pure Python)             │
│                                                       │
│   list[ColumnMeta] × PKIndex × frozenset[DeclaredPair]│
│     → (list[InferredRef], InferenceCounters)          │
│                                                       │
│ Attribute access only. Enums for name-match and PK-   │
│ evidence kinds; RejectionReason / ScoreBucket keys    │
│ into the counters maps.                               │
└───────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────┐
│ schema_graph.build_inferred_metadata_references_rel    │
│                                                       │
│   list[InferredRef] → Spark DataFrame (5-col schema)  │
│                                                       │
│ Single InferredRef → tuple conversion point.          │
└───────────────────────────────────────────────────────┘
        │
        ▼
Spark DataFrame → Neo4j Spark Connector
```

Counters flatten to `RunSummary.row_counts` via
`InferenceCounters.as_summary_dict("fk_inferred_metadata")`. This is the
single place where enum keys become prefixed string keys; the Delta-table
column `row_counts MAP<STRING, BIGINT>` forces the string shape and is a
non-goal to change (see Phase 3.5 Non-goals).

## Types

| Type | Frozen | Owner module | Purpose |
|---|---|---|---|
| `ColumnMeta` | yes | `fk_inference` | Driver-side column metadata row |
| `ConstraintRow` | yes | `fk_inference` | `information_schema.key_column_usage` + `table_constraints` row |
| `DeclaredPair` | yes | `fk_inference` | (source_id, target_id) declared-FK suppression set member |
| `InferredRef` | yes | `fk_inference` | Emitted REFERENCES edge, 5-tuple shape |
| `PKIndex` | yes | `fk_inference` | Declared PK/UNIQUE index, composite count |
| `InferenceCounters` | no | `fk_inference` | Mutable aggregate: candidates/accepted/rejections/buckets |
| `NameMatchKind` | enum | `fk_inference` | EXACT / SUFFIX |
| `PKEvidence` | enum | `fk_inference` | DECLARED_PK / UNIQUE_OR_HEUR |
| `RejectionReason` | enum | `fk_inference` | NAME, TYPE, PK, SUB_THRESHOLD, TIE_BREAK, DUPLICATE_DECLARED |
| `ScoreBucket` | enum | `fk_inference` | 0.95 / 0.90 / 0.88 / 0.83 / 0.82 / 0.78 buckets |

## Invariants

- **Counter invariant.** After `infer_fk_pairs` returns,
  `counters.candidates == counters.accepted + sum(counters.rejections.values())`.
  Enforced by `test_counter_invariant_on_v5fk` and
  `test_counter_invariant_on_sub_threshold_fixture`. The previous silent
  `continue` for sub-threshold scores is now accounted for via
  `RejectionReason.SUB_THRESHOLD`.

- **Immutability.** `ColumnMeta`, `ConstraintRow`, `DeclaredPair`,
  `InferredRef`, `PKIndex` are all `frozen=True, slots=True`. Enforced by
  `test_*_is_frozen` cases. Immutability is load-bearing for
  `frozenset[DeclaredPair]` hashing.

- **PK-evidence purity.** `PKIndex` holds only what the DBA declared (PRIMARY
  KEY + leftmost UNIQUE). Name-based fallback (`id`, `{table}_id`) lives in
  the free function `_build_id_cols_index` consumed by `_pk_kind`. Intent: do
  not entangle declared evidence with inferred-from-column-names evidence
  inside a single type.

- **Boundary conversion is single-source.** `ColumnMeta.from_row` /
  `ConstraintRow.from_row` are the only places that rename Spark-Row keys
  (`table_catalog` → `catalog`, `column_name` → `column`). Grep for those
  classmethods before adding any other conversion point.

## PEP-8 / python-focus compliance

Per `docs/PYTHON-FOCUS-SKILL.md`:

- Pydantic is used at env-var boundary (`Settings`) only. Internal DTOs are
  `@dataclass`, per the skill's decision table.
- `_IDENTIFIER_RE` is strict (`^[a-zA-Z_][a-zA-Z0-9_]*$`); dotted
  `dbxcarta_summary_table` is split on `.` and validated per part.
- Exception catches narrowed: `AnalysisException` for Delta-missing-path,
  `Py4JJavaError` scoped to the `dbutils.fs.ls` probe in
  `_truncate_staging_root`. No `# noqa: BLE001` remains.
- Type hints on every public signature in `fk_inference.py`; strict mypy
  config lives under `[[tool.mypy.overrides]]` for the module.

# Fail-Closed Node Writes: One Source of Truth for Per-Label Properties

## Why we are fixing this

The ingest pipeline writes node properties to Neo4j by a denylist. The
node-builder functions emit DataFrames that carry both the graph
contract properties and extra columns that exist only so the embedding
stage can build embedding text. `run.py:_load` then strips those extra
columns per label with `_drop_cols` just before the connector write.
This is fail-open: the Neo4j Spark Connector persists every surviving
DataFrame column as a node property, so any helper column the loader
forgets to drop silently becomes a graph property.

This is not hypothetical. Table nodes currently ship a `table_schema`
property to Neo4j because the Table drop list omits it, even though
`build_table_nodes` documents `table_schema` as embedding-only. The
multi-catalog work made the risk worse by threading a new helper column
(`table_catalog`) through the same path. The defect is a class, not an
instance: every new transform column is one missed drop away from
leaking.

`table_schema` is not the only column reaching the graph today. The
`_drop_cols` calls in `_load` drop only `embedding_error` from the
embedding-side columns, so every embedded node (Database, Schema,
Table, Column, Value) currently also persists `embedding_text_hash`,
`embedding_model`, and `embedded_at` to Neo4j. These are graph
properties today, not near-misses. This refactor removes them from the
graph (see goal 4 and the Phase 1 declared-property decision); their
canonical home is the Delta staging table and the re-embedding ledger,
not the graph.

## What is wrong

The defect is at the write boundary, not in the shape of the
intermediate DataFrame.

1. The write boundary is fail-open. `write_node` persists every column
   on the DataFrame. Correctness depends on a hand-maintained drop
   list in `_load` staying complete. Forget one entry and a transform
   column becomes a permanent graph property with no error.

2. There is no single source of truth for "what is a property of label
   X." That knowledge is split across the builder `.select(...)`, the
   embed-text expressions in `embed_stage.py`, and the `_load` drop
   list, kept in sync by hand.

3. The embedding-text contract is an untyped set of stringly-named
   columns. `_COLUMN_EMBEDDING_TEXT_EXPR` is a SQL string that silently
   depends on `build_column_nodes` having retained exactly the right
   helper columns. A change in one place breaks the other with no type
   or test signal.

The normalization principle underneath all this is correct and stays:
structural membership (which catalog, schema, table a node belongs to)
is represented by edges, not duplicated as scalar properties on child
nodes. The retriever depends on this; it reads `s.name` for the schema
and never `t.table_schema`. Helper columns are transform inputs. They
are harmless as long as the write boundary refuses to persist anything
that is not a declared property.

## New design goals

1. The write boundary is the single place that decides what is
   persisted. It projects to the declared per-label property set before
   the connector write. A stray column cannot become a property because
   only declared properties are ever selected. Transform-only columns
   may ride along on the DataFrame; the boundary strips them.

2. One source of truth. The per-label node property set and the
   per-label embedding-text expression live in one module
   (`contract.py`), consumed by the builder, the embed stage, and the
   writer.

3. Embedding text is computed once, in the builder, as a single
   well-named `embedding_text` column, while the source columns it
   needs are still in scope. No second DataFrame and no join are
   introduced. The embed stage consumes that column instead of
   re-evaluating a SQL string against the node DataFrame.

4. Embedding-pipeline behavior is preserved exactly; the graph
   property set is intentionally narrowed. The embedding text string
   content is unchanged, so `embedding_text_hash` and the re-embedding
   ledger keep their semantics — the ledger lives in its own Delta
   table keyed by `id` + `embedding_text_hash` + `embedding_model` and
   is populated from the Delta staging table, not from Neo4j, so
   removing those columns from the graph does not touch ledger
   correctness. Failure-rate gating, stage-once, disabled-label
   passthrough, and multi-catalog catalog-qualified text all behave as
   before. A full reimport is planned, so transient hash differences
   from any incidental change are not a migration concern, but the
   intent is byte-identical text.

   This is explicitly not a no-op for the graph schema. The fail-closed
   projection removes four properties that are written to Neo4j today:
   the genuinely leaked `table_schema`, plus `embedding_text_hash`,
   `embedding_model`, and `embedded_at` on every embedded node. The
   last three are deliberate removals, not incidental: they are
   bookkeeping that belongs in the staging table and ledger, not on
   graph nodes. The consumer audit covering all four names is complete
   and clean — `packages/dbxcarta-client` has zero references and the
   retriever projects only explicit named properties (full detail in
   the risks section) — so the removal stands as designed with no
   downstream impact.

## Scope

In scope: `TABLE`, `COLUMN`, `SCHEMA`, `DATABASE`. These builders feed
the embed stage; each will attach its own `embedding_text` column and
emit only declared properties.

`VALUE` comes from the sample-value path, not `information_schema`, and
its embedding text is `value`, a real property. It is touched only to
the extent needed to keep the embed stage uniform (read an
`embedding_text` column rather than a per-label SQL string); verify
during Phase 3.

## Current vs proposed flow

Current:

- `build_*_nodes` selects contract properties plus helper columns.
- `embed_stage` holds per-label SQL text expressions that reference the
  helper columns; `add_embedding_column` evaluates the string against
  the node DataFrame.
- `_load` calls `_drop_cols(df, <helpers>, "embedding_error")` per
  label, then `write_node` persists whatever is left.

Proposed:

- `build_*_nodes` computes `embedding_text` inline using the contract
  expression, while the source columns are still in scope, then selects
  only the declared properties plus the single `embedding_text` column.
  No helper columns leave the builder.
- The embed stage reads the existing `embedding_text` column. It no
  longer holds or evaluates per-label SQL text expressions.
- The node write projects to the declared per-label property set from
  `contract.py` before the connector write. `embedding_text`,
  `embedding_error`, and any other staging-only column are dropped
  because they are not declared properties. `_drop_cols` is removed.

## Checklist

### Phase 1: contract single source of truth — COMPLETE
- [x] Add to `contract.py` a per-label declared-property tuple for
      `DATABASE`, `SCHEMA`, `TABLE`, `COLUMN`, `VALUE`: the exact
      columns that may be written to Neo4j, including `id`,
      `contract_version`, and the optional `embedding`. The
      embedding-side decision is explicit: `embedding` is the only
      embedding-side column declared writable. `embedding_text_hash`,
      `embedding_model`, `embedded_at`, and `embedding_error` are NOT
      declared and therefore are projected off the graph. This is a
      deliberate change: `embedding_text_hash`, `embedding_model`, and
      `embedded_at` are persisted to Neo4j today (only `embedding_error`
      is dropped). They remain in the Delta staging table, the
      re-embedding ledger, and the run summary, which are their
      canonical homes; they no longer ride on graph nodes.
- [x] Move the per-label embedding-text expressions
      (`_TABLE_/_COLUMN_/_SCHEMA_/_DATABASE_EMBEDDING_TEXT_EXPR`) from
      `embed_stage.py` into `contract.py` so the builder, embed stage,
      and tests share one definition. The expressions are unchanged in
      content. They reference the derived `name`, `comment`, and the
      raw `information_schema` columns (`table_catalog`, `table_schema`,
      `table_name`, `catalog_name`, `data_type`), all of which are in
      scope inside the builder before its `.select(...)`.
      Implemented as `contract.EMBEDDING_TEXT_EXPR` and
      `contract.NODE_PROPERTIES` dicts keyed by `NodeLabel`, mirroring
      the existing `REFERENCES_PROPERTIES` pattern. `embedding` is the
      last, optional member of each `NODE_PROPERTIES` tuple.

### Phase 2: builders attach embedding_text and emit only properties — COMPLETE
- [x] `build_table_nodes`: add `embedding_text` via the contract TABLE
      expression before the `.select(...)`, then select only the
      declared Table properties plus `embedding_text`. Drop
      `table_catalog` and `table_schema` from the output. Keep `layer`
      (a real, derived property). Update the docstring to state the
      node carries declared properties plus one transient
      `embedding_text` that the write boundary strips.
- [x] `build_column_nodes`: same pattern with the COLUMN expression;
      drop `table_catalog`, `table_schema`, `table_name` from the
      output.
- [x] `build_schema_nodes`: same pattern with the SCHEMA expression;
      drop `catalog_name` from the output.
- [x] `build_database_nodes`: attach `embedding_text` equal to `name`
      so the embed stage is uniform across labels. Output stays
      `id`, `name`, `contract_version`, plus `embedding_text`.
- [x] Value builder (`sample_values.py`, the `sv.sample` path that
      produces `value_node_df` with `id`, `value`, `count`,
      `contract_version`): attach `embedding_text` equal to the `value`
      column so the embed stage is uniform and no longer needs a
      per-label SQL string. This is a required code change, not a
      verification — Phase 3's removal of the `text_expr` argument
      breaks the Value path unless the builder supplies
      `embedding_text`. `value` stays a declared property in its own
      right; `embedding_text` is the transient copy the write boundary
      strips.

### Phase 3: embed stage consumes the column — COMPLETE
- [x] `embeddings.add_embedding_column`: stop taking a `text_expr`
      argument. Read the existing `embedding_text` column. Compute
      `embedding_text_hash` as `sha2(col("embedding_text"), 256)`. Keep
      dropping `embedding_text` after hashing and embedding so the Delta
      staging schema is unchanged.
- [x] `_embed_and_stage` ledger path: compute
      `_curr_hash = sha2(col("embedding_text"), 256)` instead of
      evaluating a SQL string. Confirm hit and miss paths hash the
      identical column so the ledger does not churn.
- [x] `_embed_and_stage` ledger hit-path schema alignment (new
      requirement, not optional). Today `embedding_text` is created and
      dropped entirely inside `add_embedding_column`, so it is never in
      `df.columns` at `_embed_and_stage`. After Phase 2 the builder adds
      `embedding_text`, so it IS in `df.columns`. The hit branch builds
      `hit_final` via `select(*[col(c) for c in df.columns], ...)`,
      which now carries `embedding_text` forward, while the miss branch
      (`add_embedding_column`) still drops it. `hit_final.unionByName(
      embedded_misses)` then mismatches schemas. Fix: drop
      `embedding_text` from the hit branch (drop it from `df` before the
      hit `select`, or exclude it from the carried column list) so both
      union arms match and the Delta staging schema is unchanged.
      `split_by_ledger` itself is unchanged.
- [x] Delete the `_EMBEDDING_TEXT_EXPRS` map and the per-label
      expression constants from `embed_stage.py`. `embed_label` no
      longer needs an expression argument.
- [x] `transform_embeddings` is structurally unchanged. It passes node
      DataFrames as before. Disabled labels are untouched; their
      `embedding_text` column is stripped fail-closed at the write
      boundary like any other non-declared column.
- [x] `VALUE`: with the Phase 2 Value-builder change in place, the
      sample-value embed path reads the builder-produced
      `embedding_text` column through the same `embed_label` ->
      `_embed_and_stage` -> `add_embedding_column` path as the other
      labels, with no per-label SQL string. Verify the path end to end
      after the Phase 2 change lands.

### Phase 4: fail-closed write boundary — COMPLETE
- [x] In `_load`, replace each `_drop_cols(...)` call with a projection
      to the declared per-label property tuple from Phase 1. A bare
      `df.select(*PROPERTIES[label])` throws when the optional
      `embedding` column is absent (label not embedded), so select the
      intersection in declared order:
      `df.select(*[c for c in PROPERTIES[label] if c in df.columns])`.
      `embedding` is the only declared column that can legitimately be
      absent; assert every other declared column is present so a
      genuinely missing required property fails loudly rather than
      being silently skipped. This drops `embedding_text`,
      `embedding_text_hash`, `embedding_model`, `embedded_at`,
      `embedding_error`, and any other staging-only column by
      construction.
- [x] Delete `_drop_cols` (the projection subsumes it). Remove its
      import and call sites. Replaced by `run._project(df, label)`.
- [x] Confirm `HAS_SCHEMA` / `HAS_TABLE` / `HAS_COLUMN` builders are
      unaffected: they derive ids from the source DataFrame, not from
      node-DF helper columns. Verified — `build_has_*_rel` operate on
      `schemata_df` / `tables_df` / `columns_df`, not the node DFs.

### Phase 5: tests and quality gates — COMPLETE
- [x] Contract regression test: for each label, in both the embedded
      and not-embedded case, the DataFrame handed to the writer has a
      column set exactly equal to the declared property tuple. The
      embedded case must explicitly assert `embedding_text`,
      `embedding_text_hash`, `embedding_model`, `embedded_at`, and
      `embedding_error` are absent — these are the properties this
      refactor removes from the graph, and this assertion is the
      regression guard for that removal. This is the test that makes
      drift impossible to reintroduce silently.
      Added `tests/spark/test_node_projection.py` exercising
      `run._project` for every node label in both embedded and
      not-embedded modes, plus the missing-required-property
      loud-failure path.
- [x] Builder tests: `build_table_nodes` / `build_column_nodes` /
      `build_schema_nodes` / `build_database_nodes` expose only
      declared properties plus `embedding_text`, with no helper
      columns. Rewrote `tests/spark/schema_graph/test_node_builders.py`
      (old tests asserted the now-removed helper-retention behavior).
- [x] Embedding-text test: the `embedding_text` produced by each
      builder equals the string the old in-place expression produced,
      including the multi-catalog catalog-qualified prefix
      (`bronze.sales.orders` vs `gold.sales.orders` differ; identical
      schema.table across catalogs no longer collide). Pinned to exact
      expected strings in the rewritten builder tests.
- [x] Ledger test: `embedding_text_hash` for a given row equals the
      pre-refactor hash for the same inputs (text content unchanged).
      Covered by the existing
      `test_embedding_text_hash_is_sha256_of_text` and the unchanged
      `split_by_ledger` tests (still green).
- [x] Disabled-label test: with a label's embedding flag off, its node
      DataFrame reaches the writer property-only, with `embedding_text`
      stripped and no `embedding` column. Covered by
      `test_project_not_embedded_node_omits_optional_embedding`.
- [x] Run `uv run --group test pytest tests/spark tests/presets
      tests/examples/finance-genie`, `uvx ruff check`, and
      `uvx --with-editable packages/dbxcarta-spark mypy -p dbxcarta.spark`.
      All green, zero new mypy or ruff findings. Result: 182 passed,
      1 skipped; ruff "All checks passed!"; mypy "no issues found in
      38 source files".

### Phase 6: cleanup — COMPLETE
- [x] Delete the now-dead "retained for embedding text" comments and
      docstring notes in `schema_graph.py`.
- [x] Update any proposal or reference doc that describes the old
      drop-before-write flow.

Implementation notes:
- `schema_graph.py` had no surviving "retained for embedding text"
  comments — the Phase 2 docstring rewrites already removed them
  (grep for the phrase returns no matches).
- `docs/reference/pipeline.md` Step 7 now documents the fail-closed
  write boundary (`run._project` projecting to
  `contract.NODE_PROPERTIES`, allowlist not denylist, raises on a
  missing declared property). A new "Key Design Principles" section
  encodes the lessons: one source of truth in `contract.py`; allowlist
  not denylist; helper columns are transform inputs that never reach
  the graph; embedding bookkeeping lives in Delta/ledger not the graph;
  `embedding_text` computed once in the builder; structure is edges,
  not properties.
- `docs/schema/SCHEMA.md` now reflects the narrowed graph contract:
  `embedding` remains the only embedding-side graph property, `layer`
  is documented on Table nodes, and the embedding bookkeeping columns
  are described as staging/ledger data rather than node properties.
- `tests/integration/test_semantic_search.py` no longer reads
  `Table.embedding_text` from Neo4j. The self-ranking and graph
  expansion probes use the stored `embedding` vector, which matches the
  new graph contract.

## Risks and mitigations

- Builder expression scope. The embedding-text expression must
  reference only columns present before the builder's `.select(...)`.
  Mitigation: each builder adds the derived `name` via `withColumn`
  before applying the expression, and the source DataFrame already
  carries `comment` and the qualifying columns. The embedding-text
  equality test in Phase 5 covers this directly.

- Ledger churn on first deployed run. The text content is intended to
  be byte-identical, so hashes should not change. A full reimport is
  planned regardless, so even an incidental difference is not a
  migration problem; it would only cost one re-embed.

- Optional embedding column at the write boundary. A label may or may
  not have an `embedding` column depending on whether embedding ran.
  Mitigation: the Phase 4 projection guards the optional `embedding`
  presence; the disabled-label test in Phase 5 exercises both paths.

- Hidden consumers of removed properties. Four properties leave the
  graph, so the consumer audit had to cover all four, not just
  `table_schema`. The audit is complete and clean:

  - `packages/dbxcarta-client` has zero references to
    `embedding_text_hash`, `embedding_model`, `embedded_at`, or
    `table_schema` — none in Cypher, none in client models, none in
    client tests.
  - Every `RETURN` in `graph_retriever.py` projects explicit named
    properties (`node.id`, `s.name`, `t.name`, `c.id`, `c.name`,
    `c.data_type`, `c.comment`, `c.ordinal_position`, `v.value`, plus
    scores and REFERENCES edge props). There is no `RETURN n`,
    `RETURN *`, `properties(node)`, or whole-node collection, so the
    removed properties are not read even implicitly. Vector-search
    queries hit the `column_embedding` / `table_embedding` indexes and
    read back only the `embedding` vector.
  - All other occurrences of the three embedding-metadata names are
    Spark-side and non-graph: the Delta ledger (`ledger.py`), the Delta
    staging table (`embed_stage.py`, `embeddings.py`), the run summary
    (`summary.py`, `summary_io.py`), and Spark/integration test
    fixtures. These keep their columns; only the graph copy is removed.

  Conclusion: removing these from the graph breaks no downstream
  consumer. There is no semantic layer to preserve; the only concern
  was downstream client code, and it does not read these properties.
  The earlier conditional ("reclassify as declared if a reader is
  found") is resolved — no reader exists, so the removal stands as
  designed. Re-running the grep before Phase 4 lands remains a cheap
  sanity check against new code added in the interim, but is no longer
  a gating unknown.

  Note on framing: these three properties ARE on the graph today. The
  Neo4j Spark Connector persists every surviving DataFrame column and
  `_load` strips only `embedding_error`, so `embedding_text_hash`,
  `embedding_model`, and `embedded_at` are live properties on every
  embedded node right now. This refactor genuinely removes them; it is
  not a no-op that merely formalizes an already-absent column.

## Out of scope

- The targeted one-line `table_schema` drop is explicitly not done as a
  band-aid. This refactor closes the fail-open write boundary that
  caused it.
- Changing the normalization model (membership stays edge-based, not
  duplicated scalar properties).
- Splitting embedding text into a separate DataFrame and joining it
  back on `id`. The fail-closed write boundary closes the leak class on
  its own; a join would add the only real correctness risk (row
  alignment) and the only Spark cost in the change for no additional
  correctness benefit.
- The pre-existing `cli.py` typing change (already applied separately).

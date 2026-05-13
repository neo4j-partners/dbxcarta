# Metadata Matching Proposal

## Goal

Improve metadata-based foreign-key inference by fixing the bottlenecks observed
when the current matcher is run against the schemapile benchmark, in this
priority order: target-stem matching, abbreviation and tokenization gaps, and
residual semantic gaps that require an embedding score.

The current implementation in `src/dbxcarta/ingest/fk/metadata.py` is
deliberately conservative. It accepts exact column-name matches and a fixed
suffix list (`_id`, `_fk`, `_ref`), requires the source column's stem to match
the target table name with `+s/+es` plural rules, gates candidates by type
compatibility and target primary-key evidence, adds a binary comment-overlap
signal, then applies tie-break attenuation.

Two updates change the shape of this proposal from the earlier draft:

1. `examples/schemapile` provides a labeled corpus of real schemas with
   declared foreign keys. It replaces the "build a benchmark" phase with "load
   schemapile and measure."
2. The system will scale to 10K+ tables. Per-pair cost dominates asymptotic
   shape at this size, which rules out per-pair LLM calls and pushes toward
   deterministic scorers plus precomputed embeddings.

The earlier two-track RapidFuzz/Valentine evaluation is dropped. RapidFuzz
survives as one helper inside the deterministic fix, not as a top-level track.
Valentine and other record-linkage frameworks are deferred unless
instrumentation shows a class of failures they uniquely solve.

## Assumptions

- Schemapile is the primary benchmark. Existing metadata unit tests remain
  regression tests.
- The metadata matcher remains a driver-side Python step.
- Declared foreign keys remain authoritative and continue to suppress duplicate
  inferred edges.
- Target candidates still require primary-key or unique-key evidence. A name
  match without key evidence is not a valid FK candidate.
- Comments are uneven across real schemas. Comment-based signals must not be
  required for an accepted match.
- Scale target is roughly 10K tables and ~100K columns total. Same-schema
  partitioning keeps per-schema candidate sets in the low thousands.

## Risks

- Higher recall can degrade retrieval if ambiguous matches survive. Every
  change reports precision against schemapile, not only accepted-edge count.
- Tie-break attenuation `score / sqrt(n-1)` penalizes every candidate when one
  source column has many matches. Relaxing name rules increases `n` and can
  paradoxically reduce accepted edges. A margin-based tie-break is evaluated
  alongside any recall change.
- Abbreviation dictionaries drift as new schemas are added. The dictionary
  needs an update path, not a one-time hand-curated list.
- Embedding inference adds a one-time cost per ingest. Persisting embeddings
  alongside column metadata is required to keep per-run cost bounded.
- The PK-evidence gate may itself be the dominant rejection bucket. If so, no
  name-matching work moves the needle, and the plan pivots to PK inference
  instead. Phase 1 instrumentation answers this before later phases run.

## Phase Checklist

### Phase 1: Instrument the Current Matcher on Schemapile

Status: Pending

Goal: Measure where the current matcher actually fails before changing
anything.

Checklist:

- Wire schemapile through the existing ingest path so declared FKs become
  ground-truth labels and inferred edges can be scored against them.
- Run `infer_fk_pairs` across the schemapile corpus and collect the existing
  `InferenceCounters` per schema and in aggregate.
- For every missed declared FK, classify the rejection bucket: `rejected_name`,
  `rejected_type`, `rejected_pk`, `rejected_sub_threshold`, `rejected_tie_break`,
  `rejected_duplicate_declared`, or "never enumerated as candidate."
- Within `rejected_name`, break down the failure mode further: stem-vs-table
  mismatch, suffix not in `_STEM_SUFFIXES`, camelCase boundary not tokenized,
  abbreviation mismatch, neither column ends in a key-like suffix.
- Record baseline precision, recall, F1, and runtime against the schemapile
  labels.
- Sample 30-50 false positives for qualitative review.

Validation:

- Existing metadata unit tests pass unchanged.
- A single report shows rejection-bucket counts and the within-`rejected_name`
  failure-mode breakdown.
- Decision gate: if `rejected_pk` or `rejected_type` dominates missed FKs,
  pause Phase 2 and 3 and scope PK-inference work instead.

Notes:

- This phase does not change production behavior.
- The report is reproducible so later phases compare against the same baseline
  numbers.

### Phase 2: Deterministic Stem and Tokenization Fix

Status: Pending

Goal: Recover the schemapile failures driven by stem-vs-table mismatch, suffix
coverage, camelCase, and common abbreviations. No new runtime dependencies
beyond RapidFuzz, which sits inside this phase as a scoring helper.

Checklist:

- Tokenize column and table names with camelCase, PascalCase, and snake_case
  awareness before any comparison. `jobTitleId` becomes `[job, title, id]`.
- Expand `_STEM_SUFFIXES` based on Phase 1 frequency data. Likely additions
  include `_key`, `_identifier`, `_no`, `_num`, `_code`, and camelCase `Id`.
  Drive the list from observed schemapile suffixes, not guesses.
- Replace the strict `_stem_matches_table` equality check with a token-set
  comparison that accepts singular/plural variants and known abbreviations.
  Use RapidFuzz token-set or token-sort ratios with a high cutoff inside this
  single comparison only.
- Build an abbreviation dictionary from two sources: hand-curated common short
  forms such as `cust→customer`, `addr→address`, `qty→quantity`, and
  `org→organization`; and mined pairs from schemapile's declared FKs where a
  short stem reliably maps to a longer target table name.
- Keep type compatibility and target PK evidence as mandatory gates. No score
  change weakens these.
- Replace count-based tie-break attenuation with a margin rule: accept the top
  candidate when its score exceeds the runner-up by at least δ; otherwise
  reject as ambiguous. Calibrate δ on schemapile.
- Preserve the existing `_SCORE_TABLE` shape so `NameMatchKind × PKEvidence ×
  comment_present` still maps to a discrete confidence. New match kinds get
  their own rows, not a continuous remap.
- Extend `InferenceCounters` with the new match-kind buckets and rejection
  reasons so observability remains intact.

Validation:

- Existing metadata unit tests pass.
- Schemapile precision does not regress against the Phase 1 baseline.
- Schemapile recall improves measurably on stem-vs-table, abbreviation, and
  camelCase classes identified in Phase 1.
- Margin tie-break does not reduce accepted edge count for cases that
  previously had `n=1` candidates.
- Runtime per schema stays within budget for the 10K-table target.

Notes:

- This phase is the highest-leverage work and likely closes 60-80% of the gap
  if Phase 1 confirms the stem-bottleneck hypothesis.
- The abbreviation dictionary is checked into the repo with provenance
  comments. It is not a learned artifact yet.

### Phase 3: Embedding-Backed Residual Scorer

Status: Pending, conditional on Phase 2 leaving a meaningful gap.

Goal: Recover schemapile failures that survive Phase 2, primarily semantic
mismatches in column names and comments where deterministic rules cannot
reach.

Checklist:

- Pick a small CPU-resident embedding model. Candidates: `all-MiniLM-L6-v2`
  (22M params), `BAAI/bge-small-en-v1.5`, or `fastembed`. Choose based on
  Databricks job packaging cost and inference speed.
- Encode each column once per ingest from a structured string that includes
  table name, column name, data type, and comment. Persist embeddings
  alongside column metadata so per-run cost is bounded to new or changed
  columns.
- Use the embedding score as a tiebreaker and recall booster behind the same
  PK and type gates, not as a replacement for the deterministic scorer.
- Define a fixed combining rule: deterministic score is primary, embedding
  similarity raises confidence into a higher bucket only when PK evidence
  agrees and the embedding similarity clears a threshold. Document the rule
  in code with the schemapile-derived calibration.
- Add a counter `accepted_with_embedding_boost` so the contribution of
  embeddings is observable.
- Re-evaluate the margin tie-break δ in the presence of embedding scores.

Validation:

- Existing metadata unit tests pass.
- Schemapile recall improves on the residual classes identified after Phase 2,
  with no precision regression.
- One-time embedding pass over the 10K-table target completes within an
  acceptable ingest budget.
- Storage cost for persisted embeddings is documented.

Notes:

- If Phase 2 closes the gap that schemapile reveals, Phase 3 stays on the
  shelf.
- Sentence embeddings of short strings are noisy. The structured-string format
  matters more than the model choice. Tuning time goes into the input template
  before swapping models.

## Deferred Options

These are not part of the current plan but are recorded so future work has
context.

- **Valentine.** Designed for cross-dataset schema matching, optimizes for
  "same concept" rather than asymmetric "A references B." After applying FK
  gates, little of Valentine's distinct value remains over an embedding score.
  Revisit only if Phase 3 underperforms and a structural matcher is
  hypothesized to help.
- **GLiNER for comment entity extraction.** Useful where comments are rich and
  descriptive. Overlaps with embeddings on the same use case. Consider only if
  Phase 3 reveals a class of comment-driven matches that embeddings miss but
  typed entity extraction catches.
- **Record-linkage frameworks (recordlinkage, Splink, Dedupe).** Reframe FK
  inference as candidate-pair classification with learned weights. Requires
  labeled candidate pairs beyond what schemapile alone provides. Defer until a
  labeling workflow exists.
- **Per-pair LLM scorer.** Quality is likely highest, cost is prohibitive at
  10K+ tables. Out of scope for the inference path. Possibly viable as a
  one-shot offline reviewer for low-confidence accepted edges.
- **N-gram blocking and MinHash candidate generation.** Relevant only if
  profiling in Phase 2 or 3 shows candidate enumeration is the bottleneck.
  Same-schema partitioning likely keeps inner-loop size small enough that this
  is unnecessary.

## Completion Criteria

- Phase 1 produces a rejection-bucket and failure-mode report against
  schemapile that determines whether Phase 2 or PK-inference work is the right
  next step.
- Phase 2 improves schemapile recall on stem, abbreviation, and tokenization
  classes without precision regression and without runtime regression at the
  10K-table scale.
- Phase 3 runs only if Phase 2 leaves measurable residual recall on
  schemapile.
- The selected implementation preserves the existing FK safety gates and
  run-summary observability.
- The abbreviation dictionary, suffix list, and any model or threshold choices
  are checked into the repo with provenance.

## Research Notes

### Current dbxcarta Matcher

The current path:

- Accepts exact column-name matches.
- Accepts source columns ending in `_id`, `_fk`, or `_ref` that reference the
  target table's `id` column, gated by `_stem_matches_table` (`stem == table`
  or `+s`/`+es`).
- Requires compatible data types.
- Requires target primary-key evidence: declared PK, UNIQUE leftmost, or an
  `id`/`*_id` heuristic.
- Adds a small confidence boost when source and target comments share tokens
  of length >= 4.
- Attenuates scores when a source column has multiple plausible targets via
  `score / sqrt(n-1)`.
- Suppresses pairs already covered by declared FK discovery.

Failure modes observed by inspection, to be confirmed by Phase 1 against
schemapile:

- `cust_id` fails to match `customers.id` because `cust` is not equal to
  `customer`.
- `account_identifier` fails because `_identifier` is not in `_STEM_SUFFIXES`.
- `jobTitleId` fails the lowercase suffix check unless camelCase is tokenized
  first.
- Comment overlap is binary and length-gated, so semantically similar comments
  using different vocabulary contribute nothing.

### RapidFuzz

Fast Python and C++ string-matching library. Used inside Phase 2 for token-set
and token-sort comparisons during stem-vs-table matching. Not a top-level
track in this plan.

Source: <https://github.com/rapidfuzz/RapidFuzz>

### Embedding Models for Phase 3

- `sentence-transformers/all-MiniLM-L6-v2`: 22M params, widely used, ~5ms per
  string batched on CPU.
- `BAAI/bge-small-en-v1.5`: small, strong on retrieval benchmarks.
- `fastembed`: ONNX-based runtime with bundled small models and low cold-start
  cost.

Model choice is secondary to the input template. Encoding
`f"{table}.{column} ({data_type}): {comment}"` provides more context than the
bare column name.

### Schemapile as Benchmark

`examples/schemapile` ships labeled schemas with declared FKs. Declared FKs
serve as positive labels. Curated negative labels are added incrementally for
cases where the matcher produces high-confidence false positives during Phase
1 review.

---

## Client Retrieval Diagnostics (Phase E)

### `client_retrieval` Delta table

Per-question retrieval trace written by the `graph_rag` arm alongside the
run summary. Table name: `<catalog>.<schema>.client_retrieval` (same catalog
and schema as `dbxcarta_summary_table`). One row per (run_id, question_id).

| Column | Type | Description |
|---|---|---|
| `run_id` | STRING | Job run id from `DATABRICKS_JOB_RUN_ID` or "local" |
| `question_id` | STRING | Question identifier from the questions source |
| `question` | STRING | Natural-language question text |
| `target_schema` | STRING | Ground-truth schema from `question.schema` |
| `col_seed_ids` | ARRAY\<STRING\> | Column-index vector seed IDs |
| `col_seed_scores` | ARRAY\<DOUBLE\> | Cosine scores for each column seed |
| `tbl_seed_ids` | ARRAY\<STRING\> | Table-index vector seed IDs |
| `tbl_seed_scores` | ARRAY\<DOUBLE\> | Cosine scores for each table seed |
| `schema_scores` | MAP\<STRING, DOUBLE\> | Normalized aggregate score per schema |
| `chosen_schemas` | ARRAY\<STRING\> | Schemas present in the final rendered context |
| `expansion_tbl_ids` | ARRAY\<STRING\> | Tables added via HAS_COLUMN parent and REFERENCES expansion |
| `final_col_ids` | ARRAY\<STRING\> | All column IDs in the rendered context |
| `rendered_context` | STRING | Full text handed to the LLM prompt |
| `generated_sql` | STRING | SQL produced by the LLM |
| `reference_sql` | STRING | Ground-truth SQL from the question record |
| `parsed` | BOOLEAN | Generated SQL passed the parse check |
| `executed` | BOOLEAN | Generated SQL executed without error |
| `correct` | BOOLEAN | Result set matched the reference |
| `execution_error` | STRING | Error string if parse or execution failed |
| `top1_schema_match` | BOOLEAN | Top schema by score equals `target_schema` |
| `schema_in_context` | BOOLEAN | `target_schema` appears in `chosen_schemas` |
| `context_purity` | DOUBLE | Fraction of `final_col_ids` from `target_schema` |

### Retrieval-correctness metrics

Three per-arm metrics are computed from trace data and appended to the
standard run summary (`client_run_summary` table and stdout):

- `top1_schema_match_rate` — fraction of questions where the highest-scoring
  schema by aggregate seed score equals the target schema. Measures whether
  the vector search alone points at the right schema.
- `schema_in_context_rate` — fraction of questions where the target schema
  appears anywhere in the rendered context (schema-set recall). A failure
  here means the LLM had no chance to pick the right schema.
- `mean_context_purity` — average fraction of rendered columns from the
  target schema. Low purity means the context is dominated by columns from
  the wrong schema.

### Failure triage workflow

To diagnose a failed question without re-running the client:

```sql
SELECT
  question_id, question, target_schema,
  schema_scores, chosen_schemas,
  top1_schema_match, schema_in_context, context_purity,
  parsed, executed, correct, execution_error,
  rendered_context, generated_sql, reference_sql
FROM <catalog>.<schema>.client_retrieval
WHERE run_id = '<run_id>'
  AND (correct = false OR executed = false)
ORDER BY question_id
```

Use `top1_schema_match`, `schema_in_context`, and `context_purity` to
classify the failure stage:

1. `schema_in_context = false` → schema selection failure; the right schema
   never made it into the context. Root cause: vector seeds all landed in
   the wrong schema.
2. `schema_in_context = true`, `context_purity < 0.5` → retrieval dilution;
   the right schema is present but outnumbered by columns from other schemas.
3. `context_purity >= 0.5`, `executed = false` → SQL generation or parse
   failure with the right schema in context.
4. `executed = true`, `correct = false` → wrong query despite correct schema
   context; inspect `rendered_context` and `generated_sql` side by side.

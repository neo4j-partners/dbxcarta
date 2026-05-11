# Metadata Matching Proposal

## Goal

Improve metadata-based foreign-key inference by replacing the current fixed
name/comment scoring heuristic with better matching algorithms, then compare
the results against the existing implementation before choosing a production
path.

The current implementation in `src/dbxcarta/ingest/fk/metadata.py` is
deliberately conservative. It accepts exact column-name matches and a small set
of suffix patterns such as `_id`, `_fk`, and `_ref`, gates candidates by type
compatibility and target primary-key evidence, adds a binary comment-overlap
signal, then applies tie-break attenuation. That keeps false positives low, but
it misses common naming variations such as `customer_key`, `cust_id`,
`account_identifier`, `jobTitleId`, and comments with semantically similar but
non-identical wording.

This proposal evaluates two parallel replacement tracks:

- RapidFuzz, for a low-risk improvement to the current scoring logic.
- Valentine, for a broader schema-matching approach designed around tabular
  column matching.

Both tracks should preserve the current safety gates: type compatibility,
target key evidence, duplicate suppression, confidence thresholds, provenance,
and run-summary counters.

## Assumptions

- The metadata matcher remains a driver-side Python step for this evaluation.
- Declared foreign keys remain authoritative and continue to suppress duplicate
  inferred edges.
- Candidate generation should still prefer key-like target columns. A generic
  matcher should not be allowed to propose arbitrary column-to-column joins
  without primary-key or unique-key evidence.
- The existing metadata tests are treated as regression tests, not as the full
  evaluation set.
- The comparison phase needs a small labeled benchmark of accepted and rejected
  FK candidates. Existing declared FK fixtures can seed this benchmark, but they
  are not enough by themselves.

## Risks

- Higher recall can degrade retrieval if ambiguous matches survive. The
  comparison must measure false positives, not only accepted-edge count.
- Generic schema matchers may optimize for column equivalence rather than FK
  directionality. The evaluator must distinguish "same concept" from "source
  references target key."
- Instance-based matching can become expensive if it requires loading data
  values for every candidate pair. Keep the first Valentine experiment
  schema-only unless a scoped value sample is available.
- Adding large dependencies may complicate Databricks job packaging. Dependency
  size and wheel availability should be part of the recommendation.

## Phase Checklist

### Phase 1: Baseline Harness

Status: Pending

Goal: Create a stable way to compare the current matcher, RapidFuzz, and
Valentine on the same inputs.

Checklist:

- Capture the current metadata matcher's output for existing FK fixtures.
- Build a benchmark dataset with positive pairs from declared FKs and curated
  negative pairs that are plausible but wrong.
- Include examples for exact matches, suffix matches, pluralized table names,
  abbreviated names, camel-case names, comment-only hints, cross-schema
  collisions, and high-fanout ambiguous IDs.
- Record baseline metrics: precision, recall, F1, accepted edge count,
  duplicate-suppression count, tie-break rejections, and runtime.
- Preserve current counter invariants so every candidate is accepted or rejected
  with an observable reason.

Validation:

- The existing metadata unit tests pass unchanged.
- The benchmark runner can execute the current matcher and produce a comparable
  metrics report.

Notes:

- This phase should finish before either matching track is judged.
- It does not need to change production inference behavior.

### Phase 2A: RapidFuzz Track

Status: Pending

Goal: Improve the existing deterministic matcher with better string similarity
while keeping the current architecture and safety gates.

Checklist:

- Add normalized metadata strings for source column, target column, target
  table, and source/target comments.
- Replace the binary name kind with a richer name score that accounts for
  exact match, suffix match, abbreviation, token overlap, edit distance, and
  camel-case or snake-case tokenization.
- Replace binary comment overlap with a comment similarity score.
- Preserve type compatibility and target PK evidence as mandatory gates.
- Keep tie-break attenuation or replace it only if the benchmark shows a better
  ambiguity rule.
- Map the RapidFuzz score into the existing confidence range so downstream
  thresholds remain meaningful.
- Add counters or debug fields that explain which signal contributed to an
  accepted match.

Validation:

- Existing metadata tests pass.
- Benchmark precision does not regress below the current matcher.
- Benchmark recall improves on abbreviation, camel-case, and comment-variant
  cases.
- Runtime remains acceptable for the scale target of roughly one thousand
  tables.

Notes:

- This is the lowest-risk path because it can be implemented as an internal
  scorer behind the existing candidate filters.
- RapidFuzz should be treated as an algorithmic dependency, not as a full FK
  inference framework.

### Phase 2B: Valentine Track

Status: Pending

Goal: Evaluate whether a schema-matching library can discover better column
relationships than a custom FK heuristic.

Checklist:

- Model candidate source and target metadata as DataFrames suitable for
  Valentine.
- Start with schema-only matching to avoid data-loading cost and isolate the
  value of names and comments.
- Evaluate at least one structural/schema matcher and one token/string-oriented
  matcher available through Valentine's API.
- Apply dbxcarta's type and target-key gates before accepting any Valentine
  match as a foreign-key edge.
- Convert Valentine similarity scores into dbxcarta confidence scores with a
  documented calibration rule.
- Preserve provenance in a way that distinguishes Valentine-derived metadata
  edges from the current metadata scorer during evaluation.
- Measure dependency, packaging, and runtime cost in the Databricks job
  environment.

Validation:

- Existing metadata tests pass when Valentine output is routed through the
  same FK acceptance contract.
- Benchmark recall improves on cases where schema-matching structure helps.
- False positives from concept-equivalent but non-FK columns are identified
  and counted.
- Runtime and dependency overhead are documented.

Notes:

- Valentine is more domain-specific than generic record linkage because it is
  designed for column matching across tabular datasets.
- Its output still needs FK-specific gating because a schema match is not
  automatically a valid join relationship.

### Phase 3: Comparative Evaluation

Status: Pending

Goal: Compare the baseline, RapidFuzz track, and Valentine track using the same
metrics and decide which approach should become the production matcher.

Checklist:

- Run all three matchers against the benchmark dataset.
- Compare precision, recall, F1, accepted edge count, rejected candidate count,
  duplicate suppression, tie-break behavior, and runtime.
- Compare qualitative examples where the matchers disagree.
- Identify matches that improve retrieval context versus matches that create
  ambiguous or misleading joins.
- Decide whether to adopt RapidFuzz, Valentine, a hybrid, or no replacement.
- Document calibration changes needed for confidence thresholds.
- Update the production implementation plan based on the chosen path.

Validation:

- The selected matcher has no precision regression on the curated benchmark.
- The selected matcher improves recall on at least one class of currently
  missed relationship.
- The selected matcher preserves observability: candidate counts, rejection
  reasons, accepted counts, and confidence buckets remain explainable.

Notes:

- The likely production recommendation is RapidFuzz first, with Valentine kept
  as an experimental or offline evaluation path unless it shows a clear quality
  gain that justifies the added complexity.

## Completion Criteria

- A benchmark exists that can evaluate the current matcher and both proposed
  tracks.
- RapidFuzz and Valentine have each been tested against the same input corpus.
- The final recommendation is based on measured precision, recall, runtime,
  dependency cost, and ease of explaining accepted edges.
- The selected implementation preserves dbxcarta's existing FK safety gates and
  run-summary observability.

## Research Notes

### Current dbxcarta Matcher

The current metadata inference path is deterministic and intentionally narrow.
It:

- Checks whether source and target column names match exactly.
- Checks whether a source column ending in `_id`, `_fk`, or `_ref` appears to
  reference the target table's `id` column.
- Requires compatible data types.
- Requires the target column to have declared or heuristic primary-key evidence.
- Adds a small confidence boost when source and target comments share tokens.
- Attenuates scores when a source column has multiple plausible targets.
- Suppresses pairs already covered by declared FK discovery.

This is a strong baseline for precision, but it is brittle around
abbreviations, token rearrangements, naming style differences, and comments
that use related words without exact token overlap.

### RapidFuzz

RapidFuzz is a fast Python and C++ string-matching library. It provides common
similarity algorithms such as Levenshtein-based ratios, partial ratios,
weighted ratios, token-sort ratios, and token-set ratios. It is MIT licensed,
has prebuilt wheels for common platforms, and supports Python 3.10 or later.

Source: <https://github.com/rapidfuzz/RapidFuzz>

Fit for dbxcarta:

- Best for improving the current name and comment scoring logic.
- Low architectural risk because it can sit behind the existing candidate
  gates.
- Useful for abbreviations, token order changes, punctuation differences,
  snake-case/camel-case normalization, and fuzzy comment overlap.
- Not a full FK inference system by itself.

### Valentine

Valentine is a Python package for identifying potential relationships among
columns of tabular datasets represented as pandas or Polars DataFrames. It
implements several schema- and instance-based matching algorithms behind a
single API, including COMA, Cupid, distribution-based matching, Jaccard
distance, and Similarity Flooding. It supports Python versions compatible with
this project.

Source: <https://delftdata.github.io/valentine/>

Fit for dbxcarta:

- Best domain-specific candidate for schema and column matching.
- Can evaluate whether schema-matching algorithms outperform hand-written FK
  heuristics.
- Needs FK-specific gates because column similarity is not the same as join
  validity.
- Instance-based matching may require additional sampling and runtime controls.

### Python Record Linkage Toolkit

The Python Record Linkage Toolkit provides indexing, comparison functions, and
classifiers for record linkage and deduplication. It supports string, numeric,
and date comparisons, as well as supervised and unsupervised classifiers.

Source: <https://recordlinkage.readthedocs.io/en/latest/about.html>

Fit for dbxcarta:

- Useful if FK inference is reframed as candidate-pair classification.
- Could combine name similarity, comment similarity, type compatibility, PK
  evidence, schema proximity, and historical labels into a learned model.
- More general than needed for the first improvement.
- Best suited for a later phase if labeled candidate pairs become available.

### Splink

Splink is a probabilistic record-linkage package based on the Fellegi-Sunter
model. It supports large-scale linkage using backends such as DuckDB and Spark
and includes diagnostics for understanding linkage quality.

Source: <https://moj-analytical-services.github.io/splink/index.html>

Fit for dbxcarta:

- Relevant if FK inference needs a scalable probabilistic model.
- Potentially attractive in Databricks because Spark-style execution is part of
  the design.
- Less directly schema-specific than Valentine.
- Requires multiple independent metadata features to work well, not just a
  single name/comment text field.

### Dedupe

Dedupe is a machine-learning library for fuzzy matching, deduplication, and
entity resolution. It learns similarity weights and blocking rules from human
training labels.

Source: <https://docs.dedupe.io/>

Fit for dbxcarta:

- Strong option if a human labeling workflow is added for FK candidates.
- Could learn weights instead of relying on a hand-tuned score table.
- Higher operational overhead than RapidFuzz or Valentine.
- Not the right first step unless labeled examples are part of the roadmap.

### Jellyfish

Jellyfish provides approximate and phonetic string matching, including
Levenshtein, Damerau-Levenshtein, Jaro, Jaro-Winkler, Jaccard, Hamming, and
phonetic encodings.

Source: <https://jamesturk.github.io/jellyfish/>

Fit for dbxcarta:

- Useful for individual string metrics, especially Jaro-Winkler.
- Smaller surface area than RapidFuzz for token-oriented matching.
- Phonetic encodings are less relevant for database column names than for human
  names.

### TextDistance and Similar Libraries

TextDistance, strsimpy, and related libraries provide broad collections of
string distance algorithms. They are useful for experimentation, but they are
less compelling as a production dependency here than RapidFuzz because
RapidFuzz is faster, actively used, and has practical token-based scorers.

## Option Summary

| Option | Best Use | Strength | Main Concern | Recommendation |
|---|---|---|---|---|
| RapidFuzz | Improve current heuristic | Fast, focused, low-risk fuzzy name/comment scoring | Does not infer FK semantics alone | First implementation track |
| Valentine | Schema/column matching | Purpose-built for table column matching | Needs FK gates and dependency review | Second implementation track |
| recordlinkage | Candidate-pair classification | Flexible comparisons and classifiers | More general than needed without labels | Later if benchmark labels mature |
| Splink | Probabilistic linkage at scale | Scalable and diagnostic-rich | Less schema-specific; needs rich features | Consider only for large-scale model path |
| dedupe | Human-trained entity resolution | Learns weights from labels | Requires labeling workflow | Defer |
| Jellyfish | Individual string metrics | Simple approximate and phonetic matching | Narrower than RapidFuzz for this use case | Use only if RapidFuzz is too heavy |
| TextDistance/strsimpy | Algorithm experiments | Many distance functions | Production fit is weaker than RapidFuzz | Research only |

## Recommendation

Proceed with the two-track evaluation, but treat RapidFuzz as the likely
production path unless Valentine produces a measurable quality improvement that
justifies its additional abstraction and dependency cost.

RapidFuzz aligns with the current design: keep conservative FK gates, improve
the brittle name/comment score, preserve confidence scoring, and maintain
explainable run summaries. Valentine is worth testing because it is the most
schema-specific library found in the research, but its matches must be
converted into FK-safe candidates rather than accepted as relationships
directly.

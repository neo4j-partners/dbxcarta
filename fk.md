# Proposal: Modularize and Make Configurable FK Detection

## Current State

FK detection today lives across four source files: `fk_common.py` (shared types and primitives), `fk_inference.py` (Phase 3: metadata-based inference), `fk_semantic.py` (Phase 4: embedding-based inference), and `inference.py` (orchestration and Spark boundary conversions). Configuration is spread across a dozen `DBXCARTA_*` environment variables validated by a Pydantic `Settings` class. Tests mirror the source layout in three directories (`fk_common/`, `fk_inference/`, `fk_semantic/`).

The code works, but several structural issues make it harder to extend, test, and operate than it needs to be.

---

## Problems Worth Solving

### 1. Scoring logic is baked into constants, not configuration

Phase 3's `_SCORE_TABLE` is a hard-coded dictionary mapping `(NameMatchKind, PKEvidence, comment_present)` tuples to float scores. Phase 4's floor, cap, bonus, and overlap threshold are function parameters but are not surfaced as environment-variable settings (except `threshold`). Changing any scoring parameter requires a code change, a release, and a redeploy — even for adjustments that are clearly tuning, not logic.

### 2. Name-matching rules are not pluggable

Phase 3 recognizes only two name-match patterns: exact column-name match and a suffix pattern (`_id`, `_fk`, `_ref`). Some catalogs use different conventions (prefix patterns like `fk_customer`, camelCase identifiers, or domain-specific abbreviations). Adding a new pattern today means editing `_name_match` and updating the score table — two tightly coupled changes in the same file.

### 3. The two inference phases have parallel but divergent counter structures

`InferenceCounters` and `SemanticInferenceCounters` do the same job (track candidates, acceptances, and rejections) but share no common interface. Each has its own `as_summary_dict` method with its own prefix logic. Adding a third inference phase (for example, query-history-based inference) would mean writing a third counter class with the same boilerplate.

### 4. Phase orchestration is implicit

`inference.py`'s `run_inferences` hard-codes the sequence: build PKIndex, run Phase 3, collect its output pairs, conditionally run Phase 4. The conditional gating logic (`_should_run_semantic`) is a standalone function, but the phase ordering and the data-passing contract between phases is wired into procedural code. There is no way to run Phase 4 alone, skip Phase 3 while keeping Phase 4, or slot a new phase between them without editing `run_inferences`.

### 5. Type-compatibility rules are not configurable

`canonicalize` and `_TYPE_EQUIV` handle the common integer and string families, but some catalogs have custom types or vendor-specific aliases that the current mapping does not cover. There is no way to extend the equivalence map without editing `fk_common.py`.

### 6. PK-evidence heuristics are hard-coded

The `pk_kind` function's fallback branch (column named `id` or `{table}_id`) is reasonable for most catalogs but wrong for some. Catalogs that use `pk_*`, `key_*`, or surrogate-key naming conventions get no fallback coverage. The heuristic is not overridable.

### 7. Tie-break attenuation uses a single formula

The square-root attenuation formula in Phase 3 (`score / sqrt(n - 1)`) is effective for the common case but provides no knob to adjust aggressiveness. A hard top-N cap per source column, or a configurable attenuation exponent, would let operators tune the behavior per catalog without changing code.

---

## Proposed Changes

### A. Introduce an FK inference configuration object

Create a dedicated `FKInferenceConfig` dataclass (or Pydantic model) that bundles all FK-related tuning parameters in one place. This includes the Phase 3 score table values, the stem suffixes, the threshold, the attenuation parameters, the Phase 4 floor/cap/bonus/overlap-threshold, the semantic similarity threshold, the minimum table count gate, and the type-equivalence map. The existing `Settings` fields for FK inference would delegate to this object. Operators could override any parameter via environment variables without touching code.

### B. Define an abstract inference-phase interface

Formalize what an inference phase is: a callable that accepts a common input bundle (columns, PK index, already-covered pairs, and phase-specific extras like embeddings or value indexes) and returns a common output bundle (a list of `InferredRef` plus a counter object that satisfies a shared counter protocol). Phase 3 and Phase 4 would implement this interface. New phases (query-history inference, lineage-based inference) would plug in by implementing the same interface and registering themselves in configuration.

### C. Make the counter classes share a common protocol

Define a protocol (or abstract base) for inference counters with the required attributes (`considered`/`candidates`, `accepted`, `rejections`) and the `as_summary_dict(prefix)` method. `InferenceCounters` and `SemanticInferenceCounters` would conform to this protocol. The `RunSummary` flattening code would iterate over a list of phase counters rather than handling each by name.

### D. Extract name-matching into a strategy

Move `_name_match`, `_stem_matches_table`, and `_STEM_SUFFIXES` behind a name-match strategy interface. The default strategy implements the current exact-plus-suffix rules. Alternative strategies (prefix matching, regex-based, catalog-specific) could be provided via configuration. The score table would reference the strategy's match-kind enum rather than hard-coding `NameMatchKind`.

### E. Make the type-equivalence map configurable

Allow the `_TYPE_EQUIV` dictionary and the regex patterns for decimal and string families to be extended or overridden through the FK inference configuration object. The default values would be exactly what exists today.

### F. Make PK-evidence heuristics pluggable

Extract the name-based fallback logic from `pk_kind` into a configurable heuristic. The default heuristic would be the current `id` / `{table}_id` rule. Operators could supply additional patterns (regex-based or list-based) for catalogs with non-standard PK naming.

### G. Make tie-break attenuation configurable

Expose the attenuation formula's parameters (currently hard-coded `sqrt(max(0, n - 1))`) as configuration. Options would include the current square-root formula with a configurable exponent, a hard top-N cap per source column, or both. The default behavior would be unchanged.

### H. Refactor phase orchestration into a pipeline runner

Replace the procedural `run_inferences` function with a small runner that iterates over an ordered list of enabled inference phases. Each phase receives the accumulated covered-pairs set from all prior phases. The runner collects all `InferredRef` lists and counter objects, then builds the combined output. This makes phase ordering explicit, skipping a phase trivial (just omit it from the list), and adding a new phase a configuration change rather than an orchestration rewrite.

---

## What Does Not Change

- The frozen dataclass types in `fk_common.py` (`ColumnMeta`, `ConstraintRow`, `DeclaredPair`, `InferredRef`, `PKIndex`) stay as they are. They are the load-bearing contract between phases and the Spark boundary.
- The `EdgeSource` enum stays in `contract.py`. New phases add new enum members; existing ones do not change.
- The Spark boundary conversions (`.from_row` classmethods, `build_inferred_references_rel`) stay in their current locations. The boundary is well-defined and should not be blurred.
- The existing test fixtures and assertions remain valid. The refactoring should be additive — existing tests pass unchanged, and new tests cover the new configuration and strategy surfaces.
- The run-summary wire format (Delta table schema, JSON contract) does not change. New phases' counters flatten through the same `as_summary_dict(prefix)` mechanism.

---

## Code Quality Improvements (Included in the Refactoring)

### Reduce duplication between Phase 3 and Phase 4 counter classes

Both counter classes follow the same pattern: a mutable aggregate with `record_*` methods and an `as_summary_dict` flattener. The shared protocol (proposed in section C) eliminates the need to write parallel boilerplate for each new phase.

### Consolidate the "already-covered" suppression logic

Phase 3 checks `DeclaredPair in declared_pairs` inside the per-source loop. Phase 4 builds a `covered = declared_pairs | metadata_inferred_pairs` union and checks against that. The suppression logic should be a single reusable function that takes a pair and a frozenset and returns a boolean, so the check is not reimplemented per phase.

### Improve the inner-loop structure of infer_fk_pairs

The Phase 3 entry point iterates over all column pairs twice: once to build `per_source` candidates, then again to apply tie-break attenuation. The two-pass design is correct but the second pass rebuilds the same `(score, tgt_id)` tuples it just stored. A clearer separation between the candidate-scoring pass and the attenuation-and-emit pass would make the logic easier to follow and to test independently.

### Add type annotations to internal helpers

Several private functions (`_stem_matches_table`, `_comment_tokens`, `_name_match`) lack return-type annotations in the source despite being covered by mypy-strict in the project config. Adding explicit annotations improves readability and catches regressions earlier.

### Make the Phase 4 cosine function swappable

The pure-Python cosine implementation is adequate at the current scale target, but the function is private and not injectable. Making it a parameter of the semantic inference function (defaulting to the pure-Python implementation) would let operators substitute a numpy-backed or BLAS-backed version on larger catalogs without forking the module.

---

## Suggested Order of Work

1. Introduce `FKInferenceConfig` and wire existing constants into it. All tests still pass with default values. This is a pure refactor with no behavior change.
2. Define the counter protocol and make both counter classes conform to it. Update `RunSummary` to iterate over a list of phase counters.
3. Define the inference-phase interface and wrap Phase 3 and Phase 4 as implementations. Replace `run_inferences` with the pipeline runner.
4. Extract name-matching into a strategy and make it configurable.
5. Make type-equivalence, PK heuristics, and tie-break attenuation configurable.
6. Add new environment variables to `Settings` for each new configurable, with defaults that preserve current behavior.
7. Write integration tests that exercise non-default configurations (custom name-match strategy, extended type equivalence, adjusted attenuation).

Each step is independently shippable and testable. No step changes observable behavior under default configuration.

---

## Risks and Mitigations

- **Over-abstraction.** The current two-phase system works. Adding a full strategy/plugin architecture before a third phase exists is speculative. Mitigation: keep the abstractions lightweight (protocols, not class hierarchies) and do not build a registration/discovery mechanism until there is a concrete third phase to register.
- **Configuration sprawl.** Surfacing every tuning knob as an environment variable makes the system harder to operate. Mitigation: group all FK-related settings under a single `FKInferenceConfig` object with sensible defaults. Document which knobs matter for common tuning scenarios and which are for advanced use only.
- **Test matrix expansion.** Configurable scoring and strategies multiply the test surface. Mitigation: keep the default-configuration tests as the regression baseline. Test non-default configurations with focused unit tests on the specific strategy or parameter, not by re-running the full fixture suite with every combination.

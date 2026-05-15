# Clean Boundaries v2 Proposal

**Status:** Draft. Builds on `docs/proposal/clean-boundaries.md` (v1, landed 2026-05-14).

**Migration mode:** Single atomic cutover, one PR, one commit — same discipline as v1.

---

## Goals

1. **Define the contract between core and backends precisely.** The protocol's Input is the minimum vocabulary every backend can interpret. Everything else is backend-internal.
2. **One translation, in one place.** When per-build Input meets pre-built backend configuration, exactly one merge happens, inside the backend's `build_semantic_layer` method. No parallel copies of the same translation.
3. **The Result describes what was built, not how it was built.** Counts by node label and relationship type, timing, identifiers, and the Input that was served.
4. **One signature per public function.** `run_dbxcarta` takes spark settings or none, nothing else. `build_semantic_layer` takes a `SemanticLayerInput`, nothing else. No dual-input branches.
5. **Future backends drop in without touching core.** A BigQuery or Postgres backend ships its own settings class and its own builder. Core stays unchanged.

---

## Overview

v1 split the dbxcarta monorepo into four published distributions (`dbxcarta-core`, `dbxcarta-spark`, `dbxcarta-client`, `dbxcarta-presets`) and introduced a `SemanticLayerBuilder` protocol with a `SemanticLayerConfig` neutral configuration type. The structural split landed cleanly. The contract between core and the spark backend, however, ended up shim-shaped: `SemanticLayerConfig` named three fields that `SparkIngestSettings.from_semantic_config` copied into the real backend settings, and `run_dbxcarta` grew a `settings | semantic_config` dual-input branch. None of that translation carried behavior.

v2 fixes the contract. It replaces `SemanticLayerConfig` with a strictly neutral `SemanticLayerInput`, deletes the field-copy classmethod, collapses `run_dbxcarta` back to one signature, and locates the single legitimate translation inside the spark builder. After v2, "what does each layer promise" is answerable by reading three dataclasses in `dbxcarta.core.builder`.

---

## Why we are fixing this

Three concrete pains in the v1 shape:

### `SemanticLayerConfig` is a partial configuration

Its three fields — `source_catalog`, `source_schemas`, `embedding_endpoint` — can never run a build on their own. The spark backend needs roughly thirty more fields (summary sinks, secret scope, embedding flags, ledger paths, neo4j batch size, verify gate, ...). Every caller paths through a translation. The "neutral config" doesn't pull its own weight; it's a partial config dressed as a contract.

### The translation is encoded in two places

`SparkIngestSettings.from_semantic_config` is a literal field-copy classmethod. `SparkSemanticLayerBuilder.build_semantic_layer` does the same copy with a slightly different fallback rule. Two implementations of one mapping is a maintenance hazard: a future field gets added to one and missed in the other.

### `run_dbxcarta` has two ways to be called

```python
def run_dbxcarta(
    *,
    settings: SparkIngestSettings | None = None,
    semantic_config: SemanticLayerConfig | None = None,
    spark: "SparkSession | None" = None,
) -> RunSummary:
    if settings is not None and semantic_config is not None:
        raise ValueError(...)
```

The XOR branch exists to support an abstraction that no caller actually needs. Type checkers cannot enforce it. It must be tested for every code path that may pass either.

None of these crash. They make the boundary noisy, the next refactor harder, and the answer to "what is the contract here" longer than it should be. Fix the shape before more code accretes around it.

---

## What to avoid

- **Behavioral flags in the contract.** `include_sample_values`, `infer_foreign_keys`, `embedding_endpoint` — anything that describes *how* the build runs instead of *what* is being requested. These belong in the backend's own configuration. Contracts should describe *what*, not *how*.
- **Vocabulary that leaks a single backend's model.** "Catalog" and "schema" are UC-flavored; "FK" is relational. The Input uses neutral names (`source_ref`, `scope`) so a graph-native or document-oriented backend doesn't have to pretend.
- **Partial-config types.** A contract type that no backend can run from is decorative. If three fields aren't enough to start a build, do not pretend they describe a build.
- **Two copies of one translation.** If `Input.source_ref` maps to `SparkIngestSettings.dbxcarta_catalog`, that mapping lives in exactly one function. Not in a `from_semantic_config` classmethod and a builder method simultaneously.
- **Compatibility wrappers, re-exports, deprecation warnings.** Pre-1.0; atomic cutover (matching v1).
- **Cross-extension imports.** Spark and client never import each other. Presets is the one operational umbrella (Decision 2 in v1).
- **Settings models that construct runtime objects.** Settings hold data; the builder constructs Spark sessions, Neo4j drivers, embedding clients. Sessions and drivers never live on settings.

---

## Defined interfaces

The contract surface is three types and one protocol method, in `dbxcarta.core.builder`.

### `SemanticLayerInput`

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class SemanticLayerInput:
    """What to build.

    source_ref
        Opaque identifier for the source system. UC backend interprets this
        as a catalog name; a BigQuery backend would interpret it as a
        project id; a Postgres backend, a database URL. The contract does
        not constrain the format.
    scope
        Within-source filter. UC backend interprets entries as schema
        names; BigQuery as datasets; etc. Empty tuple means "every
        namespace inside source_ref".
    """

    source_ref: str
    scope: tuple[str, ...] = ()
```

**Why only two fields?** They are the only vocabulary every relational, document, or graph source can interpret without translation. Anything more (embedding settings, FK inference toggles, sampling thresholds) either leaks one backend's model or describes behavior the contract has no business pinning down.

### `SemanticLayerResult`

```python
from dataclasses import dataclass, field
from datetime import datetime

@dataclass(frozen=True)
class SemanticLayerResult:
    """What was built.

    Presence of a Result implies success; failures raise. node_counts and
    edge_counts use the NodeLabel.value / RelType.value strings from the
    core graph contract; missing keys mean "not produced".

    The Input is echoed so callers can correlate a Result with the request
    that produced it (useful for orchestrators running many builds).
    """

    run_id: str
    contract_version: str
    input: SemanticLayerInput
    node_counts: dict[str, int] = field(default_factory=dict)
    edge_counts: dict[str, int] = field(default_factory=dict)
    started_at: datetime | None = None
    ended_at: datetime | None = None
```

**Why no `status` field?** A Result is produced only on success; failures raise. Status would be redundant noise.

**Why `dict[str, int]` instead of typed counts?** The graph contract enum is shared across backends, but a particular backend may not produce every label/rel type. A dict with missing keys conveys that honestly; a frozen dataclass with all-zero defaults would lie.

### `SemanticLayerBuilder` protocol

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class SemanticLayerBuilder(Protocol):
    """A pre-configured semantic-layer builder.

    Backend-specific configuration (output store, secrets, embedding
    settings, performance knobs) is injected at construction time. The
    protocol method takes only the per-build Input and returns the Result.
    """

    def build_semantic_layer(self, input: SemanticLayerInput) -> SemanticLayerResult: ...
```

**Why the Input parameter and not a parameterless method?** A pre-configured builder can serve many builds at different scopes. Holding the Input out of the constructor keeps the builder reusable and makes the runtime overlay explicit.

### Spark implementation

```python
class SparkSemanticLayerBuilder:
    """Databricks/Spark implementation of SemanticLayerBuilder.

    All backend configuration (catalog, summary sinks, secret scope,
    embedding endpoint, ledger paths, batch sizes, feature flags) lives in
    SparkIngestSettings. When `settings` is None the builder falls back to
    env-driven defaults.
    """

    def __init__(
        self,
        settings: SparkIngestSettings | None = None,
        spark: "SparkSession | None" = None,
    ) -> None: ...

    def build_semantic_layer(self, input: SemanticLayerInput) -> SemanticLayerResult: ...
```

The implementation merges Input into the pre-configured settings exactly once:

```python
def build_semantic_layer(self, input: SemanticLayerInput) -> SemanticLayerResult:
    base = self._settings or SparkIngestSettings()
    merged = base.model_copy(update={
        "dbxcarta_catalog": input.source_ref,
        "dbxcarta_schemas": ",".join(input.scope),
    })
    summary = run_dbxcarta(settings=merged, spark=self._spark)
    return _result_from_summary(summary, input)
```

`run_dbxcarta` returns to a single signature:

```python
def run_dbxcarta(
    *,
    settings: SparkIngestSettings | None = None,
    spark: "SparkSession | None" = None,
) -> RunSummary: ...
```

`SparkIngestSettings.from_semantic_config` is deleted. `SemanticLayerConfig` is deleted from core. `DEFAULT_EMBEDDING_ENDPOINT` lives only in the spark backend (it is not a contract concept).

### Client and presets

`dbxcarta.client` is query-time only. It does not implement `SemanticLayerBuilder` and is unchanged by v2.

`dbxcarta.presets` is the operational umbrella. The `dbxcarta preset --run` CLI:

1. Overlays the preset's `env()` onto `os.environ` via `dbxcarta.core.env.apply_env_overlay`.
2. Constructs `SparkSemanticLayerBuilder()` (env-driven settings).
3. Reads catalog/schemas from env, builds a `SemanticLayerInput`, calls `build_semantic_layer`.
4. Prints the `SemanticLayerResult` (run id, contract version, counts).

The `dbxcarta-ingest` wheel entrypoint does the same, without any caller-side scope override (env-driven Input).

---

## Summary of what v1 implemented

For context, v1 (`docs/proposal/clean-boundaries.md`) delivered:

- Four published distributions: `dbxcarta-core`, `dbxcarta-spark`, `dbxcarta-client`, `dbxcarta-presets`.
- PEP 420 namespace under `dbxcarta.*` with no top-level `dbxcarta/__init__.py`.
- Console scripts owned by the layer that implements them: `dbxcarta-ingest` in `dbxcarta-spark`, `dbxcarta-client` in `dbxcarta-client`, the `dbxcarta` CLI in `dbxcarta-presets`. Core registers zero scripts.
- Layer-mirrored test directories (`tests/core`, `tests/spark`, `tests/client`, `tests/presets`, `tests/examples`) plus `tests/boundary`.
- Boundary tests that subprocess-import each layer and assert no forbidden modules load; an AST-level test that scans source for forbidden cross-layer imports.
- `EnvOverlay` protocol in core so preset env overlays go through core; the `spark→preset` import removed.
- First version of the `SemanticLayerBuilder` protocol, `SemanticLayerResult`, `SemanticLayerConfig`, and `SparkSemanticLayerBuilder` introduced.
- Internal sample consumers (Finance Genie, SchemaPile, dense-schema) and the external `sql-semantics` preset package updated to import from the new paths.

The structural and packaging work is done and remains correct under v2. v2 only touches the contract types between core and backends and the few callers that bridge them.

---

## Implementation plan

Six phases, all landing in the same cutover PR. The order is chosen so structural changes land before example/test updates depend on them.

### Phase 1 — Core: define the v2 contract

**Outcome:** `dbxcarta.core.builder` exports the v2 shape. `SemanticLayerConfig` is gone.

Checklist:

- [ ] Add `SemanticLayerInput(source_ref: str, scope: tuple[str, ...] = ())` to `dbxcarta.core.builder`.
- [ ] Replace `SemanticLayerResult` with the v2 shape: `run_id`, `contract_version`, `input: SemanticLayerInput`, `node_counts`, `edge_counts`, `started_at`, `ended_at`.
- [ ] Update `SemanticLayerBuilder.build_semantic_layer` signature to `(self, input: SemanticLayerInput) -> SemanticLayerResult`.
- [ ] Delete `dbxcarta.core.settings.SemanticLayerConfig` and its export from `dbxcarta.core.__init__`.
- [ ] Move `DEFAULT_EMBEDDING_ENDPOINT` out of core (or delete it; spark settings can carry the literal).
- [ ] Update `dbxcarta.core.__init__.__all__`.

Tests:

- [ ] `tests/core/test_input.py`: defaults (`scope == ()`), frozen, immutable.
- [ ] `tests/core/test_result.py`: construction, dict factories, frozen.
- [ ] Delete `tests/core/test_settings.py` (it covered `SemanticLayerConfig`).

### Phase 2 — Spark: simplify the backend

**Outcome:** One signature on `run_dbxcarta`. The builder is the sole translation site.

Checklist:

- [ ] Delete `SparkIngestSettings.from_semantic_config`.
- [ ] Drop the `semantic_config` parameter from `run_dbxcarta`. Single signature: `settings | None`, `spark | None`. Returns `RunSummary`.
- [ ] Remove the `from dbxcarta.core.settings import SemanticLayerConfig` import in spark settings.
- [ ] Rewrite `SparkSemanticLayerBuilder.build_semantic_layer(input)` per the spec above: `model_copy` two fields, call `run_dbxcarta`, project result.
- [ ] Update `dbxcarta.spark.__init__.__all__` (no exports of deleted symbols).

Tests:

- [ ] `tests/spark/test_builder.py`: protocol satisfaction, scope merge into `dbxcarta_catalog` / `dbxcarta_schemas`, result echoes Input, counts partition correctly by NodeLabel/RelType.
- [ ] `tests/spark/test_run.py`: verify the single-signature path; remove any tests that exercised `semantic_config`.

### Phase 3 — Client: confirm no contract change

**Outcome:** `dbxcarta.client` is unchanged. Verify explicitly so we don't drift.

Checklist:

- [ ] Confirm `dbxcarta.client.__all__` is unchanged.
- [ ] Confirm no client module imports `SemanticLayerConfig` (it shouldn't; this is a regression guard).
- [ ] No new tests needed for client.

### Phase 4 — Presets and CLI

**Outcome:** The `dbxcarta preset --run` action and the `dbxcarta-ingest` wheel entrypoint both use `SparkSemanticLayerBuilder`. The CLI prints a useful summary.

Checklist:

- [ ] In `packages/dbxcarta-presets/src/dbxcarta/presets/cli.py`, in `_handle_preset` `--run` branch: construct `SparkSemanticLayerBuilder()`, read catalog/schemas from env (`SparkIngestSettings()`), build `SemanticLayerInput`, call `build_semantic_layer`, format and print the `SemanticLayerResult`.
- [ ] In `packages/dbxcarta-spark/src/dbxcarta/spark/entrypoint.py`: same pattern. The Databricks wheel task ignores the return value, but the entrypoint logs the result for run-log visibility.
- [ ] Define a small `format_result(result: SemanticLayerResult) -> str` helper, either in `dbxcarta.core` or duplicated in two callers (one-liner; not worth a helper if only two callers).

Tests:

- [ ] `tests/presets/test_cli.py`: cover the new `--run` flow; assert the builder is invoked with the expected Input.

### Phase 5 — Examples and external consumers

**Outcome:** Worked examples demonstrate the v2 builder/Input pattern.

Checklist:

- [ ] `examples/integration/finance-genie/README.md`: rewrite the programmatic snippet to use `SparkSemanticLayerBuilder` and `SemanticLayerInput`.
- [ ] `examples/integration/finance-genie/`: if any tested entry point calls `run_dbxcarta` directly, migrate to the builder.
- [ ] `examples/integration/schemapile/` and `examples/integration/dense-schema/`: same migration if applicable.
- [ ] External `sql-semantics` repo: add `src/sql_semantics/programmatic_build.py` (or similar) that constructs `SparkSemanticLayerBuilder` and demonstrates `build_semantic_layer(SemanticLayerInput(...))`. Update its README to point to the new pattern.
- [ ] Internal examples tests under `tests/examples/` exercise the new builder path.

### Phase 6 — Boundary tests and documentation

**Outcome:** v2 surface is guarded by tests and documented.

Checklist:

- [ ] Extend `tests/boundary/test_import_boundaries.py` with a test that imports `dbxcarta.core` and asserts `SemanticLayerInput`, `SemanticLayerResult`, `SemanticLayerBuilder` are exposed by name. Catches accidental removal.
- [ ] Confirm subprocess-import boundary tests and the AST-level cross-layer import scan remain green.
- [ ] Update repo `README.md` section on "building a semantic layer" to use the v2 pattern.
- [ ] Mark this proposal `Implemented` once the cutover lands.

---

## Merge gate

The cutover PR can merge when, and only when:

- `pytest tests/core/` passes in a `dbxcarta-core`-only environment.
- `pytest tests/spark/`, `tests/client/`, `tests/presets/` each pass in their minimal environments.
- `pytest tests/boundary/` passes; subprocess import checks and AST scan both clean; v2 surface assertions pass.
- Sample consumers (Finance Genie, SchemaPile, dense-schema, `sql-semantics`) build and test green against the cutover branch.
- `pip install dbxcarta-core` still registers zero console scripts; only `dbxcarta-presets` registers the `dbxcarta` CLI.
- The README and example READMEs describe the v2 builder/Input pattern.

---

## Risks

- **Reach.** Every existing caller of `run_dbxcarta`, `SparkSemanticLayerBuilder`, and `SemanticLayerConfig` updates in this PR. Mitigation: phase 2 lands signatures and phase 5 lands example fixes in the same commit; CI matrix gates.
- **Result shape may need to grow.** A future caller may want verify-report fields, warning counts, or per-label timing. Mitigation: `SemanticLayerResult` is a frozen dataclass with default-factory fields; adding a field is non-breaking. Resist adding fields preemptively.
- **The strict Input may feel thin.** Callers used to passing many flags may want runtime override of behavioral toggles. Mitigation: document the dependency-injection pattern (configure builder once, call many times). If a real consumer demands runtime override of a single behavioral flag, add a single `Optional[bool] = None` field — additive and principled.
- **`run_id` semantics.** Today the spark backend reads `DATABRICKS_JOB_RUN_ID` or falls back to `"local"`. Different backends will have different identifier conventions. Mitigation: document `run_id` as opaque; do not promise format.
- **External `sql-semantics` checkout drift.** Same risk noted in v1. Mitigation: same — coordinated branch, retested from a clean environment after publish.

---

## Open questions deferred to first follow-up

These are intentionally not in v2 because the right answer depends on having a second backend or a real consumer:

- Should `SemanticLayerInput` grow `include_sample_values: bool | None`? Defensible if a second backend supports it; premature today.
- Should `SemanticLayerResult` carry a verify `Report`? Defensible once verify outcomes drive consumer behavior; today verify violations log/raise inside `run_dbxcarta`.
- Should `SemanticLayerBuilder` expose a separate `verify(...)` method? Defensible if verify becomes its own backend-pluggable concern; today it is internal to spark.

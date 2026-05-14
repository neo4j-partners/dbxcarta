# Clean Boundaries Proposal

**Status: Proposed**

This proposal separates dbxcarta into a small stable semantic-layer core plus
opt-in extension layers. The intent is to keep core focused on semantic graph
creation while moving Spark job orchestration, client evaluation, presets,
questions, and synthetic data helpers into clearly named non-core surfaces.

The current package has a useful public API discipline, but the boundaries have
started to blur. The top-level package exports both semantic-layer creation and
client execution. Preset protocols mention demo-question upload. Example
presets repeat readiness and upload helpers. Dense-schema and SchemaPile repeat
synthetic materialization utilities. Those are useful capabilities, but they do
not all belong in the core semantic-layer library.

The best practice is to split by responsibility and dependency pressure:

- **Core:** stable semantic graph contracts and semantic-layer creation API.
- **Spark extension:** Databricks Spark extraction, transform, embedding, and
  graph loading job implementation.
- **Client extension:** query-time retrieval, prompt construction, SQL parsing,
  result comparison, and optional evaluation harness.
- **Preset extension:** optional environment adapters and readiness helpers for
  repeatable operational workflows.
- **Examples:** domain fixtures, client questions, generated data,
  demonstration CLIs, and synthetic benchmark materialization.

This mirrors the useful pattern in `neo4j-agent-memory`: a small required
install, optional dependency groups for capabilities such as CLI, MCP,
embeddings, extraction, and integrations, and rich examples outside the core
runtime. dbxcarta should copy the boundary discipline, but avoid copying the
large top-level re-export style. The top-level `dbxcarta` package should remain
small and deliberate.

---

## Goal

Make dbxcarta easy to understand and safe to consume as a library:

- A consumer can install the core library without pulling in demo, client,
  benchmark, or Databricks job-runner concerns.
- Semantic-layer creation has one clear public contract.
- Spark job orchestration is an opt-in implementation layer, not the identity
  of the whole library.
- Query-time client utilities are optional and do not define the core package.
- Presets remain useful, but they are operational adapters rather than core
  semantic concepts.
- Example helpers stay in examples or example-support packages.

---

## Non-Goals

- Do not remove the existing examples.
- Do not remove the CLI immediately.
- Do not introduce plugin auto-discovery or a central preset registry.
- Do not move synthetic benchmark materialization into core.
- Do not make client questions or evaluation arms part of the core semantic
  graph contract.
- Do not require multiple packages on day one if an incremental single-repo
  migration is safer.

---

## Target Boundaries

### Core Layer

Core should contain only stable concepts required to define and create the
semantic layer:

- Graph contract enums and constants.
- Stable identifier generation.
- Settings models that describe semantic-layer creation.
- Databricks identifier and path validation helpers.
- Public semantic-layer creation entrypoint.
- Verification primitives that validate created graph output.
- Minimal protocols only when they describe semantic-layer creation itself.

Core should not contain:

- Client questions.
- Evaluation arms.
- Demo question upload.
- Synthetic table materialization.
- Example readiness checks.
- Framework-specific client behavior.
- Databricks job submission as the primary integration surface.

### Spark Extension Layer

The Spark layer should own the Databricks data pipeline implementation:

- Unity Catalog metadata extraction.
- Spark transforms.
- Embedding generation inside Spark.
- Staging and ledger implementation.
- Neo4j Spark connector writes.
- Databricks wheel entrypoint for ingestion.
- Job-runner or Databricks Jobs submission helpers.

This layer may depend on Databricks and Spark-specific packages. Core should
not depend on this layer at import time.

### Client Extension Layer

The client layer should own query-time utilities:

- Graph retriever.
- Context bundle rendering.
- Question embedding.
- Prompt construction.
- SQL parsing and read-only SQL guards.
- Result comparison.
- Optional Text2SQL evaluation harness.

Client questions and arms belong here or in an evaluation sublayer, not in core.
If evaluation remains bundled, it should be explicitly named as evaluation code,
not generic client runtime.

### Preset Extension Layer

The preset layer should own optional operational adapters:

- Environment overlay builders.
- Preset loading from import specs.
- Optional readiness check protocols.
- Optional question upload helpers.
- CLI actions that operate on presets.

The core package can define a very small configuration protocol if needed, but
question upload and readiness checks should not be promoted as core semantic
layer concepts.

### Examples And Example Support

Examples should own domain-specific and benchmark-specific helpers:

- Finance Genie questions and local demo.
- SchemaPile candidate selection.
- Dense-schema generation.
- Synthetic materialization utilities.
- Question generation and validation for synthetic benchmarks.
- Example-specific bootstrap scripts.

If dense-schema and SchemaPile need shared utilities, put them in an
example-support package or a shared examples module. Do not move them into
core.

---

## Proposed Package Shape

Preferred long-term shape:

- `dbxcarta-core`: core semantic graph contract and semantic-layer API.
- `dbxcarta-spark`: Databricks Spark pipeline implementation and ingest job
  entrypoints.
- `dbxcarta-client`: query-time retrieval and Text2SQL utilities.
- `dbxcarta-presets`: optional preset protocol and CLI helpers.
- `dbxcarta-example-utils`: optional shared utilities for examples only, if the
  duplicated example code justifies it.

Acceptable incremental shape before splitting distributions:

- `dbxcarta.core`
- `dbxcarta.spark`
- `dbxcarta.client`
- `dbxcarta.presets`
- `dbxcarta.example_support`

The incremental shape should still enforce import boundaries and optional
dependencies so core does not import Spark, client, presets, or example-support
modules during normal import.

---

## Lessons From agent-memory

Patterns worth copying:

- Keep required dependencies small.
- Use optional extras for capability layers.
- Put framework or operational adapters under clearly named extension
  namespaces.
- Keep rich examples outside the core package.
- Provide testing helpers under a named testing namespace, not mixed into core.
- Make missing optional dependencies fail with actionable messages.

Patterns to avoid copying:

- Do not re-export too many optional integrations from the top-level package.
- Do not let convenience imports make the public API look larger than it is.
- Do not hide extension boundaries behind one large client object unless that
  object is truly the product surface.

---

## Assumptions

- Existing users are still primarily local development, examples, and Finance
  Genie style consumers.
- Breaking changes are acceptable if they are planned and documented before a
  stable external release.
- The current examples remain valuable, but they should not determine the core
  package contract.
- Databricks Spark remains the first supported implementation of semantic-layer
  creation, but not the only conceptual layer in the package design.
- The repo can migrate in phases without immediately publishing multiple
  distributions.

---

## Risks

- Moving too much at once can break the working examples and obscure the actual
  public contract.
- Keeping everything in one distribution too long can preserve accidental
  coupling even if module names improve.
- Splitting packages before boundaries are proven can create release overhead
  and dependency friction.
- Leaving `run_client` at top level will continue to imply client evaluation is
  part of core.
- Leaving question upload in core preset protocols will continue to mix demo
  support with semantic-layer creation.
- Moving duplicated example helpers into core would make benchmark fixture code
  look like product API.

---

## Phase Checklist

### Phase 1: Define And Document The Public Core

**Status: Pending**

**Outcome:** The repo has a written public contract for the core semantic-layer
library.

Checklist:

- [ ] List the symbols that belong in the core public API.
- [ ] List the symbols currently exported from `dbxcarta.__init__` that should
  move to extension layers.
- [ ] Define whether `run_dbxcarta` remains core or becomes a Spark extension
  entrypoint wrapped by core.
- [ ] Document that client questions, arms, question upload, and synthetic
  materialization are non-core.
- [ ] Add import-boundary tests that prove core import does not require Spark,
  Databricks job runner, client evaluation, or example code.

Validation:

- A new reader can explain what core does without mentioning examples,
  questions, evaluation arms, or synthetic fixtures.
- `import dbxcarta` exposes only semantic-layer concepts or intentionally
  stable compatibility symbols.

### Phase 2: Create Extension Namespaces

**Status: Pending**

**Outcome:** The codebase has clear module boundaries even before separate
distributions exist.

Checklist:

- [ ] Create or rename modules so semantic graph contract lives under a core
  namespace.
- [ ] Move Spark pipeline code under a Spark-specific namespace.
- [ ] Keep client retrieval and evaluation under a client-specific namespace.
- [ ] Move preset helpers under a preset-specific namespace.
- [ ] Make legacy import paths compatibility shims where needed.
- [ ] Update tests to import from the intended namespaces.

Validation:

- Core modules do not import extension modules.
- Extension modules may import core, but not the reverse.
- Compatibility shims are documented as temporary.

### Phase 3: Separate Preset And Demo Concerns

**Status: Pending**

**Outcome:** Presets no longer imply that demo-question upload and readiness
checks are core semantic-layer capabilities.

Checklist:

- [ ] Reduce the core preset concept to configuration, or move the full preset
  concept to the preset extension layer.
- [ ] Move question upload helper behavior out of core-facing documentation.
- [ ] Move readiness helpers into preset extension or examples.
- [ ] Add shared preset helper functions only in the preset extension layer.
- [ ] Update Finance Genie, SchemaPile, and dense-schema examples to consume
  preset helpers from the extension layer or own them locally.

Validation:

- A minimal preset can be described as configuration only.
- Demo-question upload remains possible without being part of core.
- Readiness checks remain possible without being part of core.

### Phase 4: Split Client Runtime From Evaluation Harness

**Status: Pending**

**Outcome:** Query-time retrieval utilities are distinct from benchmark and
Text2SQL evaluation workflows.

Checklist:

- [ ] Identify client modules that are reusable runtime primitives.
- [ ] Identify modules that are benchmark or evaluation harness code.
- [ ] Move questions and arms into an evaluation sublayer or example package.
- [ ] Keep retriever, context rendering, prompt helpers, SQL parsing, and result
  comparison in the client extension when broadly reusable.
- [ ] Remove `run_client` from the top-level core public surface or mark it as a
  compatibility export with a deprecation plan.

Validation:

- A consumer can use graph retrieval without adopting question fixtures or eval
  arms.
- The evaluation harness can still run from examples or an explicit client/eval
  command.

### Phase 5: Move Synthetic Materialization Out Of Product API

**Status: Pending**

**Outcome:** Dense-schema and SchemaPile shared utilities are deduplicated
without polluting core.

Checklist:

- [ ] Inventory duplicated materialization helpers in dense-schema and
  SchemaPile.
- [ ] Decide whether duplication is acceptable or whether an example-support
  package is warranted.
- [ ] If shared, place synthetic type coercion, name sanitization, SQL literal
  rendering, and insert generation in example support.
- [ ] Keep benchmark question generation and validation in examples or example
  support.
- [ ] Ensure no core docs present synthetic materialization as a library
  capability.

Validation:

- Core imports no synthetic materialization utilities.
- Dense-schema and SchemaPile still run or test through their example paths.
- Shared example utilities are named so users do not confuse them with core.

### Phase 6: Introduce Optional Dependencies And Packaging Boundaries

**Status: Pending**

**Outcome:** Installation shape reflects the architecture.

Checklist:

- [ ] Define minimal required dependencies for core.
- [ ] Move Spark-only dependencies behind a Spark extra or separate package.
- [ ] Move client-only dependencies behind a client extra or separate package.
- [ ] Move preset CLI dependencies behind a preset or CLI extra.
- [ ] Keep test and example dependencies out of required install.
- [ ] Decide when to split into multiple published distributions.

Validation:

- Base install remains small.
- Installing client, Spark, or presets is an explicit choice.
- Optional dependency errors are actionable.

### Phase 7: Update Documentation And Migration Notes

**Status: Pending**

**Outcome:** Users understand the new model and can migrate safely.

Checklist:

- [ ] Update README to present core first and extensions second.
- [ ] Add a boundary diagram or table that maps capabilities to layers.
- [ ] Add migration notes for old imports and CLI commands.
- [ ] Update example READMEs to state which extension layers they use.
- [ ] Document deprecation timelines for compatibility exports.

Validation:

- A new consumer can choose the right layer without reading source code.
- Existing examples remain discoverable and runnable.
- Deprecated paths have clear replacements.

---

## Completion Criteria

The boundary cleanup is complete when:

- Core can be imported without Spark, job-runner, client evaluation, or examples.
- The top-level public API exposes only core concepts and deliberate
  compatibility symbols.
- Spark pipeline code has a named extension boundary.
- Client runtime code has a named extension boundary.
- Evaluation questions and arms are outside core.
- Preset readiness and question upload are outside core.
- Synthetic materialization utilities are outside core.
- README and examples describe the same architecture the code enforces.
- Tests cover import boundaries and representative example workflows.

---

## Recommended First Implementation Step

Start with documentation and import-boundary tests before moving files. The
first implementation should make the intended core public API explicit and
verify that importing it does not load Spark, Databricks job-runner, client
evaluation, or example modules. That creates a guardrail for every later phase.

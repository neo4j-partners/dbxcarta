# Clean Boundaries Proposal

**Status: Accepted**

**Migration mode: single atomic cutover.** The entire plan ships as one PR
and one commit. The work areas in the execution plan are decomposition for
review, not separate landings. Sample consumers (Finance Genie, SchemaPile,
dense-schema, `sql-semantics`) are updated inside the same PR.

## Design Decisions

The eleven decisions below settle the questions that drove this proposal.
Each section restates the question for context, then captures the final
decision and the best practices behind it.

### 1. Where does `run_dbxcarta` live: core or spark extension?

This is THE pivotal question. `run_dbxcarta` is the function that does the
actual semantic-layer build: read Unity Catalog metadata, compute embeddings,
write to Neo4j.

- **Keep in core:** core transitively depends on Spark, Databricks SDK, and the
  Neo4j connector. The boundary cleanup becomes namespace-deep but not
  dependency-deep. A consumer who only wants the graph contract still pulls
  the full Spark stack. The optional-extras goal fails.
- **Move to spark extension:** core becomes genuinely small (contract, IDs,
  validators, Settings). Breaks `from dbxcarta import run_dbxcarta` for every
  existing consumer. Since the only consumers are internal sample consumers
  updated as part of this proposal, that break is the explicit cutover.

**Final Decision:** Move `run_dbxcarta` to the spark extension. Core owns the
protocol for "build a semantic layer." Each extension owns one concrete
implementation. The point of the split is that future backends (BigQuery,
Neo4j-native re-projection, anything else that produces the same semantic
graph contract) can live as new extensions without touching core.

Concrete shape:

- `dbxcarta.core` defines a `SemanticLayerBuilder` protocol or a
  `build_semantic_layer(config) -> SemanticLayer` abstract entrypoint, plus
  the graph contract types and verification primitives.
- `dbxcarta.spark.run` implements that protocol against Databricks/Spark.
- A future `dbxcarta.bigquery.run` implements it against BigQuery without
  changing core.

Cutover, not compatibility: `dbxcarta.run_dbxcarta` is removed entirely.
Every internal caller (tests, examples, sample consumers like
`sql-semantics`) is updated to import from `dbxcarta.spark.run` in the
same PR as the move. No re-export, no deprecation warning, no shim. The
library is new enough that there is no external API contract to preserve.

**Best practices:**

- Ports and adapters: core owns abstractions, extensions own
  implementations. The contract is the product. The implementation is
  replaceable.
- Enforce the boundary with import tests (Decision 9), not just module
  naming.
- Use optional extras (`pip install dbxcarta[spark]`) with actionable
  `ImportError` messages when the requested backend is missing.
- Resist putting a "default" implementation in core. If core references a
  concrete backend, the boundary leaks and a second backend is no longer
  cheap to add.

### 2. Where does the `dbxcarta` CLI live?

The CLI today straddles every layer:

- `upload`, `submit`, `submit-entrypoint`, `validate`, `logs`, `clean` are
  spark/orchestration.
- `preset` is preset extension.
- `catalog`, `schema`, `volume` are thin Databricks SDK wrappers.

Putting the CLI in core means core transitively requires every layer.
Splitting into per-layer CLIs ships four binaries which is bad UX. Likely
answer is "CLI lives in `dbxcarta-presets`" treating it as an operational tool
that depends on the other layers, but this is not stated in the proposal.

**Final Decision:** Start with the CLI in `dbxcarta-presets` as an
operational umbrella that depends on `dbxcarta-spark` and `dbxcarta-client`.
This matches today's behavior, avoids a new plugin mechanism, and keeps the
install story simple. A user who wants the CLI runs `pip install
dbxcarta-presets` and gets the operational tool plus everything it
orchestrates. A user who only needs the contract installs `dbxcarta-core`
and gets no CLI.

If later a new extension needs to register subcommands without depending on
`dbxcarta-presets`, migrate to a plugin pattern via entry points
(`[project.entry-points."dbxcarta.cli_commands"]`). Defer that complexity
until at least one concrete case demands it.

**Best practices:**

- Treat the CLI as an application, not a library. Its distribution depends
  on every layer it touches.
- Console scripts live in the distribution that owns the code they invoke
  (Decision 3), not in an aggregator.
- Avoid shipping multiple binaries to the user. One CLI with subcommands is
  better UX than four separate executables.
- Plugin discovery via entry points is the standard escape hatch when
  subcommand authors and the CLI shell live in different distributions; it
  is overkill before that happens.

### 3. Who owns the `dbxcarta-ingest` and `dbxcarta-client` console scripts?

`pyproject.toml [project.scripts]` currently exposes these on the `dbxcarta`
distribution. They are the entry points a Databricks `python_wheel_task`
invokes. After a multi-distribution split they almost certainly live in
`dbxcarta-spark` and `dbxcarta-client` respectively, but the proposal does not
say so. This decision determines what a job library install actually pulls
onto the cluster.

**Final Decision:**

- `dbxcarta-ingest` lives in `dbxcarta-spark`. It is the wheel entry point
  for the Databricks ingest job, and the wheel uploaded to the cluster must
  already carry Spark, Databricks SDK, and Neo4j connector dependencies.
- `dbxcarta-client` lives in `dbxcarta-client`. It is the client runtime
  and evaluation entry point.
- The top-level `dbxcarta` CLI lives in whichever package owns the CLI
  (Decision 2).

**Best practices:**

- A console script and the code it invokes ship in the same distribution.
  Do not define a script in package A whose body imports package B unless B
  is in A's required dependencies.
- For Databricks `python_wheel_task`, the wheel that registers the entry
  point is the wheel uploaded to Volumes. Putting `dbxcarta-ingest` in
  `dbxcarta-spark` means the spark wheel is what gets uploaded, not core.
- Verify after the split: `pip install dbxcarta-core` registers zero
  console scripts. If it registers any, core has leaked an operational
  concern.

### 4. Are cross-extension imports allowed, and under what rule?

The proposal states "extension modules may import core, but not the reverse."
It does not address spark→preset, spark→client, or client→spark imports.

This question has immediate code consequences. The recent SSOT change in
`dbxcarta/entrypoints/_bootstrap.py` adds a lazy `spark→preset` import so the
entry-point bootstrap can resolve `DBXCARTA_PRESET` and overlay
`preset.env()`. Allowed or not?

**Final Decision:** Forbid direct extension-to-extension imports. Every
extension imports core; no extension imports another extension. When
extension A needs behavior from extension B, mediate through core:

1. Core defines a protocol that captures the needed behavior.
2. Extension B implements the protocol.
3. Extension A accepts an instance of the protocol via dependency
   injection.
4. The caller (CLI, job, test) wires them together.

For the `_bootstrap` change: lift the preset env-overlay function into
`dbxcarta.core` as a small protocol (e.g. `EnvOverlay`). Presets implement
it. The spark entry point calls through the core protocol, never through
`dbxcarta.presets` directly. This removes the `spark→preset` edge without
losing the SSOT behavior the recent change introduced.

No transitional lazy direct imports. The lift into core happens in the
same PR as the rule taking effect. If a cross-extension import exists
today (such as `spark→preset` in `_bootstrap`), the fix is to lift the
helper, not to mark the import as temporary.

**Best practices:**

- Hub-and-spoke through core, never a ring of extensions.
- `typing.Protocol` (structural) or ABCs (nominal) both work for the
  contract; pick one convention and use it everywhere.
- Dependency injection at the application boundary, not deep inside
  extension code.
- `ImportError` messages name the extra to install: "install
  `dbxcarta[preset]` to use presets."

### 5. How do Settings models split across layers?

Today `Settings` is roughly core-ish and `ClientSettings` is client-layer.
After a split, does each extension carry its own Settings model? How do they
compose at runtime when a job needs both (e.g. ingest reads catalog/schema
plus embedding endpoint)? The proposal mentions "Settings models that
describe semantic-layer creation" stay in core but does not address
composition.

**Final Decision:** One Settings model per layer. Compose them via nesting
at the application boundary rather than via a single mega-Settings in core.

Concrete shape:

- `dbxcarta.core.SemanticLayerConfig` carries only what defines a semantic
  layer at the contract level: graph store target URI, source catalog
  identifiers, embedding endpoint URI as an opaque string. No Spark, no
  Databricks SDK, no Neo4j driver objects.
- `dbxcarta.spark.SparkIngestSettings` carries Spark and Databricks knobs:
  cluster runtime, embedding compute, staging volume paths.
- `dbxcarta.client.ClientSettings` carries retrieval knobs: top-k, prompt
  template, SQL parser flavor.
- `dbxcarta.presets.Preset` is the composition envelope. It bundles a
  `SemanticLayerConfig` plus the optional extension settings for an
  operational target, and exposes the env-overlay function (Decision 4).

Each Settings model uses its own env prefix (`DBXCARTA_CORE_`,
`DBXCARTA_SPARK_`, `DBXCARTA_CLIENT_`). A job constructs only the models
for the layers it uses. The ingest job constructs `SemanticLayerConfig +
SparkIngestSettings`. The client runtime constructs `SemanticLayerConfig +
ClientSettings`.

**Best practices:**

- Settings models hold data, not runtime objects. No Spark sessions, no
  Neo4j drivers, no embedding clients are constructed inside Settings.
- One layer's Settings should load and validate without other layers
  installed.
- Pydantic `model_config` env prefixes namespace env vars across layers
  without collisions.
- Document required vs optional fields in code through Pydantic field
  defaults and validators, not in prose.

### 6. Is `run_client` "client runtime" or "evaluation harness"?

The original proposal named this ambiguity but did not resolve it. Today `run_client`
runs question fixtures through multiple retrieval arms and compares results.
That is benchmark/evaluation behavior, not generic graph retrieval. The
generic retriever, prompt helpers, and SQL guards are reusable client
runtime. The proposal should either name an `evaluation` sublayer or move the
fixtures and arms to examples and keep `run_client` as a sample binary.

**Final Decision:** Split. Treat runtime and evaluation as two distinct
surfaces.

- `dbxcarta-client` owns reusable runtime: graph retriever, context bundle
  rendering, question embedding, prompt construction, SQL parsing,
  read-only SQL guards, result comparison.
- An evaluation sublayer `dbxcarta.client.eval` owns the harness: arms,
  comparison orchestration, the `run_client` entry point. No separate
  `dbxcarta-eval` distribution; it lives inside `dbxcarta-client`.
- Question fixtures (the concrete test question lists) leave the library
  entirely. They belong in examples or a consumer-owned package. The
  library should not ship opinionated test data.

`run_client` is an evaluation binary. Remove it from the top-level public
API and move it to `dbxcarta.client.eval`.

**Best practices:**

- Library code is what consumers import. Evaluation is what library
  authors use to validate the library. Different audiences, different
  surfaces.
- Reusable retrieval primitives can be tested against any question set,
  including consumer-owned ones. Shipping a question set inside the
  library couples release cycles to fixture churn.
- An eval harness importing retriever primitives is fine. A retriever
  primitive importing the harness is a layering bug.

### 7. What is the cutover strategy for sample consumers?

dbxcarta is a new library. The only existing consumers are internal sample
consumers (Finance Genie, SchemaPile, dense-schema) and the external
sample `sql-semantics` package. There is no published stable API and no
external user base depending on import paths. Every boundary change can
be a clean cutover, not a deprecation campaign.

**Final Decision:** Clean cutover. No compatibility re-exports, no shims,
no `DeprecationWarning` plumbing. For each symbol the cutover moves:

- Enumerate every consumer that imports the symbol at its old path
  (internal tests, examples, scripts, and named external samples).
- Update them in the same PR as the move.
- CI gate: the PR cannot land until every named consumer passes against
  the new layout.

This is viable because the consumer set is enumerable and
internal-controlled. It would be the wrong call for a library with
anonymous external users on a stable major version. For dbxcarta with
named sample consumers it is the right call.

**Best practices:**

- Pre-1.0 libraries cut over cleanly. Shim trails calcify into permanent
  extras and never get removed.
- Enumerate consumers explicitly. An unwritten "we will catch the
  breakage" is not a plan.
- Atomic moves with paired consumer fixes are easier to review than a
  multi-PR shim dance.
- A real deprecation policy is something a library earns by having
  anonymous external users. dbxcarta is not there yet, so the policy
  does not exist yet.

### 8. How do tests reorganize across the new layers?

Current tests are organized by feature (`tests/unit/fk_discovery/`,
`tests/unit/embeddings/`, etc.). Post-split, tests should mirror the layer
structure so each test only imports from the layer it tests plus core. This
is a non-trivial reorg the proposal does not address.

**Final Decision:** Mirror the source layer structure in tests, and run the
import-boundary check in CI on every PR.

```text
tests/
  core/         only imports dbxcarta.core
  spark/        imports dbxcarta.core and dbxcarta.spark
  client/       imports dbxcarta.core and dbxcarta.client
  presets/      imports dbxcarta.core and dbxcarta.presets
  examples/     integration tests against tested examples
  boundary/     import-boundary guard tests (Decision 9)
```

Each layer's test directory must run independently. `pytest tests/core/`
must pass with only `dbxcarta-core` installed. That is what forces honest
layering rather than nominal layering. Running `tests/spark/` requires the
spark extras; running `tests/client/` requires the client extras.

A CI matrix executes each layer's tests in a minimal environment that
contains only the deps that layer is allowed to need. The matrix is the
proof that extras are honest.

**Best practices:**

- Tests mirror source structure. A test for layer X imports only layer X
  and core.
- The CI matrix is the proof that optional extras work as advertised, not
  the README claim.
- A `tests/boundary/` directory holds the import discipline tests so they
  are easy to find and to extend when new layers appear.
- Each test directory has its own `conftest.py` that asserts the
  forbidden extras are not present in the environment when the test
  module loads.

### 9. What is the exact form of the import-boundary test?

The original proposal says "add import-boundary tests" but does not
specify the assertion form. The standard pattern is a subprocess that imports the core entry and
inspects `sys.modules` to confirm no forbidden modules loaded:

```python
result = subprocess.check_output(
    [sys.executable, "-c",
     "import dbxcarta; import sys; print('\n'.join(sys.modules))"]
)
forbidden = {"pyspark", "neo4j", "databricks.sdk", ...}
assert not (forbidden & set(result.splitlines()))
```

**Final Decision:** Codify the subprocess sketch as an explicit pytest test
with a fixed forbidden set. The test is an acceptance criterion of the
cutover commit.

```python
import subprocess
import sys

FORBIDDEN_FOR_CORE = {
    "pyspark", "py4j",
    "neo4j", "neo4j_spark_connector",
    "databricks.sdk",
    "dbxcarta.spark", "dbxcarta.client", "dbxcarta.presets",
}

def test_core_does_not_load_spark_or_databricks() -> None:
    result = subprocess.run(
        [sys.executable, "-c",
         "import dbxcarta.core; import sys; "
         "print('\\n'.join(sorted(sys.modules)))"],
        check=True,
        capture_output=True,
        text=True,
    )
    loaded = set(result.stdout.splitlines())
    leaked = {
        m for m in loaded
        if m in FORBIDDEN_FOR_CORE or m.split(".")[0] in FORBIDDEN_FOR_CORE
    }
    assert not leaked, f"Forbidden modules loaded by dbxcarta.core: {leaked}"
```

Subprocess isolation is essential. A previous test in the same process may
have already loaded forbidden modules and would mask the regression.

The forbidden set is data, checked into the repo. Reviewers see at a glance
what core promises not to pull.

Extend the same pattern to other layers: `dbxcarta.client` must not load
Spark or Databricks SDK; `dbxcarta.spark` must not load client eval. Each
gets its own subprocess test with its own forbidden set.

**Best practices:**

- One subprocess per assertion. Each top-level package gets its own test.
- Forbidden set is data, not logic.
- Fail-fast with a message that names the leaked modules.
- Run on every PR, not just nightly.
- When a new transitive dependency is added, the test forces the author to
  decide whether the dep belongs in core or in an extension.

### 10. Are examples first-class consumers or just demos?

`examples/finance-genie/` is BOTH a worked example AND a tested package
consumed by `tests/`. The external `sql-semantics` package is a near-copy of
the internal one. The proposal treats "examples" as one bucket; in practice
some examples are demo fixtures and others are integration-test consumers
that must keep working through the cutover.

**Final Decision:** Distinguish two kinds explicitly in the directory
layout and in the CI matrix.

- `examples/demos/` holds reference snippets and walkthroughs. May be
  updated, replaced, or deleted as part of a boundary change. Not
  migration-blocking.
- `examples/integration/` holds examples consumed by `tests/` or by
  external integration suites. These must keep working through every
  cutover. They are listed by name as merge-gate consumers (see
  [Sample Consumers](#sample-consumers)).

`finance-genie` today is a hybrid. Split it. The walkthrough stays in
`examples/demos/finance-genie/`. The tested fixtures and helpers move to
`examples/integration/finance-genie/`, or to a `dbxcarta-example-utils`
package if shared with other integration examples.

Each integration example has its own `pyproject.toml` and is installed by
CI. Each demo has a README stating "demo only, not migration-blocking."

**Best practices:**

- Tested examples are part of the public contract, even if not part of the
  core API. Treat changes to integration examples as breaking changes.
- A demo example nobody runs is dead code. Either delete it or promote it
  to tested.
- The migration plan lists every integration example by name as a
  merge-gate consumer, not as a footnote.
- External consumers (e.g. `sql-semantics`) get the same treatment as
  internal integration examples: their CI is the cutover merge gate
  (Decision 11).

### 11. How do sample consumers gate the cutover?

Sample consumers (such as `sql-semantics`) keep landing fixes and depend
on a stable preset surface. Two implications:

- The preset protocol (`Preset`, `ReadinessCheckable`, `QuestionsUploadable`)
  is the working contract today.
- The cutover PR cannot merge unless `sql-semantics` and every other
  named sample consumer still build and validate against the cutover
  branch.

**Final Decision:** Treat sample consumers as a CI merge gate on the
cutover PR.

Concrete actions:

- Add a CI job that builds and tests every named sample consumer
  (Finance Genie, SchemaPile, dense-schema, `sql-semantics`) against the
  cutover branch.
- Any update to the preset protocol (`Preset`, `ReadinessCheckable`,
  `QuestionsUploadable`) ships in the cutover commit alongside the
  matching sample-consumer fix. No protocol change lives outside the
  cutover commit.
- "Sample consumer CI is green" is the merge gate for the cutover PR.

**Best practices:**

- Sample consumer integration tests in CI are the truth signal, not
  internal unit tests alone.
- Protocol changes ship as one atomic change across the library and every
  sample consumer; never partial.
- The merge gate is the only gate. There are no per-phase gates because
  there are no per-phase landings.
- If the protocol is unstable, fix the protocol first, then ship the
  cutover.

---

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
- Do not remove the CLI.
- Do not introduce plugin auto-discovery or a central preset registry.
- Do not move synthetic benchmark materialization into core.
- Do not make client questions or evaluation arms part of the core semantic
  graph contract.
- Do not introduce compatibility shims, deprecation warnings, or
  transitional re-exports. The cutover is atomic.

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

## Package Shape

The cutover lands four published distributions in a single monorepo. Each
distribution owns its slice of the `dbxcarta.*` PEP 420 namespace package.
There is no top-level `dbxcarta/__init__.py`; consumers always import from
a layer (`dbxcarta.core`, `dbxcarta.spark`, ...).

### Distributions

| Distribution        | Import path        | Depends on                                   | Console scripts    |
| ------------------- | ------------------ | -------------------------------------------- | ------------------ |
| `dbxcarta-core`     | `dbxcarta.core`    | pydantic, neo4j driver                       | none               |
| `dbxcarta-spark`    | `dbxcarta.spark`   | `dbxcarta-core`, pyspark, databricks-sdk, neo4j-spark-connector | `dbxcarta-ingest` |
| `dbxcarta-client`   | `dbxcarta.client`  | `dbxcarta-core`, sqlparse                    | `dbxcarta-client`  |
| `dbxcarta-presets`  | `dbxcarta.presets` | `dbxcarta-core`, `dbxcarta-spark`, `dbxcarta-client` | `dbxcarta` (the CLI) |

### Repository layout

```text
packages/
  dbxcarta-core/
    pyproject.toml
    src/dbxcarta/core/
  dbxcarta-spark/
    pyproject.toml
    src/dbxcarta/spark/
  dbxcarta-client/
    pyproject.toml
    src/dbxcarta/client/
      eval/
  dbxcarta-presets/
    pyproject.toml
    src/dbxcarta/presets/
examples/
  demos/
  integration/
    finance-genie/
    schemapile/
    dense-schema/
tests/
  core/
  spark/
  client/
  presets/
  examples/
  boundary/
```

### Install patterns

- `pip install dbxcarta-core`: bare semantic-graph contract; no Spark,
  no client, no CLI.
- `pip install dbxcarta-spark`: Databricks ingestion path; what a
  `python_wheel_task` uploads.
- `pip install dbxcarta-client`: query-time retrieval and eval harness.
- `pip install dbxcarta-presets`: operational umbrella; pulls every
  layer and registers the `dbxcarta` CLI.

### Why this shape

- Each backend (Spark today, BigQuery or Neo4j-native tomorrow) is a
  separate distribution. Heavy, conflicting dependencies stay isolated.
- `dbxcarta-core` stays minimal so the contract can evolve without
  forcing reinstalls of heavyweight backends.
- The `dbxcarta` CLI lives in `dbxcarta-presets` so the binary appears
  only for users who installed the operational umbrella.
- Example utilities live inline under `examples/`. No
  `dbxcarta-example-utils` distribution: the duplication is small enough
  to deduplicate via a directory-local module if needed, not via a
  published wheel.

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

- Existing users are local development, examples, and Finance Genie style
  consumers. There are no anonymous external users on a stable API.
- All consumers are internal or named sample consumers updated atomically
  in the cutover commit. No compatibility shims, re-exports, or
  deprecation warnings.
- The current examples remain valuable, but they should not determine the core
  package contract.
- Databricks Spark is the first backend implementation. Core is designed so
  future backends (BigQuery, Neo4j-native re-projection) can land as new
  extensions without changing core.
- Multi-distribution packaging lands inside the cutover commit. Module
  paths and distribution names are committed up front, not deferred.

---

## Risks

- The cutover PR is large. Mitigation: organize commits by work area
  within the PR; require the import-boundary tests to pass at every
  work-area boundary.
- A bug in any work area blocks the whole cutover. Mitigation: ship the
  empty-package skeleton and the import-boundary tests first inside the
  PR so the structure validates before code moves.
- Sample-consumer fixes take longer than expected and stall the cutover.
  Mitigation: enumerate the consumer set up front; treat each consumer
  update as a precondition, not a follow-up.
- The `EnvOverlay` lift may surface a deeper protocol gap. Mitigation:
  prototype the lift before the cutover PR opens, not during review.
- New multi-distribution publishing machinery is unfamiliar work.
  Mitigation: validate the `pyproject.toml` layout locally with `uv build`
  and `pip install -e .` for each distribution before the cutover PR
  opens.
- The cutover branch diverges from main during review. Mitigation: open
  the PR early, rebase aggressively, freeze unrelated boundary churn on
  main until the cutover lands.

---

## Sample Consumers

The cutover commit updates every named sample consumer in the same PR.
These are the consumer set the CI merge gate validates against:

- `examples/integration/finance-genie/`: Finance Genie tested example.
- `examples/integration/schemapile/`: SchemaPile candidate selection
  and tests.
- `examples/integration/dense-schema/`: dense-schema generation and
  tests.
- `sql-semantics/` (external repo): sample consumer of the preset
  protocol and the core public surface.

A new consumer added in the future does not retroactively become a merge
gate; the gate is the named list at the time of the cutover PR.

---

## Execution Plan

The plan ships as **one atomic PR with one merge commit**. The work areas
below are the suggested decomposition for review and for tracking progress
inside the PR. Each work area maps to a logical section of the diff. None
of them ship independently.

The work areas are ordered to surface structural mistakes early: the
package skeleton and import-boundary tests land first inside the PR so the
boundaries validate against empty packages before code moves in.

### Work Area 1: Public Core Contract

**Outcome:** The repo has a written public contract for the core semantic-layer
library and a concrete `dbxcarta.core` skeleton.

Checklist:

- [ ] List the symbols that belong in the core public API.
- [ ] Define the `SemanticLayerBuilder` protocol (or
  `build_semantic_layer(config)` abstract entrypoint) in `dbxcarta.core`.
- [ ] Define `SemanticLayerConfig` Settings model in `dbxcarta.core`
  (Decision 5).
- [ ] Define the `EnvOverlay` protocol in `dbxcarta.core` so presets can
  implement it (Decision 4).
- [ ] Document that client questions, arms, question upload, and synthetic
  materialization are non-core.
- [ ] Land the import-boundary test from Decision 9 against the empty
  `dbxcarta.core` package so the guard is wired before code moves.

### Work Area 2: Layer Move And `_bootstrap` Lift

**Outcome:** Every module lives in its target layer; the `spark→preset`
import is removed.

Checklist:

- [ ] Move Spark pipeline code into `dbxcarta.spark`. Move `run_dbxcarta`
  to `dbxcarta.spark.run` (Decision 1).
- [ ] Move client retrieval into `dbxcarta.client`; move `run_client` and
  question fixtures into `dbxcarta.client.eval` (Decision 6).
- [ ] Move preset helpers into `dbxcarta.presets`.
- [ ] Lift `apply_preset_env_overlay` from `dbxcarta/entrypoints/_bootstrap.py`
  into `dbxcarta.core` as the `EnvOverlay` protocol; have presets
  implement it; rewire the spark entry point to call through the core
  protocol. Remove the `spark→preset` import (Decision 4).
- [ ] Update every internal call site to the new import paths. No
  compatibility shims, no re-exports.

Validation:

- Core modules do not import extension modules.
- Extension modules import core, never another extension.
- Old import paths are deleted, not redirected.

### Work Area 3: Preset And Demo Separation

**Outcome:** Presets no longer imply that demo-question upload and readiness
checks are core semantic-layer capabilities.

Checklist:

- [ ] Reduce the core preset concept to configuration only. The full
  preset concept lives in `dbxcarta.presets`.
- [ ] Move question upload helper behavior out of core-facing
  documentation.
- [ ] Move readiness helpers into `dbxcarta.presets` or `examples/`.
- [ ] Update Finance Genie, SchemaPile, and dense-schema examples to
  consume preset helpers from `dbxcarta.presets`.

### Work Area 4: Client Runtime / Evaluation Split

**Outcome:** Query-time retrieval utilities are distinct from benchmark and
Text2SQL evaluation workflows.

Checklist:

- [ ] Keep retriever, context rendering, prompt helpers, SQL parsing,
  and result comparison in `dbxcarta.client`.
- [ ] Move arms, comparison orchestration, and `run_client` into
  `dbxcarta.client.eval`.
- [ ] Remove `run_client` from any top-level public surface; update every
  caller.
- [ ] Question fixtures leave the library: move into
  `examples/integration/` or consumer-owned packages.

### Work Area 5: Synthetic Materialization Out Of Product API

**Outcome:** Dense-schema and SchemaPile shared utilities live inside
`examples/`, not in any library distribution.

Checklist:

- [ ] Move synthetic type coercion, name sanitization, SQL literal
  rendering, and insert generation under `examples/integration/`.
- [ ] If duplication remains between dense-schema and SchemaPile, share
  via a directory-local module inside `examples/`, not via a published
  distribution.
- [ ] Confirm no library distribution imports synthetic materialization
  utilities.

### Work Area 6: Multi-Distribution Packaging

**Outcome:** The four distributions in [Package Shape](#package-shape)
build, install, and register the right console scripts.

Checklist:

- [ ] Create `packages/dbxcarta-core/pyproject.toml`,
  `packages/dbxcarta-spark/pyproject.toml`,
  `packages/dbxcarta-client/pyproject.toml`,
  `packages/dbxcarta-presets/pyproject.toml`.
- [ ] Register `dbxcarta-ingest` console script in `dbxcarta-spark`.
- [ ] Register `dbxcarta-client` console script in `dbxcarta-client`.
- [ ] Register the `dbxcarta` CLI console script in `dbxcarta-presets`.
- [ ] Verify each distribution builds with `uv build` and installs in
  isolation.
- [ ] Verify `pip install dbxcarta-core` registers zero console scripts.
- [ ] Add the per-layer pytest matrix (Decision 8) to CI.

### Work Area 7: Sample Consumer Updates

**Outcome:** Every named sample consumer imports from the new paths and
passes its tests against the cutover branch.

Checklist:

- [ ] Update `examples/integration/finance-genie/` to import from
  `dbxcarta.core`, `dbxcarta.spark`, and `dbxcarta.presets`.
- [ ] Update `examples/integration/schemapile/` likewise.
- [ ] Update `examples/integration/dense-schema/` likewise.
- [ ] Update the `sql-semantics` external repo on a coordinated branch
  that is merged in lockstep with the cutover PR.
- [ ] Wire the sample-consumer CI job as the cutover PR's merge gate
  (Decision 11).

### Work Area 8: Documentation

**Outcome:** README and example READMEs reflect the new architecture.

Checklist:

- [ ] Update README to present core first and extensions second.
- [ ] Add a boundary diagram or table that maps capabilities to layers.
- [ ] Add migration notes that map old imports and CLI commands to their
  new locations, for anyone updating local checkouts of sample consumers.
- [ ] Update example READMEs to state which distributions they require.

---

## Merge Gate

The cutover PR can merge when, and only when, all of the following pass on
the cutover branch:

- `pytest tests/core/` passes with only `dbxcarta-core` installed.
- `pytest tests/spark/`, `tests/client/`, `tests/presets/` each pass in
  their own minimal environment (Decision 8).
- `pytest tests/boundary/` passes; no forbidden modules load when any
  distribution is imported (Decision 9).
- Each sample consumer in [Sample Consumers](#sample-consumers) builds
  and tests green against the cutover branch (Decision 11).
- `pip install dbxcarta-core` registers zero console scripts; the
  `dbxcarta` CLI appears only after `pip install dbxcarta-presets`.
- README and example READMEs describe the same architecture the code
  enforces.

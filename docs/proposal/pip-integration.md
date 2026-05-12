# Pip-Integration Plan: One Package Per Integration

**Status: Part A and Part A.5 complete. Part B not started. Live ingest validation and publish-location decision pending user.**

This is a phased plan to adopt the integration pattern used by LangChain and
LlamaIndex: keep `dbxcarta` as a lean core library and let each downstream
project ship its own small package that supplies its preset, registers itself
by import path, and is pulled in with a normal pip install. No central
registry, no plugin discovery, no forking of dbxcarta.

This is a clean migration. There are no current external consumers, so the
plan removes the built-in Finance Genie preset entirely rather than leaving a
shim or a compatibility path. After the migration, dbxcarta core knows
nothing about Finance Genie; the Finance Genie preset lives only as an
external package, with the in-repo `examples/finance-genie/` tree refactored
to be the canonical example of an external preset.

The plan has three parts. Part A puts dbxcarta core in a shape where it can
be consumed cleanly by an outside project. Part A.5 rewrites the in-repo
Finance Genie example as the reference external preset, living inside the
dbxcarta repo for documentation but built and consumed exactly like an
outside package. Part B then proves the pattern end to end by wiring up
`graph-on-databricks/finance-genie` as a real outside consumer.

---

## Progress at a glance

| Phase | Status | Notes |
|-------|--------|-------|
| A1 — Audit and define the public API | Complete | Audit at `worklog/pip-integration-a1-audit.md` |
| A2 — Make the package import-clean | Complete | Top-level re-exports; verified side-effect-free |
| A3 — Move runnable entrypoints inside the package | Complete | Wheel tasks submit `dbxcarta-{ingest,client}` |
| A4 — Formalize preset protocol and remove built-in | Complete | `Preset` + optional capabilities; built-in deleted |
| A5 — Distribution readiness | Mostly complete | Wheel clean; publish-location decision deferred |
| A5.1 — Reshape `examples/finance-genie/` as a standalone package | Complete | Standalone package builds independently |
| A5.2 — Implement example preset against the protocol | Mostly complete | Unit tests green; live end-to-end deferred |
| A5.3 — Update READMEs and docs | Complete | Forward-looking docs use import-path form |
| Run and test | Complete (offline) | Core 177 / Example 21 passing; both wheels build |
| **B1–B5** | **Not started** | Awaits Part A.5 sign-off and publish location |

---

## Part A: Prepare dbxcarta core for external consumption — COMPLETE

### Phase A1 — Complete: Audit and define the public API

- [x] Read through every module under `src/dbxcarta/` and list the symbols a
      downstream project would realistically need: ingest entry function,
      settings model, Neo4j client wrapper, preset protocol types,
      contract types, CLI helpers if any are reusable.
- [x] Mark everything else as internal. The goal is a small, deliberate
      surface, not a re-export of everything that happens to be importable
      today.
- [x] Decide on the top-level import shape. For example, what should be
      reachable as `from dbxcarta import ...` versus `from dbxcarta.ingest
      import ...` versus `from dbxcarta.presets import ...`.
- [x] Write the audit results into a short notes file in the worklog so the
      next phase has a checklist to implement against. No code yet.
- [x] Review with user before Phase A2.

**Result:** `worklog/pip-integration-a1-audit.md` records the audit. The
agreed top-level surface is the 18 symbols re-exported from
`src/dbxcarta/__init__.py`, including the new `Preset`,
`ReadinessCheckable`, `QuestionsUploadable`, `ReadinessReport`,
`load_preset`, and `format_env` added by later phases.

### Phase A2 — Complete: Make the package import-clean

- [x] Replace the near-empty top-level package docstring with deliberate
      re-exports that match the audit from A1.
- [x] Confirm that importing the package does not trigger heavy work: no
      Spark imports at module top level, no Neo4j connection attempts, no
      reading of environment variables at import time. Anything that touches
      the world must live inside a function or a lazily-instantiated object.
- [x] Verify the `py.typed` marker is still present and that mypy strict
      coverage still passes against the modules already listed in the
      pyproject overrides.
- [x] Confirm that every public symbol from the A1 audit can be imported
      from a brand-new Python process with only `pip install dbxcarta` and
      no environment variables set.
- [x] Review with user before Phase A3.

**Result:** `import dbxcarta` is side-effect-free under
`env -i uv run python -c "import dbxcarta"`. The stricter property — clean
import without Spark installed at all — was not satisfied at this point
because `run_dbxcarta` reached `sample_values.py`, which imported `pyspark`
and `py4j` at module load. That gap was closed during the review hardening
pass below, which also added a subprocess test that blocks `pyspark`,
`py4j`, and `delta` before importing the package. **Known caveat:** mypy
strict coverage flagged pre-existing errors in `contract.py:77/89`,
`ingest/summary.py`, and `ingest/preflight.py`. These predate this work and
are out of scope for this migration; they should be tracked separately.

### Phase A3 — Complete: Move runnable entrypoints inside the package

- [x] Today the Databricks-side runner lives in `scripts/run_dbxcarta.py`,
      which is a per-repo file. Create the canonical entrypoint inside the
      package itself, for example under `dbxcarta.entrypoints`, so any
      consumer can submit `python -m dbxcarta.entrypoints.ingest` against
      the installed wheel without copying any script files.
- [x] Delete the old `scripts/run_dbxcarta.py` outright. No shim, no
      forwarding stub. Update any internal demo flow that referenced it to
      call the new in-package entrypoint directly.
- [x] Do the same for the client entrypoint if the demo client is meant to
      be reusable. If it is example-only, leave it where it is and let
      Phase A.5 move it into the example package.
- [x] Confirm the demo flow still completes end-to-end after the cutover.
- [x] Review with user before Phase A4.

**Result:** New package `src/dbxcarta/entrypoints/` with `_bootstrap.py`
(the `inject_params` KEY=VALUE-to-env helper), `ingest.py`, and `client.py`.
`pyproject.toml` exposes them as `dbxcarta-ingest` and `dbxcarta-client`, and
`dbxcarta submit-entrypoint {ingest|client}` submits them as Databricks
`python_wheel_task`s. Deleted `scripts/run_dbxcarta.py`,
`scripts/run_dbxcarta_client.py`, `scripts/_cluster_bootstrap.py`. The
`dbxcarta.client.run_client` function stayed in core because it is
preset-agnostic; the Finance Genie-specific `local_demo.py` moved into the
example package in Phase A5.1.

### Phase A4 — Complete: Formalize preset protocol and remove built-in

- [x] Decide on the smallest possible contract a preset must satisfy. The
      shape should be: a function or attribute that returns the dbxcarta
      environment overlay, plus optional helpers for readiness checks and
      demo-question upload. Write this contract down in prose, in the
      public API docs, as a short section. No code yet.
- [x] Implement the preset protocol in dbxcarta core as types and helpers
      only. Core does not ship any preset implementations.
- [x] Delete the built-in Finance Genie preset module at
      `src/dbxcarta/presets/finance_genie.py` and the re-exports from
      `src/dbxcarta/presets/__init__.py`. The Finance Genie names, table
      lists, and readiness helpers leave dbxcarta core in this phase and
      do not come back.
- [x] Update the CLI so the preset name is always an import path, such as
      `mycorp_preset:preset`. There are no built-in shortcut names. The
      `dbxcarta preset finance-genie` form no longer exists; the
      equivalent post-migration form points at the external package
      created in Phase A.5.
- [x] Update or remove any tests, docs, and README sections that reference
      the old built-in preset name. The README should describe the new
      import-path form and nothing else.
- [x] Review with user before Phase A5.

**Result:** `src/dbxcarta/presets.py` defines `Preset` (required `env()`
method only), plus `ReadinessCheckable` and `QuestionsUploadable` as
separate `runtime_checkable` protocols for optional capabilities. This
shape was tightened during review when the original single-protocol design
was discovered to require all three methods at `isinstance` time, breaking
the documented "env is the only required method" promise.
`src/dbxcarta/preset_loader.py` provides `load_preset(spec)`. The CLI's
`dbxcarta preset <import-path>` subcommand drives all three actions via
`isinstance` checks against the optional protocols. The
`src/dbxcarta/presets/` package and `tests/unit/presets/test_finance_genie.py`
were deleted outright.

### Phase A5 — Mostly complete: Distribution readiness

- [x] Confirm the wheel build is reproducible and that the wheel contains
      exactly the modules the A1 audit expects and nothing more. In
      particular, examples and scripts must not be inside the wheel.
- [ ] **(pending user)** Decide where the wheel is published for downstream
      projects to consume. Options to discuss with the user: a private
      index, a git tag plus pip install from git URL, a UC Volume that
      downstream projects pull from, or public PyPI. No work is done until
      the user picks one.
- [x] Document the version contract in the README in plain prose: which
      symbols are public, what counts as a breaking change, how a
      downstream pins a known-good version.
- [x] Review with user before Part A.5.

**Result:** Wheel inspection confirms only `dbxcarta/*` modules ship; no
`scripts/`, `examples/`, `tests/`, `worklog/`, or `docs/` files are
included. README gained a "Public API and version contract" section. The
publish-location decision is the only blocker preventing this phase from
being fully complete.

---

## Part A.5: Rebuild the in-repo Finance Genie example as the reference external preset — MOSTLY COMPLETE

The goal of this part is to leave dbxcarta with one canonical worked example
of the new pattern, sitting in the repo for discoverability but structured
exactly the way a real outside consumer would structure their own preset.
By the end of A.5 the example is a separate package, built independently,
depending on dbxcarta as a normal pip dependency. There is no special path
that this example takes that an outside consumer would not also take.

### Phase A5.1 — Complete: Reshape the example tree as a standalone package

- [x] Convert `examples/finance-genie/` from a loose collection of scripts
      and notes into a self-contained Python package directory with its
      own project metadata file and its own source folder. Pick a package
      name that is distinct from anything inside dbxcarta core, for
      example `dbxcarta_finance_genie_example`.
- [x] The example package declares dbxcarta as a dependency by version,
      not as a path or editable reference back into the parent repo. The
      install instructions must work for a user who has only the example
      directory and a pip index, not the rest of the dbxcarta source.
- [x] Move any Finance Genie demo client code, demo questions fixtures,
      and run scripts that previously lived in `scripts/` or
      `src/dbxcarta/presets/` into the example package. The dbxcarta core
      source tree should contain no Finance Genie references by the end
      of this phase.
- [x] Review with user before Phase A5.2.

**Result:** New package `dbxcarta-finance-genie-example` (PyPI name) /
`dbxcarta_finance_genie_example` (import name) at
`examples/finance-genie/`, with its own `pyproject.toml`, `src/` layout,
and `tests/`. The `[project] dependencies` line is the version-pinned
`dbxcarta>=0.2.32` an outside consumer would also use. The
`[tool.uv.sources]` editable override is a dev-only convenience and does
not change the runtime install. `local_demo.py`, `upload_questions.py`,
and `questions.json` all moved into the package. dbxcarta core source
tree has zero Finance Genie references.

### Phase A5.2 — Mostly complete: Implement the preset against the new protocol

- [x] Inside the example package, implement a single preset object that
      satisfies the protocol defined in Phase A4. This object owns the
      Finance Genie environment overlay, the base and gold table lists,
      the readiness check, and the demo-questions upload helper.
- [x] Expose the preset under an import path, for example
      `dbxcarta_finance_genie_example:preset`, that can be passed
      verbatim to the dbxcarta CLI.
- [ ] **(pending user, requires live access)** Confirm by walking through
      the documented flow on a clean environment that installing dbxcarta
      plus the example package and pointing the CLI at the preset's
      import path produces the same Neo4j graph the old built-in
      produced. Same node counts by label, same relationship counts by
      type, same sampled embeddings.
- [x] Review with user before Phase A5.3.

**Result:** `finance_genie.py` (renamed from `preset.py` during review to
avoid a package-attribute vs submodule name clash) contains
`FinanceGeniePreset` plus the exported `preset` instance. The preset
implements `Preset` (required `env()`), `ReadinessCheckable`
(`readiness()`), and `QuestionsUploadable` (`upload_questions()`). The
env overlay matches the old built-in exactly (20 keys, byte-identical to
the previously captured `dbxcarta preset finance-genie --print-env`
output). The example test suite (21 tests) covers protocol satisfaction,
optional-capability protocol membership, env-validates-against-Settings,
readiness logic with mocked UC calls, and identifier validation. The live
end-to-end ingest run is the one outstanding item, blocked on the user's
go-ahead for compute and Neo4j writes.

### Phase A5.3 — Complete: Update docs and the top-level README

- [x] Rewrite the dbxcarta README's example section so that the Finance
      Genie flow is described as: install dbxcarta, install the example
      package, run the CLI with the preset import path. The README
      should never imply that dbxcarta core knows about Finance Genie.
- [x] Add a short prose section to the example package's own README that
      walks an outside reader through how the example is structured and
      why, so it functions as a template for a real external preset.
- [x] Remove any remaining mentions of the old built-in preset shortcut
      from docs, worklog notes, and command help text.
- [x] Review with user before Part B.

**Result:** `README.md`, `examples/finance-genie/README.md`, and inline
references inside `scripts/run_demo.py` updated to the new import-path
form. The example README has an upfront "What lives here, and why" plus a
"Template guidance for a new preset package" section that calls out the
four things to change when copying the example. Historical worklog and
`docs/validate-finance-genie.md` entries that recorded past completed
work were intentionally left as-is.

---

## Review hardening changes

During implementation review, several follow-up fixes were made to bring the
code and docs in line with the intent of this plan:

- **Added real wheel-task submission for package entrypoints.** The proposal
  moved runnable code into `dbxcarta.entrypoints`, but the documented
  `python -m dbxcarta.entrypoints.ingest` command would run locally rather
  than submit a Databricks job. `pyproject.toml` now exposes
  `dbxcarta-ingest` and `dbxcarta-client` console scripts, and
  `dbxcarta submit-entrypoint {ingest|client}` submits those entrypoints as
  Databricks `python_wheel_task`s against the uploaded wheel. This keeps the
  migration true to the "installed package, no copied runner script" goal.

- **Rewired internal automation to the new submit path.** `scripts/run_autotest.py`
  now submits `dbxcarta submit-entrypoint ingest` instead of the deleted
  `run_dbxcarta.py` script and no longer uploads scripts for the ingest path.
  `scripts/run_demo.py`, the top-level README, the Finance Genie example
  README, and `tests/fixtures/README.md` were updated to document the same
  wheel-task flow.

- **Made core import-clean without Spark installed.** Review found that
  top-level `import dbxcarta` re-exported `run_dbxcarta`, which reached
  `sample_values.py`; that module imported `pyspark` and `py4j` at module load
  even though Spark is not a core package dependency. The Phase A2 smoke check
  did not catch this because `uv run` activated the project venv where Spark
  is installed. Those imports were moved inside the functions that actually
  run on Databricks. A new unit test blocks `pyspark`, `py4j`, and `delta` in
  a subprocess and confirms that `import dbxcarta` still succeeds, which is
  the property an outside consumer who installs only the wheel actually
  needs.

- **Updated the remaining script bootstrap reference.** `scripts/run_spike_ai_query.py`
  now imports `inject_params` from `dbxcarta.entrypoints._bootstrap` instead
  of the deleted `scripts/_cluster_bootstrap.py`, preserving the old script's
  ability to run through `dbxcarta submit run_spike_ai_query.py`.

Validation after these fixes:

- `uv run pytest -q` passed: 177 passed, 1 skipped, 3 deselected.
- `uv run pytest examples/finance-genie/tests -q` passed: 21 passed.
- `uv build --wheel` passed for dbxcarta.
- `uv build --wheel` passed for `examples/finance-genie`.
- `uv lock --check` passed.
- Wheel inspection confirmed `dbxcarta-ingest` and `dbxcarta-client` are
  present in `entry_points.txt`.

---

## What is left

1. **Live end-to-end validation (Phase A5.2).** Run the ingest with the
   example preset against the validated UC scope
   (`graph-enriched-lakehouse.graph-enriched-schema`) and confirm the
   Neo4j node and relationship counts match what the old built-in
   produced. Requires the user's Databricks workspace and Neo4j write
   credentials. Estimate: one ingest run plus a counts-comparison query.
2. **Publish-location decision (Phase A5).** Pick one of: private index,
   git tag plus pip install from git URL, UC Volume, or public PyPI. Then
   automate `uv build && publish` accordingly.
3. **Autotest live validation.** The autotest now needs a live run through
   `dbxcarta submit-entrypoint ingest` to confirm the new wheel-task submit
   path in the target workspace.
4. **Part B.** Build the real external preset inside
   `graph-on-databricks/finance-genie` using the in-repo example as the
   structural template. Five phases, sketched below, none started.

---

## Part B: Prototype the pattern in graph-on-databricks — NOT STARTED

Part B validates that an outside repository, with no special relationship to
dbxcarta, can build and ship its own preset package using only the public
API and the example as a template.

### Phase B1: Decide the package layout for the external preset

- [ ] Pick a name that follows the LangChain convention: a short, distinct
      package name that makes the dependency relationship obvious, for
      example `dbxcarta-finance-genie` or
      `graph-on-databricks-finance-preset`. Get the user to approve the
      name before any files are created.
- [ ] Decide where the package source lives inside the
      `graph-on-databricks/finance-genie` tree. The two reasonable choices
      are a new top-level folder dedicated to the preset, or a folder
      colocated with the existing enrichment-pipeline package. Discuss
      tradeoffs with the user before choosing.
- [ ] Confirm with the user whether the prototype should also be
      independently pip-installable from the graph-on-databricks repo, or
      whether it is acceptable for now to install it via editable install
      from a local path.
- [ ] Review with user before Phase B2.

### Phase B2: Create the external preset package

- [ ] Create a minimal Python package at the chosen location with its own
      project metadata, a dependency on dbxcarta at the version picked in
      Phase A5, and nothing else it does not need. Use the in-repo example
      package from Part A.5 as the structural template.
- [ ] Implement the preset against the same protocol. The package now owns
      the Finance Genie environment overlay, the table lists, the readiness
      check, and the demo-questions upload helper, but specialized for the
      graph-on-databricks layout rather than the dbxcarta example layout.
- [ ] Make sure the new package can be imported from a plain Python
      session after installation and that doing so does not require any
      dbxcarta environment variables to be set.
- [ ] Review with user before Phase B3.

### Phase B3: Wire the prototype into the finance-genie workflow

- [ ] Update the finance-genie README and any setup scripts so that the
      documented flow is: install dbxcarta, install the new preset
      package, then run dbxcarta with the preset specified by its import
      path. There is no fallback to a built-in.
- [ ] Confirm by walking through the documented flow on a clean checkout
      that a user who has never touched dbxcarta can install both
      packages, point at the right workspace, and successfully run the
      preset against Unity Catalog.
- [ ] Review with user before Phase B4.

### Phase B4: Validate the round trip

- [ ] Run the full ingest with the external preset and confirm the Neo4j
      graph that results is identical in shape and content to the graph
      produced by the in-repo example preset from Part A.5. Same node
      counts by label, same relationship counts by type, same sampled
      embeddings.
- [ ] Run the demo client against the resulting graph using the same
      questions file. Confirm the answers are at least as good as the
      example flow.
- [ ] Record any rough edges discovered during validation as a short
      punch list. These are inputs to a follow-up cleanup phase, not
      blockers for declaring the prototype successful.
- [ ] Review with user before Phase B5.

### Phase B5: Capture lessons and pick the next adopter

- [ ] Document the lessons from the prototype in a short note: what was
      easy, what was awkward, what should change in the dbxcarta public
      API or preset protocol before a second external preset is built.
- [ ] Identify the next candidate external preset, if any, inside the
      graph-on-databricks repo so that the second adopter validates the
      pattern again. Common second adopters expose gaps the first one
      did not.
- [ ] Final review with user.

---

## Out of scope for this plan

- No central plugin registry, no Python entry points, no automatic
  discovery. The whole point of this pattern is that consumers state
  their dependency explicitly by import path.
- No shims, no compatibility wrappers, no deprecated names kept around.
  There are no current consumers, so the migration is a clean cut.
- No multi-package monorepo split inside dbxcarta core. If that becomes
  desirable later, it is a separate plan.
- Pre-existing mypy strict failures in `contract.py`, `ingest/summary.py`,
  and `ingest/preflight.py` discovered during A2 validation. These are
  tracked as a separate cleanup; this plan did not touch them.

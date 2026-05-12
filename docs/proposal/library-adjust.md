# Library-First Adjustment Proposal

**Status: Partially implemented. Phases 1, 2, and 4 are complete; Phase 3 is
complete for documentation, with the local artifact verification and the
follow-up to promote or document the example demo's reach into
`dbxcarta.client.*` outstanding; Phase 5 is pending outside-repository
validation.**

This proposal adjusts the pip-integration work so dbxcarta feels first like a
small reusable library and only second like a command-line workflow. The goal
is still the same: an outside project, such as Finance Genie in a separate
repository, should be able to consume dbxcarta without forking it or copying
runner scripts. The change is in emphasis and complexity.

The current proposal moved in the right direction by removing the built-in
Finance Genie preset and proving that an outside package can supply its own
configuration. The concern is that the pattern now presents too much surface
area at once: preset packages, optional capability protocols, CLI preset
actions, Databricks wheel-task submission, and publish-location decisions. That
can make a consumer think they must adopt the whole integration framework
before they can use the library.

This plan makes the library path primary. A consumer should be able to import
dbxcarta, provide normal Python configuration, and call a stable public
function. The preset import path and CLI helpers remain useful, but they become
conveniences built on top of the library, not the architectural center.

---

## Goal

Make dbxcarta reusable by outside consumers through a library-first pattern:

- dbxcarta core provides stable public functions, settings types, and small
  helper types.
- External projects own their domain-specific configuration, fixtures, readiness
  checks, and demos.
- Presets are plain Python adapters, not a plugin system.
- The CLI remains a convenience for local demos, automation, and Databricks job
  submission.
- The next external-consumer proof focuses first on importing and calling the
  library from an outside package, then on CLI automation.

---

## Progress at a glance

| Phase | Status | Notes |
|-------|--------|-------|
| 1 — Reframe the public contract | Complete | `run_dbxcarta(settings=..., spark=...)` is the preferred library entrypoint. |
| 2 — Simplify preset responsibilities | Complete | Optional hooks remain in `dbxcarta.presets` for CLI/demo use, not top-level public API. |
| 3 — Make the example package smaller | In progress | Example tree documented as a template, dev-only override marked, and generated artifacts confirmed untracked; the standalone `upload_questions.py` script and the local demo's reference-comparison flow stay (the latter is annotated as a brittle dep on `dbxcarta.client.client`). |
| 4 — Route the CLI through the library path | Complete | `dbxcarta preset <spec> --run` overlays env and calls `run_dbxcarta()`; the single private job-runner reference is annotated as a follow-up. |
| 5 — Prove the pattern outside this repo | Pending | Needs Finance Genie outside-repository validation and live Databricks/Neo4j approval. |

Latest offline validation: `uv run pytest` passed with 178 tests, 1 skipped,
and 3 deselected.

---

## Assumptions

- There are no current external consumers that need backward compatibility with
  the first pip-integration shape.
- Finance Genie is still the reference external consumer.
- The package should keep the import-path preset option because it is useful for
  repeatable demos and automation.
- The package should not introduce central plugin discovery, automatic entry
  point scanning, or a registry.
- The Databricks job submission flow is still needed, but it should not define
  the core integration model.
- Live Databricks and Neo4j validation still requires user approval because it
  can consume compute and write data.

---

## Risks

- If the public library function is not clear enough, consumers may still reach
  for CLI internals or environment-variable overlays directly.
- If the preset protocol is reduced too far, the CLI may lose useful checks
  that made demos repeatable.
- If the example package stays too elaborate, it may continue to look like a
  framework integration instead of a small consumer-owned adapter.
- If the Databricks submission helper depends on private behavior from
  databricks-job-runner, it can remain brittle even if the library pattern is
  simplified.
- If Part B validates only the CLI path, the real outside-library consumption
  risk remains unproven.

---

## Proposed Direction

The adjusted pattern should be:

- **Library first:** an external package imports dbxcarta and calls a public
  function with explicit settings or a small environment overlay.
- **Preset second:** a preset is a small plain Python object or function that
  returns dbxcarta configuration for one domain.
- **Optional helpers stay outside core:** readiness checks and demo-question
  upload logic are useful, but they should be treated as consumer-owned helpers
  unless repeated consumers prove they belong in core.
- **CLI wraps the library:** command-line commands should resolve configuration
  and then call the same public library path an outside project can call.
- **Databricks submission is operational:** wheel-task submission is important,
  but it should be documented as deployment automation, not as the only way to
  integrate with dbxcarta.

---

## Phase 1: Reframe the Public Contract

**Status: Complete**

**Outcome:** The docs and code identify one primary library contract that an
outside consumer should use before reaching for CLI automation.

Checklist:

- [x] Review the current public exports and decide which symbol is the preferred
  programmatic entrypoint for ingestion.
- [x] Document the preferred library usage in prose without making the preset CLI
  the first concept.
- [x] Clarify that environment overlays are one configuration input, not the whole
  integration contract.
- [x] Mark CLI-specific helpers and Databricks submission helpers as operational
  conveniences.
- [x] Identify any public exports that are only present for the CLI and should not
  be promoted as external API.

Implementation note: `run_dbxcarta(settings=..., spark=...)` is now the
preferred programmatic ingest entrypoint. The no-argument form remains the
Databricks wheel/CLI entrypoint and loads `Settings` from the environment.

Validation:

- A reader can understand how an outside package would use dbxcarta without
  reading the CLI implementation.
- The README and proposal language no longer imply that a consumer must build a
  preset package before using the library.

---

## Phase 2: Simplify Preset Responsibilities

**Status: Complete**

**Outcome:** Presets are documented and implemented as small configuration
adapters, not as a plugin framework.

Checklist:

- [x] Keep the required preset contract limited to producing dbxcarta
  configuration.
- [x] Reclassify readiness checks as example-owned validation helpers unless there
  is a clear second consumer for the same core abstraction.
- [x] Reclassify demo-question upload as example-owned demo support, not a core
  preset capability.
- [x] Decide whether the optional readiness and question-upload protocols should
  remain public, move to the example package, or be renamed as CLI extension
  hooks.
- [x] Update the proposal and README language so optional helper methods do not
  look required.

Implementation note: optional readiness and question-upload protocols remain in
`dbxcarta.presets` for CLI/demo use, but they are no longer promoted from the
top-level `dbxcarta` public surface.

Validation:

- A minimal external preset can be described in one short paragraph.
- Finance Genie can still perform readiness checks and question upload, but
  those actions are not presented as part of every consumer's required
  integration.

---

## Phase 3: Make the Example Package Smaller

**Status: In progress**

**Outcome:** The Finance Genie example reads like a template an outside team
would copy, not like a special framework integration.

Checklist:

- [x] Separate the example README into two clear paths: library usage first, CLI
  usage second.
- [x] Move Finance Genie-specific validation and demo helpers under an example
  section rather than presenting them as core concepts.
- [x] Keep only the files that a real outside package would reasonably need.
  The standalone `upload_questions.py` script is kept on purpose as a minimal
  library-consumer template; it complements, rather than duplicates,
  `preset.upload_questions()`.
- [x] Remove generated local artifacts from the example tree if they are not meant
  to be committed. `.venv/`, `.pytest_cache/`, `dist/`, and `__pycache__/`
  directories exist locally but are gitignored and untracked, so nothing is
  shipped that should not be.
- [x] Ensure the example package dependency on dbxcarta reflects a real external
  install path, with any local development override clearly marked as local
  development only.
- [ ] Decide whether the four underscore-prefixed helpers `local_demo.py`
  imports from `dbxcarta.client.client` (`_compare_result_sets`,
  `_embed_questions`, `_load_questions`, `_parse_sql`) should be promoted to a
  documented `dbxcarta.client` public surface or stay private with the existing
  brittle-dep comment.

Validation:

- [x] A new consumer can understand the example structure and delete Finance
  Genie details without untangling core behavior.
- [x] The example package tests still pass.
- [x] The example package still builds after final cleanup.

---

## Phase 4: Route the CLI Through the Library Path

**Status: Complete (live validation gated on user approval, same as Phase 5)**

**Outcome:** CLI commands become thin wrappers around the same public library
contract external consumers use directly.

Checklist:

- [x] Review the ingest and client entrypoints to confirm they call public or
  intentionally supported library functions.
- [x] Ensure preset resolution produces configuration and then hands off to the
  normal library path.
- [x] Keep Databricks wheel-task submission as a separate deployment helper.
- [x] Document that job submission is useful for Databricks execution, but not
  required for library consumption.
- [x] Identify any private databricks-job-runner dependency that should either be
  wrapped locally or tracked as a follow-up risk.

Implementation note: `dbxcarta-ingest` still calls the no-argument
`run_dbxcarta()` wrapper, which now delegates to the same public ingest function
shape used by library consumers. The preset CLI now exposes
`dbxcarta preset <spec> --run`, which overlays `preset.env()` onto the process
environment (existing values win) and invokes `run_dbxcarta()`. The one private
databricks-job-runner reference, `Runner._compute`, is marked in
`_submit_wheel_entrypoint` as a brittle follow-up risk.

Validation:

- [x] Local library usage and CLI usage share the same ingestion behavior at the
  ingest function boundary.
- [ ] The CLI remains compatible with the existing demo and automation commands
  after live validation.

---

## Phase 5: Prove the Pattern in an Outside Repository

**Status: Pending**

**Outcome:** Part B validates the simplest external-consumer path before
validating automation.

Checklist:

- Build the Finance Genie external package around normal imports from dbxcarta.
- Prove that the external package can construct its configuration and call the
  library without relying on dbxcarta repository files.
- Add the preset import-path wrapper only after the direct library path works.
- Add CLI and Databricks submission validation after the library path is proven.
- Compare the resulting Neo4j graph to the old built-in Finance Genie behavior
  during live validation.

Validation:

- The external repository can use dbxcarta from an installed package.
- The external repository does not copy dbxcarta runner scripts.
- The external repository does not require a central preset registry.
- The CLI automation still works as a convenience after direct library usage is
  proven.

---

## Completion Criteria

This adjustment is complete when:

- [x] The public docs describe dbxcarta first as a library.
- [x] External consumers have a clear direct-import path.
- [x] Presets are small configuration adapters, not a required plugin framework.
- [x] Finance Genie remains outside core.
- [x] CLI commands and Databricks submission still work, but are documented as
  convenience and deployment layers.
- [ ] The outside-repository prototype proves direct library usage before proving
  CLI automation.
- [x] Offline tests pass after the simplification.
- [x] Wheel builds pass after final example cleanup.

---

## Decision Points

Current decisions:

- Optional preset capability protocols stay in `dbxcarta.presets` as CLI/demo
  extension hooks, but are not exported from top-level `dbxcarta`.
- The primary ingestion entrypoint is `run_dbxcarta(settings=..., spark=...)`;
  the no-argument form remains for wheel/CLI execution.
- Databricks wheel-task submission stays in the CLI and is documented as
  deployment automation.
- The outside-repository prototype install path is still open: published
  package, git install, or editable local install for the first validation pass.

---

## Recommended First Pass

Initial pass status:

- [x] Update the README and proposal language to say library-first, CLI-second.
- [x] Keep the current code working while reducing the conceptual contract.
- [x] Treat Finance Genie readiness and question upload as example-owned
  behavior.
- [ ] In Part B, prove direct library consumption before adding CLI automation.

This keeps the useful work already done while making the pattern easier for an
outside consumer to understand and adopt.

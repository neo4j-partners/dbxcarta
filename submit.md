# Publish dbxcarta-submit so neocarta can submit Spark jobs with it

## Goal

Make `packages/dbxcarta-submit` something the neocarta project can install and use to
submit Databricks Spark jobs. Then add a small example to neocarta's
`examples/databricks/` that is a simple `uv` script. The script installs the neocarta
connector and `dbxcarta-submit`, then submits the finance-genie ingest job.

The end state in one line: neocarta runs one `uv` script that builds the connector
wheel, stages it, and submits the finance-genie ingest job, using `dbxcarta-submit` as
the submit tool.

## Quick answer: "library" or "package"?

It is a **package**. On PyPI you publish a *distribution package*. "Library" is just the
informal word for code another project imports or calls. For `dbxcarta-submit`:

- The distribution name (what you `pip install`) is `dbxcarta-submit`.
- The import path is `dbxcarta.submit`.
- The console command it installs is `dbxcarta`.

So the precise phrasing is "publish `dbxcarta-submit` as a package on a Python index, and
neocarta uses it as a library."

## What we have today

- `dbxcarta-submit` already knows how to submit a neocarta ingest job. Its ingest path
  already stages the `neocarta` wheel and runs the `neocarta-databricks-ingest` entry
  point. It is already neocarta-aware.
- It provides operator commands: `bootstrap`, `publish-wheels`, `submit-entrypoint
  ingest`, `materialize`, `teardown`, `ready`.
- It depends on two things at runtime:
  - `databricks-job-runner==0.6.2`, which is already on public PyPI.
  - `dbxcarta-core`, which is a local workspace package, not published, and shared with
    the materialize and client packages.

`dbxcarta-core` stays in the repo as the single source of truth. The published
`dbxcarta-submit` wheel bundles only the core modules submit actually needs, so neocarta
installs **one** package. Submit's core closure is 7 modules: `catalogs`, `config`,
`env`, `executor`, `identifiers`, `volume_io`, `workspace`. The heavy `materialize.py`,
plus `questions.py` and `sql_safety.py`, are excluded. The set is import-closed and none
of the 7 reach into the excluded three. `core/__init__.py` re-exports nothing, so copying
a subset cannot drag in an excluded module.

## The easiest approach (recommendation)

Publish one self-contained `dbxcarta-submit` package and add one small, focused submit
helper so neocarta does not have to drive the full operator CLI. In plain terms:

- Keep `dbxcarta-core` in the repo. Have the published `dbxcarta-submit` wheel bundle the
  7 core modules submit needs, and drop `dbxcarta-core` from submit's published
  dependencies. neocarta then installs one package whose only external dependency,
  `databricks-job-runner`, is already public.
- Add one small public function to `dbxcarta-submit`, `submit_neocarta_ingest(...)`, that
  does two things: stage a prebuilt neocarta wheel on the Volume, then submit the ingest
  entry point. This is the clean "library" surface neocarta calls. It reuses the code the
  CLI already has. It lives in its own `dbxcarta/submit/api.py` and is re-exported from
  `dbxcarta/submit/__init__.py` (declared in `__all__`), so the public library surface is
  explicit and kept apart from `cli.py`'s internals.
- The neocarta example is a `uv` script that builds the neocarta wheel, then calls that
  one function.

The repo already has this bundle pattern: the sibling `dbxcarta-materialize` wheel
carries `dbxcarta/core` at build time through `_core_bundled_into` in `cli.py`, with
`module-name = "dbxcarta"` and `namespace = true`. Submit reuses the same mechanism,
narrowed to copy only the 7 needed modules instead of the whole package, and goes one
step further by dropping the published core dependency so the wheel is fully
self-contained.

Why this and not "just call the CLI": the existing `publish-wheels` command also builds
the `dbxcarta-materialize` wheel from the dbxcarta source tree. A pip-installed package
has no source tree, so that step would fail. neocarta only needs *stage the neocarta
wheel + submit ingest*, never materialize. A small dedicated function avoids the
materialize build entirely and gives neocarta a real library call instead of shelling
out to a CLI.

A zero-code-change fallback exists: publish the packages unchanged and have the example
shell out to `dbxcarta submit-entrypoint ingest`, staging the wheel itself. It works but
the example is clunkier and has to dodge the materialize build by hand. The small helper
is the cleaner "use as a library" the goal asks for, so it is the recommendation.

## Decisions to confirm before starting

- **Which index.** neocarta's own plan publishes its wheel to a *private* index, not
  public PyPI. The simplest match is to publish `dbxcarta-submit` to the same index
  neocarta uses, so both projects pull from one place. If a public PyPI release is wanted
  instead, the `dbxcarta-submit` name must be free on PyPI. Only one distribution is
  published now, since core is bundled. Recommend: match neocarta's index.
- **Library call or CLI shell-out.** Decided: the small `submit_neocarta_ingest` helper.
- **Where finance-genie config lives in the example.** Either a small `.env` shipped next
  to the script, or plain constants at the top of the script. Recommend a small `.env`
  beside the script so secrets stay out of the committed file.

## Decided design details

- **Helper signature, explicit not env-sourced.** A library function that reaches into
  `os.environ` for its real inputs is impure and hard to test, and hides its dependencies.
  The helper takes explicitly the things that vary per call and are not already
  env-resolved: the neocarta wheel path, the compute mode, and the wait flag. Its shape is
  `submit_neocarta_ingest(neocarta_wheel: Path, *, compute_mode: str = "cluster", no_wait:
  bool = False) -> None`. Everything else (catalog, volume, profile, and the Neo4j secrets
  resolved on the cluster) comes from the selected overlay exactly as the CLI resolves it.
  The helper adds no new `os.environ` reads of its own. A fully pure helper that threaded
  catalog/volume/profile through parameters would mean refactoring the env-driven module
  `runner` and the overlay loader, which is larger than this task; the chosen shape matches
  the existing architecture instead of bolting a parallel pure path next to it.
- **Helper placement.** The function lives in `dbxcarta/submit/api.py` and is re-exported
  from `dbxcarta/submit/__init__.py` with an explicit `__all__`, so the public surface is
  intentional and ruff does not flag the re-export as unused.
- **Version bump: `1.0.0` to `1.1.0`.** Per SemVer this is a minor bump: the public API
  only gains a function (a feature), and dropping `dbxcarta-core` is not a breaking change
  for any consumer since core was never published.

## Things to handle (the gotchas)

- The bundled core subset must track submit's real imports. A curated copy of 7 modules
  works only as long as submit imports nothing else from core. If a later change makes
  submit import an 8th core module, in-repo dev still passes because the workspace install
  carries the whole of core, but the published wheel breaks at runtime. The guard is the
  clean-room install test in Phase 1: install the built wheel alone in a fresh
  environment and exercise the submit path, so a missing module fails there.
- Do not install a published `dbxcarta-core` in the same environment as the published
  `dbxcarta-submit`. Both would provide `dbxcarta/core`, and two distributions shipping
  the same files conflict. neocarta never does this; it installs only `dbxcarta-submit`.
- `publish-wheels` cannot run standalone. It builds the materialize wheel from
  `packages/dbxcarta-materialize` in the dbxcarta repo. The example must not use it.
  Staging the neocarta wheel must be a repo-independent step.
- The neocarta wheel still has to be built first. `uv build` in the neocarta repo produces
  it. The example points at that built wheel.
- The Neo4j Spark Connector is a JVM cluster library and a classic (non-serverless)
  cluster is required. The submit tooling already checks for this; the example just notes
  it as a prerequisite.

---

## Phased implementation

### Phase 1: Make a self-contained submit wheel

- Bundle the 7 needed core modules (`catalogs`, `config`, `env`, `executor`,
  `identifiers`, `volume_io`, `workspace`) plus `core/__init__.py` and `core/py.typed`
  into the submit wheel at build time. A new `scripts/build_submit_wheel.py` is the
  narrowed twin of the `_core_bundled_into` pattern from `materialize`: it copies the
  curated module list into submit's `src/dbxcarta/core/`, runs `uv build --wheel`, asserts
  the wheel physically carries `dbxcarta/core/` (the same check `_assert_wheel_bundles_core`
  makes), then removes the copied core so the working tree is left unchanged. This is a
  PyPI build, distinct from the Volume-staging `publish-wheels` path. Source of truth stays
  single in `dbxcarta-core`.
- Change submit's build config to `module-name = "dbxcarta"` with `namespace = true`, so
  the wheel can carry both `dbxcarta/submit` and the copied `dbxcarta/core`.
- Drop `dbxcarta-core` from submit's published dependencies, so the wheel is
  self-contained and `databricks-job-runner` is its only external dependency. In-repo dev
  keeps importing `dbxcarta.core` through the workspace install (the workspace root depends
  on `dbxcarta-core`, so `uv sync` keeps it present).
- Bump the version to `1.1.0`, point `readme` at a new `README.md`, and write that README
  so the index page reads well.
- Add a clean-room install test as the drift guard, marked `slow`: build the wheel via the
  script, `pip install` it alone in a fresh virtualenv, import `dbxcarta.submit`, and
  exercise the stage-and-submit path. A missing core module surfaces here rather than on a
  cluster.

**Done when:** a fresh virtualenv can `pip install` the built `dbxcarta-submit` wheel
alone (no separate core install), import `dbxcarta.submit`, and run the submit path.

### Phase 2: Add the small standalone submit helper

- Add the public function `submit_neocarta_ingest` in `dbxcarta/submit/api.py`,
  re-exported from `dbxcarta/submit/__init__.py` via `__all__`. It stages a prebuilt
  neocarta wheel onto the Volume (reusing `_publish_prebuilt_wheel`), ships the runner
  bootstrap script (`runner.upload_all()`), then submits the ingest entry point (reusing
  `_submit_bootstrap_entrypoint("ingest", ...)`), with no materialize build.
- Signature: `submit_neocarta_ingest(neocarta_wheel: Path, *, compute_mode: str =
  "cluster", no_wait: bool = False) -> None`. Only the per-call inputs are parameters; the
  catalog, Volume, profile, and Neo4j secrets come from the selected overlay exactly as the
  CLI resolves them. The helper adds no new `os.environ` reads.
- Confirm `submit-entrypoint ingest` and this helper work from a pip-installed package
  with no dbxcarta repo present.

**Done when:** the helper stages a wheel and submits an ingest job from a clean
environment with no dbxcarta source checkout.

### Phase 3: Publish to the chosen index

- Publish the single `dbxcarta-submit` wheel to the agreed index.
- Test the install from that index in a clean environment before announcing it.
- If public PyPI is chosen, do a dry run to TestPyPI first.

**Done when:** `pip install dbxcarta-submit` from the index pulls `databricks-job-runner`
automatically and imports cleanly, with no separate core install.

### Phase 4: Add the neocarta finance-genie example

- Add `examples/databricks/submit_finance_genie.py` to the neocarta repo as a `uv` script
  with inline dependency metadata listing `neocarta[databricks-spark]` and
  `dbxcarta-submit`.
- The script: build the neocarta wheel (or point at an already-built one), then call
  `submit_neocarta_ingest` for the finance-genie catalog and Neo4j target.
- Ship a small `.env` (or constants block) next to it for the finance-genie catalog,
  Volume, Neo4j, and embedding endpoint values.
- Add a short README section explaining the prerequisites: a classic cluster, the Neo4j
  Spark Connector attached, a reachable Neo4j, and a built neocarta wheel.

**Done when:** `uv run examples/databricks/submit_finance_genie.py` submits the
finance-genie ingest job using `dbxcarta-submit`, with no dbxcarta repo checkout.

### Phase 5: End-to-end test from a clean machine

- On a machine with only the neocarta repo, run the example end to end against a test
  catalog and Neo4j.
- Confirm the ingest job runs on the cluster and produces the expected graph.
- Fix any packaging gaps the clean run surfaces, for example a missing module in a wheel.

**Done when:** the example runs the finance-genie ingest job successfully on a clean
machine using only the published `dbxcarta-submit` package and the neocarta wheel.

---

## What the neocarta example looks like (sketch)

A single `uv` script in `examples/databricks/`:

- Top of file: inline `uv` metadata declaring `neocarta[databricks-spark]` and
  `dbxcarta-submit` as dependencies.
- Body: read the finance-genie config, build or locate the neocarta wheel, then call the
  one submit helper.

The reader runs it with `uv run examples/databricks/submit_finance_genie.py`. `uv`
installs both dependencies into a throwaway environment, so there is no manual setup
beyond filling in the config values and having workspace access.

## What is deliberately left out

- **No materialize, no client, no overlay machinery in the example.** neocarta only needs
  stage-the-wheel plus submit-ingest. The rest of the dbxcarta operator surface is not part
  of this example.
- **No public PyPI if the team prefers a private index.** The index choice is a decision,
  not a default.
- **No change to how the connector wheel itself is built.** That stays in neocarta's own
  build and publish flow.

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
  - `dbxcarta-core`, which is a local workspace package and is **not published yet**.

## The easiest approach (recommendation)

Publish the two packages as-is and add one small, focused submit helper so neocarta does
not have to drive the full operator CLI. In plain terms:

- Publish `dbxcarta-core` and `dbxcarta-submit` to a Python index. They install cleanly
  because their only other dependency, `databricks-job-runner`, is already public.
- Add one small public function to `dbxcarta-submit`, for example
  `submit_neocarta_ingest(...)`, that does two things: stage a prebuilt neocarta wheel on
  the Volume, then submit the ingest entry point. This is the clean "library" surface
  neocarta calls. It reuses the code the CLI already has.
- The neocarta example is a `uv` script that builds the neocarta wheel, then calls that
  one function.

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
  public PyPI. The simplest match is to publish `dbxcarta-submit` and `dbxcarta-core` to
  the same index neocarta uses, so both projects pull from one place. If a public PyPI
  release is wanted instead, the two distribution names must be free on PyPI. Recommend:
  match neocarta's index.
- **Library call or CLI shell-out.** The recommendation is the small `submit_neocarta_ingest`
  helper. Confirm that, or accept the heavier shell-out fallback.
- **Where finance-genie config lives in the example.** Either a small `.env` shipped next
  to the script, or plain constants at the top of the script. Recommend a small `.env`
  beside the script so secrets stay out of the committed file.

## Things to handle (the gotchas)

- `dbxcarta-core` must be published too. It is a hard dependency of `dbxcarta-submit`.
  Without it, `pip install dbxcarta-submit` fails for anyone outside this repo.
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

### Phase 1: Make the two packages publishable

- Confirm `dbxcarta-core` and `dbxcarta-submit` build standalone wheels and sdists with
  `uv build` from their package folders.
- Set release metadata on both: final version numbers, description, license, and a
  minimal README per package so the index page reads well.
- Confirm the dependency line in `dbxcarta-submit` resolves from the index, not the
  workspace, for an outside consumer. The `databricks-job-runner` pin is already public.

**Done when:** both wheels build, and a fresh virtualenv can `pip install` the built
`dbxcarta-submit` wheel together with its `dbxcarta-core` wheel and import
`dbxcarta.submit`.

### Phase 2: Add the small standalone submit helper

- Add one public function to `dbxcarta-submit`, for example `submit_neocarta_ingest`, that
  stages a prebuilt neocarta wheel onto the Volume and submits the ingest entry point. It
  reuses the existing internal stage-and-submit code, just without the materialize build.
- Make it take its inputs plainly: the path to the built neocarta wheel, the catalog, the
  Volume path, the Neo4j connection, and the embedding settings. It can read the same
  environment variables the CLI reads so the example stays small.
- Confirm `submit-entrypoint ingest` and this helper work from a pip-installed package
  with no dbxcarta repo present.

**Done when:** the helper stages a wheel and submits an ingest job from a clean
environment with no dbxcarta source checkout.

### Phase 3: Publish to the chosen index

- Publish `dbxcarta-core` first, then `dbxcarta-submit`, to the agreed index.
- Test the install from that index in a clean environment before announcing it.
- If public PyPI is chosen, do a dry run to TestPyPI first.

**Done when:** `pip install dbxcarta-submit` from the index pulls `dbxcarta-core` and
`databricks-job-runner` automatically and imports cleanly.

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
machine using only the published packages.

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

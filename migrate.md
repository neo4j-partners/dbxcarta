# Migrating dbxcarta into neocarta

## Key goals

- Turn the neocarta repo into a uv workspace so it can host more than one package.
- Integrate dbxcarta's durable Databricks capability, the `dbxcarta-core` and `dbxcarta-spark` packages, into that workspace.
- Publish that capability as optional extras on neocarta: `neocarta[dbxcarta-core]` and `neocarta[dbxcarta-spark]`.
- Treat `dbxcarta-client` and the dbxcarta `examples` as temporary. They come over so dbxcarta keeps working, but they get folded into neocarta's own client and examples during the full integration, then dropped.
- Keep `dbxcarta-submit` as its own package for running Databricks build jobs. It stays a repo tool, not a published extra.
- Keep dbxcarta working through the transition: same commands, same builds, same tests.
- Avoid disturbing neocarta's existing `neocarta` package, build, and tests.

## How the extras work

The extra names match the published packages one-to-one, so there is no invented abstraction:

- `neocarta[dbxcarta-core]` installs `dbxcarta-core`, the light, Spark-free shared base (identifiers, catalogs, workspace, executor, presets, env).
- `neocarta[dbxcarta-spark]` installs `dbxcarta-spark`, the full Databricks semantic-layer builder. Because spark already depends on core, this pulls core in too.

So the two extras layer cleanly: pick `dbxcarta-core` for the light base, or `dbxcarta-spark` for the whole builder.

## How the directories map over

dbxcarta moves under a single `dbxcarta/` directory, with one subfolder per package and no `packages/` layer:

| Today (dbxcarta repo) | After the move (inside neocarta) |
|---|---|
| `packages/dbxcarta-core` | `dbxcarta/dbxcarta-core` |
| `packages/dbxcarta-spark` | `dbxcarta/dbxcarta-spark` |
| `packages/dbxcarta-client` (temporary) | `dbxcarta/dbxcarta-client` |
| `packages/dbxcarta-submit` | `dbxcarta/dbxcarta-submit` |
| `examples/` (temporary) | `dbxcarta/examples` |
| `tests/` | `dbxcarta/tests` |
| `docs/` | `dbxcarta/docs` |
| `scripts/` | `dbxcarta/scripts` |
| `README.md`, `CLAUDE.md`, `.env` files | `dbxcarta/README.md`, `dbxcarta/CLAUDE.md`, `dbxcarta/.env` files |

Neocarta's own top-level directories stay where they are: `neocarta/`, `agent/`, `eval/`, `datasets/`, `examples/`, `tests/`, `docs/`. The only new top-level item is the `dbxcarta/` folder.

The dbxcarta workspace-root `pyproject.toml` does not move as a file. Its workspace config (members, sources, pytest, ruff, mypy) merges into neocarta's root `pyproject.toml` per the decisions below. There is intentionally **no** `dbxcarta/pyproject.toml`; everything lives in the single neocarta root config.

## Settled decisions

These were walked through and decided. The rest of the document reflects them.

1. **Single workspace, dev floor rises to 3.12.** dbxcarta packages become full members of neocarta's one `uv.lock`. Because the lock's `requires-python` is the intersection of all members, the development floor becomes 3.12. The published `neocarta` wheel still declares `>=3.10`, so `pip install neocarta` keeps working on 3.10; only the dbxcarta extras require 3.12. The neocarta test matrix drops 3.10 and 3.11, leaving 3.12 and 3.13.
2. **Drop the `live`/`slow` pytest markers; separate by directory instead.** Mirror neocarta's existing path-based convention (`tests/unit`, `tests/integration`, `tests/smoke` with per-path Make targets). No new `dbxcarta/pyproject.toml`.
3. **Consolidate onto one ruff config and one mypy config block.** Ruff: adopt neocarta's `select = ["ALL"]` config as the single repo standard, with two added knobs (ignore `D213`, per-path ignore `PLC0415` for dbxcarta source). Mypy: one root `[tool.mypy]` block with dbxcarta's strict settings, enforcement scoped to the dbxcarta packages for now.
4. **neo4j version validated.** The 6.x Python driver officially supports connecting to 4.4.x, 5.x, 2025.x, and 2026.x servers (Neo4j Python Driver Manual), so neocarta's `neo4j>=6.1.0` resolving to 6.x against the deployed 5.27-aura server is supported. Code-level compatibility is still verified by running dbxcarta's Neo4j tests against the 6.x driver (see fix #7).
5. **Example packaging deferred.** The dbxcarta example packages are mid-refactor. Leave both options (demote to scripts, or keep as packaged members) in the plan and decide after the initial migration.
6. **CHANGELOG / docstring gates exempted intentionally.** While dbxcarta code is scoped to `dbxcarta/`, it is exempt from neocarta's per-PR CHANGELOG and numpy-docstring gates. This exemption is deliberate and recorded, not an oversight, and is revisited at full integration.

## What needs to change during the migration

- **Make neocarta a uv workspace.** Add `[tool.uv.workspace]` to neocarta's root `pyproject.toml` listing the dbxcarta packages as members, and add matching `[tool.uv.sources]` entries (`{ workspace = true }`) so they resolve locally during development. Do not copy dbxcarta's `package = false`; neocarta's root stays a real package.
- **Add the extras.** Declare `dbxcarta-core` and `dbxcarta-spark` as optional dependencies on the `neocarta` package, so `pip install neocarta[dbxcarta-core]` and `neocarta[dbxcarta-spark]` work.
- **Publish core and spark as their own distributions.** For the extras to install off PyPI, `dbxcarta-core` and `dbxcarta-spark` have to be published packages. The workspace link only covers local development. Before the first publish, pin spark's internal dependency: `dbxcarta-core>=1.1.0,<2`, and version the two in lockstep (fix #4 below). Each package keeps its own build backend; mixing neocarta's hatchling with dbxcarta's `uv_build` is fine.
- **Set the Python floor per decision #1.** Base neocarta's published metadata stays `>=3.10`. The dbxcarta packages stay `>=3.12`. The shared `uv.lock` therefore resolves at `>=3.12`, which is the development floor. Trim `pr-main-tests.yml` to `['3.12', '3.13']`.
- **Merge the lockfiles (fix #8).** The workspace uses one `uv.lock` at the neocarta root; dbxcarta's separate lockfile goes away. Resolve and diff the merged lock: confirm none of neocarta's existing pins move (especially `pandas>=2,<3`), confirm neo4j, pydantic, and python-dotenv stay compatible, and keep `pyspark` and `databricks-sdk` behind the extras so a plain base `uv sync` stays lean.
- **Plan the temporary pieces.** `dbxcarta-client` and the dbxcarta `examples` are kept only so dbxcarta runs end to end during the move. The full integration absorbs their capability into neocarta's own client and examples, after which they are removed from `dbxcarta/`.
- **Settle the two CLAUDE files.** Neocarta has a `claude.md` and dbxcarta has its own `CLAUDE.md`. dbxcarta's rules stay scoped to its folder (`dbxcarta/CLAUDE.md`); the parent can gain a short pointer into `dbxcarta/` for Databricks work. Never create both `claude.md` and `CLAUDE.md` at the same directory level on a case-insensitive filesystem.
- **Sort out the environment and secret files.** dbxcarta's `.env`, overlays, and `setup_secrets.sh` come over as-is, scoped to the `dbxcarta/` folder. Confirm gitignore rules carry over so no secret gets committed.
- **Update paths in docs and scripts.** Anything that assumes dbxcarta is the top of the tree gets repointed at `dbxcarta/`. The README's "inspired by neocarta" line can become a direct reference now that they share a repo.

## Tests (decision #2)

Drop the `live` and `slow` markers. The markers are narrow today (`live` is 2 files needing a live Databricks profile, Neo4j, and a previously-loaded catalog; `slow` is 1 pure-Spark guard file), so replacing them with directory-based separation is a net simplification and aligns dbxcarta with neocarta's existing convention.

- dbxcarta's live integration tests already sit under `dbxcarta/tests/integration/`. The default test command does not include that directory; a dedicated target runs it when live credentials are present, exactly like neocarta's `make test-it`.
- The single `slow` Spark-guard file gets its own target so today's "not slow by default" behavior is preserved. It needs no credentials, only time.
- No `dbxcarta/pyproject.toml`. The one dbxcarta-specific pytest setting, `--import-mode=importlib`, is passed on the dbxcarta Make target's command line rather than in a config file, so neocarta's root `[tool.pytest.ini_options]` is untouched.
- Add dbxcarta Make targets alongside neocarta's, for example a fast lane that runs `dbxcarta/tests` with `--import-mode=importlib` and ignores the integration directory, an integration target, and a slow-guard target.
- Keep the two suites runnable independently at first; wire dbxcarta into the workspace test flow once it is stable.

## Lint and type checking (decision #3)

**Ruff:** consolidate onto neocarta's existing config as the single repo standard (`select = ["ALL"]`, line length 100, curated ignore list). Running that posture over dbxcarta's source surfaces ~730 findings, of which ~469 are auto-fixable; the manual remainder collapses to two config knobs:

- Ignore `D213` repo-wide. dbxcarta writes first-line docstring summaries; neocarta's rule wants the second line. Ignoring `D213` is permissive, breaks nothing in neocarta, and avoids rewriting ~226 docstrings.
- Add a per-path `PLC0415` (import-outside-top-level) ignore for dbxcarta source, where lazy imports are intentional (optional Spark deps, avoiding collection-time loads). Neocarta already carries a per-file `PLC0415` ignore for the same reason.

Then run `ruff --fix` over the dbxcarta tree once and keep `make lint`/`make fmt` governing the whole repo.

**Mypy:** neocarta currently runs no mypy. Consolidate the config without forcing a large new typing effort on neocarta:

- One root `[tool.mypy]` block carrying dbxcarta's strict settings (`disallow_untyped_defs`, `warn_return_any`, pydantic plugin, etc.).
- Enforcement stays scoped to the dbxcarta packages for now via `mypy -p dbxcarta.core -p dbxcarta.spark -p dbxcarta.client -p dbxcarta.submit`. neocarta opts in later, package by package.

## Remaining fixes

These were agreed during the walkthrough and fold into the steps above.

1. **Pin spark to core before publishing.** `dbxcarta-spark` currently lists `dbxcarta-core` with no version because the workspace resolves it locally. Pin `dbxcarta-core>=1.1.0,<2` and version core and spark in lockstep before the first PyPI publish.
2. **Enumerate workspace members explicitly (or demote examples).** Every member needs its new `dbxcarta/...` path in both `[tool.uv.workspace] members` and `[tool.uv.sources]`. The three example sub-packages (`dense-schema`, `finance-genie`, `schemapile`) are packaged members today; per decision #5 their fate is deferred. Leave both options open: either list all three with new paths, or demote them to plain scripts under `dbxcarta/examples` and drop their packaging. Decide after the initial migration.
3. **Audit `dbxcarta-submit` path logic for the bundled cluster wheel.** `dbxcarta-spark` uses `module-name = "dbxcarta"` with `namespace = true`, and `dbxcarta-submit publish-wheels` physically copies core's source into the spark wheel for the cluster (`_core_bundled_into`). That copy logic assumes the old `packages/dbxcarta-core` paths and the flattening to `dbxcarta/dbxcarta-core` will break it silently. Re-point the paths and add a smoke test that builds the bundled cluster wheel and asserts it contains both `dbxcarta/core` and `dbxcarta/spark`.
4. **Verify the neo4j 6.x driver against dbxcarta's code.** Protocol compatibility is confirmed (decision #4). Still run dbxcarta's Neo4j-touching tests against the resolved 6.x driver, and confirm the 6.x driver against the 5.27-aura server, before merge. Major-version driver bumps may carry API breaks; this is a code check, not a version-string check.
5. **Diff the merged lockfile.** Covered under "Merge the lockfiles" above. Resolve, diff, and confirm no existing neocarta pin moves and that pyspark/databricks stay behind the extras.

## Process and history

- **Tests run independently first.** Neocarta drives tests with Make targets; dbxcarta uses pytest. Keep them runnable independently at first (new dbxcarta Make targets), then wire dbxcarta into the workspace test flow once it is stable.
- **Gate exemption is intentional (decision #6).** While scoped to `dbxcarta/`, dbxcarta code is exempt from neocarta's per-PR CHANGELOG-update and numpy-docstring gates. Record this exemption explicitly so it reads as deliberate. Revisit at full integration.
- **Preserve git history if it matters.** Decided: **fresh snapshot, no history preserved** (2026-06-03). The tree was copied/updated in place rather than grafted, per the simpler option.

## Status (2026-06-03)

Stage A and Stage B are complete. Stage C (Phases 5–9) is implemented as a fresh snapshot, and Phase 10's non-live verification is green: dbxcarta fast lane 616 passed / 1 skipped, cluster-wheel bundle smoke test passing, scoped mypy clean across 77 files, and neocarta's own unit suite still green (no regression). Outstanding: Phase 10's two live checks (Neo4j 5.27-aura connectivity, Databricks e2e) need credentials the user runs; Phase 11's deferred items remain open; and review finding F1 (the `*_secret*` gitignore rule hiding `tests/core/test_databricks_secret.py`) awaits a go-ahead before fixing. See `migrate-plan.md` for the phase-by-phase status and findings.
</content>
</invoke>

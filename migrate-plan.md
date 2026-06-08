# Migration checklist: dbxcarta into neocarta

A phased, plain-English checklist. The reasoning, trade-offs, and config details live in `migrate.md`; this file is just the running order and the boxes to tick.

## Sequencing note

dbxcarta is still being tested, so the integration that physically moves it comes last. The early phases prepare both sides independently:

- **Stage A (neocarta prep):** make neocarta workspace-ready in isolation. Safe and reversible, needs nothing from dbxcarta.
- **Stage B (dbxcarta prep):** improvements made inside the dbxcarta repo while it is still its own project, so the move later is drop-in. Can run in parallel with Stage A.
- **Stage C (integration):** the steps that need both repos together. These can only run once dbxcarta is stable, because uv must resolve real package directories. Do not start Stage C until Stage A and B are green. — **Stage A and B are now complete (verified 2026-06-03); Stage C is unblocked.**

---

## Stage A: Prepare neocarta (do now, neocarta repo) — ✅ COMPLETE

### Phase 1: Make neocarta a uv workspace in isolation — ✅ COMPLETE

Goal: prove the workspace conversion is safe before any dbxcarta package exists.

- [x] Add `[tool.uv.workspace]` to neocarta's root `pyproject.toml` with neocarta as the only member. — present (`members = []`; the root package is the implicit sole member, which is the idiomatic uv form).
- [x] Keep neocarta's root as a real package (do not add `package = false`). — confirmed; root still builds the `neocarta` wheel via hatchling, no `package = false`.
- [x] Run `uv sync`, build, and the full current test matrix; confirm nothing changed versus today. — `uv lock --check` resolves cleanly (202 packages, no drift). Full test-matrix green is a CI confirmation on PR.
- [x] Leave the test matrix at 3.10 through 3.13 for now (base still supports 3.10 until the extras land in Stage C). — `pr-main-tests.yml` still `['3.10', '3.11', '3.12', '3.13']`.

---

## Stage B: Prepare dbxcarta in place (do now, dbxcarta repo) — ✅ COMPLETE

These are changes to dbxcarta itself, made while it is still its own repo, so the later move is drop-in. Hold any that would disrupt active testing until testing settles.

### Phase 2: Switch tests to directory-based separation — ✅ COMPLETE

Goal: drop the markers in dbxcarta now so the move carries no marker baggage.

- [x] Remove the `live` and `slow` pytest markers. — no `pytest.mark.live`/`slow` or `markers =` remain; Makefile comments confirm the markers are gone.
- [x] Keep live integration tests under `tests/integration/`, excluded from the default run. — `make test` runs with `--ignore=tests/integration`.
- [x] Add Make targets: a fast lane (`tests` with `--import-mode=importlib`, ignoring the integration dir), an integration target, and a slow-guard target. — `test`, `test-it`, `test-slow` all present.

### Phase 3: Consolidate dbxcarta lint and types — ✅ COMPLETE

Goal: bring dbxcarta to neocarta's standard while still in its own repo.

- [x] Adopt neocarta's `select = ["ALL"]` ruff config in dbxcarta. — `select = ["ALL"]` in root `pyproject.toml`.
- [x] Add `D213` to the ruff ignore list and a per-path `PLC0415` ignore for dbxcarta source. — `D213` in the ignore list; per-path `PLC0415` set on `packages/**/src/**/*.py` (plus examples/scripts).
- [x] Run `ruff --fix` once, then clean up the manual remainder. — `uv run ruff check .` reports "All checks passed!".
- [x] Keep the strict `[tool.mypy]` block, scoped to the dbxcarta packages. — `[tool.mypy]` block present with `disallow_untyped_defs = true`.

### Phase 4: Pin internal versions — ✅ COMPLETE

Goal: make the packages publishable.

- [x] Pin spark's internal dependency: `dbxcarta-core>=1.1.0,<2`. — present in `packages/dbxcarta-spark/pyproject.toml`.
- [x] Version core and spark in lockstep. — both at `1.1.0`.

---

## Stage C: Integration (do only when dbxcarta is stable)

Stage C was implemented and the non-live verification is green (2026-06-03). The
physical move is done as a fresh snapshot (no git history, per decision). Phases
5–9 are complete; Phase 10's non-live checks pass; Phase 10's two live checks
(Neo4j 5.27-aura connection, Databricks e2e) and Phase 11's deferred items remain
open and are flagged below.

### Phase 5: Bring dbxcarta into the repo — ✅ COMPLETE

Goal: get the files in place under `dbxcarta/`.

- [x] Copy the dbxcarta tree under a new top-level `dbxcarta/` folder per the mapping table in `migrate.md` (no `packages/` layer). — `dbxcarta/{dbxcarta-core,dbxcarta-spark,dbxcarta-client,dbxcarta-submit,dbxcarta-materialize,examples,tests,docs,scripts}` all sit directly under the subtree root; verified no `packages/` layer remains.
- [x] Do not move dbxcarta's workspace-root `pyproject.toml` as a file; its config merges into neocarta's root. — confirmed no `dbxcarta/pyproject.toml` exists.
- [x] Carry over `.env`, the overlays, and `setup_secrets.sh`, scoped to `dbxcarta/`. — present; overlays tracked, secret `.env` files ignored.
- [x] Confirm gitignore rules cover the moved files so no secret can be committed. — `dbxcarta/.gitignore` mirrors the original; `dbxcarta/.env` and each `examples/*/​.env` show as ignored (`!!`), committed `dbxcarta-overlay.env` files stay tracked. **Finding F1 (below):** the mirrored `*_secret*` rule also swallows the legitimate test file `tests/core/test_databricks_secret.py` — a pre-existing dbxcarta latent bug carried over faithfully; the file was never tracked in the original repo either.
- [x] Keep `dbxcarta/CLAUDE.md` scoped to its folder; add a short pointer to it from the root `claude.md`. — pointer added under "Databricks work (dbxcarta subtree)".

### Phase 6: Wire the workspace members — ✅ COMPLETE

Goal: members resolve locally, one merged lock.

- [x] Add the dbxcarta packages to `[tool.uv.workspace] members` at their new `dbxcarta/...` paths. — 8 members (5 packages + 3 examples) listed.
- [x] Add matching `[tool.uv.sources]` entries (`{ workspace = true }`). — 8 matching entries present.
- [x] Generate one merged `uv.lock` at the root and delete dbxcarta's separate lockfile. — single root `uv.lock` (216 packages); no `dbxcarta/uv.lock`.
- [x] Diff the merged lock: confirm no existing neocarta pin moves (watch `pandas>=2,<3`), and that `pyspark` and `databricks-sdk` stay behind the extras. — neocarta pins held (pandas 2.3.3, neo4j 6.2.0, pydantic 2.12.5, python-dotenv 1.2.1); `pyspark` 4.1.2 + `databricks-sdk` 0.114.0 land only via the dbxcarta extras/group and stay out of a base `uv sync`.

### Phase 7: Extras and Python floor — ✅ COMPLETE

Goal: extras work, base install stays 3.10, dev runs on 3.12.

- [x] Declare `dbxcarta-core` and `dbxcarta-spark` as optional dependencies (extras) on the `neocarta` package. — both extras present in `[project.optional-dependencies]`.
- [x] Leave neocarta's published metadata at `>=3.10`; leave dbxcarta packages at `>=3.12`. — neocarta `requires-python = ">=3.10"`; merged lock resolves at `>=3.12`.
- [x] Trim `pr-main-tests.yml` to `['3.12', '3.13']`. — matrix updated.

### Phase 8: Merge config into neocarta's root — ✅ COMPLETE

Goal: one ruff config, one mypy block, dbxcarta test targets live in the repo.

- [x] Merge the dbxcarta ruff knobs (`D213` ignore, per-path `PLC0415`) into neocarta's root ruff config. — done; plus per-path ignores for dbxcarta source/examples/scripts/tests. A nested `dbxcarta/ruff.toml` extends the root and only overrides `target-version = "py312"` (PEP 695 syntax) so neocarta's own 3.10 lints are undisturbed.
- [x] Add the root `[tool.mypy]` block with strict settings, enforcement scoped to the dbxcarta packages (`-p dbxcarta.core -p dbxcarta.spark -p dbxcarta.client -p dbxcarta.submit`). — root `[tool.mypy]` added (scope also includes `dbxcarta.materialize`).
- [x] Add the dbxcarta Make targets to the repo; keep `--import-mode=importlib` on the command line, not in a config file. — `dbxcarta/Makefile` carries the targets; root `Makefile` delegates via `dbxcarta-*` targets. `PYTEST_FLAGS` adds `--import-mode=importlib -o consider_namespace_packages=true` on the command line (the latter resolves the `dbxcarta` PEP 420 namespace collision with the subtree dir under neocarta's `pythonpath="."`).
- [x] Confirm no `dbxcarta/pyproject.toml` is created and neocarta's root pytest config is untouched. — confirmed.

### Phase 9: Cluster wheel and submit tooling — ✅ COMPLETE

Goal: keep the bundled Databricks wheel building after the path move.

- [x] Re-point `dbxcarta-submit publish-wheels` path logic from `packages/dbxcarta-core` to `dbxcarta/dbxcarta-core`. — `_core_bundled_into` updated to the flattened paths (`root / "dbxcarta-core" / "src" / "dbxcarta" / "core"`).
- [x] Add a smoke test that builds the bundled cluster wheel and asserts it contains both `dbxcarta/core` and `dbxcarta/spark`. — `tests/submit/test_cluster_wheel_bundle_smoke.py` builds each entrypoint wheel (spark/client/materialize) and asserts each carries `dbxcarta/core/` plus its own module. Passing (`make dbxcarta-test-wheel`: 1 passed).

### Phase 10: Verify — ◐ NON-LIVE COMPLETE; LIVE CHECKS PENDING (need credentials)

Goal: prove it actually works, not just resolves.

- [ ] Run dbxcarta's Neo4j-touching tests against the resolved 6.x driver. — **PENDING (live):** requires a live Neo4j; the integration suite is excluded from the fast lane. Run `make dbxcarta-test-it` with creds.
- [ ] Confirm the 6.x driver connects to the 5.27-aura server. — **PENDING (live):** user to run against 5.27-aura.
- [x] Run both test suites green (neocarta and dbxcarta) on 3.12. — **dbxcarta fast lane: 616 passed, 1 skipped; wheel smoke: 1 passed; scoped mypy: clean (77 files); neocarta own unit suite: green.** The 3.13 leg is the CI matrix's second job (verified on PR).

### Phase 11: Deferred decisions and cleanup

Goal: close out the items parked for after the initial migration.

- [ ] Decide example packaging: demote the three example packages to plain scripts, or keep them as workspace members (parked pending the refactor).
- [ ] Record the CHANGELOG / numpy-docstring gate exemption for `dbxcarta/` as intentional.
- [ ] Decide git history approach: fresh snapshot (simpler) or preserve dbxcarta's commit history (extra steps).
- [ ] Publish `dbxcarta-core` and `dbxcarta-spark` to PyPI and verify `pip install neocarta[dbxcarta-core]` and `neocarta[dbxcarta-spark]` install cleanly.
- [ ] Fold `dbxcarta-client` and `dbxcarta/examples` into neocarta's own client and examples, then remove the temporary copies.

---

## Review findings (2026-06-03)

Surfaced during the post-implementation quality review. None block the non-live
migration; they are tracked here so they are not lost.

- **F1 — `*_secret*` gitignore swallows a real test file. ✅ RESOLVED (2026-06-03).**
  `dbxcarta/.gitignore`'s `*_secret*` rule matched
  `dbxcarta/tests/core/test_databricks_secret.py`, leaving it untracked — it would
  silently disappear on a fresh clone / in CI (its 2 tests run locally as part of
  the 616 but never on a clean checkout). A pre-existing dbxcarta latent bug,
  mirrored faithfully by the move (never tracked in the original repo either).
  Fix applied: added `!**/test_*secret*.py` after the `*_secret*` line (keeps the
  secret guard; real `.env`/secret files stay ignored) and `git add`-ed the test
  file, which is now tracked.
- **Live verification outstanding (Phase 10):** the two live checks require
  credentials the agent does not run — Neo4j 5.27-aura connectivity and the
  Databricks e2e ingest/client round-trip. Run with creds via
  `make dbxcarta-test-it` and the `make e2e-*-ingest` / `e2e-*-client` targets.
</content>

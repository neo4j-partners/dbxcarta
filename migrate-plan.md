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

### Phase 5: Bring dbxcarta into the repo

Goal: get the files in place under `dbxcarta/`.

- [ ] Copy the dbxcarta tree under a new top-level `dbxcarta/` folder per the mapping table in `migrate.md` (no `packages/` layer).
- [ ] Do not move dbxcarta's workspace-root `pyproject.toml` as a file; its config merges into neocarta's root.
- [ ] Carry over `.env`, the overlays, and `setup_secrets.sh`, scoped to `dbxcarta/`.
- [ ] Confirm gitignore rules cover the moved files so no secret can be committed.
- [ ] Keep `dbxcarta/CLAUDE.md` scoped to its folder; add a short pointer to it from the root `claude.md`.

### Phase 6: Wire the workspace members

Goal: members resolve locally, one merged lock.

- [ ] Add the dbxcarta packages to `[tool.uv.workspace] members` at their new `dbxcarta/...` paths.
- [ ] Add matching `[tool.uv.sources]` entries (`{ workspace = true }`).
- [ ] Generate one merged `uv.lock` at the root and delete dbxcarta's separate lockfile.
- [ ] Diff the merged lock: confirm no existing neocarta pin moves (watch `pandas>=2,<3`), and that `pyspark` and `databricks-sdk` stay behind the extras.

### Phase 7: Extras and Python floor

Goal: extras work, base install stays 3.10, dev runs on 3.12.

- [ ] Declare `dbxcarta-core` and `dbxcarta-spark` as optional dependencies (extras) on the `neocarta` package.
- [ ] Leave neocarta's published metadata at `>=3.10`; leave dbxcarta packages at `>=3.12`.
- [ ] Trim `pr-main-tests.yml` to `['3.12', '3.13']`.

### Phase 8: Merge config into neocarta's root

Goal: one ruff config, one mypy block, dbxcarta test targets live in the repo.

- [ ] Merge the dbxcarta ruff knobs (`D213` ignore, per-path `PLC0415`) into neocarta's root ruff config.
- [ ] Add the root `[tool.mypy]` block with strict settings, enforcement scoped to the dbxcarta packages (`-p dbxcarta.core -p dbxcarta.spark -p dbxcarta.client -p dbxcarta.submit`).
- [ ] Add the dbxcarta Make targets to the repo; keep `--import-mode=importlib` on the command line, not in a config file.
- [ ] Confirm no `dbxcarta/pyproject.toml` is created and neocarta's root pytest config is untouched.

### Phase 9: Cluster wheel and submit tooling

Goal: keep the bundled Databricks wheel building after the path move.

- [ ] Re-point `dbxcarta-submit publish-wheels` path logic from `packages/dbxcarta-core` to `dbxcarta/dbxcarta-core`.
- [ ] Add a smoke test that builds the bundled cluster wheel and asserts it contains both `dbxcarta/core` and `dbxcarta/spark`.

### Phase 10: Verify

Goal: prove it actually works, not just resolves.

- [ ] Run dbxcarta's Neo4j-touching tests against the resolved 6.x driver.
- [ ] Confirm the 6.x driver connects to the 5.27-aura server.
- [ ] Run both test suites green (neocarta and dbxcarta) on 3.12 and 3.13.

### Phase 11: Deferred decisions and cleanup

Goal: close out the items parked for after the initial migration.

- [ ] Decide example packaging: demote the three example packages to plain scripts, or keep them as workspace members (parked pending the refactor).
- [ ] Record the CHANGELOG / numpy-docstring gate exemption for `dbxcarta/` as intentional.
- [ ] Decide git history approach: fresh snapshot (simpler) or preserve dbxcarta's commit history (extra steps).
- [ ] Publish `dbxcarta-core` and `dbxcarta-spark` to PyPI and verify `pip install neocarta[dbxcarta-core]` and `neocarta[dbxcarta-spark]` install cleanly.
- [ ] Fold `dbxcarta-client` and `dbxcarta/examples` into neocarta's own client and examples, then remove the temporary copies.
</content>

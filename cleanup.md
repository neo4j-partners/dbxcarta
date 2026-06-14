# Cleanup: questions filename + dead derivation chain — DONE

## Goal

- Every benchmark example uses **one** questions filename: `questions.json`.
  No more `dense_questions.json`.
- Questions is just a local file with a fixed name. Generators write it
  (`--output` defaults to `questions.json`), the client eval reads it (the
  overlays point `DBXCARTA_CLIENT_QUESTIONS` at `examples/<name>/questions.json`).
  The ops-Volume-path derivation in the middle was dead and is removed.

## Why this was safe

- Both generators already default `--output` to `questions.json`
  (`dense .../question_generator.py:52`, `schemapile .../question_generator.py:68`).
- Each example config set a `questions_path` field, but **nothing read it**
  except its own `test_config.py`. The generators write to `--output`, not to
  `questions_path`. So the field and the derivation feeding it were dead.

## Part A — name unified to `questions.json`

Resolved by deletion (Option 2 below) rather than rename: the only live
`dense_questions.json` references were the standalone `.env`/`.env.sample`
lines and the core derivation, all removed.

- [x] core `config.py` comment that mentioned `dense_questions.json` removed
- [x] `dense_questions.json` gone from all live code/config (only historical
      `docs/proposals/*` mention it, left as a record)

## Part B — removed the dead questions-path derivation chain

### core
- [x] `core/config.py`: removed `client_questions` from `DerivedOpsConfig`
- [x] `core/config.py`: removed the `questions_filename` parameter
- [x] `core/config.py`: removed the `DEFAULT_QUESTIONS_FILENAME` constant
- [x] `core/config.py`: updated module + class docstrings

### example configs
- [x] `dense-schema/.../config.py`: removed `questions_path` field + assignment + unused `derive_ops_config` import
- [x] `schemapile/.../config.py`: removed `questions_path` field + assignment + unused `derive_ops_config` import

### tests
- [x] `tests/core/test_ops_config.py`: dropped the import, the `client_questions` asserts, `test_questions_filename_is_an_example_choice`, `test_default_questions_filename`
- [x] `tests/examples/dense-schema/test_config.py`: dropped the `questions_path` assert
- [x] `tests/examples/dense-schema/test_generator.py`: dropped the `questions_path=` kwarg
- [x] `tests/examples/schemapile/test_config.py`: dropped the `questions_path` assert
- [x] `tests/examples/schemapile/test_slice_runner.py`: dropped the kwarg
- [x] `tests/examples/schemapile/test_question_generator.py`: dropped the kwarg
- [x] `tests/examples/schemapile/test_candidate_selector.py`: dropped the kwarg

## Option 2 — orphaned env lines removed

After Part B the example standalone configs no longer read
`DBXCARTA_CLIENT_QUESTIONS`, so the line was orphaned in the standalone files
(the overlays still use the var for the client eval and are already local/correct).

- [x] `examples/dense-schema/.env.sample` — removed the orphaned line
- [x] `examples/schemapile/.env.sample` — removed the orphaned line
- [x] `examples/finance-genie/.env.sample` — removed the orphaned line (finance's local demo reads `DEFAULT_QUESTIONS = questions.json`, never this var)
- [x] `examples/dense-schema/.env` (gitignored local) — kept in sync
- [x] `examples/schemapile/.env` (gitignored local) — kept in sync

## Validation

- [x] `uv run ruff check` on all changed files — clean
- [x] `uv run mypy -p dbxcarta.core -p dbxcarta.client -p dbxcarta.submit -p dbxcarta.materialize` — clean (42 files)
- [x] full default suite — **419 passed**
- [x] grep: no `client_questions` / `questions_filename` / `DEFAULT_QUESTIONS_FILENAME`; no live `dense_questions.json`

## Root graph_rag demo — hard cutover DONE

The root demo used `demo_questions.json` on a `/Volumes/...` upload path (the
old cluster-client flow). With the local client, questions is a local file.
Hard cutover:

- [x] `git mv tests/fixtures/demo_questions.json tests/fixtures/questions.json`
- [x] `scripts/run_demo.py`: docstring + next-steps point at
      `tests/fixtures/questions.json`; dropped the vestigial
      `upload --data tests/fixtures` step; removed the now-unused `volume_path`
      arg from `_print_next_steps`
- [x] root `.env.sample` §9: comment + `DBXCARTA_CLIENT_QUESTIONS=tests/fixtures/questions.json` (no Volume, no upload)
- [x] `README.md`: quickstart prose, config bullet, and §3 build block (dropped the `upload --data` line)
- [x] `tests/fixtures/README.md`: file table + build block
- [x] `tests/fixtures/insert_test_data.sql`: comment reference
- [x] grep: no `demo_questions` and no `upload --data tests/fixtures` remain in live code/docs

## Still open (one item, your call)

- **Pre-existing lint**: `tests/client/test_entrypoint.py:13` trips ruff
  `TC003` (committed in `29e8c89`, unrelated to this cleanup). Left as-is —
  one-line fix if you want it.

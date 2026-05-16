# Final Fixes for `docs/proposals/fix-spark-v3.md` Phases 3 and 4

Review date: 2026-05-16

## Findings

### 1. Guardrail skip reuses `row_counts["fk_skipped"]`, which already has a declared-FK meaning

Severity: High

`RunSummary._build_row_counts()` writes guardrail skip state to `fk_skipped` when `summary.fk_skipped` is true:

- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/summary.py:229`
- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/summary.py:230`

That key already belongs to declared FK accounting. `verify/references.py` reads it as `fk_declared - fk_resolved`:

- `packages/dbxcarta-spark/src/dbxcarta/spark/verify/references.py:68`
- `packages/dbxcarta-spark/src/dbxcarta/spark/verify/references.py:73`

With a guardrail-only skip, `row_counts` can contain `fk_skipped = 1` while `fk_declared` and `fk_resolved` are absent or zero, which produces `references.accounting_mismatch`. I verified this with:

`uv run python -c "from dbxcarta.spark.verify.references import _check_accounting; print([v.code for v in _check_accounting({'row_counts': {'columns': 1001, 'fk_skipped': 1, 'fk_skipped_column_count': 1001, 'fk_skipped_column_limit': 1000}})])"`

Output:

`['references.accounting_mismatch']`

Fix:

- Keep declared-FK `fk_skipped` reserved for declared FK accounting.
- Rename guardrail keys to a non-conflicting namespace, for example `fk_discovery_skipped`, `fk_discovery_skip_reason`, `fk_discovery_skipped_column_count`, and `fk_discovery_skipped_column_limit`.
- Add a verifier test proving a guardrail skip does not fail declared-FK accounting.

### 2. Phase 4 asks for a skip reason, but the implementation only records count and limit

Severity: Medium

The proposal checklist says to record `fk_skipped: bool` plus a reason/limit:

- `docs/proposals/fix-spark-v3.md:463`
- `docs/proposals/fix-spark-v3.md:464`

The implementation records only:

- `fk_skipped`
- `fk_skipped_column_count`
- `fk_skipped_column_limit`

There is no durable reason field. This matters because future FK skips may come from a different gate, and the summary should make the skip cause unambiguous without reading logs.

Fix:

- Add a reason field such as `fk_discovery_skip_reason = "column_limit_exceeded"`.
- Serialize it with the renamed guardrail keys.
- Update tests to assert the reason.

### 3. Guardrail only works when `summary.extract.columns` is populated

Severity: Medium

The checklist allows reading from `summary.extract.columns` if populated, otherwise falling back to `columns_df.count()`:

- `docs/proposals/fix-spark-v3.md:452`
- `docs/proposals/fix-spark-v3.md:454`

`_fk_guardrail_tripped()` currently only reads `summary.extract.columns`:

- `packages/dbxcarta-spark/src/dbxcarta/spark/ingest/fk/discovery.py:243`

The normal `_run()` path populates this in `extract()`, but `run_fk_discovery()` is still a public-ish module boundary used directly in tests and library code. If it is called with a default `RunSummary`, a configured guardrail will not trip because the count is zero.

Fix:

- Either move the gate immediately before `run_fk_discovery()` in `run.py`, where the populated summary is guaranteed, or pass `extract.columns_df` into the guardrail helper and fall back to a scalar `.count()` when the summary count is zero or unavailable.
- Add a test for the fallback or explicitly narrow the contract in docs if direct `run_fk_discovery()` calls are unsupported.

### 4. Documentation still states relationship writes are always `coalesce(1)`

Severity: Medium

Phase 3 makes relationship write parallelism configurable, but existing docs still state the old invariant:

- `docs/reference/best-practices.md:71`
- `docs/reference/best-practices.md:77`
- `README.md:374`

This contradicts `dbxcarta_rel_write_partitions` and the updated `_load` docstring.

Fix:

- Update `docs/reference/best-practices.md` to say relationship writes default to `coalesce(1)` and may be explicitly repartitioned with `DBXCARTA_REL_WRITE_PARTITIONS` after production evidence.
- Update `README.md` design text the same way.
- Keep the Neo4j lock-contention warning prominent.

### 5. New settings are not discoverable in `.env.sample`

Severity: Low

`.env.sample` includes `DBXCARTA_NEO4J_BATCH_SIZE` but not:

- `DBXCARTA_REL_WRITE_PARTITIONS`
- `DBXCARTA_FK_MAX_COLUMNS`

Fix:

- Add both settings to `.env.sample` with conservative defaults and comments.
- Add them to any integration example `.env.sample` files only if those examples are expected to expose tuning knobs.

### 6. Phase 3 tests cover `_rel_partition()`, but not `_load()` using it at every write boundary

Severity: Low

`tests/spark/test_rel_partition.py` correctly verifies `coalesce(1)` vs `repartition(n)` behavior on the helper:

- `tests/spark/test_rel_partition.py:33`
- `tests/spark/test_rel_partition.py:41`

It does not prove `_load()` routes every relationship write through `_rel_partition()`. The proposal's validation asks for writer fakes asserting the path taken by the writes, and Phase 3's implementation depends on all seven `write_rel` sites using the helper.

Fix:

- Add a focused `_load()` unit with fake relationship DataFrames and a monkeypatched `write_rel`.
- Assert all emitted relationship DataFrames were partitioned through the configured path for `n = 1` and `n > 1`.

### 7. Phase 4 tests do not cover the full skip-through-load-and-verify path

Severity: Low

`tests/spark/fk_guard/test_fk_column_guardrail.py` exercises `_fk_guardrail_tripped()` and `_skipped_result()` directly:

- `tests/spark/fk_guard/test_fk_column_guardrail.py:43`
- `tests/spark/fk_guard/test_fk_column_guardrail.py:85`

That catches the helper contract, but not the full behavior promised by the phase: FK skipped, extract and load still write, summary records the skip, and verify remains valid.

Fix:

- Add a narrow orchestration-level test or verifier-level test that builds a summary with a guardrail skip and confirms REFERENCES accounting does not fail.
- If `_load()` is too heavy for a unit test, test the summary dictionary passed to `verify.references.check()`.

## Validation Run During Review

Focused tests passed:

`uv run pytest tests/spark/test_rel_partition.py tests/spark/settings/test_partition_and_guardrail_settings.py tests/spark/fk_guard/test_fk_column_guardrail.py tests/spark/test_verify_scope.py`

Result: 31 passed.

An initial attempt to run `tests/spark/verify` failed because that directory does not exist.

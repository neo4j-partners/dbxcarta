# fix-spark-v3.md — Phase 3 & 4 Review: Proposed Fixes

Scope: review of `docs/proposals/fix-spark-v3.md` Phases 3 and 4 against the
working tree. The Phase 3 and 4 *code* is implemented and correct, and the
three new test files pass (`uv run pytest` on the guardrail, settings, and
rel-partition suites: 25 passed). The fixes below are proposal/spec
consistency items plus one real validation gap. No Phase 3/4 code change is
required except where noted.

## Phase 3 (Configurable Relationship Write Parallelism)

### F1 — Status field is stale (doc fix, required)

`docs/proposals/fix-spark-v3.md` Phase 3 still reads `Status: Pending`, but
the work is done and verified:

- `dbxcarta_rel_write_partitions: int = 1` + `_validate_rel_write_partitions`
  rejecting `< 1` (`settings.py:92`, `settings.py:116-127`).
- `_rel_partition(df, n)` next to `_project` (`run.py:439-448`),
  `coalesce(1) if n <= 1 else repartition(n)`.
- Helper applied to every `write_rel` site in `_load`.
- `_load` docstring rewritten to describe the configurable behavior
  (`run.py:314-322`) — no longer asserts the old `coalesce(1)` invariant.
- Path-asserting spy tests (`tests/spark/test_rel_partition.py`) and the
  default-pinning settings test (`tests/spark/settings/test_partition_and_guardrail_settings.py`).

Proposed fix: set `Status: Complete` and add a dated `Done:` note in the
Phase 0/1/2 style.

### F2 — "six call sites" miscount (doc fix, required)

The Phase 3 checklist says "every `write_rel` call in `run.py:_load` (six
call sites)" then enumerates seven (HAS_VALUE, HAS_SCHEMA, HAS_TABLE,
HAS_COLUMN, declared, metadata, semantic). The code correctly wraps all
**seven** `write_rel` calls (`run.py:356,362,368,374,384,395,406`). The code
is right; the proposal text is wrong.

Proposed fix: change "six call sites" to "seven call sites" in the proposal.
No code change.

## Phase 4 (Thin Guardrail and Documentation)

### F3 — Status field is stale (doc fix, required)

Phase 4 reads `Status: Pending`, but the work is done and verified:

- `dbxcarta_fk_max_columns: int = 0` + `_validate_fk_max_columns` rejecting
  negatives (`settings.py:110`, `settings.py:129-139`), documented inline.
- Skip-not-cap: `_fk_guardrail_tripped` / `_skipped_result`
  (`discovery.py:226-271`), gated at the top of `run_fk_discovery`
  (`discovery.py:81-82`), reusing the existing `None`-edge load contract.
- Skip recorded on `RunSummary` (`summary.py:203-205`) and surfaced as flat
  `fk_skipped*` keys via `_build_row_counts` (`summary.py:229-232`);
  `row_counts` is `MapType(StringType(), LongType())` in `summary_io.py:160`,
  so the new keys serialize to Delta without a schema change.
- Tests cover disabled-default, below/at/above limit, summary recording, the
  all-`None` result, and row-count surfacing
  (`tests/spark/fk_guard/test_fk_column_guardrail.py`).

Proposed fix: set `Status: Complete` and add a dated `Done:` note.

### F4 — Column-count fallback in the spec is unreachable (doc fix, required)

The Phase 4 checklist says obtain the count from `summary.extract.columns`
"if it is populated before `run_fk_discovery`; otherwise a single
`columns_df.count()` is acceptable." In practice `extract()` *unconditionally*
sets `summary.extract = ExtractCounts(..., columns=columns_df.count())`
(`extract.py:115-119`) and runs before `run_fk_discovery` (`run.py:197` then
`run.py:205`). The count is therefore always populated; the
`columns_df.count()` fallback branch is dead. The implementation correctly
relies on `summary.extract.columns` with no fallback (`discovery.py:243`).

Proposed fix: tighten the proposal wording to state the extract summary
column count is always populated before FK discovery, so no fallback path
exists, so the absence of a `columns_df.count()` call is not a missing item.
No code change.

### F5 — Phase 4 validation criterion 2 is unsatisfiable (real gap, decision needed)

Phase 4 Validation: "The Phase 1/2 large-catalog bounded-driver-memory
fixture runs with the guardrail disabled, so the guardrail cannot mask that
OOM regression test." No such fixture exists. `tests/spark/fk_guard/`
contains only `test_fk_column_guardrail.py` and
`test_single_execution_guard.py`; there is no synthetic large-catalog /
bounded-driver-memory OOM fixture anywhere under `tests/spark/`. Phase 1's
own validation ("a synthetic large-catalog fixture under a bounded
driver-memory ceiling") references the same missing artifact, so this is a
Phase 1/2 deliverable that Phase 4's validation depends on transitively.

This is the only substantive gap. Options:

1. Create the synthetic large-catalog bounded-driver-memory fixture (the
   originally specified Phase 1/2 deliverable) and let Phase 4's criterion
   stand. The `dbxcarta_fk_max_columns` default of `0` already means the
   fixture runs with the guardrail disabled by construction.
2. If that fixture is intentionally deferred, amend both the Phase 1/2 and
   Phase 4 validation sections to record it as deferred with a reason,
   rather than leaving an unmet pass/fail criterion that reads as satisfied.

Recommend option 1 (the proposal's stated Completion Criteria still lists "A
synthetic large-catalog run completes with bounded driver memory").

### F6 — best-practices.md cited rule was not added (decision needed, low priority)

Phase 4 checklist: "add a cited rule to `docs/reference/best-practices.md` if
one is established." The proposal itself establishes a usable rule — a
bounded scalar `count()` is not a catalog-scale driver collect and does not
violate §5. That distinction is currently only in the proposal and the
`_fk_guardrail_tripped` docstring.

Proposed fix: either add a one-line cited clarification to best-practices §5
("a single aggregate/`count()` action is bounded and permitted; §5 forbids
collecting catalog-scale *rows*, not reading one scalar"), or explicitly note
in the proposal that no new rule was established so the conditional item is
closed rather than open. The item is conditional ("if one is established"),
so this is low priority but should be resolved to a decision, not left
ambiguous.

## Summary

| ID | Type | Severity | Action |
|----|------|----------|--------|
| F1 | Proposal status stale (Phase 3) | Required | Mark Complete + Done note |
| F2 | "six" should be "seven" | Required | Proposal text only |
| F3 | Proposal status stale (Phase 4) | Required | Mark Complete + Done note |
| F4 | Dead fallback in spec | Required | Proposal wording only |
| F5 | Missing large-catalog OOM fixture | Real gap | Create fixture or record deferral |
| F6 | best-practices rule not added | Low | Add cited line or close the item |

The Phase 3 and 4 implementation is functionally complete and correct;
F1–F4 are documentation/spec hygiene, F5 is the one real outstanding gap
(and it is a Phase 1/2 deliverable surfaced through Phase 4's validation),
F6 is a small open decision.

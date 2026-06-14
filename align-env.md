# Aligning the example environment variables

## CRITICAL IMPLEMENTATION GUIDE

* Make this a complete and hard cut over to the new environment variables. There should be no compatibility layers or legacy migration. 

## TL;DR

Goal: make the three examples' env files consistent and remove variables that nothing reads anymore.

What needs to be done:

- Define one canonical shape for `examples/<name>/.env.sample`. It holds only standalone-tooling config and `NEO4J_*` secrets. It does not hold ingest, embedding, value-sampling, or eval-client keys.
- Sweep `finance-genie/.env.sample` and `schemapile/.env.sample` down to that shape. They still carry the full retired `DBXCARTA_*` ingest and embedding block.
- Trim `dense-schema/.env.sample` too. It is the cleanest of the three but still carries a stray `DBXCARTA_SUMMARY_TABLE` and two eval-client tuning keys, so it is not a perfect template as-is.
- Decide how to handle value sampling. neocarta turns it on by default, no overlay sets it, and the stale `DBXCARTA_INCLUDE_VALUES` in `.env.sample` looks authoritative while controlling nothing.
- Leave external embedding mode out. That is Phase 7.

Options:

- **Option A: Sweep only.** Trim all three `.env.sample` files to the canonical shape. Do not touch the overlays. Value sampling stays on neocarta's defaults. Fastest and lowest risk. Sampling behavior stays implicit.
- **Option B: Sweep plus make sampling explicit. (Recommended.)** Everything in A, plus add the three value-sampling keys to all three overlays as `NEOCARTA_DATABRICKS_*` so nothing important is implicit and the three examples follow one shape. Requires updating the overlay tests.
- **Option C: Sweep plus sampling explicit plus a shared documented template.** Everything in B, plus write the canonical `.env.sample` shape into the docs as a copyable template for future examples. Most thorough and most work.

Recommendation: **Option B.** It removes the dead variables, makes the three examples match, and turns the one remaining implicit behavior into an explicit setting, without the extra documentation work of C.

**DECISION**: Option B

---

## Background: the three env files and who reads them

After the neocarta cutover each example has three layers of config. Each layer has one job.

- **Repo-root `.env`** is the shared base. It holds Databricks infra and the base Neo4j secrets. It is never edited per example.
- **`examples/<name>/dbxcarta-overlay.env`** is the dbxcarta CLI config and the neocarta ingest contract. It is committed and secret-free. The ingest job reads its `NEOCARTA_DATABRICKS_*` keys. The operator CLI and the local eval client read its `DBXCARTA_*` and `DATABRICKS_*` keys. This is the single source of truth for everything the CLI, the ingest job, and the eval client need.
- **`examples/<name>/.env` and `.env.sample`** are the standalone-tooling config. They drive the local demo, the slice and materialize tooling, and `setup_secrets.sh`, and they hold the per-example `NEO4J_*` secrets. The ingest job and the eval client do not read these files. The eval client reads the overlay plus the repo-root `.env`.

The key fact: any `DBXCARTA_*` ingest, embedding, value-sampling, or eval-client key sitting in `.env.sample` is read by nothing. It is a leftover from the old single-config layout before the split.

## The problem

Three issues, all in the `.env.sample` files.

1. **Dead variables.** `finance-genie/.env.sample` and `schemapile/.env.sample` still carry the old ingest and embedding block: the five `DBXCARTA_INCLUDE_EMBEDDINGS_*` flags, `DBXCARTA_EMBEDDING_ENDPOINT`, `DBXCARTA_EMBEDDING_DIMENSION`, `DBXCARTA_EMBEDDING_FAILURE_MAX`, `DBXCARTA_INCLUDE_VALUES`, `DBXCARTA_SAMPLE_LIMIT`, `DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD`, `DBXCARTA_SCHEMAS`, and `DBXCARTA_SUMMARY_TABLE`. No code reads any of these from `.env.sample`. Ingest reads `NEOCARTA_DATABRICKS_*` from the overlay. `DBXCARTA_INCLUDE_EMBEDDINGS_VALUES` is doubly dead, because neocarta removed Value embedding entirely.

2. **The examples disagree with each other.** `dense-schema/.env.sample` was already trimmed of the embedding block. The other two still carry it. So the three examples do not follow one shape, and a reader cannot tell which keys matter by comparing them.

3. **Value sampling is implicit.** neocarta's `include_values` defaults to `True`, with `sample_limit` 10 and `sample_cardinality_threshold` 50. No overlay sets these, so all three examples sample values only because of the defaults. The stale `DBXCARTA_INCLUDE_VALUES=true` in `.env.sample` looks like it controls this, but it does not.

## The canonical `.env.sample` shape

A clean `.env.sample` holds only what the standalone tooling and `setup_secrets.sh` actually read.

Keep:

- `DATABRICKS_PROFILE` and `DATABRICKS_WAREHOUSE_ID` for standalone Databricks auth.
- `DBXCARTA_CHAT_ENDPOINT` for the local demo.
- `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` for `setup_secrets.sh` and the local demo.
- `DBXCARTA_CATALOG`, `DATABRICKS_VOLUME_PATH`, `DBXCARTA_SUMMARY_VOLUME` for the ops location the standalone materialize path uses.
- The per-example generation parameters: the `DENSE_*` keys for dense-schema and the `SCHEMAPILE_*` keys for schemapile.

Remove:

- All ingest and embedding keys: the `DBXCARTA_INCLUDE_EMBEDDINGS_*` flags, `DBXCARTA_EMBEDDING_ENDPOINT`, `DBXCARTA_EMBEDDING_DIMENSION`, `DBXCARTA_EMBEDDING_FAILURE_MAX`, and `DBXCARTA_SCHEMAS`.
- All value-sampling keys: `DBXCARTA_INCLUDE_VALUES`, `DBXCARTA_SAMPLE_LIMIT`, `DBXCARTA_SAMPLE_CARDINALITY_THRESHOLD`.
- `DBXCARTA_SUMMARY_TABLE`. neocarta has no field for it and the core resolver derives it.
- All eval-client keys: `DBXCARTA_CLIENT_ARMS`, `DBXCARTA_INJECT_CRITERIA`, `DBXCARTA_SCHEMA_DUMP_MAX_CHARS`, `DBXCARTA_CLIENT_MAX_EXPANSION_TABLES`, `DBXCARTA_CLIENT_MAX_QUESTIONS`. The eval client reads these from the overlay, not from `.env.sample`.

## What to change, file by file

### `finance-genie/.env.sample`

Remove lines 28 to 49: the summary-table line, the value-sampling block, the embedding block, and the two eval-client lines. Keep the profile, warehouse, chat endpoint, Neo4j secrets, catalog, volume path, and summary volume. Also remove `DBXCARTA_SCHEMAS`.

### `schemapile/.env.sample`

Remove the summary-table line, the value-sampling and embedding block in section 6, and the eval-client arms block in section 8. Keep the `SCHEMAPILE_*` slice, candidate, and question parameters, the profile and warehouse, the catalog, the volume path, the summary volume, and the secret scope. Also remove `DBXCARTA_SCHEMAS`.

### `dense-schema/.env.sample`

Remove `DBXCARTA_SUMMARY_TABLE`, `DBXCARTA_SCHEMA_DUMP_MAX_CHARS`, and `DBXCARTA_CLIENT_MAX_EXPANSION_TABLES`. Keep the profile, catalog, warehouse, volume path, summary volume, and the `DENSE_*` generation parameters.

### Overlays (Option B and C only)

Add value sampling to all three `dbxcarta-overlay.env` files so behavior is explicit:

```
NEOCARTA_DATABRICKS_INCLUDE_VALUES=true
NEOCARTA_DATABRICKS_SAMPLE_LIMIT=10
NEOCARTA_DATABRICKS_SAMPLE_CARDINALITY_THRESHOLD=50
```

These values match neocarta's current defaults and the behavior the examples have today. Setting them to `true`, 10, and 50 codifies value sampling as on. If any example should not sample values, set its flag to `false` instead. This change makes value sampling visible in the one file that drives ingest.

## Options in detail

### Option A: Sweep only

Trim all three `.env.sample` files to the canonical shape above. Do not touch the overlays.

- Pro: smallest change, no test updates, removes every dead variable, makes the three examples match.
- Con: value sampling stays implicit on neocarta's defaults, so a reader of the overlay still cannot see that values are sampled.

### Option B: Sweep plus explicit sampling (recommended)

Everything in Option A, plus add the three `NEOCARTA_DATABRICKS_*` value-sampling keys to all three overlays.

- Pro: every example follows one shape, every dead variable is gone, and the one remaining implicit behavior becomes an explicit setting in the file that controls it.
- Con: the overlay structural tests assert the overlay key set, so adding keys requires updating those tests.

### Option C: Sweep plus explicit sampling plus a documented template

Everything in Option B, plus write the canonical `.env.sample` shape into the docs as a copyable template, so a new example starts from one agreed shape.

- Pro: best for onboarding new examples and prevents the divergence from happening again.
- Con: most work, and the docs sweep can drift if it is not kept next to the examples.

## How to verify

After the edits:

- Grep the examples for the removed keys and confirm zero hits:
  ```
  grep -rn "INCLUDE_EMBEDDINGS\|EMBEDDING_DIMENSION\|EMBEDDING_FAILURE\|SUMMARY_TABLE\|DBXCARTA_SCHEMAS\|DBXCARTA_INCLUDE_VALUES\|SAMPLE_LIMIT\|SAMPLE_CARDINALITY" examples/*/.env.sample
  ```
- Confirm the three `.env.sample` files now hold the same categories of keys.
- Run the test suite. For Option B and C, update the overlay structural tests first: `tests/examples/finance-genie/test_overlay.py` and `tests/ops/test_ops_config_golden.py` assert the overlay key set, so the new sampling keys must be added to their expected sets.
- Run `make test`, then `uv run ruff check .` and `uv run mypy`.

## Out of scope: external embedding mode

External embedding mode stays out of this work. It is Phase 7 in `align.md`. All three overlays are wired for inline mode today, with the four `NEOCARTA_DATABRICKS_INCLUDE_EMBEDDINGS_*` flags on and `EMBEDDING_STAGING_VOLUME` set. Switching an example to external mode means turning those flags off and running the separate `neocarta databricks embed` CLI step, which Phase 7 covers. This plan only aligns and cleans the existing variables. It does not add an external-mode path.

## Status and progress

**Status: Complete (Option B, hard cutover).** The dead keys were removed outright. No commented-out blocks, no fallback reads, no aliases.

What was done:

- **Overlays, all three.** Added the explicit value-sampling block (`NEOCARTA_DATABRICKS_INCLUDE_VALUES=true`, `NEOCARTA_DATABRICKS_SAMPLE_LIMIT=10`, `NEOCARTA_DATABRICKS_SAMPLE_CARDINALITY_THRESHOLD=50`, matching neocarta's defaults and the examples' current behavior) and pinned the client query-time endpoint `DBXCARTA_EMBEDDING_ENDPOINT=databricks-gte-large-en` so graph_rag embeds questions with the same model that built the graph.
- **`finance-genie/.env.sample`.** Removed `DBXCARTA_SCHEMAS`, `DBXCARTA_SUMMARY_TABLE`, the value-sampling block, the full embedding block, and the eval-client keys. Kept the standalone local-demo ops location, the chat endpoint, the Neo4j secrets, and the profile and warehouse. Updated the section header to name it as the standalone local-demo location, not an overlay.
- **`schemapile/.env.sample`.** Removed `DBXCARTA_SCHEMAS`, `DBXCARTA_SUMMARY_TABLE`, the value-and-embedding block, the eval-client arms, and the duplicate `DATABRICKS_SECRET_SCOPE`. Kept the `SCHEMAPILE_*` pipeline params, the ops location, the profile, and the warehouse.
- **`dense-schema/.env.sample`.** Removed the stray `DBXCARTA_SUMMARY_TABLE` and the two eval-client tuning keys. Kept the `DENSE_*` generation params and the ops location.
- **Tests.** Strengthened `tests/examples/finance-genie/test_overlay.py` to assert the new explicit value-sampling keys and both embed-endpoint keys. Fixed a stale key name in the `tests/integration/test_semantic_search.py` docstring (`DBXCARTA_INCLUDE_EMBEDDINGS_TABLES` to `NEOCARTA_DATABRICKS_INCLUDE_EMBEDDINGS_TABLES`).

Two deviations from the plan, both made to honor the hard-cutover directive:

- **Removed the duplicate `DATABRICKS_SECRET_SCOPE` from `schemapile/.env.sample`.** The plan said to keep the secret scope. Verification of `setup_secrets.sh` showed the scope name is read only from the committed overlay, which is the single source of truth. The copy in `.env.sample` was a dead duplicate that could drift, so it was removed.
- **Correct test-path name.** The plan named `tests/ops/test_ops_config_golden.py`. The file is at `tests/core/test_ops_config_golden.py`. It is lenient about the overlay key set, so adding keys did not break it and it needed no edit.

Open follow-up, not done (needs a decision):

- **`NEO4J_*` placeholders are missing from `dense-schema/.env.sample` and `schemapile/.env.sample`.** Only `finance-genie/.env.sample` carries them. `setup_secrets.sh` requires `NEO4J_*` in each example's standalone `.env` to provision that example's secret scope, so a reader copying these two templates has no prompt to fill them in. Adding placeholders would complete the alignment but is an addition rather than a sweep, so it was left for a separate decision.

Validation:

- Grep confirms zero dead keys remain in any `.env.sample`, and all three overlays carry the new value-sampling and embed-endpoint keys.
- `tests/examples/finance-genie/test_overlay.py` and `tests/core/test_ops_config_golden.py` pass.
- `make test` green: 419 passed.
- `uv run ruff check .` clean.
- mypy on the changed test file is clean. No source `.py` files were changed, so types cannot regress. Running mypy across multiple package `src` trees at once trips a pre-existing namespace path-mapping quirk in this monorepo, unrelated to this change.

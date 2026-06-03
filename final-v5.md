# Align-catalog review: what else needs fixing

## The short version

The `align-catalog.md` plan is almost fully implemented and it works. All 500 tests pass. The three example presets are now byte-for-byte identical, the shared `StandardPreset` carries the one readiness rule, the layer tag rides on the catalog list, bronze is gone, and the docs were updated. The goal of "same contract and same shape, simpler presets and overlays" is essentially met.

This document lists the few things still left to fix. One is a real bug, the rest are polish and consistency notes.

## What is already correct (no action needed)

- The three `preset.py` files are identical one-liners that build `StandardPreset` from the bundled `questions.json`. See `examples/dense-schema/.../preset.py`, `examples/schemapile/.../preset.py`, `examples/finance-genie/.../preset.py`.
- The shared readiness rule and the question upload live once in `packages/dbxcarta-spark/src/dbxcarta/spark/presets.py`.
- The catalog parser is shared by readiness and the pipeline. See `resolve_catalogs` in `packages/dbxcarta-spark/src/dbxcarta/spark/settings.py`.
- The layer tag is folded into the catalog list. The separate `DBXCARTA_LAYER_MAP` variable and its validator are gone. The finance overlay uses the `catalog:layer` form. See `examples/finance-genie/dbxcarta-overlay.env`.
- finance-genie's old `finance_genie.py` is deleted, its `questions.json` moved to the example root, and bronze is removed from code, tests, and docs.
- Docs match the new shape: `docs/reference/architecture.md`, the root `README.md`, and `.env.sample` all describe the `:layer` suffix instead of the old map.

## What still needs fixing

### 1. The finance-genie standalone demo points at the wrong questions file (real bug)

- **What is wrong:** `align-catalog.md` moved finance-genie's `questions.json` out of the package folder and up to the example root. But `local_demo.py` was not updated. It still looks for the questions file right next to itself, inside the package folder, where the file no longer exists.
- **Where:** `examples/finance-genie/src/dbxcarta_finance_genie_example/local_demo.py`. The `DEFAULT_QUESTIONS` line near the top builds the path from "next to this file," and three command options use that as their default.
- **Why it matters:** Anyone who runs the finance-genie local demo without typing an explicit `--questions` path will hit a missing-file error. The demo is broken out of the box for the common case.
- **Why the tests did not catch it:** The local-demo test in `tests/examples/finance-genie/test_local_demo.py` always passes its own temporary questions file, so it never exercises the broken default.
- **Recommended fix:** Point `DEFAULT_QUESTIONS` at the example root, the same place the preset already reads from. The file's own comment a few lines down already explains that "two levels up" is the example root, so reuse that same anchor for the questions path. Then either add a tiny test that the default path resolves to a real file, or assert the default equals the preset's `questions_file`, so this cannot silently break again.

### 2. The readiness report still carries "optional" wording that no longer means anything (polish)

- **What is wrong:** Under the new design every listed catalog is simply required, so the "optional" idea is dead. But the report object still has an empty `missing_optional` slot, the `--strict-optional` flag still exists, and the printed output always ends with a line that says "optional catalogs: ready" even though there is no such thing as an optional catalog anymore.
- **Where:** `ReadinessReport.format` in `packages/dbxcarta-spark/src/dbxcarta/spark/presets.py`, and the `--strict-optional` flag in the CLI.
- **Why it matters:** It is harmless but confusing. A reader of the output sees a category that the design deliberately removed. The plan kept this on purpose to avoid changing method signatures, which is a reasonable call, so this is a cleanup choice rather than a defect.
- **Recommended fix (optional):** Either drop the always-empty "optional" line from the printed output and leave the data fields alone, or decide explicitly to keep the flag for future use and add a one-line comment in the report saying the optional path is intentionally inert today. Pick one so the next reader is not left guessing.

### 3. The single-catalog examples and the multi-catalog example are not written the same way (consistency note)

- **What is the situation:** finance-genie lists its catalogs in `DBXCARTA_CATALOGS`. dense and schemapile leave `DBXCARTA_CATALOGS` blank and rely on the parser falling back to the single `DBXCARTA_CATALOG`. Both paths work and both are allowed by the plan.
- **Where:** the three `dbxcarta-overlay.env` files under `examples/`.
- **Why it matters:** The stated goal is "same shape." Today two examples express their catalog set one way and the third expresses it another way. This is a deliberate and acceptable shortcut, not a bug, because the fallback is documented behavior. It is worth a conscious decision rather than an accident.
- **Recommended fix (optional):** Decide whether "same shape" should mean every overlay always lists its catalogs in `DBXCARTA_CATALOGS`, even when there is only one. If yes, add the single catalog to the dense and schemapile overlays so all three read alike. If no, leave it and note in the env docs that a single-catalog overlay may omit `DBXCARTA_CATALOGS`.

## Suggested order of work

1. Fix the finance-genie demo questions path. This is the only item that breaks something for a real user.
2. Decide and act on the "optional" wording cleanup.
3. Decide whether to make all three overlays list their catalogs the same way.

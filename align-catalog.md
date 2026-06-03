# Make presets carry behavior, with one readiness rule for every example

## What is changing (ELI5)

- **Hardcoded config**: deleted. Presets no longer bake in catalog, schema, or table names. The overlay becomes the only source.
- **Readiness check**: now one rule for all three examples. For each catalog the overlay lists, does it hold a schema? Yes to every catalog means ready.
- **Shared preset**: the three separate readiness implementations collapse into one `StandardPreset` class in `dbxcarta-spark`, built with each example's questions file.
- **Layer tag**: the separate `DBXCARTA_LAYER_MAP` variable folds into the catalog list as `catalog:layer` entries, so the layer rides on the catalog it belongs to.
- **finance-genie**: bronze is removed everywhere. The silver-versus-gold split and the optional-table tier are gone. Silver and gold become a plain catalog list like every other example.
- **DBXCARTA_SCHEMAS**: readiness stops reading it. The ingest pipeline still uses it to scope which schemas to pull.

## The new design

- One readiness rule: does each ingested catalog hold at least one data schema, meaning a schema other than the auto-created `information_schema` and `default`?
- One implementation: a `StandardPreset` class in `dbxcarta.spark.presets`, next to the `ReadinessReport` and the protocols it already defines.
- Each example's `preset.py` is the same one-liner: it constructs `StandardPreset` with its bundled `questions.json`, found at an identical relative path. There is no per-example override variable and no per-example helper, so the three files and their `__init__.py` exports are byte-for-byte identical.
- The overlay is the single source of truth for which catalogs to check and ingest.
- Catalog and layer travel together inside `DBXCARTA_CATALOGS`, so there is no parallel map to keep in sync.

## Key goals of the unified design

- **No drift.** Readiness and the pipeline read the same catalog list through one parser, so they can never validate different catalogs.
- **No special cases.** finance-genie runs the same rule as dense and schemapile.
- **Behavior in code, config in the overlay.** Presets hold only the bundled questions file and the readiness and upload behavior.
- **One home for shared logic.** It lives in `dbxcarta-spark`, the dependency every example already imports, so it adds no new library.
- **State each fact once.** The layer rides on the catalog entry, removing the parallel `DBXCARTA_LAYER_MAP` and its sync validator.

## Resolved design questions

Each of these was a fork during review. All are resolved toward the simpler, unified path, and the detail lives in the sections below.

- **finance-genie coverage**: schema-presence is the gate, by design. A separate upstream project owns and validates finance-genie's tables, so dbxcarta confirms only that silver and gold exist and hold a schema. There is no table-count check.
- **Report output**: the scalar `catalog` field holds the joined catalog list and `schema` is empty. `.format()` lists the catalogs and relabels its lines from tables to catalogs. `ok()` and the CLI `--strict-optional` flag stay unchanged.
- **Questions override**: removed. There is no `DENSE_QUESTIONS_FILE` or `SCHEMAPILE_QUESTIONS_FILE` env override. The bundled `questions.json` at the example root is the only source, so `upload_questions()` takes no per-example variable and every preset behaves identically.
- **Validation**: readiness stays light and does not build the full `SparkIngestSettings`, but it validates catalog identifiers through the shared parser, so a malformed catalog fails loud the way an ingest run would.
- **Questions file location**: standardized. All three keep `questions.json` at the example root, so the path each preset computes is the identical expression. finance-genie's copy moves out of its package to the example root to match.

## What a preset is

A "preset" is a small Python object for each example: dense, schemapile, finance-genie. It runs the readiness check and uploads the demo questions for that example. The dbxcarta CLI loads it by name, like `dbxcarta preset dbxcarta_dense_schema_example:preset`. Per the preset module docstring, the preset is meant to carry only behavior. The per-example config is supposed to live in the committed `dbxcarta-overlay.env`, not in the preset.

## The problem: presets hardcode config that can lie

The preset object is built the instant its file is loaded, before the CLI has even run. At that moment it bakes in the catalog name as a hardcoded value, for example `"schemapile_lakehouse"`. The actual ingest pipeline never reads that hardcoded value. It reads `DBXCARTA_CATALOG` and `DBXCARTA_CATALOGS` from the overlay through `SparkIngestSettings`, bypassing the preset. So there are two separate sources for "which catalog": the preset's hardcoded copy, used only by the readiness check, and the overlay value, used by everything else.

This is worse than two strings that must agree by hand. The readiness check exists to answer one question: is the data the pipeline is about to ingest actually present? When the preset's catalog and the overlay's catalog differ, the readiness check validates a different catalog than the one the pipeline ingests. That is a health check that lies. It can report "ready" against a stale catalog while the real ingest target is empty, or report "not ready" against a catalog nobody uses. This is exactly what bit dense: its preset said `schemapile_lakehouse` while its overlay said `dense-schema_example`, so the check was green on the wrong catalog.

## finance-genie has the same problem

finance-genie's preset hardcodes `_SILVER_CATALOG`, `_GOLD_CATALOG`, and `_DEFAULT_SCHEMA`, the same strings already in its overlay's `DBXCARTA_CATALOG`, `DBXCARTA_CATALOGS`, and `DBXCARTA_SCHEMAS`. It also hardcodes three table lists and splits readiness into a silver-versus-gold, required-versus-optional check. So finance-genie has the same drift problem as the others, spread across more variables. It feels different only because its readiness was modeled as a medallion, but a separate upstream project owns that medallion's catalogs, schemas, and tables. dbxcarta only validates that the expected catalogs exist, ingests them, and ships a sample client. Schema enforcement, materialization, and table-level checking are not dbxcarta's job.

## The fix: one readiness rule, "does each ingested catalog hold a schema?"

Readiness checks exactly the catalogs the overlay says to ingest, and nothing else. The rule is the same for all three examples:

> For each catalog in `resolved_catalogs()`, does it contain at least one schema other than `information_schema` and `default`? If every ingested catalog does, the run is ready.

Unity Catalog auto-creates both `information_schema` and an empty `default` schema in every new catalog, so the rule excludes both. A catalog that holds only those two has nothing materialized and reports not ready. This closes a latent gap rather than introducing one: schemapile's current rule excludes only `information_schema`, which is safe only because its catalog is dedicated and data-only. finance-genie's silver and gold are owned by an upstream project whose provisioning order dbxcarta does not control, so the gate must not read a bare `default` as data.

That is the whole rule. There is no `DBXCARTA_SCHEMAS` branch, no named-schema matching, no silver-versus-gold split, and no required-versus-optional tier. If you want a catalog checked, you list it for ingestion. If you do not, you leave it off. The list is the contract.

This is schemapile's current "is there a materialized schema yet" check, applied to every catalog the run ingests. It dissolves the two semantic worries from earlier reviews instead of trading them off:

- There is no cross-catalog table matching to get wrong, because each catalog is checked on its own for a schema.
- There is no optional tier, because a catalog is either listed and checked, or not listed.

dense gives up verifying its specific named schemas. A readiness gate only needs to confirm the upstream step produced data, and "a data schema exists" confirms that. The named-schema list was ceremony.

finance-genie gives up more, and this is a deliberate choice. Today it confirms five named silver tables and three gold tables. The new rule only asks whether each catalog holds a schema, so a silver catalog whose schema is present but empty now reports ready. That coverage drop is acceptable because a separate upstream project owns finance-genie's medallion and validates its tables with its own gold-table gate. dbxcarta's readiness only needs to confirm the catalogs it is about to ingest are present and populated with a schema. Both silver and gold already exist and must exist, so the check is simply whether both are there. If the upstream contract ever needs table-level assurance inside dbxcarta, that belongs in a separate explicit check, not in the readiness gate.

Readiness reads only the catalog list. `DBXCARTA_SCHEMAS` stays in the overlay because the ingest pipeline still uses it to scope which schemas to pull, but the readiness check ignores it. Fail-loud still holds: `dbxcarta_catalog` is a required field with no default, matching the `config.py` precedent that requires `DATABRICKS_VOLUME_PATH` with no fallback.

## Where the shared logic lives

The readiness rule is now identical for all three examples, so it lives once in `dbxcarta.spark.presets`, the module that already defines `ReadinessReport` and the `Preset` / `ReadinessCheckable` / `QuestionsUploadable` protocols. This is the existing dependency every example already imports from, so it adds no new library.

A single concrete `StandardPreset` dataclass implements `readiness()` and `upload_questions()`. It is constructed with the one thing that differs per example: the bundled questions file path.

- `readiness()` reads `DBXCARTA_CATALOG` and `DBXCARTA_CATALOGS` from `os.environ` and resolves the catalog list through the shared `resolve_catalogs()` function, which validates each catalog name with the same `validate_identifier` the pipeline uses. It then checks each catalog for a data schema. Readiness is a deliberately light check. It does not construct the full `SparkIngestSettings`, which carries required fields readiness does not use, but it does fail loud on a malformed catalog name, the same way an ingest run would. It also fails loud with a clear message when `DBXCARTA_CATALOG` is absent, rather than surfacing a bare `KeyError`.
- `upload_questions()` reads the bundled `questions.json` at the constructed path, validates it, and uploads it to `DBXCARTA_CLIENT_QUESTIONS`. There is no override variable. The destination is validated once, for every example, as a `/Volumes/...json` path, the check that two of the three presets already enforced. The questions-file validation is a stdlib-only structural check (a non-empty JSON array whose entries each carry a non-empty `question_id` and `question`) rather than the client's `load_questions`, because the cross-layer import boundary forbids `dbxcarta-spark` from importing `dbxcarta-client`. The client validates the full `Question` model when it loads the uploaded file at query time.

Each example's `preset.py` is the identical one-liner: construct `StandardPreset` with `Path(__file__).resolve().parents[2] / "questions.json"`, and export it as `preset`. The path expression is identical because all three keep `questions.json` at the same relative location, the example root. finance-genie's copy moves out of its package to the example root so its path resolves the same way. The helpers that fetch schemas, ensure the upload directory, and validate the questions file all live in `StandardPreset`, so no example carries its own copy.

## finance-genie specifics: drop bronze, fold layer into the catalog list

finance-genie ingests silver and gold only. Bronze is dropped from every file. Both silver and gold already exist and must exist, so readiness is simply "do both catalogs hold a schema," the same flat check every example runs.

The Table-node `layer` property is the one medallion fact finance-genie still expresses in the graph, and it is a write-time graph attribute, unrelated to readiness. The simplest way to carry it is as an attribute of each catalog in the one list, rather than a separate parallel map:

```
DBXCARTA_CATALOGS=graph-enriched-finance-silver:silver,graph-enriched-finance-gold:gold
```

Each entry is `catalog` or `catalog:layer`. `resolve_catalogs()` reads the names with the layer suffix stripped. `layer_map()` reads the layers from the same list. dense and schemapile write plain catalog names with no suffix, so their layer is null and nothing changes for them.

This folds the layer into the catalog list and deletes the separate `DBXCARTA_LAYER_MAP` variable. The catalog and its layer can no longer drift, because the layer rides on the catalog entry. The cross-field validator that exists only to catch a layer mapped to a never-ingested catalog also deletes, because that mismatch becomes impossible to write. It removes the env-internal redundancy where catalog names appeared in both `DBXCARTA_CATALOGS` and `DBXCARTA_LAYER_MAP`.

The single `DBXCARTA_CATALOG` anchor stays. The ops, summary, and verify paths still key off it, per the known limitation documented in `settings.py`. For finance-genie it is the silver catalog.

## Settings changes

In `packages/dbxcarta-spark/src/dbxcarta/spark/settings.py`:

- Add a module-level `resolve_catalogs(catalog: str, catalogs: str) -> list[str]` holding the split, layer-strip, dedupe, single-catalog fallback, and a `validate_identifier` call on each resolved name. `SparkIngestSettings.resolved_catalogs()` delegates to it, so readiness and the pipeline share one parser, validate identically, and can never disagree on which catalogs the run touches.
- `layer_map()` reads layers from the same `dbxcarta_catalogs` list instead of a separate field.
- `_validate_catalogs` splits off the optional `:layer` suffix before validating each catalog name as an identifier, and keeps the layer-token checks the deleted `_validate_layer_map` enforced: an entry has at most one colon, and when a layer is present it is a non-empty alphanumeric/underscore token. A malformed entry like `cat:` or `cat:a:b` fails at startup rather than surfacing as a confusing graph attribute.
- Delete the `dbxcarta_layer_map` field, the `_validate_layer_map` validator, and the layer-checking half of `_validate_feature_coherence`. The layer-token validation it performed moves into `_validate_catalogs` (above), so no enforcement is lost.

## The report shape for multiple catalogs

`ReadinessReport` keeps its dataclass fields. `present` and `missing_required` hold catalog names instead of table names: a catalog is `present` when it holds a data schema and `missing_required` when it does not. `missing_optional` stays empty, as dense and schemapile already leave it, so `ok()` and the CLI `--strict-optional` flag keep working with no signature change.

Two fields need concrete values in the multi-catalog case. The scalar `catalog` field holds the comma-joined resolved catalog list, and `schema` holds the empty string, because readiness no longer targets a single schema. `.format()` is updated to match: its scope line lists the catalogs and omits the trailing schema when it is empty, and its labels change from tables to catalogs, so it reads `present catalogs`, `missing required: <catalogs>`, and `catalogs: ready`. This relabeling is the only behavior change to the shared report, and it unifies the output for single-catalog and multi-catalog runs.

## What this removes

- **Python-to-overlay duplication.** Every hardcoded constant that restates an overlay value: dense's and schemapile's `_DEFAULT_CATALOG`, finance-genie's `_SILVER_CATALOG`, `_GOLD_CATALOG`, and `_DEFAULT_SCHEMA`, every expected-table list, and the `base_tables` and `optional_tables` fields. After the fix the presets hold no catalog, schema, or table config, so Python and the overlay can never disagree. This is the class that can lie, so this is the important one.
- **The separate layer map.** `DBXCARTA_LAYER_MAP` folds into the catalog list, removing the parallel variable and its sync validator.
- **finance-genie's vestigial ops fields.** `volume_path` and the `ops_catalog` / `ops_schema` / `ops_volume` fields are referenced only by their own test, never by the CLI or loader. The overlay's `DATABRICKS_VOLUME_PATH` is what the pipeline reads, so these fields and that test are deleted.
- **The three per-example readiness implementations.** They become one shared `StandardPreset`.

## What stays per example

Only the contents of the bundled `questions.json` and the choice of which catalogs the overlay lists. The question set is genuinely per-example, and the catalog choice is overlay config, not preset code. Every line of preset Python is shared or identical across the three examples.

## What changes per example

- dense and schemapile: delete the `catalog` field, `_DEFAULT_CATALOG`, the per-example readiness method, and the per-example helpers and override that now live in `StandardPreset`. Each `preset.py` becomes the identical `StandardPreset` one-liner, and each `__init__.py` exports only `preset`. Neither sets a layer.
- finance-genie: delete `catalog`, `gold_catalog`, `schema`, the `_SILVER_CATALOG` / `_GOLD_CATALOG` / `_DEFAULT_SCHEMA` constants, all table-list constants, the `base_tables` and `optional_tables` fields, the silver-versus-gold readiness logic, and the vestigial ops fields. `finance_genie.py` is deleted outright and replaced by the same `preset.py` one-liner the other two use; its `questions.json` moves from the package to the example root, and `__init__.py` re-exports `preset` from `preset.py`. Its overlay sets `DBXCARTA_CATALOGS=graph-enriched-finance-silver:silver,graph-enriched-finance-gold:gold` and drops `DBXCARTA_LAYER_MAP`. Bronze is removed everywhere.

## Why this is better

- The readiness check can no longer validate a different catalog than the pipeline ingests, because both read the same resolved catalog list through one parser. Drift becomes impossible by construction.
- All three examples share one readiness rule and one implementation, so finance-genie stops being a special case.
- It matches the docstring's stated intent that presets carry behavior and the overlay carries config.

## Where this lives in the code

The hardcoded config to delete:

- dense: `examples/dense-schema/src/dbxcarta_dense_schema_example/preset.py`. `_DEFAULT_CATALOG` (line 27), the `catalog` field and its `__post_init__` validation (lines 33-36), and the `readiness()` method (lines 45-71). Singleton built at import (line 128).
- schemapile: `examples/schemapile/src/dbxcarta_schemapile_example/preset.py`. `_DEFAULT_CATALOG` (line 29), the `catalog` field and validation (lines 44-47), and the `readiness()` method (lines 49-82). Singleton at line 143.
- finance-genie: `examples/finance-genie/src/dbxcarta_finance_genie_example/finance_genie.py` is deleted in full. A new `preset.py` holds the identical `StandardPreset` one-liner, `__init__.py` re-exports `preset` from it, and `questions.json` moves from the package to `examples/finance-genie/questions.json`. The deleted file held the `_SILVER_CATALOG` / `_GOLD_CATALOG` / `_DEFAULT_SCHEMA` constants, all four table-list constants, the `catalog` / `gold_catalog` / `schema` and ops fields, `volume_path`, the silver-versus-gold `readiness()`, and the import-time singleton.

The shared implementation and the settings it reads:

- `packages/dbxcarta-spark/src/dbxcarta/spark/presets.py`: add `StandardPreset` with `readiness()` and `upload_questions()`, alongside the existing `ReadinessReport` and protocols.
- `packages/dbxcarta-spark/src/dbxcarta/spark/settings.py`: `resolved_catalogs()` (lines 178-189) and `layer_map()` (lines 191-200) get their parser extracted into a module-level `resolve_catalogs()`; the layer-map field and its two validators are deleted. `dbxcarta_catalog` is a required field with no default, so fail-loud is already in place. The full `SparkIngestSettings` also requires `databricks_secret_scope` (line 35) and `dbxcarta_summary_volume` / `dbxcarta_summary_table` (lines 69-70), none of which readiness uses, which is why readiness calls the standalone parser rather than constructing settings.

The tests to rewrite: deleting the three per-example readiness methods and folding `_validate_questions_file` and the schema fetch into `StandardPreset` breaks all three example preset test modules, not just finance-genie's. Each constructs the now-deleted per-example preset class, imports helpers that move into `presets.py`, or asserts on the old per-example report semantics.

- `tests/examples/finance-genie/test_preset.py` imports `_EXPECTED_TABLES` (line 19) and asserts on table-level `missing_required` and `missing_optional` (lines 91-130). Rewrite it to monkeypatch the schema fetch and assert catalog-level readiness. Delete `test_volume_path_is_volumes_subpath` (lines 84-88) with the `volume_path` field. Drop the `FinanceGeniePreset`/`_EXPECTED_TABLES` imports.
- `tests/examples/schemapile/test_preset.py` constructs `SchemaPilePreset(catalog=...)` (lines 30, 67), imports `_QUESTIONS_FILE` and `_validate_questions_file` from the example (lines 11-16), and asserts `report.present == ("sp_a", "sp_b")` (line 62, schema names) and `scope: schemapile_lakehouse.sp_a` (line 72). After the change those symbols live in `presets.py`, `present` holds catalog names, and `schema` is empty. The module shrinks to asserting the shared `preset` satisfies the protocols and resolves via its import path; the readiness, format, and questions-file assertions move to the shared `tests/spark` module.
- `tests/examples/dense-schema/test_preset.py` constructs `DenseSchemaPreset(catalog=...)` (line 16), reads `preset.catalog` (line 11), monkeypatches the per-example `_fetch_schema_names` (line 29), and asserts `report.present == ("dense_1000",)` (line 35, schema names). The per-example preset class, catalog field, and fetch helper are gone, so the module shrinks the same way schemapile's does: assert the shared `preset` satisfies the protocols and resolves via its import path.
- Add a shared `tests/spark/test_presets.py` module for `StandardPreset`: the readiness rule (a catalog with only `information_schema` and `default` is not ready; one data schema is ready; a multi-catalog list reports each catalog), the relabeled `ReadinessReport.format()`, the stdlib `_validate_questions_file`, the `/Volumes/...json` destination check, and the fail-loud on missing `DBXCARTA_CATALOG`. This is where the logic now lives, so this is where it is tested once.
- `tests/spark/settings/test_settings_validators.py` has a layer-map block (lines 186-257): `test_layer_map_parses_pairs`, `test_layer_map_empty_by_default`, `test_settings_rejects_malformed_layer_map`, `test_settings_rejects_layer_map_for_non_ingested_catalog`, `test_settings_accepts_layer_map_subset_of_ingested`, and `test_settings_layer_map_validated_against_single_catalog_fallback`. All construct `SparkIngestSettings(dbxcarta_layer_map=...)`, a field that is deleted. Rewrite them against the folded form: layers now ride on `DBXCARTA_CATALOGS` entries, so `layer_map()` parses `cat:layer` from that list, malformed entries are rejected by `_validate_catalogs`, and the never-ingested-catalog test deletes outright because a layer can no longer name a catalog that is not in the list.

Docs and samples to update for the folded `DBXCARTA_LAYER_MAP`:

- `.env.sample` lines 49 and 56 document `DBXCARTA_LAYER_MAP` as a separate variable. Replace with the folded `catalog:layer` form on `DBXCARTA_CATALOGS`.
- `README.md` lines 117 and 270 describe the `Table.layer` tier as derived from `DBXCARTA_LAYER_MAP`, and the code snippet at lines 131-132 passes `dbxcarta_layer_map=`. Repoint all three to the layer suffix on `DBXCARTA_CATALOGS`.
- `docs/reference/architecture.md` lines 35 and 69 reference `DBXCARTA_LAYER_MAP[catalog]` and the `catalog:layer` pairs variable. Update to the folded list.

## Clear bronze out completely

finance-genie drops to silver and gold, so every trace of its bronze tier is removed. Note that bronze is dead in the pipeline today: the overlay's `DBXCARTA_CATALOGS` already lists only silver and gold, and `_BRONZE_TABLES` is an unreferenced constant whose own comment admits it is "not a readiness gate." So removing it changes no ingest behavior; it deletes dead code and stale narrative.

finance-genie's own bronze, all removed:

- `examples/finance-genie/src/dbxcarta_finance_genie_example/finance_genie.py`: the `_BRONZE_TABLES` constant and its comment (lines 53-60), and the "Bronze and gold are ingested" docstring line (line 73). Already inside this proposal's deletion of the table-list constants.
- `README.md` line 269: the "three-catalog medallion ... bronze and silver ... `graph-enriched-finance-bronze`, `-silver`, and `-gold`" description becomes a two-catalog silver-and-gold layout.
- `docs/reference/architecture.md` line 29 (the `graph-enriched-finance-bronze :bronze` diagram row), line 86 ("a raw bronze one"), and line 136 ("sees bronze, silver, and gold"): redraw and reword to silver and gold only.
- `tests/spark/test_verify_scope.py`: the `_BRONZE = "graph-enriched-finance-bronze"` fixture (line 40), commented "the production Finance Genie shape," and its use in the multi-catalog scope tests (lines 121, 124, 174, 184, 192, 200, 221, 231, 250-260). The multi-catalog scope behavior under test is real and must stay covered, so reduce the fixture to the now-accurate silver-and-gold pair rather than deleting the cases.

Bronze that is NOT finance-genie and stays: bronze/silver/gold is the canonical vocabulary for the `Table.layer` feature, which this proposal keeps (it only folds the layer into `DBXCARTA_CATALOGS`). The illustrative `bronze`/`silver`/`gold` sample names in `docs/schema/SCHEMA.md` (line 80), `docs/reference/best-practices.md` (line 168), `contract.py` (lines 13, 122), `client/settings.py` (line 22), `client/schema_dump.py` (line 69), `carta-comparisson.md` (lines 17, 45), and the unit-test fixtures in `test_node_builders.py`, `test_settings_validators.py`, `fk_metadata.py`, and `test_retriever.py` document and test that feature with throwaway catalog names. They are not finance-genie's bronze and removing them would be churn against a feature that is not changing, so they stay.

The pattern to copy, env read at call time, which is safe because the CLI loads env before importing the preset:

- `cli.py`, `_handle_preset()`: `_load_env(argv)` then `load_preset(args.spec)`.
- `loader.py`: `load_preset()` imports the module after env is loaded.

The single source of truth, the overlay:

- dense: `examples/dense-schema/dbxcarta-overlay.env`, `DBXCARTA_CATALOG=dense-schema_example`.
- schemapile: `examples/schemapile/dbxcarta-overlay.env`, `DBXCARTA_CATALOG=schemapile_lakehouse`.
- finance-genie: `examples/finance-genie/dbxcarta-overlay.env`, `DBXCARTA_CATALOG` plus `DBXCARTA_CATALOGS` with the folded layer suffixes.

The fail-loud, no-fallback precedent to match:

- `examples/dense-schema/src/dbxcarta_dense_schema_example/config.py`, which requires `DATABRICKS_VOLUME_PATH` and raises rather than falling back.
- `SparkIngestSettings.dbxcarta_catalog`, a required field with no default.

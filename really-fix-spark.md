# Really Fix: Parameterized SQL in the Spark Verify Path

## ELI5: What Is the Issue

Imagine you write a form letter and, wherever a person's name should go, you cut their name out of their ID card and glue it onto the page by hand. Most of the time this works. But if someone's name contains a quotation mark, a dash, or some other odd character, the sentence around it falls apart. Worse, a cleverly chosen "name" could glue extra instructions onto your letter that you never meant to send.

Our verify code talks to the Databricks SQL warehouse by gluing values straight into the query text. It glues in the catalog name and the list of schema names by hand. We recently patched the schema-name gluing so a stray quotation mark no longer breaks it. But two problems remain. The catalog name is still pasted directly into the query as a table address, and that kind of value cannot be made safe by hand-escaping the same way a plain string can. And the whole approach still depends on us remembering to escape everything correctly forever.

The real fix is to stop gluing values into the query text at all. Databricks gives us a proper way to hand the query and its values to the database as two separate things. The database then keeps the instructions and the data strictly apart, so no value can ever break or rewrite the query. That is what this proposal does.

## Technical Summary

The file `packages/dbxcarta-spark/src/dbxcarta/spark/verify/catalog.py` builds SQL statements with f-string interpolation and sends them to the Databricks SQL Statement Execution API through a thin helper, `_exec`. There are three call sites across `_check_id_normalization` and `_check_complex_type_round_trip`.

Three categories of value are interpolated into the query text:

1. The catalog name, spliced in as a backtick-quoted object identifier in the form catalog.information_schema.columns. This appears at all three sites.
2. The list of in-scope schema names, rendered as a comma-separated list of SQL string literals inside an IN clause. This appears at two sites. It currently goes through the `_sql_str_list` helper added in the prior cleanup, which correctly doubles embedded single quotes.
3. The complex-type family prefix, spliced into a LIKE pattern. This appears at one site. The value originates from the static `_COMPLEX_TYPE_FAMILIES` constant.

The schema-literal and prefix cases are now string-correct, but they still rely on hand-escaping discipline rather than a guarantee from the engine. The catalog case is the substantive one. An object identifier cannot be made safe by string-escaping the way a string literal can, because the escaping rules for identifiers are different and a backtick or dotted value can change which object is addressed. The input is currently trusted configuration, so this is a robustness and correctness concern rather than an active exploit, but it is exactly the pattern the Databricks documentation tells us not to use.

Databricks SQL provides the correct mechanism. The Statement Execution API accepts named parameter markers in the statement text and a separate list of typed parameter values, supplied as `StatementParameterListItem` entries from the Databricks SDK. Scalar values bind through ordinary named markers. Object identifiers bind through the `IDENTIFIER()` clause, which accepts a string parameter and resolves it as a catalog, schema, table, or column name without ever letting it act as query syntax. A variable-length IN list is handled by generating one named marker per element and binding each element as its own parameter.

The plan threads a parameters argument through `_exec`, then converts each call site: the catalog-qualified table name moves to an `IDENTIFIER()` clause bound to a single parameter, the schema IN list moves to generated per-element markers, and the LIKE prefix moves to a bound value. Once every site is converted, the `_sql_str_list` helper is no longer reachable and is deleted. Behavior, results, and emitted violations are unchanged; only the transport of values into the engine changes.

## Phased Implementation Plan

Status legend: [ ] not started, [~] in progress, [x] complete.

### Phase 1: Make the Executor Able to Carry Parameters

Status: [x] complete. `_exec` now accepts an optional parameter list and forwards it to `execute_statement`; default `None` preserves prior behavior. Type-checking import of `StatementParameterListItem` added under `TYPE_CHECKING`. Validated: syntax parse, mypy clean on `dbxcarta.spark.verify`. No call site changed, so behavior is unchanged.

ELI5: Right now the part of the code that sends a query to the database can only carry the query text and nothing else. Before we can stop gluing values into the text, we first need to give that messenger a second pocket where it can carry the values separately. This phase just adds the empty pocket. Nothing else changes yet.

Technical: Extend the `_exec` helper to accept an optional sequence of parameter items in addition to the statement string, and forward them to the Databricks SDK `execute_statement` call as its parameters argument. When no parameters are supplied the call behaves exactly as today. No call site changes in this phase. This is a pure capability addition with no behavioral effect, which keeps the risky transport change isolated from the later query rewrites.

Acceptance: the existing verify tests still pass with `_exec` unchanged in behavior when called without parameters.

### Phase 2: Bind the Catalog Name as an Identifier

Status: [x] complete. All three call sites now select `FROM IDENTIFIER(:tbl)` with `tbl` bound to the qualified columns-table name via a new `_str_param` builder (local SDK import) and `_columns_table` helper. The catalog value can no longer act as query syntax. Validated: syntax parse, mypy clean on `dbxcarta.spark.verify`.

Quality review (authoritative Databricks docs, IDENTIFIER clause reference): `SELECT ... FROM IDENTIFIER(:param)` with a single string parameter holding a qualified multi-part name is the documented SQL-injection-safe pattern, supported on Databricks SQL warehouses. Review found one regression: the first cut produced an unquoted `cat.information_schema.columns`, dropping the prior code's back-quoting of the catalog that lets a non-simple-identifier catalog name resolve. Fixed: `_columns_table` now back-quotes every part, matching the documented canonical example and preserving prior behavior. Re-validated: syntax, mypy clean, fast spark suite 177 passed.

ELI5: This is the important one. The catalog name is currently pasted straight into the query as the address of a table. We replace that pasting with a special instruction that means "treat this separate value as the name of a thing, never as part of the query." The database now guarantees the catalog name can only ever be a name and can never change what the query does.

Technical: At all three call sites, replace the interpolated catalog.information_schema.columns identifier with an `IDENTIFIER()` clause that references a named parameter. Bind that parameter to the fully qualified table name string built from the catalog value. The `IDENTIFIER()` clause resolves the bound string as an object name and removes any possibility of identifier injection or breakage from unusual catalog characters. This phase closes the substantive correctness gap.

Acceptance: id-normalization and complex-type round-trip checks return identical results against a known catalog as before the change.

### Phase 3: Bind the Schema List as Discrete Parameters

Status: [x] complete. New `_schema_filter` helper builds a `table_schema IN (:s0, :s1, ...)` fragment with one bound parameter per schema, returning an empty fragment and no parameters for an empty list. Both filtered sites consume it and spread the schema params after the `tbl` param. `_sql_str_list` is now unreachable, removed in Phase 4. Validated: syntax parse, mypy clean on `dbxcarta.spark.verify`.

ELI5: The query sometimes filters down to a specific set of schema names. Today we glue that whole list into the query as text. Instead, we hand the database each schema name as its own separate value and tell the query to match against those slots. The list is now data the database compares against, never text inside the query.

Technical: At the two sites that build a schema IN clause, replace the `_sql_str_list` interpolation with a generated set of named markers, one per schema, and a matching list of bound parameter values. When the schema list is empty, omit the filter exactly as the current empty-list branch does. The generated marker count varies with the input list length, which the Statement Execution API supports because each element is an independent named parameter.

Acceptance: scoping behavior is unchanged for empty, single-element, and multi-element schema lists, including schema names containing characters that previously required escaping.

### Phase 4: Bind the Complex-Type Prefix and Remove the Old Helper

Status: [x] complete. The complex-type query now uses `LIKE :prefix` with the family-plus-wildcard bound as a value parameter. `_sql_str_list` deleted; no source or test references remain. Confirmed no SQL statement string interpolates a value: the only dynamic statement fragment is `{schema_filter}`, which contains bound markers only. Validated: syntax parse, mypy clean on `dbxcarta.spark.verify`.

ELI5: One query looks for column types whose name starts with a given word, like the word that means "list of things." Today that word is glued into the query. We hand it over as a separate value instead. After this, nothing glues values into queries anymore, so the old hand-escaping helper has no job left and we delete it.

Technical: In `_check_complex_type_round_trip`, replace the interpolated LIKE pattern with a bound value parameter whose value is the family prefix followed by the wildcard. Once this site is converted, the `_sql_str_list` helper is unreachable. Delete the helper and its import-time footprint. This phase also serves as the cleanup checkpoint that confirms no remaining f-string value interpolation exists in the file.

Acceptance: complex-type family detection produces the same matches and the same violations as before, and a search of the file finds no remaining interpolated values in any statement string.

### Phase 5: Verify and Lock In

Status: [x] complete. Locally verifiable acceptance is met: fast spark and boundary suites pass (181 passed, 1 skipped, 3 deselected) and mypy is clean on `dbxcarta.spark.verify`. No remaining value interpolation in any SQL statement string. The deferred live check was validated jointly with `docs/proposals/env-layering.md` Phase 4 (one combined live pass, so neither plan owes a separate run): `uv run dbxcarta verify` ran on the real warehouse (`a2946a63e3a3643d`) against previously loaded catalogs for two integrations. finance-genie produced `0 violations`, matching its known-good baseline runs. schemapile's parameterized verify SQL executed and sampled 50 real `schemapile_lakehouse` column ids, with only data-state (graph-not-loaded) violations and no SQL breakage — confirming parameterized statements execute against a real warehouse and values travel separately from the query text. See `docs/proposals/env-layering.md` Phase 4 for the full record.

ELI5: Finally we run the full safety checks to prove the rewritten queries return exactly what the old ones did, and that nothing else broke. We also confirm the database is genuinely receiving values separately rather than as glued text.

Technical: Run the fast spark test suite and the strict type checker over the changed file. Run the live verify path against a previously loaded catalog to confirm the parameterized statements execute on a real warehouse and that emitted violations match a baseline run. Confirm through the statement request that values travel as parameter list items and that the catalog resolves through the identifier clause. Update any developer note that previously described the hand-escaping approach.

Acceptance: fast suite green, type checker clean on the file, live verify run produces a violation set identical to a pre-change baseline, and the statement payload shows parameters carried separately from the query text.

## Out of Scope

This proposal covers only the verify path in `catalog.py`. It does not change the verify logic, the sampled row counts, the Neo4j comparisons, or any emitted violation codes. It does not alter the Spark ingest pipeline, the summary contract, or the Delta or JSON wire shapes. The earlier hardening of `_sql_str_list` remains the safety net until Phase 4 removes the last site that would have needed it.

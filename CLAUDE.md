# DBxCarta — CLAUDE.md

## Critical design rules

Before writing or changing Spark code, read `docs/reference/best-practices.md`. In particular:

- **Never use a Python UDF or `pandas_udf` for rule logic in the Spark pipeline.** Catalyst cannot optimize a UDF: it blocks predicate pushdown and whole-stage codegen and pays Arrow serialization per batch, which dominates large or n²-shaped joins. Logic driven by small static tables (suffix lists, type-class maps, score tables, stopwords) must be expanded into native `Column` expressions and broadcast-join lookups at plan-construction time. Python builds the plan; Spark evaluates every row. See `docs/reference/best-practices.md` §7 (Spark / Databricks).
- **Never collect catalog-scale data to the driver.** Keep columns, constraints, values, and FK candidates in DataFrames. See `docs/reference/best-practices.md` §5.

`docs/reference/best-practices.md` is the authoritative list of pipeline design rules with sources. Add to it (with a cited source) when a rule is established; do not let these rules regress.

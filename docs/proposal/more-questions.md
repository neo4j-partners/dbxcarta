# Generating 30 More Evaluation Questions

A reusable prompt for expanding `examples/schemapile/questions.json` from
9 questions to 39. The shape and distribution targets are tuned to the
v3 plan in `schemapile-v3.md`: more multi-table joins, FK-grounded join
keys, single-schema grounding per question, and breadth across schemas
so the cross-schema distraction failure mode in `graph_rag` can actually
be measured.

The prompt expects three pasted inputs:

1. The schema dump (the output of `dbxcarta.client.schema_dump.fetch_schema_dump`).
2. An FK list, one per line, formatted as
   `schema.table.column -> schema.table.column`. Produce this from Neo4j
   with a Cypher query over `REFERENCES` edges where `confidence >= 0.8`.
3. A sample-value list, one per line, formatted as
   `schema.table.column: value1, value2, value3, ...`. Produce this from
   Neo4j with a Cypher query over `HAS_VALUE` edges grouped by column.

Validate the LLM's output through the existing validator in
`question_generator.py` before merging into `questions.json`; the
validator is the ground truth for SELECT-only, single-statement,
in-catalog, and table-existence checks.

---

## The prompt

```
You are generating evaluation questions for a Text-to-SQL benchmark that
measures how well an LLM can answer natural-language questions against a
Databricks Unity Catalog. The benchmark compares three retrieval arms
(no_context, schema_dump, graph_rag), so the questions must be answerable
from schema knowledge alone, no questions about data history, business
intent that is not visible in the schema, or external facts.

Your task: generate exactly 30 question records as a single JSON array.
Each record must be runnable against the catalog described below, must
have exactly one correct answer set, and must be phrased the way a
business user or analyst would actually ask it.

============================================================
INPUTS
============================================================

Catalog name: schemapile_lakehouse

Catalog shape: 20 disjoint schemas, each derived from a separate
open-source project's database. Schemas do not share foreign keys with
each other. Every question you write must stay inside one schema.

Schema dump (tables and columns, grouped by schema):

<<<PASTE THE OUTPUT OF fetch_schema_dump HERE>>>

Foreign-key relationships available (declared and high-confidence
inferred), one per line as `schema.table.column -> schema.table.column`:

<<<PASTE THE FK LIST HERE. Query Neo4j for REFERENCES edges with
confidence >= 0.8 and format as above.>>>

Sample values for low-cardinality columns (status enums, booleans,
categorical fields), one per line as
`schema.table.column: value1, value2, value3, ...`:

<<<PASTE THE SAMPLE-VALUE LIST HERE. Query Neo4j for HAS_VALUE
edges and group by column.>>>

============================================================
DISTRIBUTION TARGETS (exactly 30 total)
============================================================

Shape distribution, match these counts exactly:

  - 12 questions of shape "two_table_join" or "three_table_join"
      * 6 two-table inner joins
      * 3 two-table left joins (where the left-join semantics matter,
        e.g., "include parents with zero children")
      * 3 three-table joins (chain or star)
  - 9 questions of shape "aggregation"
      * 5 aggregations that require a join (GROUP BY on one table,
        COUNT/SUM/AVG over another)
      * 4 aggregations on a single table (GROUP BY plus a HAVING
        clause or ORDER BY count DESC)
  - 6 questions of shape "single_table_filter"
      * 2 equality filters
      * 2 range or comparison filters
      * 1 LIKE / pattern match
      * 1 IN / multi-value filter
  - 3 questions of shape "advanced"
      * 1 window function (ROW_NUMBER, RANK, or running total)
      * 1 correlated subquery or EXISTS
      * 1 common table expression (WITH ... SELECT)

Schema coverage:
  - Use at least 12 different schemas across the 30 questions.
  - Bias toward schemas that have at least 4 tables and at least one
    declared or inferred FK; these can sustain joins.
  - Do NOT write more than 4 questions for any single schema.

============================================================
QUALITY BAR
============================================================

Every question must satisfy all of the following. If you cannot meet
all of them for a candidate question, throw it out and write a
different one.

1. ONE correct answer set. The question is unambiguous enough that
   two competent SQL writers, given the schema, would produce SQL
   returning the same rows (allowing for column order differences).

2. REAL columns and tables. Every identifier in your reference_sql
   must appear verbatim in the schema dump. Match case exactly. Quote
   identifiers with backticks: `catalog`.`schema`.`table`.

3. SELECT-only. No INSERT, UPDATE, DELETE, MERGE, CREATE, DROP,
   ALTER, or multi-statement bodies.

4. Fully qualified table names everywhere, three parts, backtick
   quoted, in every FROM and JOIN clause. No bare table names.

5. NATURAL phrasing. The question reads like something a human would
   say out loud, not a translation of SQL. Prefer business words
   ("active users", "top sellers") over column names ("rows where
   is_active = TRUE"). Do not paste column names into the question
   when a natural phrase exists. Exception: a column that is itself
   a domain term (e.g., "ISBN", "VIN") may appear in the question.

6. FILTER VALUES MUST EXIST. If you write WHERE column = 'foo', then
   'foo' must appear in the sample-values list for that column. If
   the column is not in the sample list, do not invent a literal:
   either pick a different filter or pick a different column.

7. ANSWER IS NON-EMPTY ON A REASONABLE DATASET. The reference SQL
   should plausibly return at least one row. Avoid filters that are
   likely to match nothing (e.g., a status value not in the sample
   list, a date range outside any plausible data range). The
   materialized tables are populated from the original VALUES
   inserts in the source SQL files, so use values you have seen.

8. JOIN QUESTIONS USE A REAL FK. For every two_table_join,
   three_table_join, and join-based aggregation, the join condition
   must match an FK relationship from the FK list above. This is
   what lets the graph_rag arm shine: if the join cannot be derived
   from the FK list, do not write the question.

9. SINGLE SCHEMA. Every table in the reference SQL must live in the
   same schema. The `schema` field on the record names that schema.

10. NO CROSS-QUESTION CORRELATION. The 30 questions should be
    independently answerable. Do not chain ("for the result of the
    previous question, ...") and do not reference each other.

============================================================
ANTI-PATTERNS, do NOT do any of these
============================================================

  - Questions whose answer depends on row ordering when no ORDER BY
    is implied ("what's the first row in users?").
  - Questions whose answer depends on the physical table size
    ("how many tables are in the schema?", "how many columns does X
    have?"). Schema metadata is not a valid answer target.
  - Questions whose answer is a boolean trivia about the schema
    ("does the users table have an email column?").
  - Questions that mix two unrelated asks ("list users and also tell
    me the top product"). One question, one answer set.
  - Questions that require knowledge the schema does not encode
    ("which users are most loyal?", "which products are best?"
    unless "loyalty" or "best" is a column).
  - Questions whose reference SQL uses LIMIT without an explicit
    ORDER BY to disambiguate ties.
  - Reference SQL that uses SELECT *. Always project explicit columns.
  - Reference SQL that buries a constant in a CTE only to filter it
    later. Keep the SQL straightforward.
  - Aggregation questions where the GROUP BY column is a primary key
    (every group has size 1; the aggregation is meaningless).
  - Left-join questions where every parent has children in the data,
    so the left-join semantics never trigger. Pick a parent-child
    pair where the FK is nullable or where some parents are known to
    have no children from the sample values.

============================================================
OUTPUT FORMAT
============================================================

Emit exactly one JSON array. No prose before or after. No code fences.
Each element is an object with these fields and only these fields:

  - question_id   : string of the form "sp_NNNN", starting at "sp_0009"
                    and incrementing by 1 (the first 9 ids are already
                    taken by the existing question set).
  - question      : string, the natural-language question. One sentence.
                    Ends with a period or question mark.
  - reference_sql : string, the single SELECT statement that answers
                    the question. No trailing semicolon.
  - schema        : string, the schema name (must appear in the schema
                    dump exactly).
  - source_id     : string, copy verbatim from the schema dump's
                    schema header if present; otherwise the schema
                    name itself.
  - shape         : string, one of
                    "single_table_filter", "two_table_join",
                    "three_table_join", "aggregation", or "advanced".
  - join_keys     : array of strings, only present when shape is a
                    join shape or a join-based aggregation. Each entry
                    is one FK predicate from the FK list above, in the
                    form "schema.table.column -> schema.table.column".
                    This lets the benchmark verify the question
                    actually exercises the FK graph.

============================================================
EXAMPLES (style reference, do not copy these verbatim)
============================================================

[
  {
    "question_id": "sp_0009",
    "question": "Which books were uploaded by teachers based in Beijing?",
    "reference_sql": "SELECT b.`id`, b.`name` FROM `schemapile_lakehouse`.`sp_535132_zhuanglang`.`book` b JOIN `schemapile_lakehouse`.`sp_535132_zhuanglang`.`teacher` t ON b.`upload` = t.`username` WHERE t.`city` = 'Beijing'",
    "schema": "sp_535132_zhuanglang",
    "source_id": "535132_zhuanglang.sql",
    "shape": "two_table_join",
    "join_keys": ["sp_535132_zhuanglang.book.upload -> sp_535132_zhuanglang.teacher.username"]
  },
  {
    "question_id": "sp_0010",
    "question": "For each teacher, how many books have they uploaded and how many sections do those books contain in total?",
    "reference_sql": "SELECT t.`username`, COUNT(DISTINCT b.`id`) AS book_count, COUNT(s.`id`) AS section_count FROM `schemapile_lakehouse`.`sp_535132_zhuanglang`.`teacher` t LEFT JOIN `schemapile_lakehouse`.`sp_535132_zhuanglang`.`book` b ON t.`username` = b.`upload` LEFT JOIN `schemapile_lakehouse`.`sp_535132_zhuanglang`.`section` s ON b.`id` = s.`belong` GROUP BY t.`username`",
    "schema": "sp_535132_zhuanglang",
    "source_id": "535132_zhuanglang.sql",
    "shape": "three_table_join",
    "join_keys": [
      "sp_535132_zhuanglang.book.upload -> sp_535132_zhuanglang.teacher.username",
      "sp_535132_zhuanglang.section.belong -> sp_535132_zhuanglang.book.id"
    ]
  },
  {
    "question_id": "sp_0011",
    "question": "Rank shoes within each brand by price from highest to lowest, returning the brand, shoe name, price, and the rank.",
    "reference_sql": "SELECT `brand`, `name`, `price`, RANK() OVER (PARTITION BY `brand` ORDER BY `price` DESC) AS price_rank FROM `schemapile_lakehouse`.`sp_169810_init`.`Shoe`",
    "schema": "sp_169810_init",
    "source_id": "169810_init.sql",
    "shape": "advanced"
  }
]

============================================================
SELF-CHECK BEFORE YOU EMIT
============================================================

Run through this checklist mentally for every question. If any item
fails, fix the question or replace it.

  [ ] The question reads naturally; would a human ask it this way?
  [ ] Every table and column in reference_sql appears in the schema dump.
  [ ] Every join condition matches an entry in the FK list.
  [ ] Every literal in a WHERE clause appears in the sample-values list.
  [ ] All tables are in the same schema as the `schema` field.
  [ ] The reference SQL is a single SELECT, no trailing semicolon.
  [ ] The shape field correctly classifies the SQL.
  [ ] The question is not in the anti-pattern list above.
  [ ] The shape counts across all 30 questions match the targets exactly.
  [ ] At least 12 distinct schemas are represented across the 30.
  [ ] No schema has more than 4 questions.

Now generate the 30 questions. Emit only the JSON array.
```

---

## Why this prompt is shaped this way

A few choices that are not obvious from the prompt itself.

**Why mandate FK-grounded joins.** The whole point of generating more
questions is to make `graph_rag` show up. If the LLM invents joins
(`users.id = orders.customer_id` when the actual FK is
`orders.user_id`), `schema_dump` will get them right by luck because
the schema dump contains the column list, and `graph_rag` will not get
extra credit for the FK navigation. Forcing the join key to come from
the FK list aligns the benchmark with the thing under test.

**Why require literals to come from sample values.** Half the v2
`graph_rag` non-empty failures were probably "SQL executes but returns
no rows" because the literal did not exist in the data. That is not a
retrieval failure, it is a question-generator hallucination. Pinning
literals to observed values eliminates the noise.

**Why 12+ distinct schemas with a cap of 4 each.** Forces breadth. The
cross-schema distraction failure mode in v2 only shows up when retrieval
has to pick among many schemas. If 20 questions land in one schema,
`graph_rag` looks artificially better.

**Why include "advanced" at only 10%.** Window functions and CTEs are
interesting but they stress SQL generation, not retrieval. The
benchmark's job is to compare retrieval arms, so most questions should
isolate retrieval as the variable.

**Why supply sample values, not just schema text.** SchemaPile's table
comments are often empty, so the model has nothing to anchor "active
users" to without seeing that `fl_ativo` has values `TRUE, FALSE`.
Pasting the value list takes two minutes and dramatically improves
question realism.

---

## Operational tips

- Run the prompt once at temperature 0.7 and once at 0.2, then pick
  the cleaner output. Higher temperature produces more diverse joins;
  lower temperature produces more compliant validity.
- Validate the output through the existing `question_generator.py`
  validator before adding to `questions.json`. It already enforces
  SELECT-only, in-catalog, single-statement, and table-existence. The
  prompt asks the LLM to do this, but the validator is the truth.
- For the FK list and sample-value list, the Neo4j queries are
  essentially the inverse of `_references_criteria` and `_fetch_values`
  in `graph_retriever.py`: same shape, just dump everything instead of
  filtering by seed.

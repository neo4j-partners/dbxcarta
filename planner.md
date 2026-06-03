# Materialize: run it as a Spark job

## What is wrong (ELI5)

Materialize creates the demo tables. To run SQL it makes one REST call per
statement, waits for that call, then makes the next one. The Statement
Execution API runs a single statement per request, so creating 500 tables means
thousands of little submit-and-wait round trips.

Each round trip is slow because of network plus polling, not because the
warehouse is busy. So the code spins up 20 Python threads to fire many round
trips at once and hide that wait.

That is the root problem: the **client** is scheduling parallel work that the
**compute** is built to schedule itself.

```
   TODAY: the client does the scheduling

   +-------------------+     one HTTP call per statement, x thousands
   | local materialize |  -----------------------------------------+
   | 20 worker threads |  --------------------------------------+  |
   +-------------------+   -----------------------------------+  |  |
                                                              v  v  v
                                              +-----------------------------+
                                              |   SQL warehouse             |
                                              |   fed one statement at a    |
                                              |   time from the client      |
                                              +-----------------------------+
```

A second problem makes the first worse. Each table is built with three or four
separate statements, and every one needs its own "what if this fails" rule:

- `CREATE TABLE` fails        -> skip this table, keep the run going
- `INSERT` fails              -> skip the rows, keep the table
- `SET NOT NULL` fails        -> drop the whole primary key for this table
- `ADD PRIMARY KEY` fails     -> skip the key, keep the table

More statements means more round trips to parallelize and more failure branches
to choreograph. Both the thread pool and the failure matrix are symptoms of the
same design: single statements driven one at a time from the client.

What needs fixing:

- Send fewer statements per table.
- Move the work onto the compute, so the client stops running a thread pool.

## Fix part 1: fold the constraints into CREATE TABLE

Unity Catalog lets a `CREATE TABLE` declare `NOT NULL` columns and an inline
`PRIMARY KEY` in one statement. Primary keys and foreign keys are informational
in Databricks, so they are metadata, not enforced checks. That means three
statements collapse into one:

```
   BEFORE (3 statements, 3 failure branches)
     CREATE TABLE t (id BIGINT, name STRING, ...)
     ALTER TABLE t ALTER COLUMN id SET NOT NULL
     ALTER TABLE t ADD CONSTRAINT pk_t PRIMARY KEY (id)

   AFTER (1 statement)
     CREATE TABLE t (
       id BIGINT NOT NULL,
       name STRING,
       ...,
       CONSTRAINT pk_t PRIMARY KEY (id)
     )
```

The failure matrix then shrinks to two real cases:

- `CREATE TABLE` (with its inline constraints) fails -> skip this table
- `INSERT` fails -> skip the rows, keep the table

Foreign keys still run in a second pass, because a foreign key can only point at
a table that already exists. That is one `ALTER ... ADD CONSTRAINT ... FOREIGN
KEY` per edge, after all tables are created. The two-phase order (tables first,
foreign keys second) stays.

## Fix part 2: run materialize as a Spark job

Ingest and Client already run as Databricks jobs. Materialize joins them. Spark
plus Delta is the native tool for creating tables and loading data, and the
cluster runs the SQL, so the client thread pool, the `make_execute` factory, and
the thread-safety question all go away.

```
   AFTER: the compute does the work

   dbxcarta-submit                         Databricks job (Spark)
   +-------------------+   submit job      +-----------------------------+
   | stage blueprint   | ----------------> |  driver runs spark.sql()    |
   | forward overlay   |                   |  CREATE TABLE (+ inline PK) |
   +-------------------+                   |  INSERT sample rows         |
                                           |  then FK pass               |
                                           +-----------------------------+
```

- The driver issues each statement with `spark.sql(...)`. There is one driver
  thread, so nothing in the client runs in parallel and there is no
  thread-safety rule to enforce.
- Core stays Spark-free and SDK-free. The shared spine still takes one injected
  runner, and the Spark entrypoint supplies `lambda sql, label: spark.sql(sql)`.
  The factory and the worker count are removed.
- This matches the rest of the pipeline, which is already job-based.

## Open decisions

Committing to a Spark job raises real choices. Each has a suggested default;
the ones marked NEEDS A CALL change behavior or operational shape and want a
deliberate answer.

### 1. How the blueprint reaches the cluster (NEEDS A CALL)

The candidate JSON (the Blueprint output) is a local file under `.cache/`
today. A cluster cannot read the operator's disk.

- Suggested default: stage it to a UC Volume from `dbxcarta-submit`, the same
  way client questions are already uploaded, and pass the volume path to the
  job.
- Open: which volume. The ops volume root already exists from bootstrap, so a
  `.../dbxcarta/blueprint/<example>.json` tail is the natural home.

### 2. DDL parallelism on the driver (NEEDS A CALL)

`spark.sql` runs from the driver and is serial unless threaded. Table creation
is metadata work, so serial is simpler but slower for 500 tables.

- Suggested default: start serial. Measure. It removes all concurrency code.
- If serial is too slow: a small driver-side thread pool over `spark.sql` is
  safe, because `SparkSession` is thread-safe by design. The difference from
  today is that the contract sits on a known Spark type, not an opaque injected
  callable.
- Open: accept serial, or keep a bounded driver pool for table creation.

### 3. Who creates the data catalog (NEEDS A CALL)

Today materialize creates the data catalog itself, including the
Default-Storage MANAGED LOCATION quirk handling.

- Option: move data-catalog creation into `dbxcarta-submit bootstrap`, which
  already creates the ops catalog and volume, so the job only creates schemas
  and tables.
- Suggested default: move it to bootstrap, so the job assumes the catalog
  exists. This keeps catalog provisioning in one place.

### 4. How sample rows are written

- Suggested default: keep rendering `INSERT` via `spark.sql`, reusing the core
  `build_insert_statement`. This preserves today's behavior, where a quoted
  string literal is cast to the column type by Delta.
- Alternative: build a typed Spark DataFrame and write it. More native, but it
  needs a source-to-Spark type map and careful handling of the messy schemapile
  values. Defer unless inserts become a bottleneck.

### 5. Where the entrypoint lives

- Suggested default: a thin Spark entrypoint per example that reads the staged
  blueprint, builds the `spark.sql` runner, and calls the core spine. The spine
  stays in `dbxcarta.core.materialize` and stays Spark-free.
- The warehouse poller `execute_ddl_blocking` stays for bootstrap and teardown,
  which still run locally against a warehouse. Only materialize stops using it.

### 6. Submit surface and sequencing

- Suggested default: a `dbxcarta-submit materialize` command that stages the
  blueprint, forwards the overlay as job params, and submits the job. It runs
  after bootstrap and before ingest.

### 7. Local dev loop

- Materialize is easy to run locally today. As a job, the loop becomes submit
  and wait.
- Suggested default: keep the core spine unit-tested with a fake runner, and
  allow the Spark entrypoint to run against a local `SparkSession` for dev.

### 8. Stats and reporting

- Materialize prints `MaterializeStats` to stderr today. As a job that output
  lands in job logs.
- Suggested default: log the stats line in the job. Add a summary record only
  if run history needs it, matching how ingest and client report.

## What changes and what stays the same

Changes:

- `CREATE TABLE` carries inline `NOT NULL` and `PRIMARY KEY`, so the
  SET-NOT-NULL and ADD-PK statements go away.
- The client thread pool, `make_execute`, and the executor factory are removed.
- Materialize runs as a Spark job; the cluster runs the SQL.
- The blueprint is staged to a UC Volume instead of read from local disk.

Stays the same:

- The same tables, rows, primary keys, and foreign keys come out the other end.
- The two-phase order: all tables first, then foreign keys.
- Skip-on-error behavior: a bad table is skipped and the run continues. The
  matrix is just smaller, because there are fewer statements per table.
- The core spine stays Spark-free and SDK-free, driven by one injected runner.

## Why this is better than a thread pool plus a docstring

- Thread-safety stops being a written rule passed across a boundary. With the
  job running the SQL on one driver thread, nothing in the client runs in
  parallel, so there is nothing to make thread-safe.
- The code that builds SQL gets simpler, because each table is one CREATE plus
  an optional INSERT, not a four-statement sequence with four failure branches.
- It uses Databricks the way Databricks is meant to be used: the cluster runs
  the work, not a hand-built thread pool in the client.

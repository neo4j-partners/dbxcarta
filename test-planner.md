# Test planner: run the materialize cutover end to end

## What this is (ELI5)

The materialize change in `planner.md` is finished in code, and the offline test
suite is green (604 passed, 1 skipped). What it has never had is a real run: the
new materialize Spark job has not been submitted to a live workspace, and neither
has the full pipeline that sits on top of it. `planner.md` says so plainly and
leaves that run to the operator.

This plan is that run. Two people (or two agents) work in parallel, each driving a
full pipeline against a real Databricks plus Neo4j workspace, and each ticks off a
plain checklist as they go. One side proves the new materialize job on the two
examples that use it. The other side proves the query path on the example that
does not. When both checklists are clean, the cutover is verified live.

There is no code to write here. This is a runbook and a set of yes or no checks.

## The two-agent split

```
   Agent A                                  Agent B
   +---------------------------+            +---------------------------------+
   | finance-genie             |            | schemapile  +  dense            |
   |                           |            |                                 |
   | bootstrap                 |            | bootstrap                       |
   | (no materialize)          |            | MATERIALIZE  <- the new job     |
   | ingest                    |            | ingest                          |
   | client                    |            | client                          |
   +---------------------------+            +---------------------------------+
        query path only                       both exercise the cutover
```

Agent A takes **finance-genie** on its own. It has no blueprint and no materialize
stage, so it runs bootstrap, then ingest, then client against tables the upstream
Finance Genie project already created in the workspace. It proves the query path
end to end without touching the new materialize job.

Agent B takes **schemapile and dense** together, run one after the other. These are
the only two examples that own a committed blueprint and run the new materialize
job, so pairing them puts both materialize runs under one set of eyes: dense is a
synthetic 500-table fixture, schemapile is a slice of 20 real schemas totaling 152
tables. The materialize checklist is the same for both, so the second run reuses
the first.

The two agents are independent and safe to run at the same time. Each example has
its own data catalog, its own ops schema, its own teardown target, and its own
Neo4j secret scope, so nothing one agent does collides with the other. The one
shared resource is the run-summary Delta table, and every stage tags its row with
a `job_name` (`dbxcarta_materialize`, `dbxcarta_ingest` style names, and
`dbxcarta_client`), so concurrent writers do not confuse each other.

## How materialize is invoked now

The cutover replaced the old per-example `dbxcarta-dense-materialize` and
`dbxcarta-schemapile-materialize` scripts with one shared command for every
example: `dbxcarta-submit materialize` with the example overlay selected. It
stages the committed blueprint to the ops Volume and submits a serverless Spark
job. There is no make target for it; the make targets cover only ingest, client,
and teardown. The dense and schemapile READMEs already use this command, so
following them as written is safe.

One prerequisite the make targets hide: the materialize job runs the published
`dbxcarta-materialize` wheel from the example's own ops Volume, so the wheels must
be published to that Volume first. The `-ingest` make target runs `publish-wheels`
for you, but a standalone `dbxcarta-submit materialize` does not. Run
`dbxcarta-submit publish-wheels` with the same overlay before the first
materialize on a freshly bootstrapped example, or the job fails with a missing
wheel.

## Shared preconditions (both agents)

Before either agent starts, confirm the ground is the same for both.

- [ ] The intended Databricks workspace profile is selected and active.
- [ ] The SQL warehouse named in config is running and reachable.
- [ ] The repo-root base `.env` holds the Neo4j secrets and shared infra, and is
      not edited per example.
- [ ] Wheels are published from current source, so the jobs run the code under
      test and not an old build.
- [ ] Each agent has selected the correct overlay for its example before every
      command, by exporting the env file or passing it on the command line.

## Agent A: finance-genie (query path, no materialize)

finance-genie reads tables the upstream project owns. Its job is to prove that
ingest builds a healthy graph from those tables and that the client evaluation
runs all three arms against it.

- [ ] The finance-genie overlay is selected.
- [ ] Readiness passes: both the silver and the gold catalog each hold a real data
      schema, not just the empty defaults. If readiness reports not ready, the
      upstream data is missing and ingest should not run yet.
- [ ] The ops plane is bootstrapped (ops catalog, ops schema, ops volume). This
      does not create the upstream data catalogs.
- [ ] The Neo4j secret scope is provisioned.
- [ ] The demo question set is uploaded to the ops volume.
- [ ] The ingest job is submitted and finishes with status SUCCESS.
- [ ] Verification passes: node counts look right, foreign keys account cleanly,
      sample-value structure is correct, and node IDs are stable.
- [ ] An ingest run-summary row is written to the summary table, and the matching
      JSON file is present in the summary volume.
- [ ] The client job is submitted and finishes with status SUCCESS.
- [ ] All three arms report numbers: the zero-context baseline, the schema dump,
      and graph_rag.
- [ ] graph_rag meets or beats the schema dump on correct rate. This is the result
      the run is checking for, not a fixed pass or fail threshold.
- [ ] A client run-summary row and JSON file are present for the run.

## Agent B: schemapile and dense (full pipeline, includes the new materialize job)

Run schemapile first or dense first, the order does not matter, but run each one all
the way through this list before starting the next. The materialize block is the
heart of the cutover, so give it the most attention.

### Per example, in order

- [ ] The example overlay is selected.
- [ ] Bootstrap runs and creates the data catalog and the ops plane. Bootstrap now
      owns the data catalog; the materialize job no longer creates it. Bootstrap is
      idempotent, so re-running it is safe.

**Materialize (the new serverless job)**

- [ ] Materialize is launched with the new shared command, not the deleted
      per-example script.
- [ ] The committed blueprint is staged to the ops Volume at the canonical
      blueprint path.
- [ ] The serverless Spark job finishes with status SUCCESS.
- [ ] A materialize run-summary row is written, tagged with the job name
      `dbxcarta_materialize`, alongside its JSON file in the summary volume.

**Materialize outcome checks (read the summary row)**

- [ ] The number of tables created matches the blueprint.
- [ ] Primary-key counts match the expected split for the example:
      - dense: all 500 tables keep their primary key, because the synthetic `id`
        column is never null.
      - schemapile: 130 primary-key constraints land out of 152 tables. Of the 22
        without one, 11 declare no primary key at all, and 11 declare one but carry
        a sample row with a null in a primary-key column, so the job keeps the rows
        and leaves the key off, by design.
- [ ] Foreign keys are added in the second pass, after all tables exist. For
      schemapile, expect roughly 112 of the 174 declared foreign keys to land. The
      rest are rejected by Unity Catalog because the source schema's foreign-key
      and referenced-key column types do not match, or the referenced table lost
      its primary key to the null-sample rule above. These rejections are tolerated
      and the job still succeeds; they reflect the real source data, not a defect.
- [ ] The skipped-tables tally is sane: no tables were skipped that should have
      landed.
- [ ] Type fallbacks are sane. For schemapile expect about 33 of 666 columns to
      fall back to string. Those are SchemaPile custom domain types such as
      `generic_string`, `TYNYINT`, and `email` that are not standard SQL types, so
      string is the correct, expected result. A materialize run where most or all
      columns fall back means column types were lost upstream and must be
      investigated, not accepted.

**Spot-check in Unity Catalog**

- [ ] A few tables exist with their columns declared not null and an inline primary
      key, proving the folded create statement worked.
- [ ] Rows are present in the tables that carried sample values.
- [ ] Foreign keys are present on the tables that declare them.

**Ingest and client**

- [ ] The ingest job is submitted and finishes with status SUCCESS.
- [ ] Verification passes for this example.
- [ ] The client job is submitted and finishes with status SUCCESS, all three arms
      report, and graph_rag is compared against the schema dump.

### Per-example notes

- dense materializes into the schema named in its overlay, `dense_500`, from the
  committed 500-table blueprint. The blueprint and the schema name must agree.
- schemapile materializes one schema per candidate, named with an `sp_` prefix, and
  the ingest auto-discovers them because the schema list in the overlay is blank on
  purpose.

## Cross-cutting things to watch

These are the risks the cutover introduced, worth keeping in mind on both agents
but especially on Agent B.

- The old materialize script names are gone. Use the one shared
  `dbxcarta-submit materialize` command. The READMEs already reflect this.
- The two-phase order must hold: every table is created first, then every foreign
  key is added. A foreign key cannot point at a table that does not exist yet.
- The only concurrency is a small bounded worker pool on the cluster, default five,
  set by the materialize worker count variable. There is no client-side thread pool
  anymore. If the run hangs or behaves oddly, this pool on the cluster is where to
  look, not the local machine.

## Teardown

When an example is done, tear it down so the next run starts clean. Each teardown
drops only what that example's overlay names as its target.

- [ ] finance-genie teardown drops only its ops schema. The upstream data catalogs
      are left alone.
- [ ] dense teardown drops its data catalog and its ops schema.
- [ ] schemapile teardown drops its data catalog and its ops schema.
- [ ] The shared ops catalog survives every teardown, so the other examples keep
      working.

## Sign-off

The cutover is verified live when all of the following are true.

- [ ] Every submitted job, across both agents, finished with status SUCCESS.
- [ ] Every stage wrote its run-summary row and JSON file.
- [ ] Both materialize runs matched their expected table, primary-key, and
      foreign-key counts (dense all 500 keep the key; schemapile 152 tables, 130
      primary keys, about 112 foreign keys, about 33 type fallbacks).
- [ ] Verification passed for all three examples.
- [ ] graph_rag held up against the schema dump on all three examples.
- [ ] Teardown ran clean for each example, leaving the shared ops catalog intact.

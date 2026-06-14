# Aligning dbxcarta with neocarta: remove the Spark pipeline, pull the connector wheel

## Goal

The Spark ingest pipeline moved to neocarta. It now lives there as the `neocarta` connector wheel with a `databricks-spark` extra. This project, dbxcarta, should stop carrying its own copy of that pipeline and instead pull the neocarta wheel and run it.

This plan removes the `dbxcarta-spark` package from dbxcarta, repoints the operator tooling at the neocarta wheel, turns the example client into plain local Python, and updates every workspace file, CI workflow, and doc that still mentions the old package.

We do all of this so it can be tested fully on a local machine before neocarta publishes its wheel to a package index. The tooling pulls the neocarta wheel from a local build folder for now. A later step flips that source to a real package index, and nothing else has to change.

## The end state in one line

After this work, the only Spark in dbxcarta is the job-submit tooling that launches cluster jobs and the `materialize` job. Everything else, including the example client, is plain local Python talking to Neo4j and the SQL warehouse.

## Key design decisions

- **No compatibility wrappers.** When code moves or a name changes, the old form is deleted and every caller points at the new one in the same change. No aliases, no "old name still works" fallbacks.
- **No legacy migration paths.** The project does not support the old layout and the new layout at the same time. There is no transition window.
- **Complete hard cutover.** Each phase fully replaces what it touches. After a phase lands, the old shape is gone, not deprecated.
- **One operator command.** The surviving command is named `dbxcarta`. The old pipeline command owned that name; when the pipeline package is deleted, the operator tool takes the name over. There is one command, not two.
- **Local wheel now, index later.** The wheel source is a single setting. It points at neocarta's local build folder today and at a package index plus a version later. The swap is one setting and nothing else.
- **Repoint before delete.** The ingest path is pointed at the neocarta wheel and proven to run before the old package is removed, so the pipeline is never broken in the middle.
- **Spark only where it is required.** The job-submit tooling and `materialize` stay on Spark because they have to be. The client moves off Spark to plain local Python, since its only Spark use was reading and writing a couple of tables.

## What stays and what goes (in plain words)

- **Goes away:** the Spark ingest pipeline that reads Unity Catalog tables and writes the semantic graph into Neo4j. That is the `dbxcarta-spark` package. neocarta owns this now.
- **Goes away with it:** the `dbxcarta verify` command. It checks the output of a finished pipeline run, so it belongs with the pipeline. neocarta dropped this check during its migration, so dbxcarta drops it too.
- **Stays on Spark:** the operator job-submit tooling and the `materialize` job. `materialize` runs `spark.sql` to build Delta tables, so it genuinely needs a cluster.
- **Stays, but moves off Spark:** the example client. Its real work is Neo4j retrieval, prompt building, model calls, and scoring, all plain Python. It only used a cluster session to read the question table and write the summary, which it can do straight against the SQL warehouse instead. So it becomes a local Python command.
- **Moves, does not die:** the `ready` and `upload-questions` commands. They are operator helpers, so they move out of the Spark package and into the operator tool. The readiness logic already lives in the shared core, so only the two thin command handlers move.

## The command-line tools, so the change is clear

- **`dbxcarta`** is the single operator command after this change. Today this name belongs to the old pipeline command inside the Spark package, which offers `verify`, `ready`, and `upload-questions`. The pipeline command is deleted. `verify` is retired. `ready` and `upload-questions` move to the operator tool, and the operator tool is renamed to `dbxcarta`.
- **The cluster ingest entry point** is what the job runs on the cluster. Today it points at the Spark package. After this change it points at the neocarta connector's ingest entry point inside the neocarta wheel.
- **`dbxcarta-client`** stays a command, but runs locally instead of on a cluster.

---

## Phase 1: Point the ingest job at a local neocarta wheel

Make the operator tool stage and run the neocarta wheel from a local folder, while the old Spark package is still present.

- Build the neocarta wheel on this machine from the neocarta project at `/Users/ryanknight/projects/neo4j-field/neocarta`, so there is a wheel file in neocarta's build folder to point at.
- Add a setting in the operator tool for where the neocarta wheel comes from. For now this is the local build folder path. Later it becomes a package index plus a version number, and only this one setting changes.
- Change the publish step so that, for the ingest job, it copies the prebuilt neocarta wheel onto the Unity Catalog Volume instead of building the old Spark wheel.
- Stop copying the shared core code into the ingest wheel. The neocarta wheel already carries everything it needs as normal modules.
- Point the cluster ingest entry point at the neocarta connector's ingest entry point, not the old Spark one.
- Set the fixed list of pinned dependencies for the ingest job to the list neocarta was tested against. Copy neocarta's pinned set rather than inventing a new one.
- Keep the existing cluster check for the Neo4j Spark Connector. It is a Java library attached to the cluster, not a Python dependency.

**Phase 1 is done when:** the operator tool stages the local neocarta wheel onto a Volume and submits an ingest job that runs against a test catalog and Neo4j, end to end. The old Spark package has not been touched yet.

## Phase 2: Move the operator commands, rename, and delete the Spark package

Rescue the operator-facing commands and the command name, then remove the package and every workspace wiring that points at it, in one cutover.

- Move the `ready` and `upload-questions` command handlers from the Spark package into the operator tool. The readiness logic they call already lives in the shared core, so only the thin handlers move.
- Retire the `verify` command. Do not move it. It checks pipeline output, which is now neocarta's job.
- Rename the operator tool's command from `dbxcarta-submit` to `dbxcarta`, so the surviving command takes the name the old pipeline command used.
- Delete the whole `dbxcarta-spark` package folder.
- Remove it from the project's list of workspace members, the dependency list, and the workspace source entry that pointed at it.
- Regenerate the project lock file so the package and its build artifacts are gone from it.
- Fix the one helper script that imports from the old Spark package. Point it at the new home of that code, or drop the part that is no longer needed.

**Phase 2 is done when:** `dbxcarta ready` and `dbxcarta upload-questions` work, the project builds and installs with no reference to the old package, and the old pipeline command is gone.

## Phase 3: Turn the client into local Python and shrink the submit tooling

Take the client off the cluster, then remove the cluster machinery that only the client and the old ingest wheel needed.

- Change the client so it reads the question table and writes the summary straight against the SQL warehouse, the same way the local demo already does, instead of asking a cluster session to do it.
- Remove the cluster session call from the client run path, so the client is plain local Python connecting to Neo4j and the warehouse.
- Remove the client from the operator tool's set of cluster entry points, so there is no "submit the client to a cluster" path.
- Remove the client's pinned dependency list and the client branch of the core-copying step. The only package the tool still builds and copies core into is `materialize`.
- Confirm the examples run the client locally and still pass their checks.
- With the client gone, the generic submit-entrypoint dispatch now has one caller left, ingest. Collapse it into a dedicated "submit ingest" path. Remove the indirection that chose between entry points by name, since there is nothing left to choose between.
- Shrink the entry-point lookup tables to the single ingest entry, and delete the now-unused client and old-ingest entries.

**Phase 3 is done when:** the client runs locally end to end with no cluster, the submit tooling has a single dedicated ingest-submit path with no name-based dispatch, and it only knows about staging the neocarta ingest wheel and building the `materialize` job.

## Phase 4: Update the automation (CI)

Fix every automated workflow that still builds, versions, tests, or type-checks the old package.

- In the main test workflow, remove the test entries that run the Spark package's tests and the entries that add it to example test runs.
- In the same workflow, remove the type-check step that targets the Spark package.
- In the publishing workflow, remove the step that builds the Spark wheel.
- In the release workflow, remove the step that bumps the Spark package's version.

**Phase 4 is done when:** every workflow runs clean with no step that touches the removed package.

## Phase 5: Update the documents

Rewrite the docs so they describe the new shape: the pipeline lives in neocarta, dbxcarta pulls the connector wheel, and the client is local.

- Update the project readme so it describes dbxcarta as the operator tooling that pulls and runs the neocarta connector, not as the home of the Spark pipeline.
- Update the architecture doc so the diagram and text show the pipeline coming from the neocarta wheel and the client running locally.
- Delete the public interface doc. It only maps the old pipeline module names, which no longer exist here. There is nothing left to map.
- Update the release doc to remove the steps that build and version the old Spark wheel.
- Update the simulate-publish doc to stage the neocarta wheel instead of building the old Spark wheel.
- Update the add-a-data-source tutorial to point at the neocarta connector instead of building the old Spark wheel.
- Update the schema doc so it points at the neocarta connector for the graph contract.
- Update the test fixtures readme so it no longer says the self-check lives in the old Spark package.

**Phase 5 is done when:** no doc tells a reader to build, install, or run the removed package, and the two-step flow is described: run the ingest job from the neocarta wheel, then run the operator follow-up.

## Phase 6: Full local end-to-end test and final sweep

Prove the whole thing works on a local machine, with neocarta still unpublished.

- Run the complete operator flow locally: set up the catalog and volume, stage the local neocarta wheel, and submit the ingest job against a test catalog and Neo4j.
- Run the client locally against the resulting graph and confirm it works with no cluster.
- Confirm the ingest job produces the expected graph.
- Run the full project test suite and the linters, and confirm they pass with the Spark package gone.
- Do one project-wide search for the old package name, the old import path, and any retired command, across code, workflows, and docs, and confirm it is clean. This is the one sweep for the whole change.

**Phase 6 is done when:** the ingest job runs end to end off the local neocarta wheel, the client runs locally, all tests and linters pass, and the old package leaves no trace.

---

## What is deliberately left for later

- **Publishing the neocarta wheel to a package index.** This plan stages the wheel from a local folder. When neocarta publishes, the only change here is the one wheel-source setting from Phase 1: swap the local folder for the index plus a version number.
- **The version bump for the release.** The local testing uses whatever version the neocarta build currently carries. The agreed release version is set at publish time, not here.

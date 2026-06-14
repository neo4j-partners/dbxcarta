# align-v2: what got done, in plain English

## The big picture

dbxcarta used to carry its own copy of the Spark pipeline that reads Unity Catalog
tables and builds the Neo4j semantic graph. That pipeline now lives in a separate
project called neocarta. The work in `align.md` was about cutting the pipeline out
of dbxcarta and having dbxcarta pull and run neocarta's prebuilt connector instead.

After this work, dbxcarta is just operator tooling. It stages the neocarta connector
onto a Databricks Volume, sets up the catalogs and volumes, and submits the ingest
job. The only Spark left in dbxcarta is the job-submit tooling and the `materialize`
job. Everything else, including the example client, is now plain local Python that
talks to Neo4j and the serving endpoints directly, with no cluster.

Most of the plan is finished. The code changes are done and the test suite passes.
What is left is mostly the live end-to-end run on a real cluster, the CI workflow
cleanup, and flipping the wheel source from a local folder to a real package index
once neocarta publishes.

## What changed (bullet list)

- The old `dbxcarta-spark` package was deleted entirely, along with its tests, its
  workspace entries, and its references in the lock file.
- The ingest job now points at the neocarta connector wheel instead of the old
  in-repo package. The submit tooling stages a prebuilt neocarta wheel from a local
  build folder onto the Volume.
- The `verify` command was retired. It checked finished pipeline output, which is
  now neocarta's responsibility.
- The `ready` command was kept and moved into the operator tool. It checks warehouse
  catalog state and is still a genuine operator helper.
- The operator command was renamed from `dbxcarta-submit` to `dbxcarta`, taking over
  the name the old pipeline command used.
- The `upload-questions` command was deleted. The client now reads questions from a
  local `questions.json` file, so nothing needs questions staged to a Volume anymore.
- The example client was taken fully off Spark. Model calls became plain local web
  calls to the serving endpoint, the same pattern `client/embed.py` already used.
  Question reads, response caching, and result output all moved to local files. The
  three old Delta output tables and the JSON summary file were dropped because
  nothing read them.
- A working local run command was added for the client. It loads the example overlay
  and the base `.env` the same way the operator CLI does, so a single command runs
  the client locally end to end.
- The overlays were rewritten to carry neocarta's `NEOCARTA_DATABRICKS_*` settings,
  including the inline embedding contract and a required staging volume. The old
  spark-ingest keys were removed.
- The pinned dependency list for the ingest job was reduced to neocarta's tested
  closure for the inline path. It deliberately leaves out `pyspark`, `pandas`, and
  `numpy` because the Databricks Runtime supplies them.
- The docs were rewritten across the board. The README, architecture doc, schema
  doc, release doc, tutorials, and example READMEs now describe dbxcarta as operator
  tooling that pulls and runs the neocarta connector. The public interface doc was
  deleted because it only mapped pipeline module names that no longer exist here.

## Does it align with neocarta?

Short answer: yes. The dbxcarta work in `align.md` and the neocarta work described in
`simple.md` and `cli.md` are two halves of the same split, and they agree on the
contract between them.

Where they line up:

- `cli.md` says neocarta keeps the connector and builds a versioned wheel with a
  `databricks-spark` extra, and dbxcarta becomes operator tooling that pulls and
  stages that wheel. That is exactly what the dbxcarta side built.
- Both sides use the same settings naming. dbxcarta's overlays write
  `NEOCARTA_DATABRICKS_*` keys, and `simple.md` confirms the connector reads that
  same prefix. The handoff needs no translation step.
- `simple.md` ported inline embeddings back into the connector as a native
  `ai_query` call. dbxcarta runs that inline mode by default, so a single ingest job
  produces a fully embedded graph.
- `simple.md` added the `neocarta databricks embed` CLI command for the external
  embedding path, and `cli.md` notes that command needs the `neocarta[cli]` extra.
  dbxcarta's deferred Phase 7 matches this exactly: install `neocarta[cli]` and run
  `neocarta databricks embed` as the post-ingest step.
- The staging volume setting and the optional summary volume setting that dbxcarta's
  overlays set were both added on the neocarta side, so the required keys exist where
  dbxcarta expects them.
- Both plans agree the wheel comes from a local folder during testing and flips to a
  package index later, with the version as the single handoff contract.

The two-step flow both sides describe is the same: run the Spark ingest job on a
cluster, then run the operator follow-up. For dbxcarta the default follow-up is just
`materialize`, since inline mode already embedded the graph. The external embed
command is the deferred alternative.

## What work remains and what needs fixing

These are the open items, in rough priority order.

- **The live end-to-end run has not happened.** Everything is proven in the test
  suite, but the real ingest submit against a Spark 4 cluster and a Neo4j instance
  is still pending. This is `align.md` Phase 6 and the neocarta-side live pass.
- **The Maven coordinate for the Neo4j Spark Connector still says Spark 3.** It must
  be bumped to the Spark 4 connector JAR before the live submit passes its preflight
  check. This is the most concrete fix waiting in the submit tooling.
- **The CI workflows still name the deleted package.** Phase 4 was postponed on
  purpose. The `.github/` workflows still build, version, test, and type-check
  `dbxcarta-spark`, which no longer exists. They are held until neocarta publishes,
  and `docs/reference/release.md` carries a note explaining the lag.
- **The wheel still comes from a local folder.** The wheel source is a single
  setting today pointed at neocarta's local build folder. When neocarta publishes to
  a package index, this one setting flips to an index plus a version number. Nothing
  else changes.
- **One test is structurally checking the overlay instead of importing neocarta.**
  The finance-genie `test_overlay.py` cannot import neocarta's settings yet because
  neocarta is not on a package index. For now it validates the overlay key contract
  by structure. The full repoint rides along with the index publish.
- **A separate cleanup is still pending in `cleanup.md`.** This is unrelated to the
  neocarta split. It unifies every example's questions file to the single name
  `questions.json` and removes a dead questions-path derivation chain in the core
  config and example configs. It has an open decision flagged for the user about
  whether to also remove an orphaned `.env` line. None of it is started.
- **The external embedding path is deferred.** Phase 7 of `align.md` and the
  matching neocarta CLI command exist, but the dbxcarta side has not installed
  `neocarta[cli]` or wired the `neocarta databricks embed` step. This is intentional
  because inline mode already produces an embedded graph.
- **On the neocarta side, the wheel dependencies are not pinned yet.** `cli.md`
  Phase 6 found that a clean-room install resolves an untested pyspark 4.x from the
  unpinned floor. The runtime dependencies need pinning before the index publish, and
  that pinned set is what dbxcarta's ingest closure will consume.

No live bug or broken code path was found in the completed work. The open items are
deferred steps and coordination points, not regressions. The main risk to watch is
the live submit, where the Maven coordinate bump and the Spark 4 runtime requirement
have to be right together before the first real run will pass.

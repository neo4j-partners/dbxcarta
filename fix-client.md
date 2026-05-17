# Proposal: tighten the dbxcarta-client / examples boundary

This document proposes two changes to `packages/dbxcarta-client`. Each section
opens with a plain-language overview, then states the proposal in English with
no code.

---

## Fix 1: stop the finance-genie demo from re-implementing the graph_rag pipeline

### Explain it like I'm five

The client package has a machine that takes a question and turns it into an
answer. It does this in stages: turn the question into numbers, look up related
tables in the graph, write a hint sheet for the language model, ask the model
for SQL, then run the SQL.

The big evaluation harness uses that machine. But the small finance-genie demo
could not reach the middle stages, so the demo built its own copy of the same
machine by hand. Now there are two machines that are supposed to behave the
same. If someone fixes or improves one, the other quietly drifts and starts
giving different answers for the same question. The fix is to take the shared
middle of the machine, give it a real front door in the client package, and
have both the harness and the demo walk through that same door.

### What is duplicated today

`run_graph_rag_question` in
`examples/integration/finance-genie/src/dbxcarta_finance_genie_example/local_demo.py`
re-creates the retrieval-and-prompt sequence that already lives inside
`_run_graph_rag_arm` in
`packages/dbxcarta-client/src/dbxcarta/client/eval/arms.py`. Both perform the
same ordered steps: embed the question, construct a `GraphRetriever`, call
`retrieve`, render the returned bundle to context text, and build the
`graph_rag` prompt. The two paths only diverge after the prompt is built. The
harness sends the prompt through `generate_sql_batch`, which runs `ai_query`
on Spark and records evaluation traces and summary rows. The demo sends the
same prompt through `generate_sql_local`, which calls a Model Serving endpoint
over REST, then adds read-only SQL enforcement and an optional reference
comparison for interactive use.

The leak is specific. The reusable part is the question-to-prompt assembly. It
is not exposed as a public function, so the demo had to copy the harness
internals to get at it.

### Proposal in English

Add one public, transport-neutral seam to the client package that performs the
shared graph_rag work for a single question: take a workspace client, the
client settings, and a question; optionally accept a pre-computed embedding;
embed the question if no embedding was supplied; build and run the graph
retriever; close the retriever; render the retrieved context to text; build
the graph_rag prompt; and return a small immutable result that carries the
question, the seed identifiers, the rendered context text, and the finished
prompt. This new seam stops before any model call, because the model transport
is exactly where the harness and the demo legitimately differ.

Rework the finance-genie demo so its `run_graph_rag_question` calls this new
seam to obtain context and prompt, then keeps only the parts that are genuinely
demo-specific: the local Model Serving call, the read-only SQL guard, the
reference comparison, and the table printing. The demo keeps its own command
parser and output formatting. It no longer owns any retrieval logic.

Refactor `_run_graph_rag_arm` so the per-question retrieval-and-prompt step is
obtained from the same new seam rather than from inline code. The harness keeps
everything that is harness-specific and must not move into the shared seam:
batch generation through Spark `ai_query`, the threaded retrieval concurrency
loop, the reference cache, evaluation metrics, retrieval traces, and summary
emission. The shared seam is called once per question from inside the harness
loop so the existing concurrency and ordering behavior is preserved.

Place the new seam in its own module within the client package so the public
surface is explicit, and export it from the client package init alongside the
existing public names. Add a focused unit test that asserts the seam returns a
prompt containing the rendered context for a stubbed retriever, so future
changes to retrieval cannot silently break either consumer.

Expected impact. One definition of the graph_rag retrieval-and-prompt
behavior. The demo shrinks and can no longer drift from the harness. The
harness keeps its evaluation-only logic. No change to runtime behavior for
either path, because the extracted steps are identical to what both already do.

Main risk and mitigation. The harness currently interleaves retrieval with
trace construction inside its threaded loop. The extraction must keep retrieval
and trace-building separable so the seam returns context and prompt while the
harness still builds traces from the same bundle. The seam should therefore
also expose the underlying retrieved bundle, not only the rendered text, so the
harness can compute traces without a second retrieval.

Alternative considered and rejected. Exposing a full single-question
end-to-end function that also calls the model. Rejected because the harness and
the demo deliberately use different model transports and different
post-generation handling, so an end-to-end function would either not fit the
harness or would re-introduce branching that defeats the purpose.

---

## Fix 2: make Neo4j an optional dependency of the client package

### Explain it like I'm five

The client package can do several small jobs: load questions, parse SQL out of
a model reply, and compare two result sets. It can also do one big job: walk
the graph to find relevant tables. The big job needs a graph database driver
called Neo4j. Right now, anyone who installs the client package is forced to
install Neo4j even if they only want the three small jobs. The fix is to make
Neo4j an optional add-on, so people who only need the small jobs get a lighter
install, and people who need the graph walk ask for the add-on explicitly.

### Current coupling

`packages/dbxcarta-client/pyproject.toml` lists `neo4j>=5.0` as an
unconditional dependency. The advertised public API in the client package init
is `compare_result_sets`, `load_questions`, `parse_sql`, and `Question`. None
of those need Neo4j.

Only two client modules import Neo4j at module load: `graph_retriever.py` and
`schema_dump.py`. `neo4j_utils.py` and `retriever.py` do not import Neo4j.
`settings.py` only references the string name of a secret scope. The harness
already imports `GraphRetriever` lazily from inside the graph_rag arm function,
so importing the client package and using the small public API never touches
Neo4j today except through the always-on dependency declaration.

### Proposal in English

Move `neo4j>=5.0` out of the unconditional dependency list and into an optional
dependency group named `graph` in the client package project file. The base
install then covers the question loader, the SQL parser, the result-set
comparator, the settings model, the executor, and embedding. The graph
retriever and the Neo4j schema dump become available only when the consumer
installs the client package with the `graph` extra.

Keep the Neo4j imports where they are, at the top of `graph_retriever.py` and
`schema_dump.py`, because both modules are only ever imported lazily by code
that actually intends to use the graph path. Add a clear, actionable error so
that if either module is imported without the `graph` extra installed, the
failure states that Neo4j is missing and that the fix is to install the client
package with the `graph` extra. The error should name the extra so the message
is self-correcting.

Update the three example projects so their dependency on the client package
requests the `graph` extra, because each of them reaches the graph path
somewhere. The finance-genie demo uses the graph retriever and the schema
dump. The schemapile and dense-schema examples use the Neo4j schema dump
through their question-context tooling. The local-development workspace source
overrides in the example project files stay as they are; only the dependency
specifier for the client package gains the extra.

Document the split in the client package so consumers know the base install is
for question loading, SQL parsing, and result comparison, and the `graph`
extra adds graph retrieval and the Neo4j schema dump.

Expected impact. Consumers who only want the advertised lightweight API no
longer pull a graph database driver. The "client as a reusable library" story
becomes honest: the base surface has no graph database in its dependency
closure. The graph path keeps working unchanged for everyone who asks for the
extra.

Main risk and mitigation. A consumer who currently relies on the implicit
Neo4j install and uses the graph path without declaring the extra would break
at import time after this change. The mitigation is the explicit, named error
message plus the documentation change, and updating the in-repo examples in the
same change so the repository itself stays installable and green.

Alternative considered and rejected. Splitting the graph retriever into a
separate distributable package. Rejected as heavier than the problem warrants:
an optional extra solves the dependency-closure complaint without fragmenting
the codebase or adding a second package to version and release.

---

## Suggested order

Fix 1 and Fix 2 are independent and can land separately. Fix 2 is smaller and
lower risk, so it goes first. Fix 1 is the more valuable change because it
removes a real source of behavioral drift between the demo and the evaluation
harness.

---

## Implementation plan

### Fix 2: Neo4j optional extra

- [x] Move `neo4j>=5.0` from `dependencies` to an optional `graph` group in
      `packages/dbxcarta-client/pyproject.toml`.
- [x] Guard the top-level Neo4j import in `graph_retriever.py` with an
      actionable `ModuleNotFoundError` that names the `graph` extra.
- [x] Guard the top-level Neo4j import in `schema_dump.py` the same way.
- [x] Update the three example project files to depend on
      `dbxcarta-client[graph]`.
- [x] Add a package docstring to the client init that documents the base
      surface versus the `graph` extra.

### Fix 1: shared graph_rag retrieval-and-prompt seam

- [x] Add a new client module `graph_rag.py` with an immutable
      `GraphRagContext` result and a transport-neutral
      `build_graph_rag_context` function that stops before any model call,
      accepts an optional precomputed embedding, and accepts an optional
      caller-owned retriever.
- [x] Export the new seam from the client package init.
- [x] Refactor `local_demo.py::run_graph_rag_question` to obtain context and
      prompt from the seam and keep only the demo-specific model call,
      read-only guard, reference comparison, and printing.
- [x] Refactor `eval/arms.py::_run_graph_rag_arm` to obtain each question's
      context and prompt from the seam inside the existing threaded loop while
      keeping batch generation, the reference cache, traces, and summary.
- [x] Add a unit test asserting the seam returns a prompt containing the
      rendered context for a stubbed retriever and does not close a
      caller-owned retriever.
- [x] Run the client test suite and confirm it is green.

---

## Status / progress

| Item | Status |
|---|---|
| Fix 2: pyproject extra | Done |
| Fix 2: import guards | Done |
| Fix 2: example dependencies | Done |
| Fix 2: package docstring | Done |
| Fix 1: `graph_rag.py` seam | Done |
| Fix 1: init export | Done |
| Fix 1: demo refactor | Done |
| Fix 1: harness refactor | Done |
| Fix 1: unit test | Done |
| Test suite | Green (see notes) |

### Notes

All items implemented and verified.

- Client test suite: `163 passed` (`tests/client`), including the new
  `tests/client/test_graph_rag.py`.
- Cross-cutting check: `tests/client/test_graph_rag.py tests/boundary
  tests/examples/finance-genie` together `27 passed`.
- Import sanity: `import dbxcarta.client` exposes `GraphRagContext` and
  `build_graph_rag_context` alongside the existing public names;
  `dbxcarta_finance_genie_example.local_demo` and
  `dbxcarta.client.eval.arms` import cleanly after the refactor.
- No behavior change to either graph_rag path: the seam performs the exact
  embed, retrieve, render, and prompt steps both call sites previously
  inlined; the harness keeps batch generation, the reference cache, traces,
  and summary, and reuses one shared retriever across the threaded loop
  (the seam does not close a caller-owned retriever).
- The harness's batch embedding plus the embedding-count alignment guard are
  unchanged; the seam receives the pre-computed per-question embedding.
- Neo4j is no longer in the base dependency closure; the `graph` extra adds
  it. `graph_retriever.py` and `schema_dump.py` raise an actionable
  `ModuleNotFoundError` naming the extra when Neo4j is absent. All three
  example projects now depend on `dbxcarta-client[graph]`.
- Type check clean: `uv run mypy -p dbxcarta.client` reports `Success: no
  issues found in 25 source files`. (mypy is in the `test` dependency group;
  install it with `uv sync --group test`.) The check surfaced one defect in
  the new seam: it closed a `retriever` typed as the `Retriever` ABC, which
  declared only `retrieve`. Fixed by adding a documented no-op `close()`
  default to the `Retriever` ABC. `GraphRetriever` (the only concrete
  implementation) already overrode `close()`; the default is non-breaking
  for any future lightweight retriever.

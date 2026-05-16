# Dense-1000 graph_rag Embedding Fix

## Context

The dense-1000 client eval ran clean for `no_context` and `schema_dump`, but
`graph_rag` produced `attempted=505 parsed=0 executed=0`. The run logged:

```
embedding call to databricks-gte-large-en failed: BAD_REQUEST: Input embeddings
size is too large, exceeding 150 limit for databricks-gte-large-en.
```

Two problems compounded:

1. The embedding call exceeds the endpoint batch limit, so every question in the
   `graph_rag` arm gets a `None` embedding.
2. The failure is swallowed. The arm logs a warning and keeps going, producing
   505 empty results instead of stopping. The eval reports a "complete" run with
   a degraded arm, which is the worst possible signal: it looks like a modeling
   problem, not a broken pipeline.

## What is actually wrong

The `graph_rag` arm does **not** embed the schema. Schema and column embeddings
are computed once at ingest time on the Databricks cluster and stored in Neo4j
vector indexes. The client only ever embeds the **question**, which is correct
in principle.

The bug is batching. `_run_graph_rag_arm()` collects every question into one
list and embeds them all in a single request:

`packages/dbxcarta-client/src/dbxcarta/client/eval/arms.py:197`

```python
texts = [q.question for q in questions]
embeddings, embed_error = _embed_questions(ws, settings.dbxcarta_embed_endpoint, texts)
```

`embed_questions()` passes that list straight through with no chunking:

`packages/dbxcarta-client/src/dbxcarta/client/embed.py:25`

```python
data = ws.api_client.do(
    "POST",
    f"/serving-endpoints/{endpoint}/invocations",
    body={"input": texts},
)
```

`databricks-gte-large-en` accepts at most 150 inputs per request. At 505
questions the request is rejected, `embed_questions()` returns `(None, error)`,
and the arm continues with no embeddings.

So the user's instinct is right: the client should embed just the question. It
does. The defect is that the harness pre-embeds **all** questions in one call
for efficiency, and that batch exceeds the endpoint limit at dense-1000 scale.
The fix is to chunk the batch under the limit, not to change what gets embedded.

## Proposal

Three changes, in priority order.

### 1. Fail fast when embeddings do not work

Today a `None` embedding degrades silently into 505 empty rows and a green run.
That is the core reliability defect, independent of the batch size.

- In `_run_graph_rag_arm()`, after the embed call, if `embed_error` is set or
  any embedding is `None`, **raise** instead of looping. The eval should fail
  loudly with the endpoint error message, not record `parsed=0`.
- Add a guard so a partial batch (some embeddings present, some `None`) is also
  treated as a hard failure rather than a quietly shortened run.

This converts "looks like the model is bad at SQL" into "the embedding endpoint
rejected the request," which is the signal we actually want.

#### Tests that fail fast

Add `tests/client/test_embed.py`:

- **Batch over the limit raises.** Stub `ws.api_client.do` to raise a
  `DatabricksError` mimicking the real `BAD_REQUEST ... exceeding 150 limit`
  message. Assert `embed_questions()` returns `(None, error)` and that the
  `graph_rag` arm wrapper raises rather than yielding empty results.
- **Chunking respects the limit.** With the chunked implementation (change 3),
  pass 505 texts, assert `ws.api_client.do` is called with no batch larger than
  the configured chunk size, and assert the per-chunk results reassemble in
  input order by the response `index` field.
- **Order preservation.** Feed a known set of texts across chunk boundaries and
  assert `embeddings[i]` still corresponds to `texts[i]`.

Add one arm-level test in `tests/client/` that stubs the embed layer to return
`(None, "boom")` and asserts `_run_graph_rag_arm()` raises, so the silent-degrade
regression cannot return.

These run in CI with no Databricks connection, since `ws.api_client.do` is
stubbed.

### 2. A way to iterate on a single embedding

Add a tiny diagnostic so a single embedding can be exercised against the live
endpoint without a 37-minute eval run.

Add a console script `dbxcarta-embed-probe` that:

- Builds a `WorkspaceClient` via `build_workspace_client()`. It resolves the
  endpoint from `--endpoint`, then `DBXCARTA_EMBED_ENDPOINT`, then
  `DBXCARTA_EMBEDDING_ENDPOINT`, then `databricks-gte-large-en`. It does not
  load `ClientSettings`, so it runs without a full `.env`.
- Takes one question string as a positional argument, calls
  `embed_questions(ws, endpoint, [text])`, and prints OK or FAILED, the error
  string if any, the vector count, the dimension, and the first few components.
- Takes `--count N` to send N copies in one call. This is for confirming the
  endpoint works at a given size, not for tuning the chunk size, which is a
  fixed constant.

This gives a sub-second feedback loop for "does the endpoint work" instead of
inferring it from a full client run.

### 3. Chunk the batch under the endpoint limit

Make `embed_questions()` chunk internally so callers never have to know the
limit.

- Add a module-level `EMBED_MAX_BATCH = 100` constant. This sits well under
  the 150 hard cap with comfortable margin. Embedding is not the bottleneck.
  505 questions is 6 requests, which is milliseconds against a 37-minute eval,
  so there is no reason to tune this. A safe fixed value is the right call.
- In `embed_questions()`, split `texts` into chunks of `EMBED_MAX_BATCH`, issue
  one request per chunk, sort each chunk's response by `index`, and concatenate
  in input order so `embeddings[i]` maps to `texts[i]`.
- Keep the existing return contract: on any chunk failure return
  `(None, error)`. Combined with change 1, a failed chunk now aborts the run
  loudly instead of silently.

Chunking lives in one place, so `local_demo.py` and any future caller inherit
the fix.

#### Documented limits for `databricks-gte-large-en`

From the Databricks Foundation Model APIs docs (Azure, retrieved 2026-05-16):

| Constraint | Value | Source |
|------------|-------|--------|
| Max inputs per embedding request | **150** | Enforced by the endpoint. This is the exact `BAD_REQUEST ... exceeding 150 limit` we hit. The limits page does not tabulate it; the endpoint returns it directly. |
| Max tokens per single input | **8,192** | Vector Search GTE foundational embedding model docs ("The GTE model supports up to 8192 tokens"). |
| Max payload per request | **4 MB** | Foundation Model APIs limits, resource and payload table. |
| Queries per hour (pay-per-token) | **540,000** (~150 req/s) | Foundation Model APIs limits, embedding models table. GTE Large (En), no ITPM/OTPM cap. |
| Embeddings normalization | GTE does **not** return normalized embeddings | Foundation Model APIs limits, notes column. Cosine similarity in Neo4j is unaffected since the index normalizes, but worth noting. |

So three independent caps apply per request: at most 150 inputs, at most 4 MB
total body, and at most 8,192 tokens in any single input. For dense-1000 the
binding one is the 150 input cap. Questions are short, so neither the 4 MB
payload nor the 8,192-token per-input limit is close.

Batch size is a throughput knob, not a quality knob. Each input is embedded
independently, so chunking changes only request count, never the vectors.
There is deliberately no tuning here: `EMBED_MAX_BATCH = 100` is a safe fixed
value with margin under every cap, and embedding throughput is irrelevant next
to the eval's runtime. The probe (change 2) is for confirming the endpoint
works, not for tuning batch size.

## Files to change

| File | Change |
|------|--------|
| `packages/dbxcarta-client/src/dbxcarta/client/embed.py` | Add `EMBED_MAX_BATCH`; chunk inside `embed_questions()`; preserve input order |
| `packages/dbxcarta-client/src/dbxcarta/client/eval/arms.py` | In `_run_graph_rag_arm()`, raise when `embed_questions` returns `None`, and raise on an embedding/question count mismatch, instead of recording empty rows |
| `packages/dbxcarta-client/src/dbxcarta/client/embed_probe.py` | New single-question / N-copy diagnostic script |
| `packages/dbxcarta-client/pyproject.toml` | Register `dbxcarta-embed-probe` console script |
| `tests/client/test_embed.py` | New: empty input, chunking respects limit, order preserved across chunks, over-limit returns `(None, error)`, and graph_rag arm raises on embed failure and count mismatch |

## Verification

1. **Unit, no cluster:** `uv run pytest tests/client/test_embed.py` covers the
   chunking and the graph_rag fail-fast tests. All green, no Databricks
   connection.
2. **Single embedding, live:** `uv run dbxcarta-embed-probe "how many accounts
   are active?"` and confirm a vector of expected length comes back.
3. **Batch limit, live:** `uv run dbxcarta-embed-probe --count 200 "ping"` and
   confirm chunking returns 200 vectors with no `BAD_REQUEST`.
4. **Full arm:** `uv run dbxcarta submit-entrypoint client` and confirm
   `graph_rag` now reports nonzero `parsed`. If the endpoint is still
   misconfigured, confirm the run now **fails loudly** with the endpoint error
   rather than reporting `parsed=0`.
5. Append the outcome to `docs/dense_1000.md` Step 8 as a follow-up entry.

---

## Client test results (2026-05-16)

Command: `uv run pytest tests/client -q`

```
148 passed in 0.59s
```

`tests/client/test_embed.py` (the 6 new tests for chunking and graph_rag
fail-fast) all pass.

**Summary:** The suite is fully green. An earlier run reported
`1 failed, 147 passed`, where
`test_graph_rag_prompt_single_schema_constraint` asserted the now-removed
string `"Do not join across unrelated schemas"`. That was a `layers`-branch
prompt change, not an embedding-fix defect. It has since been resolved by
updating `tests/client/test_retriever.py` to assert the constraints the
current `prompt.py` actually emits (`"Use ONLY the exact tables and columns
listed in the context"`, `"backtick-quoted three-part names"`, and a
target-schema mention). No embedding-fix code touches `prompt.py`. See
Phase E below, now closed.

---

## Plan: finish the dense-1000 test run

`docs/dense_1000.md` ran the pipeline to completion but Step 8 left the
`graph_rag` arm degraded (`parsed=0`) because of the embedding batch-limit bug
this document fixes. Steps 1–7 and 9 are green. The work below closes out the
run.

Status legend: `[ ]` not started, `[~]` in progress, `[x]` done.

### Phase A — Land the embedding fix

- [x] A1. Chunk `embed_questions()` under `EMBED_MAX_BATCH`.
- [x] A2. Make `_run_graph_rag_arm()` fail fast on embed failure / count mismatch.
- [x] A3. Add `dbxcarta-embed-probe` console script.
- [x] A4. Add `tests/client/test_embed.py`.
- [x] A5. Run `uv run pytest tests/client`. Result recorded above:
  `148 passed`, all new embedding tests green, suite fully clean.

### Phase B — Live embedding sanity check

- [x] B1. From `examples/integration/dense-schema/`, run
  `uv run dbxcarta-embed-probe "how many accounts are active?"`.
  Result: `result: OK`, `vectors: 1`, **`dim: 1024`** against
  `databricks-gte-large-en`. Pass.
- [x] B2. Run `uv run dbxcarta-embed-probe --count 200 "ping"`.
  Result: `result: OK`, **`vectors: 200`**, `dim: 1024`, no `BAD_REQUEST`.
  200 inputs chunked as 100 + 100 crossed the 150 cap cleanly. Pass.

### Phase C — Re-run Step 8 client eval

- [~] C1. From `examples/integration/dense-schema/`, run
  `uv run dbxcarta submit-entrypoint client`. **Blocker found and worked
  around — see "Phase C blocker" below.** A first attempt
  (`run_id=885675572908818`) ran the **stale pre-fix wheel** and was
  cancelled. The fixed client wheel was rebuilt and republished manually,
  then the eval was resubmitted. Re-run in progress.
- [ ] C2. Confirm `graph_rag` now reports nonzero `parsed` and `executed`.
  Capture the three-arm result block (`no_context`, `schema_dump`,
  `graph_rag`) the same way Step 8 did.
- [ ] C3. If embeddings still fail, confirm the run now **aborts loudly** with
  the endpoint error instead of silently reporting `parsed=0`. That is also a
  passing outcome for this fix; it means the failure is endpoint config, not a
  swallowed error.
- [ ] C4. Record the new result block and run ID in this file under "Step 8
  re-run results" below.

#### Phase C blocker — the fix was never deployed (root cause + workaround)

The first re-run reproduced the **exact pre-fix bug** even though Phase A
code is correct (148 tests green, live probe chunks 200 inputs cleanly).
Investigation via the Databricks Jobs API:

- The pre-fix baseline run `220042594665556` (today, before this work)
  confirmed the original defect verbatim: `BAD_REQUEST ... exceeding 150
  limit`, swallowed, `status=success`, `graph_rag attempted=505 parsed=0`.
  Baseline arm numbers: `no_context parsed=505 executed=29
  correct_rate=69.0%`, `schema_dump parsed=505 executed=204
  correct_rate=27.9%`, `graph_rag parsed=0`.
- **Root cause:** `dbxcarta submit-entrypoint client` only *consumes*
  `dbxcarta_client-stable.whl` from the UC Volume; it never rebuilds it.
  The bootstrap manifest of the first re-run showed `wheel_sha
  00422c1adc6891b8`, byte-identical to the pre-fix baseline. The fix
  existed only in the local working tree, never on the cluster.
- **Second defect (deploy path):** the documented publish command,
  `dbxcarta upload --wheel`, fails from
  `examples/integration/dense-schema/`. `databricks-job-runner==0.6`
  sets `project_dir = Path.cwd()` and runs a bare `uv build --wheel
  --out-dir dist` (no `--package`) for both `dbxcarta-spark` and
  `dbxcarta-client` from that single cwd. In this uv workspace, a bare
  `uv build` from the example dir builds the *example* package, and from
  the repo root builds only the root `dbxcarta-workspace` package, so
  neither entrypoint wheel lands in `project_dir/dist`. `publish_wheel_stable`
  then aborts with `wheel build failed — no dbxcarta_spark-*.whl found in
  dist/`. The documented deploy path cannot rebuild either entrypoint
  wheel in this layout.
- **Workaround applied (user-approved republish + re-run):** replicated
  `publish_wheel_stable` manually for the client wheel only —
  `uv build --wheel --package dbxcarta-client --out-dir dist` (picks up
  the working-tree fix; verified `embed.py` in the wheel contains
  `EMBED_MAX_BATCH` + chunking), then uploaded it over the stable Volume
  path `/Volumes/schemapile_lakehouse/_meta/schemapile_volume/wheels/
  dbxcarta_client-stable.whl` via the Files API with `overwrite=True`
  (stable size 34,251 → 36,542 bytes). The first stale run
  `885675572908818` was cancelled. Eval resubmitted on the fixed wheel.
  - **Update (2026-05-16):** fixed upstream in `databricks-job-runner==0.6.1`,
    which now runs `uv build --wheel --package <pkg>` in `publish_wheel_stable`.
    The manual workaround above is no longer required; dbxcarta now pins
    `==0.6.1`. The 0.6 behaviour described above is retained as the record of
    what was observed at the time.
- **Third defect (cosmetic):** `submit-entrypoint`'s local SDK waiter
  times out at 20 min while the eval needs ~37 min, so the local command
  always `TimeoutError`s while the remote run continues. Results must be
  tracked via the Jobs API, not the local command's exit code.

### Phase D — Re-verify and log

- [ ] D1. Run `uv run dbxcarta verify`. Expect `OK (0 violations)`.
- [ ] D2. Append a "Step 8 (re-run)" entry to `docs/dense_1000.md` with the
  new `graph_rag` numbers and a pointer to this document.
- [ ] D3. Mark Steps 5 and 6 in the `docs/dense_1000.md` table as ✅ done; the
  step log shows they completed even though the table still reads ⏳.

### Phase E — Unrelated failure (resolved)

- [x] E1. Decided: the `layers`-branch `prompt.py` intentionally dropped the
  `"Do not join across unrelated schemas"` wording, so
  `test_graph_rag_prompt_single_schema_constraint` was updated to assert the
  constraints the current prompt actually emits rather than restoring old
  text. `tests/client/test_retriever.py` now passes and the full
  `tests/client` suite is green. This was a `layers`-branch concern, out of
  scope for the embedding fix, and did not block Phase C.

### Step 8 re-run results

_To be filled in after Phase C. Capture:_

```
no_context:  attempted=... parsed=... executed=... exec_rate=...% correct_rate=...%
schema_dump: attempted=... parsed=... executed=... exec_rate=...% correct_rate=...%
graph_rag:   attempted=... parsed=... executed=... exec_rate=...% correct_rate=...%
```

_Run ID: ___  Duration: ___  Embedding warnings: none expected._

---

## Findings and suggested fixes (2026-05-16)

The embedding fix itself (Phase A/B) is sound: unit suite green, the live
probe chunks 200 inputs across the 150 cap with no `BAD_REQUEST`, and the
baseline run confirms the exact defect the fix targets. The problems found
during Phase C are all in the **deploy path**, not the fix:

1. **`submit-entrypoint` does not rebuild the wheel (highest impact).**
   Running `submit-entrypoint client` after a code change silently runs
   stale code. There is no version or content check; the bootstrap reports
   only `wheel_sha`. *Suggested fix:* have `submit-entrypoint` either
   rebuild+publish the stable wheel as a preflight, or fail loudly when the
   local source tree is newer than the published wheel. At minimum, document
   that `dbxcarta upload --wheel` must precede `submit-entrypoint` after any
   code change (the dense-schema README omits this entirely).

2. **`dbxcarta upload --wheel` is broken in this uv workspace.**
   `publish_wheel_stable` (databricks-job-runner 0.6) runs a bare
   `uv build --wheel` from `Path.cwd()` for each entrypoint package. No
   single cwd builds both `dbxcarta-spark` and `dbxcarta-client` in this
   workspace. *Suggested fix:* pass `--package <wheel_package>` to
   `uv build` in `publish_wheel_stable`, or set each entrypoint's
   `project_dir` to its package directory. Until the helper is fixed, the
   manual replication used here (build with `--package`, Files API upload
   to the stable path with overwrite) is the working deploy.

3. **Local wait shorter than the eval (cosmetic but misleading).**
   `submit-entrypoint`'s 20-min SDK waiter always times out on a ~37-min
   eval; the traceback looks like a failure but the remote run succeeds.
   *Suggested fix:* raise the waiter timeout above the eval runtime, or
   make `submit-entrypoint` print the run URL and return 0 immediately
   (the run is already tracked server-side).

4. **Plan gap.** The plan's Verification step 4 and Phase C assumed
   `submit-entrypoint client` deploys current code. It does not. Phase C
   should explicitly include a republish-and-verify-`wheel_sha`-changed
   step before the eval. This document's Phase C section now records that.

5. **No incremental progress tracking (approved follow-up, separate
   change).** The eval (`eval/run.py:run_client()`) runs arms
   sequentially and writes its summary only once at the end via
   `_emit_summary()`; the per-question loops emit nothing, and Databricks
   `get_run_output` returns a fixed early stdout window, not a live tail,
   so a long `graph_rag` arm is opaque from outside the cluster until it
   finishes. **Decision (user-approved):** implement **A + B** as a
   separate change *after* this plan completes —
   (A) checkpoint each arm's partial summary row as soon as the arm
   finishes, and
   (B) add a per-N-questions heartbeat row (`run_id, arm, done, total,
   ts`) for true live progress, both queryable via SQL/the Databricks
   MCP mid-run.
   **Constraint:** write progress/heartbeat (and any other dbxcarta
   scratch state) to the **new dedicated dbxcarta work catalog**, not a
   production catalog, to keep dbxcarta noise out of prod. The exact
   work-catalog name is not yet wired into config (`DBXCARTA_CATALOG` is
   still `schemapile_lakehouse`); confirm the catalog name before
   implementing.

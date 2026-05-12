# Pip-Integration Plan: One Package Per Integration

This is a phased plan to adopt the integration pattern used by LangChain and
LlamaIndex: keep `dbxcarta` as a lean core library and let each downstream
project ship its own small package that supplies its preset, registers itself
by import path, and is pulled in with a normal pip install. No central
registry, no plugin discovery, no forking of dbxcarta.

This is a clean migration. There are no current external consumers, so the
plan removes the built-in Finance Genie preset entirely rather than leaving a
shim or a compatibility path. After the migration, dbxcarta core knows
nothing about Finance Genie; the Finance Genie preset lives only as an
external package, with the in-repo `examples/finance-genie/` tree refactored
to be the canonical example of an external preset.

The plan has three parts. Part A puts dbxcarta core in a shape where it can
be consumed cleanly by an outside project. Part A.5 rewrites the in-repo
Finance Genie example as the reference external preset, living inside the
dbxcarta repo for documentation but built and consumed exactly like an
outside package. Part B then proves the pattern end to end by wiring up
`graph-on-databricks/finance-genie` as a real outside consumer.

Each phase ends at a natural checkpoint where the user reviews the result
before the next phase begins.

---

## Part A: Prepare dbxcarta core for external consumption

### Phase A1: Audit and define the public API
- [ ] Read through every module under `src/dbxcarta/` and list the symbols a
      downstream project would realistically need: ingest entry function,
      settings model, Neo4j client wrapper, preset protocol types,
      contract types, CLI helpers if any are reusable.
- [ ] Mark everything else as internal. The goal is a small, deliberate
      surface, not a re-export of everything that happens to be importable
      today.
- [ ] Decide on the top-level import shape. For example, what should be
      reachable as `from dbxcarta import ...` versus `from dbxcarta.ingest
      import ...` versus `from dbxcarta.presets import ...`.
- [ ] Write the audit results into a short notes file in the worklog so the
      next phase has a checklist to implement against. No code yet.
- [ ] Review with user before Phase A2.

### Phase A2: Make the package import-clean
- [ ] Replace the near-empty top-level package docstring with deliberate
      re-exports that match the audit from A1.
- [ ] Confirm that importing the package does not trigger heavy work: no
      Spark imports at module top level, no Neo4j connection attempts, no
      reading of environment variables at import time. Anything that touches
      the world must live inside a function or a lazily-instantiated object.
- [ ] Verify the `py.typed` marker is still present and that mypy strict
      coverage still passes against the modules already listed in the
      pyproject overrides.
- [ ] Confirm that every public symbol from the A1 audit can be imported
      from a brand-new Python process with only `pip install dbxcarta` and
      no environment variables set.
- [ ] Review with user before Phase A3.

### Phase A3: Move the runnable entrypoint inside the package
- [ ] Today the Databricks-side runner lives in `scripts/run_dbxcarta.py`,
      which is a per-repo file. Create the canonical entrypoint inside the
      package itself, for example under `dbxcarta.entrypoints`, so any
      consumer can submit `python -m dbxcarta.entrypoints.ingest` against
      the installed wheel without copying any script files.
- [ ] Delete the old `scripts/run_dbxcarta.py` outright. No shim, no
      forwarding stub. Update any internal demo flow that referenced it to
      call the new in-package entrypoint directly.
- [ ] Do the same for the client entrypoint if the demo client is meant to
      be reusable. If it is example-only, leave it where it is and let
      Phase A.5 move it into the example package.
- [ ] Confirm the demo flow still completes end-to-end after the cutover.
- [ ] Review with user before Phase A4.

### Phase A4: Formalize the preset protocol and remove the built-in
- [ ] Decide on the smallest possible contract a preset must satisfy. The
      shape should be: a function or attribute that returns the dbxcarta
      environment overlay, plus optional helpers for readiness checks and
      demo-question upload. Write this contract down in prose, in the
      public API docs, as a short section. No code yet.
- [ ] Implement the preset protocol in dbxcarta core as types and helpers
      only. Core does not ship any preset implementations.
- [ ] Delete the built-in Finance Genie preset module at
      `src/dbxcarta/presets/finance_genie.py` and the re-exports from
      `src/dbxcarta/presets/__init__.py`. The Finance Genie names, table
      lists, and readiness helpers leave dbxcarta core in this phase and
      do not come back.
- [ ] Update the CLI so the preset name is always an import path, such as
      `mycorp_preset:preset`. There are no built-in shortcut names. The
      `dbxcarta preset finance-genie` form no longer exists; the
      equivalent post-migration form points at the external package
      created in Phase A.5.
- [ ] Update or remove any tests, docs, and README sections that reference
      the old built-in preset name. The README should describe the new
      import-path form and nothing else.
- [ ] Review with user before Phase A5.

### Phase A5: Distribution and versioning
- [ ] Confirm the wheel build is reproducible and that the wheel contains
      exactly the modules the A1 audit expects and nothing more. In
      particular, examples and scripts must not be inside the wheel.
- [ ] Decide where the wheel is published for downstream projects to
      consume. Options to discuss with the user: a private index, a git
      tag plus pip install from git URL, a UC Volume that downstream
      projects pull from, or public PyPI. No work is done until the user
      picks one.
- [ ] Document the version contract in the README in plain prose: which
      symbols are public, what counts as a breaking change, how a
      downstream pins a known-good version.
- [ ] Review with user before Part A.5.

---

## Part A.5: Rebuild the in-repo Finance Genie example as the reference external preset

The goal of this part is to leave dbxcarta with one canonical worked example
of the new pattern, sitting in the repo for discoverability but structured
exactly the way a real outside consumer would structure their own preset.
By the end of A.5 the example is a separate package, built independently,
depending on dbxcarta as a normal pip dependency. There is no special path
that this example takes that an outside consumer would not also take.

### Phase A5.1: Reshape the example tree as a standalone package
- [ ] Convert `examples/finance-genie/` from a loose collection of scripts
      and notes into a self-contained Python package directory with its
      own project metadata file and its own source folder. Pick a package
      name that is distinct from anything inside dbxcarta core, for
      example `dbxcarta_finance_genie_example`.
- [ ] The example package declares dbxcarta as a dependency by version,
      not as a path or editable reference back into the parent repo. The
      install instructions must work for a user who has only the example
      directory and a pip index, not the rest of the dbxcarta source.
- [ ] Move any Finance Genie demo client code, demo questions fixtures,
      and run scripts that previously lived in `scripts/` or
      `src/dbxcarta/presets/` into the example package. The dbxcarta core
      source tree should contain no Finance Genie references by the end
      of this phase.
- [ ] Review with user before Phase A5.2.

### Phase A5.2: Implement the preset against the new protocol
- [ ] Inside the example package, implement a single preset object that
      satisfies the protocol defined in Phase A4. This object owns the
      Finance Genie environment overlay, the base and gold table lists,
      the readiness check, and the demo-questions upload helper.
- [ ] Expose the preset under an import path, for example
      `dbxcarta_finance_genie_example:preset`, that can be passed
      verbatim to the dbxcarta CLI.
- [ ] Confirm by walking through the documented flow on a clean
      environment that installing dbxcarta plus the example package and
      pointing the CLI at the preset's import path produces the same
      Neo4j graph the old built-in produced. Same node counts by label,
      same relationship counts by type, same sampled embeddings.
- [ ] Review with user before Phase A5.3.

### Phase A5.3: Update docs and the top-level README
- [ ] Rewrite the dbxcarta README's example section so that the Finance
      Genie flow is described as: install dbxcarta, install the example
      package, run the CLI with the preset import path. The README
      should never imply that dbxcarta core knows about Finance Genie.
- [ ] Add a short prose section to the example package's own README that
      walks an outside reader through how the example is structured and
      why, so it functions as a template for a real external preset.
- [ ] Remove any remaining mentions of the old built-in preset shortcut
      from docs, worklog notes, and command help text.
- [ ] Review with user before Part B.

---

## Part B: Prototype the pattern in graph-on-databricks

Part B validates that an outside repository, with no special relationship to
dbxcarta, can build and ship its own preset package using only the public
API and the example as a template.

### Phase B1: Decide the package layout for the external preset
- [ ] Pick a name that follows the LangChain convention: a short, distinct
      package name that makes the dependency relationship obvious, for
      example `dbxcarta-finance-genie` or
      `graph-on-databricks-finance-preset`. Get the user to approve the
      name before any files are created.
- [ ] Decide where the package source lives inside the
      `graph-on-databricks/finance-genie` tree. The two reasonable choices
      are a new top-level folder dedicated to the preset, or a folder
      colocated with the existing enrichment-pipeline package. Discuss
      tradeoffs with the user before choosing.
- [ ] Confirm with the user whether the prototype should also be
      independently pip-installable from the graph-on-databricks repo, or
      whether it is acceptable for now to install it via editable install
      from a local path.
- [ ] Review with user before Phase B2.

### Phase B2: Create the external preset package
- [ ] Create a minimal Python package at the chosen location with its own
      project metadata, a dependency on dbxcarta at the version picked in
      Phase A5, and nothing else it does not need. Use the in-repo example
      package from Part A.5 as the structural template.
- [ ] Implement the preset against the same protocol. The package now owns
      the Finance Genie environment overlay, the table lists, the readiness
      check, and the demo-questions upload helper, but specialized for the
      graph-on-databricks layout rather than the dbxcarta example layout.
- [ ] Make sure the new package can be imported from a plain Python
      session after installation and that doing so does not require any
      dbxcarta environment variables to be set.
- [ ] Review with user before Phase B3.

### Phase B3: Wire the prototype into the finance-genie workflow
- [ ] Update the finance-genie README and any setup scripts so that the
      documented flow is: install dbxcarta, install the new preset
      package, then run dbxcarta with the preset specified by its import
      path. There is no fallback to a built-in.
- [ ] Confirm by walking through the documented flow on a clean checkout
      that a user who has never touched dbxcarta can install both
      packages, point at the right workspace, and successfully run the
      preset against Unity Catalog.
- [ ] Review with user before Phase B4.

### Phase B4: Validate the round trip
- [ ] Run the full ingest with the external preset and confirm the Neo4j
      graph that results is identical in shape and content to the graph
      produced by the in-repo example preset from Part A.5. Same node
      counts by label, same relationship counts by type, same sampled
      embeddings.
- [ ] Run the demo client against the resulting graph using the same
      questions file. Confirm the answers are at least as good as the
      example flow.
- [ ] Record any rough edges discovered during validation as a short
      punch list. These are inputs to a follow-up cleanup phase, not
      blockers for declaring the prototype successful.
- [ ] Review with user before Phase B5.

### Phase B5: Capture lessons and pick the next adopter
- [ ] Document the lessons from the prototype in a short note: what was
      easy, what was awkward, what should change in the dbxcarta public
      API or preset protocol before a second external preset is built.
- [ ] Identify the next candidate external preset, if any, inside the
      graph-on-databricks repo so that the second adopter validates the
      pattern again. Common second adopters expose gaps the first one
      did not.
- [ ] Final review with user.

---

## Out of scope for this plan

- No central plugin registry, no Python entry points, no automatic
  discovery. The whole point of this pattern is that consumers state
  their dependency explicitly by import path.
- No shims, no compatibility wrappers, no deprecated names kept around.
  There are no current consumers, so the migration is a clean cut.
- No multi-package monorepo split inside dbxcarta core. If that becomes
  desirable later, it is a separate plan.

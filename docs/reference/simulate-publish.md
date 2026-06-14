# Simulating a Published dbxcarta for Consumers

How to let an external consumer depend on dbxcarta by version, exactly as it would
once dbxcarta is on PyPI, without running the trusted-publishing route. This unblocks
development of `graph-on-databricks/finance-genie/dbxcarta` while PyPI publishing is
unavailable.

The idea: build the three published packages into a wheelhouse, **vendor that wheelhouse
into the consumer repo**, and point uv at it with a relative **find-links**. The
consumer's committed `pyproject.toml` pins normal versions with no source overrides, so
it is identical to the eventual published case; only where uv looks for the wheels
changes. Because the wheels are vendored and the find-links path is relative, a new
developer needs only `uv sync`, with no dbxcarta checkout beside the consumer.

## Local publish (the dbxcarta-side setup)

This is the only dbxcarta-side step. Run it once in the dbxcarta checkout, and again
whenever dbxcarta source changes. It is the "publish": it produces the wheels every
consumer resolves against, both locally and on a Databricks cluster.

```
cd <dbxcarta>
uv build --package dbxcarta-core
uv build --package dbxcarta-client
uv build --package dbxcarta-spark
```

The wheels and sdists land in `dbxcarta/dist/`. The consumer vendors a copy of them into
its own committed `dbxcarta-dist/` directory, which becomes the simulated index, reached
two ways (both detailed below):

- **Local resolution** (`uv sync`, tests, the local demo): a committed `uv.toml` points
  `find-links` at the relative `./dbxcarta-dist`.
- **Databricks jobs**: the bundle ships those same wheels as `whl:` libraries pointed at
  `./dbxcarta-dist`.

Nothing else happens in dbxcarta. Vendoring, pinning, syncing, and running are all
consumer-side and are covered in the consumer's own README.

## Flow

```
  dbxcarta checkout                      consumer: finance-genie/dbxcarta
  ─────────────────                      ────────────────────────────────
  uv build --package dbxcarta-core ─┐
  uv build --package dbxcarta-client├─► dist/
  uv build --package dbxcarta-spark ┘     │
                                          │  copy (scripts/refresh_dbxcarta_dist.sh)
                                          ▼
                              dbxcarta-dist/  (COMMITTED, vendored)
                                          │
                          ┌───────────────┴───────────────┐
                          ▼                                ▼
              uv.toml (COMMITTED)              databricks.yml `whl:` libraries
              find-links = ["./dbxcarta-dist"]  ./dbxcarta-dist/dbxcarta_*.whl
                          │                                │
                     uv sync                        bundle upload → cluster
                          ▼
            pyproject.toml (COMMITTED, production-identical)
              dbxcarta-core==1.1.0
              dbxcarta-spark==1.1.0
              dbxcarta-client[graph]==1.1.0
              (no [tool.uv.sources])
                          │
                          ▼
       dbxcarta-* from ./dbxcarta-dist, third-party from PyPI
       import dbxcarta.core / .spark / .client
```

## Steps

1. **Build the wheels** in the dbxcarta checkout, as in
   [Local publish](#local-publish-the-dbxcarta-side-setup) above. The wheels land in
   `dbxcarta/dist/`.

2. **Vendor them into the consumer**, into a committed `dbxcarta-dist/` beside the
   consumer `pyproject.toml`. `scripts/refresh_dbxcarta_dist.sh` in the consumer does this
   copy (it reads `dbxcarta/dist`, or `DBXCARTA_DIST`, and writes `./dbxcarta-dist`).
   Commit `dbxcarta-dist/` so other developers get the wheels with the repo.

3. **Pin the dependencies** in the consumer `pyproject.toml`, with no `[tool.uv.sources]`:

   ```
   dependencies = [
       "dbxcarta-core==1.1.0",
       "dbxcarta-spark==1.1.0",
       "dbxcarta-client[graph]==1.1.0",
       "databricks-sdk>=0.40",
       "python-dotenv",
   ]
   ```

4. **Add a committed `uv.toml`** beside that `pyproject.toml`, pointing find-links at the
   vendored directory with a relative path:

   ```
   find-links = ["./dbxcarta-dist"]
   ```

   The relative path makes the lock portable, so `uv.toml` and `uv.lock` are committed
   too: a fresh clone resolves dbxcarta with no extra setup. Only `.venv/` and the
   consumer's own `dist/` build output stay gitignored.

5. **Sync.** dbxcarta resolves from `./dbxcarta-dist`; third-party deps resolve from PyPI:

   ```
   uv sync
   ```

## Why find-links and not an index URL

A flat directory of wheels is not a PEP 503 simple repository. uv treats
`[[tool.uv.index]]` `url` entries as simple repositories, so a `file://` index pointed
at the directory fails to resolve. find-links is the legacy flat-list mechanism and is
the correct match for a wheelhouse. Confirmed: `[tool.uv] find-links` (including a
relative path) and `UV_FIND_LINKS` both resolve; a `file://` index does not.

## Flipping to real PyPI

When dbxcarta is published (see [release.md](release.md)), delete the consumer's
`uv.toml` and `dbxcarta-dist/`, flip `databricks.yml` libraries from `whl:` to `pypi:`,
and run `uv sync`. The same version pins resolve from PyPI with no change to
`pyproject.toml`. Pin the consumer to the first version actually published, not `1.1.0`,
since `1.1.0` has no artifacts on PyPI. Build the vendored wheels at that same version so
the pins you test with are the pins you ship.

## What this does and does not prove

Proven offline and on: the consumer's pins resolve, dbxcarta installs from the vendored
`dbxcarta-dist`, and `import dbxcarta.core / .spark / .client` plus the bundled
`questions.json` all work. This validates the published-library packaging end to end.

Not covered: the GitHub Actions OIDC handshake with PyPI. That is exercised only by a
real (Test)PyPI publish and is out of scope here by design.

## Verification run (finance-genie/dbxcarta)

```
cd graph-on-databricks/finance-genie/dbxcarta
uv sync                       # dbxcarta-{core,client,spark}==1.1.0 from ./dbxcarta-dist
uv run python -c "import dbxcarta.core, dbxcarta.spark, dbxcarta.client; \
  from pathlib import Path; print(Path('questions.json').exists())"
```

The lock records each dbxcarta package with `source = { registry = "dbxcarta-dist" }`, a
relative in-repo path, confirming the vendored wheelhouse served them and that the lock
is portable.

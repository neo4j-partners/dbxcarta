"""Runnable entrypoints for installed dbxcarta wheels.

Modules in this package are designed to be invoked with `python -m`:

    python -m dbxcarta.entrypoints.ingest [KEY=VALUE ...]
    python -m dbxcarta.entrypoints.client [KEY=VALUE ...]

Both entrypoints parse leading KEY=VALUE positional arguments into the process
environment via `_bootstrap.inject_params` and then dispatch to the
in-package run function. They are the canonical replacement for the older
per-repo `scripts/run_dbxcarta*.py` files.
"""

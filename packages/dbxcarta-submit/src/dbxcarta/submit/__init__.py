"""Operator CLI for submitting and uploading dbxcarta Databricks jobs.

This package owns everything that talks to ``databricks-job-runner``: the
named-entrypoint table, the runner wiring, the submit and upload commands, and
the ``ready`` operator helper. It provides the ``dbxcarta`` console command. The
other dbxcarta wheels (core, client, materialize) depend on neither this package
nor the job runner.
"""

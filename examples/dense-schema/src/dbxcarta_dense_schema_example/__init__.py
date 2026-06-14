"""Standalone tooling for this dbxcarta example.

Per-example dbxcarta config lives in the committed dbxcarta-overlay.env beside
the example, and the bundled questions.json is its only per-example data. The
shared CLI (`dbxcarta ready` / `dbxcarta upload-questions`) operates on those
directly, so this package no longer publishes a preset object.
"""

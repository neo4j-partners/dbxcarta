"""Cluster bootstrap helpers, imported by in-package entrypoints.

Moved here from `scripts/_cluster_bootstrap.py` so the wheel itself carries the
KEY=VALUE-to-environment converter; a Databricks job does not need any
separately uploaded helper file.
"""
from __future__ import annotations

import os
import sys

_PRESET_KEY = "DBXCARTA_PRESET"


def inject_params() -> None:
    """Parse KEY=VALUE argv parameters into os.environ.

    The CLI runner forwards .env variables as positional KEY=VALUE args.
    Uses setdefault so pre-existing environment variables take precedence,
    matching standard 12-factor semantics.

    If `DBXCARTA_PRESET=<module.path:attr>` appears in argv or the existing
    environment, the named preset is resolved and its `env()` overlay is
    applied via `setdefault` before the remaining KEY=VALUE pairs. KEY=VALUE
    args that follow `DBXCARTA_PRESET` in argv therefore override preset
    values, matching CLI overlay semantics.
    """
    remaining: list[str] = []
    preset_spec: str | None = None
    for arg in sys.argv[1:]:
        if "=" in arg and not arg.startswith("-"):
            key, _, value = arg.partition("=")
            if key == _PRESET_KEY and preset_spec is None:
                preset_spec = value
            os.environ.setdefault(key, value)
        else:
            remaining.append(arg)
    sys.argv[1:] = remaining

    if preset_spec is None:
        preset_spec = os.environ.get(_PRESET_KEY) or None
    if preset_spec:
        _overlay_preset_env(preset_spec)


def _overlay_preset_env(spec: str) -> None:
    """Resolve `spec` via preset_loader and overlay its env() onto os.environ.

    Uses setdefault so prior argv-parsed KEY=VALUE values still win, keeping
    the precedence rule "explicit job parameter overrides preset default".
    """
    from dbxcarta.preset_loader import load_preset

    preset = load_preset(spec)
    for key, value in preset.env().items():
        os.environ.setdefault(key, value)

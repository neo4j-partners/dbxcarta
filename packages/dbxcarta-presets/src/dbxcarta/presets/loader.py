"""Resolve a preset object from a `module.path:attr` import spec.

The CLI takes a preset spec as a positional argument, resolves it here, then
calls methods on the returned object. This is the only mechanism dbxcarta uses
to discover presets — there is no central registry and no entry-point
discovery.
"""

from __future__ import annotations

from dbxcarta.core import load_env_overlay
from dbxcarta.presets import Preset


def load_preset(spec: str) -> Preset:
    """Resolve a `package.module:attr` spec to a Preset object.

    Raises ValueError if the spec is malformed, ImportError if the module
    cannot be imported, AttributeError if the attribute does not exist, and
    TypeError if the resolved object does not satisfy the Preset protocol.
    """
    obj = load_env_overlay(spec)
    if not isinstance(obj, Preset):
        raise TypeError(
            f"object at {spec!r} does not satisfy the dbxcarta Preset protocol "
            f"(must implement env() -> dict[str, str])"
        )
    return obj

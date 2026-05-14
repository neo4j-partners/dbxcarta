"""Environment overlay protocol and cluster bootstrap helpers."""
from __future__ import annotations

import importlib
import os
import sys
from typing import Protocol, runtime_checkable

_PRESET_KEY = "DBXCARTA_PRESET"


@runtime_checkable
class EnvOverlay(Protocol):
    """Structural protocol for objects that provide environment overlays."""

    def env(self) -> dict[str, str]:
        """Return environment values to apply with lower precedence."""
        ...


def load_env_overlay(spec: str) -> EnvOverlay:
    """Resolve a `module.path:attr` spec to an EnvOverlay object."""
    module_path, sep, attr = spec.partition(":")
    if not sep or not module_path or not attr:
        raise ValueError(
            f"invalid env overlay spec {spec!r}: expected 'module.path:attr'"
        )
    module = importlib.import_module(module_path)
    obj = getattr(module, attr)
    if not isinstance(obj, EnvOverlay):
        raise TypeError(
            f"object at {spec!r} does not satisfy the dbxcarta EnvOverlay "
            "protocol (must implement env() -> dict[str, str])"
        )
    return obj


def apply_env_overlay(overlay: EnvOverlay) -> None:
    """Apply *overlay* to os.environ without replacing existing values."""
    for key, value in overlay.env().items():
        os.environ.setdefault(key, value)


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
    """Resolve `spec` as an EnvOverlay and apply it to os.environ.

    Uses setdefault so prior argv-parsed KEY=VALUE values still win, keeping
    the precedence rule "explicit job parameter overrides preset default".
    """
    apply_env_overlay(load_env_overlay(spec))

"""Environment overlay protocol and cluster bootstrap helpers."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Protocol, runtime_checkable

_PRESET_KEY = "DBXCARTA_PRESET"
_ENV_FILE_KEY = "DBXCARTA_ENV_FILE"
_ENV_FILE_OPT = "--env-file"
_BASE_ENV_FILE = Path(".env")


class EnvFileError(Exception):
    """A selected env overlay file could not be resolved.

    Raised only for an *explicitly selected* overlay (``--env-file`` or
    ``DBXCARTA_ENV_FILE``). A missing base ``.env`` is not an error: it
    preserves the historical no-overlay behaviour where ``load_dotenv``
    silently no-ops on a missing file.
    """


def resolve_env_files(argv: list[str]) -> tuple[list[Path], list[str]]:
    """Resolve the ordered env files to load and strip the CLI option.

    Returns ``(files, cleaned_argv)`` where *files* is overlay-then-base
    so a sequence of ``load_dotenv(f, override=False)`` calls yields the
    required precedence: real process env over overlay over base. With no
    overlay selected the list is just the base ``.env``, which reproduces
    today's behaviour exactly.

    The overlay is selected from ``--env-file`` on *argv*, else the
    ``DBXCARTA_ENV_FILE`` environment variable. When both are set the
    ``--env-file`` CLI option wins. A selected overlay that does not
    resolve to a readable file is a hard :class:`EnvFileError`; it never
    silently falls back to a base-only run.
    """
    selected, cleaned_argv = _select_overlay(argv)
    if selected is None:
        return [_BASE_ENV_FILE], cleaned_argv

    overlay = Path(selected)
    if not overlay.is_file():
        raise EnvFileError(
            f"selected env overlay {str(overlay)!r} does not exist or is "
            "not a readable file. Fix the path or unset the selection; "
            "the run is not silently continued on the base .env alone."
        )
    return [overlay, _BASE_ENV_FILE], cleaned_argv


def _extract_env_file_option(argv: list[str]) -> tuple[str | None, list[str]]:
    """Pull ``--env-file VALUE`` / ``--env-file=VALUE`` out of *argv*.

    Returns ``(value_or_None, argv_without_the_option)`` so the existing
    per-command argparse parsers never see the option. A bare
    ``--env-file`` with no value is a hard error, since silently ignoring
    a malformed selection reintroduces the wrong-catalog risk.
    """
    value: str | None = None
    remaining: list[str] = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--env-file":
            if i + 1 >= len(argv):
                raise EnvFileError("--env-file requires a path argument")
            value = argv[i + 1]
            i += 2
            continue
        if arg.startswith("--env-file="):
            value = arg.split("=", 1)[1]
            i += 1
            continue
        remaining.append(arg)
        i += 1
    if value is not None and not value:
        raise EnvFileError("--env-file requires a non-empty path argument")
    return value, remaining


def select_overlay_path(argv: list[str] | None = None) -> Path | None:
    """Return the selected overlay path, or ``None``, without side effects.

    Pure selection for constructing the runner: it does not strip argv,
    load files, or check existence. Existence is enforced downstream by
    :func:`resolve_env_files` (the ``verify``/``preset`` loader) and by
    the runner's ``from_env_file`` (the submit path), so a nonexistent
    overlay is still a hard error wherever it is actually consumed.

    Selection precedence matches :func:`resolve_env_files`: ``--env-file``
    on *argv* over ``DBXCARTA_ENV_FILE``. A malformed ``--env-file`` is
    treated as no selection here; the consuming path's own parser raises
    the clean error rather than failing at import.
    """
    args = sys.argv[1:] if argv is None else argv
    try:
        cli_value, _ = _extract_env_file_option(args)
    except EnvFileError:
        cli_value = None
    selected = cli_value
    if selected is None:
        selected = os.environ.get(_ENV_FILE_KEY) or None
    return Path(selected) if selected else None


def load_env_files(files: list[Path]) -> None:
    """Load *files* in order with ``override=False``.

    *files* is overlay-then-base from :func:`resolve_env_files`. With
    ``override=False`` the first writer of each key wins, so loading
    overlay first makes the overlay beat the base, and any value already
    exported in the real process environment beats both. A missing file
    is a silent no-op, matching ``python-dotenv`` semantics and today's
    no-overlay behaviour for the base ``.env``.
    """
    from dotenv import load_dotenv

    for env_file in files:
        load_dotenv(env_file, override=False)


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
    """Resolve `spec` as an EnvOverlay and apply it to os.environ."""
    apply_env_overlay(load_env_overlay(spec))

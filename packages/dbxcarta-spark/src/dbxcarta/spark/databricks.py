"""Databricks identifier and path helpers shared across dbxcarta layers."""

from __future__ import annotations

import os
import re

from databricks.sdk import WorkspaceClient

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")
_VOLUME_SUBPATH_PART_RE = re.compile(r"^[A-Za-z0-9._=-]+$")


def build_workspace_client() -> WorkspaceClient:
    """Build a WorkspaceClient from DATABRICKS_PROFILE or default SDK auth."""
    profile = os.environ.get("DATABRICKS_PROFILE")
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


def validate_identifier(value: str, *, label: str = "identifier") -> str:
    """Validate a single Databricks identifier part.

    Hyphens are accepted because Unity Catalog allows them when quoted.
    Backticks, dots, spaces, and leading digits are rejected so callers can
    safely quote the returned value with backticks.
    """
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid Databricks {label}: {value!r}")
    return value


def split_qualified_name(
    value: str,
    *,
    expected_parts: int | None = None,
    label: str = "qualified name",
) -> list[str]:
    parts = value.split(".")
    if expected_parts is not None and len(parts) != expected_parts:
        raise ValueError(
            f"Invalid Databricks {label}: {value!r}; expected "
            f"{expected_parts} dot-separated parts"
        )
    for part in parts:
        validate_identifier(part, label=f"{label} part")
    return parts


def quote_identifier(value: str) -> str:
    return f"`{validate_identifier(value)}`"


def quote_qualified_name(value: str, *, expected_parts: int | None = None) -> str:
    return ".".join(
        quote_identifier(part)
        for part in split_qualified_name(value, expected_parts=expected_parts)
    )


def validate_uc_volume_subpath(value: str, *, label: str = "UC Volume path") -> str:
    """Validate /Volumes/<catalog>/<schema>/<volume>/<subdir> paths.

    DBxCarta writes run artifacts, staging data, and ledgers below a UC Volume,
    never at the volume root. Requiring a subdirectory prevents accidental
    truncation or file creation attempts at /Volumes/<cat>/<schema>/<volume>.
    """
    parts = value.rstrip("/").lstrip("/").split("/")
    if len(parts) < 5 or parts[0] != "Volumes":
        raise ValueError(
            f"{label} must be /Volumes/<catalog>/<schema>/<volume>/<subdir>, "
            f"got {value!r}"
        )
    for name, part in zip(("catalog", "schema", "volume"), parts[1:4]):
        validate_identifier(part, label=f"volume {name}")
    for part in parts[4:]:
        if not part or part in (".", "..") or not _VOLUME_SUBPATH_PART_RE.match(part):
            raise ValueError(f"{label} contains an invalid path segment: {part!r}")
    return value.rstrip("/")


def validate_serving_endpoint_name(value: str, *, label: str = "serving endpoint") -> str:
    """Validate endpoint names interpolated into ai_query string literals."""
    return validate_identifier(value, label=label)


def uc_volume_parts(value: str) -> list[str]:
    validate_uc_volume_subpath(value)
    return value.rstrip("/").lstrip("/").split("/")


def uc_volume_parent(value: str) -> str:
    parts = uc_volume_parts(value)
    return "/" + "/".join(parts[:-1])

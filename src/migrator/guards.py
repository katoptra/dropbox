from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit


class GuardError(ValueError):
    """The error if a safety guard rejects an external identity or a path."""


def validate_dropbox_base_url(value: str, *, expected_host: str, label: str) -> None:
    try:
        parsed = urlsplit(value)
    except ValueError as exc:
        raise GuardError(f"{label} is not a valid URL") from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname != expected_host
        or parsed.port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path.rstrip("/") != "/2"
    ):
        raise GuardError(
            f"{label} must be https://{expected_host}/2 with no credentials, "
            "query, or fragment"
        )


def validate_dropbox_scope(value: str) -> str:
    if "\x00" in value or "\\" in value or ":" in value:
        raise GuardError("dropbox.root contains an unsafe character")
    if value.startswith("/"):
        raise GuardError("dropbox.root must be relative to the configured namespace")
    parts = value.split("/") if value else []
    if any(part in {"", ".", ".."} for part in parts):
        raise GuardError("dropbox.root contains an unsafe path segment")
    return "/".join(parts)


def dropbox_api_scope(value: str) -> str:
    scope = validate_dropbox_scope(value)
    return f"/{scope}" if scope else ""


def validate_proton_cli_path(value: str, *, label: str = "proton path") -> str:
    if not value.startswith("/") or "\x00" in value:
        raise GuardError(f"{label} must be an absolute CLI path")
    raw_parts = value.split("/")[1:]
    if any(part in {"", ".", ".."} for part in raw_parts):
        raise GuardError(f"{label} contains an unsafe or empty path segment")
    return "/" + "/".join(raw_parts)


def safe_relative_path(value: str, *, label: str = "relative path") -> PurePosixPath:
    if not value or value.startswith("/") or "\x00" in value or "\\" in value:
        raise GuardError(f"{label} is not a safe relative POSIX path")
    raw_parts = value.split("/")
    if any(part in {"", ".", ".."} for part in raw_parts):
        raise GuardError(f"{label} contains an unsafe path segment")
    return PurePosixPath(*raw_parts)


def contained_path(
    root: Path,
    relative: str,
    *,
    label: str = "path",
    allow_leaf_symlink: bool = False,
) -> Path:
    rel = safe_relative_path(relative, label=label)
    resolved_root = root.resolve()
    candidate = resolved_root.joinpath(*rel.parts)
    current = resolved_root
    for part in rel.parts[:-1]:
        current = current / part
        if current.is_symlink():
            raise GuardError(f"{label} traverses a symlinked parent")
        if current.exists():
            resolved = current.resolve()
            try:
                resolved.relative_to(resolved_root)
            except ValueError as exc:
                raise GuardError(f"{label} escapes its configured root") from exc
    if candidate.is_symlink() and not allow_leaf_symlink:
        raise GuardError(f"{label} resolves to a symlink")
    parent = candidate.parent.resolve()
    try:
        parent.relative_to(resolved_root)
    except ValueError as exc:
        raise GuardError(f"{label} escapes its configured root") from exc
    return candidate


def path_is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except (OSError, ValueError):
        return False


def validate_executable(value: str, *, label: str) -> None:
    if not value or "\x00" in value or os.sep in value:
        raise GuardError(f"{label} must be a bare executable name")

from __future__ import annotations

import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, fields
from math import isfinite
from pathlib import Path
from typing import Any, get_type_hints

from .guards import (
    GuardError,
    validate_dropbox_scope,
    validate_executable,
    validate_proton_cli_path,
)


class ConfigError(ValueError):
    """Raised when job configuration is unsafe or malformed."""


def _convert(value: Any, expected: Any, base: Path) -> Any:
    if expected is Path:
        if not isinstance(value, str):
            raise ConfigError("expected a path string")
        path = Path(value).expanduser()
        return path if path.is_absolute() else (base / path).resolve()
    if expected == tuple[str, ...]:
        if not isinstance(value, list):
            raise ConfigError("expected an array of strings")
        if not all(isinstance(item, str) for item in value):
            raise ConfigError("expected an array containing only strings")
        return tuple(value)
    if expected == tuple[Path, ...]:
        if not isinstance(value, list):
            raise ConfigError("expected an array of paths")
        return tuple(_convert(item, Path, base) for item in value)
    if expected is str:
        if not isinstance(value, str):
            raise ConfigError("expected a string")
        return value
    if expected is bool:
        if type(value) is not bool:
            raise ConfigError("expected a boolean")
        return value
    if expected is int:
        if type(value) is not int:
            raise ConfigError("expected an integer")
        return value
    if expected is float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError("expected a number")
        return float(value)
    raise ConfigError(f"unsupported configuration type: {expected!r}")


def _section[T](cls: type[T], data: dict[str, Any], base: Path, name: str) -> T:
    if not isinstance(data, dict):
        raise ConfigError(f"[{name}] must be a table")
    allowed = {field.name for field in fields(cls)}
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ConfigError(f"unknown keys in [{name}]: {', '.join(unknown)}")
    hints = get_type_hints(cls)
    values = {key: _convert(value, hints[key], base) for key, value in data.items()}
    try:
        return cls(**values)
    except TypeError as exc:
        raise ConfigError(f"invalid [{name}] section: {exc}") from exc


def _enum(value: str, allowed: set[str], label: str) -> None:
    if value not in allowed:
        raise ConfigError(
            f"{label} must be one of {', '.join(sorted(allowed))}; got {value!r}"
        )


def _positive(value: Any, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
        or value <= 0
    ):
        raise ConfigError(f"{label} must be greater than zero")


def _nonnegative(value: Any, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
        or value < 0
    ):
        raise ConfigError(f"{label} cannot be negative")


def _positive_int(value: Any, label: str) -> None:
    if type(value) is not int or value < 1:
        raise ConfigError(f"{label} must be a positive integer")


@dataclass(frozen=True)
class Mirror:
    id: str


@dataclass(frozen=True)
class Dropbox:
    expected_account_id: str = ""
    root: str = ""
    api_base_url: str = "https://api.dropboxapi.com/2"
    content_base_url: str = "https://content.dropboxapi.com/2"
    token_url: str = "https://api.dropboxapi.com/oauth2/token"
    timeout_seconds: float = 180
    page_limit: int = 2000
    minimum_call_interval_seconds: float = 0.1
    max_attempts: int = 12
    initial_backoff_seconds: float = 2
    maximum_backoff_seconds: float = 300
    download_workers: int = 4


@dataclass(frozen=True)
class Proton:
    expected_destination_uid: str = ""
    executable: str = "proton-drive"
    destination: str = "/my-files/Dropbox"
    list_max_attempts: int = 8
    # A mutation that fails on anything but authentication is tried again after a
    # backoff: an upload repeated over identical bytes is skipped by the CLI and the
    # summary still accounts for every item, so a retry can only cost time.
    mutation_max_attempts: int = 3
    initial_backoff_seconds: float = 3
    maximum_backoff_seconds: float = 120
    command_timeout_seconds: float = 300
    transfer_timeout_seconds: float = 3600
    walk_workers: int = 8


@dataclass(frozen=True)
class Budget:
    batch_gb: float = 4
    batch_files: int = 2000
    max_file_gb: float = 12
    run_budget_minutes: int = 165
    ceiling_gb: float = 4000
    disk_headroom_gb: float = 1
    listing_floor_ratio: float = 0.5

    @property
    def batch_bytes(self) -> int:
        return round(self.batch_gb * 1024**3)

    @property
    def max_file_bytes(self) -> int:
        return round(self.max_file_gb * 1024**3)

    @property
    def ceiling_bytes(self) -> int:
        return round(self.ceiling_gb * 1024**3)

    @property
    def headroom_bytes(self) -> int:
        return round(self.disk_headroom_gb * 1024**3)


@dataclass(frozen=True)
class Reconcile:
    weekday: int = 0  # the first run that starts on this UTC weekday walks Proton


@dataclass(frozen=True)
class Config:
    mirror: Mirror
    dropbox: Dropbox
    proton: Proton
    budget: Budget
    reconcile: Reconcile
    source_file: Path
    source_sha256: str


_MIRROR_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
SECTIONS = {"mirror", "dropbox", "proton", "budget", "reconcile"}
# Account-specific values live in the vault (op.env) and override the file, so
# the committed configuration names no account.
ENV_OVERRIDES = {
    "MIRROR_DROPBOX_ACCOUNT_ID": ("dropbox", "expected_account_id"),
    "MIRROR_PROTON_DESTINATION": ("proton", "destination"),
    "MIRROR_PROTON_DESTINATION_UID": ("proton", "expected_destination_uid"),
}


def load_config(path: str | Path, environ: Mapping[str, str] | None = None) -> Config:
    from .hashing import sha256_file

    env = os.environ if environ is None else environ
    source = Path(path).expanduser().resolve()
    try:
        data = tomllib.loads(source.read_bytes().decode("utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read configuration {source}: {exc}") from exc
    unknown = sorted(set(data) - SECTIONS)
    if unknown:
        raise ConfigError(f"unknown top-level tables: {', '.join(unknown)}")
    for name, (section, key) in ENV_OVERRIDES.items():
        if env.get(name, ""):
            data.setdefault(section, {})[key] = env[name]
    base = source.parent
    cfg = Config(
        mirror=_section(Mirror, data.get("mirror", {}), base, "mirror"),
        dropbox=_section(Dropbox, data.get("dropbox", {}), base, "dropbox"),
        proton=_section(Proton, data.get("proton", {}), base, "proton"),
        budget=_section(Budget, data.get("budget", {}), base, "budget"),
        reconcile=_section(Reconcile, data.get("reconcile", {}), base, "reconcile"),
        source_file=source,
        source_sha256=sha256_file(source),
    )
    validate_config(cfg)
    return cfg


def validate_config(cfg: Config) -> None:
    if not _MIRROR_ID.fullmatch(cfg.mirror.id):
        raise ConfigError("mirror.id must be alphanumeric with dot, underscore, hyphen")
    if not re.fullmatch(r"dbid:[A-Za-z0-9_-]+", cfg.dropbox.expected_account_id):
        raise ConfigError(
            "MIRROR_DROPBOX_ACCOUNT_ID (or dropbox.expected_account_id) "
            "must be a full dbid: identifier"
        )
    if len(cfg.proton.expected_destination_uid.strip()) < 8:
        raise ConfigError(
            "MIRROR_PROTON_DESTINATION_UID (or proton.expected_destination_uid) "
            "must be at least 8 characters"
        )
    try:
        validate_executable(cfg.proton.executable, label="proton.executable")
        validate_dropbox_scope(cfg.dropbox.root)
        validate_proton_cli_path(cfg.proton.destination, label="proton.destination")
    except GuardError as exc:
        raise ConfigError(str(exc)) from exc
    if (
        type(cfg.dropbox.page_limit) is not int
        or not 1 <= cfg.dropbox.page_limit <= 2000
    ):
        raise ConfigError("dropbox.page_limit must be in 1..2000")
    _positive(cfg.dropbox.timeout_seconds, "dropbox.timeout_seconds")
    _nonnegative(
        cfg.dropbox.minimum_call_interval_seconds,
        "dropbox.minimum_call_interval_seconds",
    )
    _positive_int(cfg.dropbox.max_attempts, "dropbox.max_attempts")
    _nonnegative(cfg.dropbox.initial_backoff_seconds, "dropbox.initial_backoff_seconds")
    _positive(cfg.dropbox.maximum_backoff_seconds, "dropbox.maximum_backoff_seconds")
    _positive_int(cfg.dropbox.download_workers, "dropbox.download_workers")
    _positive_int(cfg.proton.list_max_attempts, "proton.list_max_attempts")
    _positive_int(cfg.proton.mutation_max_attempts, "proton.mutation_max_attempts")
    _nonnegative(cfg.proton.initial_backoff_seconds, "proton.initial_backoff_seconds")
    _positive(cfg.proton.maximum_backoff_seconds, "proton.maximum_backoff_seconds")
    _positive(cfg.proton.command_timeout_seconds, "proton.command_timeout_seconds")
    _positive(cfg.proton.transfer_timeout_seconds, "proton.transfer_timeout_seconds")
    if (
        type(cfg.proton.walk_workers) is not int
        or not 1 <= cfg.proton.walk_workers <= 32
    ):
        raise ConfigError("proton.walk_workers must be 1 to 32")
    _positive(cfg.budget.batch_gb, "budget.batch_gb")
    _positive_int(cfg.budget.batch_files, "budget.batch_files")
    _positive(cfg.budget.max_file_gb, "budget.max_file_gb")
    _positive_int(cfg.budget.run_budget_minutes, "budget.run_budget_minutes")
    _positive(cfg.budget.ceiling_gb, "budget.ceiling_gb")
    _nonnegative(cfg.budget.disk_headroom_gb, "budget.disk_headroom_gb")
    if not 0 < cfg.budget.listing_floor_ratio <= 1:
        raise ConfigError("budget.listing_floor_ratio must be in (0, 1]")
    if type(cfg.reconcile.weekday) is not int or not 0 <= cfg.reconcile.weekday <= 6:
        raise ConfigError("reconcile.weekday must be 0 (Monday) to 6 (Sunday)")

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .hashing import sha256_file
from .logging import Event, utc_now

SCHEMA_VERSION = 1


SCHEMA = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS schema_info (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS migrations (
    migration_id TEXT PRIMARY KEY,
    config_path TEXT NOT NULL,
    config_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    final_status TEXT,
    source_account_id TEXT,
    source_account_display TEXT,
    source_root_namespace_id TEXT,
    destination_account_id TEXT,
    source_account_observed_at TEXT,
    destination_account_observed_at TEXT,
    destination_root_uid TEXT
);

CREATE TABLE IF NOT EXISTS phase_runs (
    id INTEGER PRIMARY KEY,
    migration_id TEXT NOT NULL REFERENCES migrations(migration_id),
    phase_number INTEGER NOT NULL,
    phase_name TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    apply_mode INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    inputs_json TEXT NOT NULL,
    outputs_json TEXT,
    tool_versions_json TEXT NOT NULL,
    command_parameters_json TEXT,
    warning_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    error_summary TEXT,
    UNIQUE(migration_id, phase_number, attempt)
);

CREATE TABLE IF NOT EXISTS artifacts (
    id INTEGER PRIMARY KEY,
    phase_run_id INTEGER NOT NULL REFERENCES phase_runs(id),
    role TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    row_count INTEGER,
    created_at TEXT NOT NULL,
    UNIQUE(phase_run_id, relative_path)
);

CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY,
    phase_run_id INTEGER REFERENCES phase_runs(id),
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    safe_argv_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    exit_code INTEGER,
    attempt INTEGER NOT NULL,
    response_category TEXT,
    stdout_artifact TEXT,
    stderr_artifact TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    phase_run_id INTEGER REFERENCES phase_runs(id),
    timestamp TEXT NOT NULL,
    level TEXT NOT NULL,
    phase TEXT NOT NULL,
    operation TEXT NOT NULL,
    object_identifier TEXT,
    retry_count INTEGER,
    provider_category TEXT,
    message TEXT NOT NULL,
    safe_raw_error TEXT,
    fields_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS identity_observations (
    id INTEGER PRIMARY KEY,
    phase_run_id INTEGER REFERENCES phase_runs(id),
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    expected_identifier TEXT NOT NULL,
    observed_identifier TEXT NOT NULL,
    matched INTEGER NOT NULL,
    observed_at TEXT NOT NULL,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dropbox_inventory_runs (
    id INTEGER PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    account_id TEXT NOT NULL,
    root_namespace_id TEXT NOT NULL,
    purpose TEXT NOT NULL DEFAULT 'baseline',
    cursor TEXT,
    has_more INTEGER,
    page_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dropbox_pages (
    inventory_id INTEGER NOT NULL REFERENCES dropbox_inventory_runs(id),
    page_number INTEGER NOT NULL,
    cursor_in_sha256 TEXT,
    cursor_out TEXT,
    entry_count INTEGER NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY(inventory_id, page_number)
);

CREATE TABLE IF NOT EXISTS dropbox_objects (
    inventory_id INTEGER NOT NULL REFERENCES dropbox_inventory_runs(id),
    object_key TEXT NOT NULL,
    tag TEXT NOT NULL,
    name TEXT NOT NULL,
    path_display TEXT,
    path_lower TEXT,
    comparison_key TEXT,
    dropbox_id TEXT,
    revision TEXT,
    size INTEGER,
    client_modified TEXT,
    server_modified TEXT,
    content_hash TEXT,
    is_downloadable INTEGER NOT NULL,
    symlink_target TEXT,
    export_info_json TEXT,
    raw_json TEXT NOT NULL,
    first_page INTEGER NOT NULL,
    last_page INTEGER NOT NULL,
    PRIMARY KEY(inventory_id, object_key)
);
CREATE INDEX IF NOT EXISTS idx_dropbox_objects_compare
ON dropbox_objects(inventory_id, comparison_key);
-- delta and report join mirror_objects to the listing by path; without this every
-- mirrored row scanned the whole listing, and delta grew from minutes to hours.
CREATE INDEX IF NOT EXISTS idx_dropbox_objects_path
ON dropbox_objects(inventory_id, path_lower);

CREATE TABLE IF NOT EXISTS proton_snapshots (
    id INTEGER PRIMARY KEY,
    purpose TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    destination_root TEXT NOT NULL,
    cli_version TEXT NOT NULL,
    destination_uid TEXT,
    identity_observed_at TEXT
);

CREATE TABLE IF NOT EXISTS proton_folders (
    snapshot_id INTEGER NOT NULL REFERENCES proton_snapshots(id),
    uid TEXT NOT NULL,
    parent_uid TEXT,
    visible_segments_json TEXT NOT NULL,
    cli_path TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error_category TEXT,
    PRIMARY KEY(snapshot_id, uid)
);

CREATE TABLE IF NOT EXISTS proton_nodes (
    snapshot_id INTEGER NOT NULL REFERENCES proton_snapshots(id),
    uid TEXT NOT NULL,
    parent_uid TEXT NOT NULL,
    visible_segments_json TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    cli_path TEXT,
    comparison_key TEXT NOT NULL,
    name TEXT NOT NULL,
    node_type TEXT NOT NULL,
    creation_time TEXT,
    modification_time TEXT,
    claimed_size INTEGER,
    claimed_modification_time TEXT,
    sha1 TEXT,
    sha1_verified INTEGER,
    raw_json TEXT NOT NULL,
    PRIMARY KEY(snapshot_id, uid)
);
CREATE INDEX IF NOT EXISTS idx_proton_nodes_compare
ON proton_nodes(snapshot_id, comparison_key);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY,
    started_at TEXT NOT NULL,
    start_epoch INTEGER NOT NULL,
    hour_utc INTEGER NOT NULL,
    weekday INTEGER NOT NULL,
    budget_minutes INTEGER NOT NULL,
    host TEXT NOT NULL,
    reconcile INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    chain INTEGER NOT NULL DEFAULT 0,
    inventory_id INTEGER,
    planned_batches INTEGER,
    remaining_batches INTEGER,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS mirror_objects (
    path_lower TEXT PRIMARY KEY,
    path_display TEXT NOT NULL,
    size INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    sha1 TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    proton_uid TEXT,
    run_id INTEGER NOT NULL,
    mirrored_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delta_changed (
    run_id INTEGER NOT NULL REFERENCES runs(id),
    path_lower TEXT NOT NULL,
    path_display TEXT NOT NULL,
    size INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    PRIMARY KEY(run_id, path_lower)
);

CREATE TABLE IF NOT EXISTS delta_deleted (
    run_id INTEGER NOT NULL REFERENCES runs(id),
    path_lower TEXT NOT NULL,
    path_display TEXT NOT NULL,
    proton_uid TEXT,
    PRIMARY KEY(run_id, path_lower)
);

CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    number INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    file_count INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    details_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(run_id, number)
);

CREATE TABLE IF NOT EXISTS batch_items (
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    path_lower TEXT NOT NULL,
    path_display TEXT NOT NULL,
    size INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    sha1 TEXT,
    sha256 TEXT,
    details_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(batch_id, path_lower)
);

CREATE TABLE IF NOT EXISTS deletions (
    run_id INTEGER NOT NULL REFERENCES runs(id),
    path_lower TEXT NOT NULL,
    path_display TEXT NOT NULL,
    proton_uid TEXT,
    status TEXT NOT NULL,
    trashed_at TEXT,
    PRIMARY KEY(run_id, path_lower)
);
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class State:
    RUN_COLUMNS = frozenset(
        {
            "status",
            "chain",
            "inventory_id",
            "planned_batches",
            "remaining_batches",
            "completed_at",
            "reconcile",
        }
    )

    def __init__(self, path: Path, migration_id: str) -> None:
        self.path = path
        self.migration_id = migration_id
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        # NORMAL in WAL mode survives a process crash; only an OS failure could lose
        # the last commits, and the canonical state is the R2 object fetched at the
        # start of every run, so a runner's disk never outlives what it holds.
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.executescript(SCHEMA)
        self.connection.execute(
            "INSERT OR IGNORE INTO schema_info(version, applied_at) VALUES (?, ?)",
            (SCHEMA_VERSION, utc_now()),
        )
        self.connection.commit()
        self.current_phase_run_id: int | None = None

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection:
            yield self.connection

    def initialize_migration(self, config_path: Path, config_sha256: str) -> None:
        now = utc_now()
        with self.connection:
            existing = self.connection.execute(
                "SELECT config_sha256 FROM migrations WHERE migration_id=?",
                (self.migration_id,),
            ).fetchone()
            if existing and existing["config_sha256"] != config_sha256:
                self.connection.execute(
                    "UPDATE migrations SET config_sha256=?, config_path=? WHERE migration_id=?",
                    (config_sha256, str(config_path), self.migration_id),
                )
            self.connection.execute(
                """
                INSERT OR IGNORE INTO migrations(
                    migration_id, config_path, config_sha256, created_at, started_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    self.migration_id,
                    str(config_path),
                    config_sha256,
                    now,
                    now,
                ),
            )

    def start_phase(
        self,
        number: int,
        name: str,
        *,
        apply: bool,
        inputs: dict[str, Any],
        tool_versions: dict[str, Any] | None = None,
        command_parameters: dict[str, Any] | None = None,
    ) -> int:
        now = utc_now()
        with self.connection:
            self.connection.execute(
                """
                UPDATE phase_runs
                SET status='INTERRUPTED', completed_at=?
                WHERE migration_id=? AND phase_number=? AND status='RUNNING'
                """,
                (now, self.migration_id, number),
            )
            row = self.connection.execute(
                """
                SELECT COALESCE(MAX(attempt), 0) + 1 AS attempt
                FROM phase_runs
                WHERE migration_id=? AND phase_number=?
                """,
                (self.migration_id, number),
            ).fetchone()
            cursor = self.connection.execute(
                """
                INSERT INTO phase_runs(
                    migration_id, phase_number, phase_name, attempt, status,
                    apply_mode, started_at, inputs_json, tool_versions_json,
                    command_parameters_json
                ) VALUES (?, ?, ?, ?, 'RUNNING', ?, ?, ?, ?, ?)
                """,
                (
                    self.migration_id,
                    number,
                    name,
                    int(row["attempt"]),
                    int(apply),
                    now,
                    _json(inputs),
                    _json(tool_versions or {}),
                    _json(command_parameters or {}),
                ),
            )
        self.current_phase_run_id = int(cursor.lastrowid)
        return self.current_phase_run_id

    def complete_phase(
        self,
        phase_run_id: int,
        status: str,
        *,
        outputs: dict[str, Any] | None = None,
        error_summary: str | None = None,
    ) -> None:
        counts = self.connection.execute(
            """
            SELECT
                SUM(CASE WHEN level='WARNING' THEN 1 ELSE 0 END) AS warnings,
                SUM(CASE WHEN level='ERROR' THEN 1 ELSE 0 END) AS errors
            FROM events WHERE phase_run_id=?
            """,
            (phase_run_id,),
        ).fetchone()
        with self.connection:
            self.connection.execute(
                """
                UPDATE phase_runs SET
                    status=?, completed_at=?, outputs_json=?,
                    warning_count=?, error_count=?, error_summary=?
                WHERE id=?
                """,
                (
                    status,
                    utc_now(),
                    _json(outputs or {}),
                    int(counts["warnings"] or 0),
                    int(counts["errors"] or 0),
                    error_summary,
                    phase_run_id,
                ),
            )
        if self.current_phase_run_id == phase_run_id:
            self.current_phase_run_id = None

    def latest_phase(self, number: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT * FROM phase_runs
            WHERE migration_id=? AND phase_number=?
            ORDER BY attempt DESC LIMIT 1
            """,
            (self.migration_id, number),
        ).fetchone()

    def phase_passed(self, number: int) -> bool:
        row = self.latest_phase(number)
        return bool(row and row["status"] == "PASS")

    def record_artifact(
        self,
        phase_run_id: int,
        role: str,
        path: Path,
        run_root: Path,
        *,
        row_count: int | None = None,
    ) -> None:
        stat = path.stat()
        try:
            relative = path.relative_to(run_root).as_posix()
        except ValueError:
            relative = str(path)
        with self.connection:
            self.connection.execute(
                """
                INSERT OR REPLACE INTO artifacts(
                    phase_run_id, role, relative_path, sha256, size,
                    row_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    phase_run_id,
                    role,
                    relative,
                    sha256_file(path),
                    stat.st_size,
                    row_count,
                    utc_now(),
                ),
            )

    def record_event(self, event: Event) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO events(
                    phase_run_id, timestamp, level, phase, operation,
                    object_identifier, retry_count, provider_category,
                    message, safe_raw_error, fields_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.current_phase_run_id,
                    event.timestamp,
                    event.level,
                    event.phase,
                    event.operation,
                    event.object_identifier,
                    event.retry_count,
                    event.provider_category,
                    event.message,
                    event.safe_raw_error,
                    _json(event.fields),
                ),
            )

    def record_identity_observation(
        self,
        provider: str,
        operation: str,
        expected_identifier: str,
        observed_identifier: str,
        *,
        matched: bool,
        details: dict[str, Any] | None = None,
    ) -> str:
        observed_at = utc_now()
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO identity_observations(
                    phase_run_id, provider, operation, expected_identifier,
                    observed_identifier, matched, observed_at, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.current_phase_run_id,
                    provider,
                    operation,
                    expected_identifier,
                    observed_identifier,
                    int(matched),
                    observed_at,
                    _json(details or {}),
                ),
            )
        return observed_at

    def record_command_start(
        self,
        provider: str,
        operation: str,
        safe_argv: list[str],
        attempt: int,
        *,
        started_at: str | None = None,
    ) -> int:
        """`started_at` lets a caller that ran the command on another thread record
        when it really began."""
        with self.connection:
            cursor = self.connection.execute(
                """
                INSERT INTO commands(
                    phase_run_id, provider, operation, safe_argv_json,
                    started_at, attempt
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    self.current_phase_run_id,
                    provider,
                    operation,
                    _json(safe_argv),
                    started_at or utc_now(),
                    attempt,
                ),
            )
        return int(cursor.lastrowid)

    def record_command_end(
        self,
        command_id: int,
        exit_code: int,
        category: str,
        *,
        stdout_artifact: str | None = None,
        stderr_artifact: str | None = None,
        completed_at: str | None = None,
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                UPDATE commands
                SET completed_at=?, exit_code=?, response_category=?,
                    stdout_artifact=?, stderr_artifact=?
                WHERE id=?
                """,
                (
                    completed_at or utc_now(),
                    exit_code,
                    category,
                    stdout_artifact,
                    stderr_artifact,
                    command_id,
                ),
            )

    def latest_completed_id(
        self,
        table: str,
        status: str = "COMPLETE",
        *,
        purpose: str | None = None,
    ) -> int:
        allowed = {
            "dropbox_inventory_runs",
            "proton_snapshots",
        }
        if table not in allowed:
            raise ValueError(f"unsupported state table: {table}")
        if purpose is not None:
            row = self.connection.execute(
                f"""
                SELECT id FROM {table}
                WHERE status=? AND purpose=? ORDER BY id DESC LIMIT 1
                """,
                (status, purpose),
            ).fetchone()
        else:
            row = self.connection.execute(
                f"SELECT id FROM {table} WHERE status=? ORDER BY id DESC LIMIT 1",
                (status,),
            ).fetchone()
        if not row:
            raise RuntimeError(f"no completed {table} record")
        return int(row["id"])

    def start_run(
        self,
        *,
        start_epoch: int,
        hour_utc: int,
        weekday: int,
        budget_minutes: int,
        host: str,
        reconcile: bool,
    ) -> int:
        now = utc_now()
        with self.connection:
            self.connection.execute(
                "UPDATE runs SET status='INTERRUPTED', completed_at=? WHERE status='RUNNING'",
                (now,),
            )
            cursor = self.connection.execute(
                """
                INSERT INTO runs(started_at, start_epoch, hour_utc, weekday, budget_minutes,
                                 host, reconcile, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'RUNNING')
                """,
                (
                    now,
                    start_epoch,
                    hour_utc,
                    weekday,
                    budget_minutes,
                    host,
                    int(reconcile),
                ),
            )
        return int(cursor.lastrowid)

    def current_run(self) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM runs WHERE status='RUNNING' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            raise RuntimeError("no run is in progress; the state step starts one")
        return row

    def update_run(self, run_id: int, **columns: Any) -> None:
        unknown = set(columns) - self.RUN_COLUMNS
        if unknown:
            raise ValueError(f"unknown runs columns: {sorted(unknown)}")
        assignments = ", ".join(f"{name}=?" for name in columns)
        with self.connection:
            self.connection.execute(
                f"UPDATE runs SET {assignments} WHERE id=?", (*columns.values(), run_id)
            )

    def finish_run(self, run_id: int, status: str) -> None:
        self.update_run(run_id, status=status, completed_at=utc_now())

    def mirror_totals(self) -> tuple[int, int]:
        row = self.connection.execute(
            "SELECT COUNT(*) AS files, COALESCE(SUM(size), 0) AS bytes FROM mirror_objects"
        ).fetchone()
        return int(row["files"]), int(row["bytes"])

    def snapshot_to(self, target: Path) -> None:
        if target.exists():
            target.unlink()
        self.connection.commit()  # VACUUM refuses to run inside an open transaction
        self.connection.execute("VACUUM INTO ?", (str(target),))

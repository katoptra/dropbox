from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

from . import session, statefile
from .config import load_config
from .env import Runtime
from .paths import WorkPaths
from .state import State
from .store import Store


def _paths(runtime: Runtime) -> WorkPaths:
    paths = WorkPaths.from_runtime(runtime)
    paths.ensure()
    return paths


def clock(runtime: Runtime, args: list[str]) -> int:
    if runtime.run_epoch is None:
        raise ValueError("MIRROR_RUN_EPOCH must be set by the Taskfile")
    paths = _paths(runtime)
    shutil.rmtree(paths.staging, ignore_errors=True)
    paths.staging.mkdir()
    for stale in (paths.report, paths.chain, paths.walked):
        stale.unlink(missing_ok=True)
    started = datetime.fromtimestamp(runtime.run_epoch, UTC)
    stamp = {
        "start_epoch": runtime.run_epoch,
        "hour_utc": started.hour,
        "weekday": started.weekday(),
    }
    paths.clock.write_text(json.dumps(stamp) + "\n", encoding="utf-8")
    print(f"clock: run started {started.isoformat()}")
    return 0


def read_clock(paths: WorkPaths) -> dict[str, int]:
    return json.loads(paths.clock.read_text(encoding="utf-8"))


def session_restore(runtime: Runtime, args: list[str]) -> int:
    paths = _paths(runtime)
    session.restore(runtime, paths, Store(runtime, paths))
    print("session: restored")
    return 0


def _fetch_state(runtime: Runtime, paths: WorkPaths) -> str:
    paths.state_db.unlink(missing_ok=True)
    for suffix in ("-wal", "-shm"):
        Path(str(paths.state_db) + suffix).unlink(missing_ok=True)
    return statefile.fetch(runtime, paths, Store(runtime, paths))


def state(runtime: Runtime, args: list[str]) -> int:
    cfg = load_config(runtime.config_path)
    paths = _paths(runtime)
    outcome = _fetch_state(runtime, paths)
    stamp = read_clock(paths)
    db = State(paths.state_db, cfg.mirror.id)
    try:
        db.initialize_migration(cfg.source_file, cfg.source_sha256)
        run_id = db.start_run(
            start_epoch=stamp["start_epoch"],
            hour_utc=stamp["hour_utc"],
            weekday=stamp["weekday"],
            budget_minutes=runtime.budget_override or cfg.budget.run_budget_minutes,
            host=runtime.host,
            # lib's toolbox `due` decided before this command, by the age of the last
            # complete walk.
            reconcile=paths.reconcile.exists(),
        )
        files, size = db.mirror_totals()
    finally:
        db.close()
    print(f"state: {outcome}; run {run_id}; mirrored files={files} bytes={size}")
    return 0


def status(runtime: Runtime, args: list[str]) -> int:
    cfg = load_config(runtime.config_path)
    paths = _paths(runtime)
    _fetch_state(runtime, paths)  # reads R2 directly and starts no run row
    db = State(paths.state_db, cfg.mirror.id)
    try:
        files, size = db.mirror_totals()
        run = db.connection.execute(
            "SELECT * FROM runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        phases = db.connection.execute(
            "SELECT phase_name, status, started_at, completed_at FROM phase_runs "
            "WHERE id IN (SELECT MAX(id) FROM phase_runs GROUP BY phase_number) ORDER BY phase_number"
        ).fetchall()
        figures = db.connection.execute(
            "SELECT fields_json FROM events WHERE operation='figures' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        db.close()
    print(
        json.dumps(
            {
                "mirrored_files": files,
                "mirrored_bytes": size,
                "last_run": dict(run) if run else None,
                "phases": [dict(row) for row in phases],
                "last_figures": json.loads(figures["fields_json"]) if figures else None,
            },
            indent=2,
        )
    )
    return 0


def state_push(runtime: Runtime, args: list[str]) -> int:
    cfg = load_config(runtime.config_path)
    paths = _paths(runtime)
    db = State(paths.state_db, cfg.mirror.id)
    try:
        label = args[0] if args else f"manual-{runtime.run_epoch or 0}"
        statefile.push(db, runtime, paths, Store(runtime, paths), label=label)
    finally:
        db.close()
    print(f"state-push: {label}")
    return 0


def state_rollback(runtime: Runtime, args: list[str]) -> int:
    paths = _paths(runtime)
    store = Store(runtime, paths)
    if not args:
        for key in store.list(statefile.HISTORY_PREFIX):
            print(key)
        print("state-rollback: pass one of the keys above")
        return 1
    statefile.rollback(store, args[0])
    print(f"state-rollback: {args[0]} is now the canonical state")
    return 0


def session_seal(runtime: Runtime, args: list[str]) -> int:
    if not args:
        raise ValueError("session-seal needs the laptop PROTON_DRIVE_CACHE_DIR path")
    paths = _paths(runtime)
    session.seal(runtime, paths, Store(runtime, paths), Path(args[0]))
    print("session-seal: uploaded")
    return 0

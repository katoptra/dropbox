from __future__ import annotations

import json

import pytest
from conftest import FakeStore

from migrator import commands
from migrator.paths import WorkPaths
from migrator.state import State


def test_clock_writes_stamp_and_clears_outputs(runtime_factory, tmp_path):
    runtime = runtime_factory(
        tmp_path, MIRROR_RUN_EPOCH="1700000000"
    )  # 2023-11-14 22:13 UTC, Tuesday
    paths = WorkPaths.from_runtime(runtime)
    paths.ensure()
    (paths.staging / "junk").write_text("x", encoding="utf-8")
    paths.chain.write_text("", encoding="utf-8")
    paths.walked.write_text("", encoding="utf-8")
    assert commands.clock(runtime, []) == 0
    stamp = json.loads(paths.clock.read_text(encoding="utf-8"))
    assert stamp == {"start_epoch": 1700000000, "hour_utc": 22, "weekday": 1}
    assert not (paths.staging / "junk").exists() and not paths.chain.exists()
    assert not paths.walked.exists()


def test_clock_requires_epoch(runtime_factory, tmp_path):
    runtime = runtime_factory(tmp_path, MIRROR_RUN_EPOCH="")
    with pytest.raises(ValueError, match="MIRROR_RUN_EPOCH"):
        commands.clock(runtime, [])


def test_state_fresh_starts_run_with_budget_override(
    state_context, plain_crypt, monkeypatch
):
    cfg, paths, state, _, runtime = state_context
    state.close()
    paths.state_db.unlink()
    commands.clock(runtime, [])
    monkeypatch.setattr(commands, "Store", lambda runtime, paths: FakeStore())
    monkeypatch.setattr(commands, "load_config", lambda _: cfg)
    runtime = runtime_factory_override(runtime, budget_override=30)
    assert commands.state(runtime, []) == 0
    fresh = State(paths.state_db, cfg.mirror.id)
    run = fresh.current_run()
    assert run["budget_minutes"] == 30 and run["start_epoch"] == 1700000000
    assert run["reconcile"] == 0
    fresh.close()


def test_state_records_the_toolbox_reconcile_decision(
    state_context, plain_crypt, monkeypatch
):
    cfg, paths, state, _, runtime = state_context
    state.close()
    paths.state_db.unlink()
    commands.clock(runtime, [])
    paths.reconcile.write_text("", encoding="utf-8")
    monkeypatch.setattr(commands, "Store", lambda runtime, paths: FakeStore())
    monkeypatch.setattr(commands, "load_config", lambda _: cfg)
    assert commands.state(runtime, []) == 0
    fresh = State(paths.state_db, cfg.mirror.id)
    assert fresh.current_run()["reconcile"] == 1
    fresh.close()


def runtime_factory_override(runtime, **changes):
    from dataclasses import replace

    return replace(runtime, **changes)


def test_status_prints_counts_only(state_context, capsys, monkeypatch):
    cfg, _paths, state, _, runtime = state_context
    monkeypatch.setattr(commands, "load_config", lambda _: cfg)
    monkeypatch.setattr(commands, "_fetch_state", lambda runtime, paths: "restored")
    state.start_run(
        start_epoch=1,
        hour_utc=0,
        weekday=0,
        budget_minutes=1,
        host="t",
        reconcile=False,
    )
    with state.connection:
        state.connection.execute(
            """INSERT INTO mirror_objects(path_lower, path_display, size, content_hash, sha1, sha256,
               run_id, mirrored_at) VALUES ('/taxes/x.pdf','/Taxes/x.pdf',9,'h','s','s',1,'now')"""
        )
    state.close()
    assert commands.status(runtime, []) == 0
    out = capsys.readouterr().out
    assert '"mirrored_files": 1' in out and "Taxes" not in out

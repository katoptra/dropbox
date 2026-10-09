from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from migrator.phases import batch, p40_batches
from migrator.phases.base import PhaseContext, PhaseError


def _ctx(state_context, apply=True, budget_minutes=10):
    cfg, paths, state, logger, runtime = state_context
    run_id = state.start_run(
        start_epoch=1000,
        hour_utc=0,
        weekday=0,
        budget_minutes=budget_minutes,
        host="t",
        reconcile=False,
    )
    phase_run_id = state.start_phase(40, "40_batches", apply=apply, inputs={})
    return PhaseContext(cfg, paths, state, logger, apply, phase_run_id, run_id, runtime)


def _planned(ctx, count):
    with ctx.state.connection:
        for number in range(1, count + 1):
            cursor = ctx.state.connection.execute(
                "INSERT INTO batches(run_id, number, bytes, file_count, status) VALUES (?, ?, 10, 1, 'PLANNED')",
                (ctx.run_id, number),
            )
            ctx.state.connection.execute(
                """INSERT INTO batch_items(batch_id, path_lower, path_display, size, content_hash, status)
                   VALUES (?, ?, ?, 10, 'h', 'PLANNED')""",
                (cursor.lastrowid, f"/f{number}", f"/f{number}"),
            )
    ctx.state.update_run(ctx.run_id, planned_batches=count)


def _fake_steps(monkeypatch, seconds_per_batch, failing=()):
    clock = {"t": 1000.0}
    monkeypatch.setattr(p40_batches, "now", lambda: clock["t"])
    monkeypatch.setattr(p40_batches, "Store", lambda runtime, paths: object())
    monkeypatch.setattr(
        p40_batches,
        "ProtonCLIProvider",
        lambda *a, **k: type(
            "P", (), {"root_uid": lambda self, phase: "uid-destination"}
        )(),
    )
    monkeypatch.setattr(p40_batches, "access_token", lambda cfg, runtime: "tok")
    monkeypatch.setattr(
        p40_batches, "DropboxAPIProvider", lambda *a, **k: SimpleNamespace(token="")
    )
    monkeypatch.setattr(p40_batches.session, "writeback", lambda *a: False)
    for name in ("fetch", "verify", "upload", "confirm"):
        monkeypatch.setattr(batch, name, lambda ctx, *a, _n=name: {_n: 1})

    def checkpoint(ctx, store, batch_id):
        clock["t"] += seconds_per_batch
        failed = 1 if batch_id in failing else 0
        with ctx.state.connection:
            ctx.state.connection.execute(
                "UPDATE batches SET status=? WHERE id=?",
                ("FAILED" if failed else "CHECKPOINTED", batch_id),
            )
        return {"checkpointed": 1 - failed, "failed": failed}

    monkeypatch.setattr(batch, "checkpoint", checkpoint)
    return clock


def test_should_start_rule():
    assert p40_batches.should_start(elapsed=0, longest=0, budget=600, completed=0)
    assert p40_batches.should_start(
        elapsed=5000, longest=0, budget=600, completed=0
    )  # the first batch always runs
    assert p40_batches.should_start(elapsed=400, longest=100, budget=600, completed=1)
    assert not p40_batches.should_start(
        elapsed=550, longest=100, budget=600, completed=1
    )


def test_all_batches_fit_no_chain(state_context, monkeypatch):
    ctx = _ctx(state_context, budget_minutes=10)
    _planned(ctx, 3)
    _fake_steps(monkeypatch, seconds_per_batch=60)
    result = p40_batches.run(ctx)
    assert result.outputs["completed"] == 3 and result.outputs["remaining"] == 0
    assert result.outputs["chain"] is False
    run = ctx.state.current_run()
    assert run["remaining_batches"] == 0 and run["chain"] == 0
    details = json.loads(
        ctx.state.connection.execute(
            "SELECT details_json FROM batches WHERE number=1"
        ).fetchone()[0]
    )
    assert details["fetch"] == 1 and "seconds" in details


def test_budget_stops_and_chains(state_context, monkeypatch):
    ctx = _ctx(state_context, budget_minutes=5)
    _planned(ctx, 5)
    _fake_steps(monkeypatch, seconds_per_batch=120)
    result = p40_batches.run(ctx)
    # 0s -> batch1 (120s) -> 120+120<300 batch2 -> 240+120>300 stop
    assert result.outputs["completed"] == 2 and result.outputs["remaining"] == 3
    assert result.outputs["chain"] is True
    assert ctx.state.current_run()["chain"] == 1


def test_failed_batch_stops_run_without_chain(state_context, monkeypatch):
    ctx = _ctx(state_context)
    _planned(ctx, 3)
    first = ctx.state.connection.execute(
        "SELECT id FROM batches WHERE number=2"
    ).fetchone()[0]
    _fake_steps(monkeypatch, seconds_per_batch=1, failing={first})
    with pytest.raises(PhaseError, match="failed"):
        p40_batches.run(ctx)
    run = ctx.state.current_run()
    assert run["remaining_batches"] == 2 and run["chain"] == 0


def test_no_progress_does_not_chain(state_context, monkeypatch):
    ctx = _ctx(state_context)
    _planned(ctx, 2)
    first = ctx.state.connection.execute(
        "SELECT id FROM batches WHERE number=1"
    ).fetchone()[0]
    _fake_steps(monkeypatch, seconds_per_batch=1, failing={first})
    with pytest.raises(PhaseError):
        p40_batches.run(ctx)
    assert ctx.state.current_run()["chain"] == 0


def test_without_apply_is_planned_only(state_context, monkeypatch):
    ctx = _ctx(state_context, apply=False)
    _planned(ctx, 2)
    result = p40_batches.run(ctx)
    assert result.status == "PLANNED" and result.outputs["planned"] == 2
    assert ctx.state.current_run()["remaining_batches"] is None


def test_token_is_minted_fresh_for_every_batch_fetch(state_context, monkeypatch):
    """Dropbox accepts an access token for four hours, but a run can continue for six
    hours. On 2026-09-06, a token from the start of the phase expired during the run,
    and batch 31 of 46 got an error."""
    ctx = _ctx(state_context, budget_minutes=100)
    _planned(ctx, 3)
    _fake_steps(monkeypatch, seconds_per_batch=60)
    minted = iter(("tok1", "tok2", "tok3"))
    monkeypatch.setattr(p40_batches, "access_token", lambda cfg, runtime: next(minted))
    seen: list[str] = []
    monkeypatch.setattr(
        batch, "fetch", lambda ctx, dropbox, bid: seen.append(dropbox.token) or {}
    )
    p40_batches.run(ctx)
    assert seen == ["tok1", "tok2", "tok3"]

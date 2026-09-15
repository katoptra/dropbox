from __future__ import annotations

import pytest

from migrator.config import Budget
from migrator.phases import p30_plan
from migrator.phases.base import PhaseContext, PhaseError


def _ctx(state_context, budget=None):
    cfg, paths, state, logger, runtime = state_context
    if budget is not None:
        from dataclasses import replace

        cfg = replace(cfg, budget=budget)
    run_id = state.start_run(
        start_epoch=1,
        hour_utc=0,
        weekday=0,
        budget_minutes=1,
        host="t",
        reconcile=False,
    )
    phase_run_id = state.start_phase(30, "30_plan", apply=False, inputs={})
    return PhaseContext(cfg, paths, state, logger, False, phase_run_id, run_id, runtime)


def _changed(ctx, rows):
    with ctx.state.connection:
        ctx.state.connection.executemany(
            "INSERT INTO delta_changed(run_id, path_lower, path_display, size, content_hash) VALUES (?, ?, ?, ?, ?)",
            [(ctx.run_id, p.lower(), p, s, "h") for p, s in rows],
        )
    with ctx.state.connection:
        cursor = ctx.state.connection.execute(
            """INSERT INTO dropbox_inventory_runs(started_at, completed_at, status, account_id, root_namespace_id, purpose)
               VALUES ('now','now','COMPLETE','dbid:test-account','ns','run:1')"""
        )
        inventory_id = int(cursor.lastrowid)
        ctx.state.connection.executemany(
            """INSERT INTO dropbox_objects(inventory_id, object_key, tag, name, path_display, path_lower, comparison_key,
               size, content_hash, is_downloadable, raw_json, first_page, last_page)
               VALUES (?, ?, 'file', ?, ?, ?, ?, ?, 'h', 1, '{}', 1, 1)""",
            [
                (
                    inventory_id,
                    p.lower(),
                    p.rsplit("/", 1)[-1],
                    p,
                    p.lower(),
                    p.lower(),
                    s,
                )
                for p, s in rows
            ],
        )
    ctx.state.update_run(ctx.run_id, inventory_id=inventory_id)


def test_pack_is_greedy_in_path_order_with_oversized_alone():
    rows = [
        {"path_lower": "/a", "size": 6},
        {"path_lower": "/b", "size": 5},
        {"path_lower": "/c", "size": 20},
        {"path_lower": "/d", "size": 1},
    ]
    batches = p30_plan.pack(rows, batch_bytes=10, batch_files=5000)
    assert [[r["path_lower"] for r in b] for b in batches] == [
        ["/a"],
        ["/b"],
        ["/c"],
        ["/d"],
    ]


def test_pack_caps_files_per_batch():
    rows = [{"path_lower": f"/{i}", "size": 1} for i in range(5)]
    assert [len(b) for b in p30_plan.pack(rows, batch_bytes=100, batch_files=2)] == [
        2,
        2,
        1,
    ]


def test_plan_writes_batches_and_items(state_context, monkeypatch):
    ctx = _ctx(
        state_context, Budget(batch_gb=10 / 1024**3, ceiling_gb=1, disk_headroom_gb=0)
    )
    _changed(ctx, [("/A/one", 6), ("/A/two", 5), ("/big", 20)])
    monkeypatch.setattr(
        p30_plan.shutil, "disk_usage", lambda _: type("U", (), {"free": 10**9})()
    )
    result = p30_plan.run(ctx)
    assert (
        result.outputs["batches"] == 3
        and result.outputs["files"] == 3
        and result.outputs["largest_file"] == 20
    )
    rows = ctx.state.connection.execute(
        "SELECT number, bytes, file_count, status FROM batches WHERE run_id=? ORDER BY number",
        (ctx.run_id,),
    ).fetchall()
    assert [tuple(r) for r in rows] == [
        (1, 6, 1, "PLANNED"),
        (2, 5, 1, "PLANNED"),
        (3, 20, 1, "PLANNED"),
    ]
    assert ctx.state.current_run()["planned_batches"] == 3
    items = ctx.state.connection.execute("SELECT status FROM batch_items").fetchall()
    assert {r["status"] for r in items} == {"PLANNED"}


def test_plan_refuses_tree_over_ceiling(state_context, monkeypatch):
    ctx = _ctx(state_context, Budget(ceiling_gb=1 / 1024**3))
    _changed(ctx, [("/a", 2)])
    monkeypatch.setattr(
        p30_plan.shutil, "disk_usage", lambda _: type("U", (), {"free": 10**9})()
    )
    with pytest.raises(PhaseError, match="CEILING"):
        p30_plan.run(ctx)


def test_plan_refuses_batch_disk_cannot_hold_staging(state_context, monkeypatch):
    ctx = _ctx(state_context, Budget(batch_gb=1, ceiling_gb=1, disk_headroom_gb=0))
    _changed(ctx, [("/a", 300), ("/b", 300)])
    monkeypatch.setattr(
        p30_plan.shutil, "disk_usage", lambda _: type("U", (), {"free": 500})()
    )
    with pytest.raises(PhaseError, match="disk"):
        p30_plan.run(ctx)


def test_plan_prunes_checkpointed_items_and_keeps_failed_ones(
    state_context, monkeypatch
):
    ctx = _ctx(state_context, Budget(ceiling_gb=1, disk_headroom_gb=0))
    _changed(ctx, [("/a", 2)])
    with ctx.state.connection:
        ctx.state.connection.execute(
            "INSERT INTO batches(id, run_id, number, bytes, file_count, status) VALUES (9, ?, 99, 2, 2, 'CHECKPOINTED')",
            (ctx.run_id,),
        )
        ctx.state.connection.executemany(
            "INSERT INTO batch_items(batch_id, path_lower, path_display, size, content_hash, status) VALUES (9, ?, ?, 1, 'h', ?)",
            [("/old", "/old", "CHECKPOINTED"), ("/bad", "/bad", "CONFIRM_FAILED")],
        )
    monkeypatch.setattr(
        p30_plan.shutil, "disk_usage", lambda _: type("U", (), {"free": 10**9})()
    )
    p30_plan.run(ctx)
    left = {
        r[0]
        for r in ctx.state.connection.execute(
            "SELECT path_lower FROM batch_items WHERE batch_id=9"
        )
    }
    assert left == {"/bad"}


def test_plan_is_idempotent_within_a_run(state_context, monkeypatch):
    ctx = _ctx(state_context, Budget(ceiling_gb=1, disk_headroom_gb=0))
    _changed(ctx, [("/a", 2)])
    monkeypatch.setattr(
        p30_plan.shutil, "disk_usage", lambda _: type("U", (), {"free": 10**9})()
    )
    p30_plan.run(ctx)
    p30_plan.run(ctx)
    assert (
        ctx.state.connection.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 1
    )


def test_plan_leaves_out_files_the_disk_or_cap_cannot_hold(state_context, monkeypatch):
    # cap 12 bytes from config; disk allows 100 - 10 headroom = 90; the config cap wins
    ctx = _ctx(
        state_context,
        Budget(
            batch_gb=10 / 1024**3,
            max_file_gb=12 / 1024**3,
            ceiling_gb=1,
            disk_headroom_gb=10 / 1024**3,
        ),
    )
    _changed(ctx, [("/a", 6), ("/big", 12), ("/huge", 13), ("/vast", 50)])
    monkeypatch.setattr(
        p30_plan.shutil, "disk_usage", lambda _: type("U", (), {"free": 100})()
    )
    result = p30_plan.run(ctx)
    assert result.outputs["oversized_files"] == 2
    assert result.outputs["oversized_bytes"] == 63
    assert result.outputs["largest_file"] == 12
    rows = ctx.state.connection.execute(
        "SELECT bytes, file_count FROM batches WHERE run_id=? ORDER BY number",
        (ctx.run_id,),
    ).fetchall()
    assert [tuple(r) for r in rows] == [(6, 1), (12, 1)]
    # a tighter disk lowers the cap below the config value
    monkeypatch.setattr(
        p30_plan.shutil, "disk_usage", lambda _: type("U", (), {"free": 21})()
    )
    result = p30_plan.run(ctx)
    assert result.outputs["file_cap_bytes"] == 11
    assert result.outputs["oversized_files"] == 3

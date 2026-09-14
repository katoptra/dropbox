from __future__ import annotations

from conftest import FakeProton, FakeStore, proton_node

from migrator.phases import p50_trash
from migrator.phases.base import PhaseContext


def _ctx(state_context, apply=True, remaining=0):
    cfg, paths, state, logger, runtime = state_context
    run_id = state.start_run(
        start_epoch=1,
        hour_utc=0,
        weekday=0,
        budget_minutes=1,
        host="t",
        reconcile=False,
    )
    state.update_run(run_id, planned_batches=1, remaining_batches=remaining)
    phase_run_id = state.start_phase(50, "50_trash", apply=apply, inputs={})
    return PhaseContext(cfg, paths, state, logger, apply, phase_run_id, run_id, runtime)


def _deleted(ctx, displays, uid=None):
    with ctx.state.connection:
        for display in displays:
            ctx.state.connection.execute(
                """INSERT INTO mirror_objects(path_lower, path_display, size, content_hash, sha1, sha256,
                   proton_uid, run_id, mirrored_at) VALUES (?, ?, 1, 'h', 's', 's', ?, 0, 'now')""",
                (display.lower(), display, uid),
            )
            ctx.state.connection.execute(
                "INSERT INTO delta_deleted(run_id, path_lower, path_display, proton_uid) VALUES (?, ?, ?, ?)",
                (ctx.run_id, display.lower(), display, uid),
            )


def _wire(monkeypatch, proton):
    monkeypatch.setattr(p50_trash, "Store", lambda runtime, paths: FakeStore())
    monkeypatch.setattr(p50_trash, "ProtonCLIProvider", lambda *a, **k: proton)
    monkeypatch.setattr(p50_trash.session, "writeback", lambda *a: False)
    # The run row starts at epoch 1 with a one-minute budget; a clock stuck at 1 keeps
    # every folder inside it.
    monkeypatch.setattr(p50_trash, "now", lambda: 1.0, raising=False)


def _folders(names):
    return {
        f"/my-files/Dropbox/{name}": [proton_node(f"u{name}", f"{name}.txt", 1, "s")]
        for name in names
    }


def test_trash_groups_by_parent_and_drops_state_rows(
    state_context, monkeypatch, plain_crypt
):
    ctx = _ctx(state_context)
    _deleted(
        ctx, ["/Docs/a.txt", "/Docs/b.txt", "/Other/c.txt", "/Docs/never-there.txt"]
    )
    proton = FakeProton(
        {
            "/my-files/Dropbox/Docs": [
                proton_node("ua", "a.txt", 1, "s"),
                proton_node("ub", "b.txt", 1, "s"),
            ],
            "/my-files/Dropbox/Other": [proton_node("uc", "c.txt", 1, "s")],
        }
    )
    proton.trashed = []
    proton.trash = lambda paths, phase: proton.trashed.append(sorted(paths))
    _wire(monkeypatch, proton)
    result = p50_trash.run(ctx)
    assert result.outputs == {
        "planned": 4,
        "trashed": 3,
        "not_found": 1,
        "listing_failed": 0,
        "folders": 2,
        "remaining": 0,
        "chain": False,
    }
    assert proton.trashed == [
        ["/my-files/Dropbox/Docs/a.txt", "/my-files/Dropbox/Docs/b.txt"],
        ["/my-files/Dropbox/Other/c.txt"],
    ]
    assert (
        ctx.state.connection.execute("SELECT COUNT(*) FROM mirror_objects").fetchone()[
            0
        ]
        == 0
    )
    statuses = {
        r["path_lower"]: r["status"]
        for r in ctx.state.connection.execute("SELECT * FROM deletions")
    }
    assert (
        statuses["/docs/never-there.txt"] == "NOT_FOUND"
        and statuses["/docs/a.txt"] == "TRASHED"
    )


def test_trash_picks_the_duplicate_matching_the_recorded_uid(
    state_context, monkeypatch, plain_crypt
):
    ctx = _ctx(state_context)
    _deleted(ctx, ["/Docs/dup.txt"], uid="u-mine")
    proton = FakeProton(
        {
            "/my-files/Dropbox/Docs": [
                proton_node("u-other", "dup.txt", 1, "s"),
                proton_node("u-mine", "dup.txt", 1, "s"),
            ]
        }
    )
    proton.trashed = []
    proton.trash = lambda paths, phase: proton.trashed.extend(paths)
    _wire(monkeypatch, proton)
    result = p50_trash.run(ctx)
    assert result.outputs["trashed"] == 1
    assert proton.trashed == ["/my-files/Dropbox/Docs/u-mine"]
    assert (
        ctx.state.connection.execute("SELECT proton_uid FROM deletions").fetchone()[0]
        == "u-mine"
    )


def test_trash_keeps_state_rows_when_a_parent_listing_fails(
    state_context, monkeypatch, plain_crypt
):
    ctx = _ctx(state_context)
    _deleted(ctx, ["/Docs/a.txt"])
    proton = FakeProton({}, fail_list=["/my-files/Dropbox/Docs"])
    proton.trash = lambda paths, phase: (_ for _ in ()).throw(
        AssertionError("nothing to trash")
    )
    _wire(monkeypatch, proton)
    result = p50_trash.run(ctx)
    assert result.outputs == {
        "planned": 1,
        "trashed": 0,
        "not_found": 0,
        "listing_failed": 1,
        "folders": 0,
        "remaining": 0,
        "chain": False,
    }
    assert (
        ctx.state.connection.execute("SELECT COUNT(*) FROM mirror_objects").fetchone()[
            0
        ]
        == 1
    )
    assert (
        ctx.state.connection.execute("SELECT status FROM deletions").fetchone()[0]
        == "LISTING_FAILED"
    )


def test_trash_skips_while_batches_remain(state_context, monkeypatch, plain_crypt):
    ctx = _ctx(state_context, remaining=2)
    _deleted(ctx, ["/Docs/a.txt"])
    _wire(monkeypatch, FakeProton({}))
    result = p50_trash.run(ctx)
    assert result.outputs == {"skipped": "batches remain", "planned": 1}
    assert (
        ctx.state.connection.execute("SELECT COUNT(*) FROM mirror_objects").fetchone()[
            0
        ]
        == 1
    )


def test_trash_without_apply_is_planned(state_context, monkeypatch, plain_crypt):
    ctx = _ctx(state_context, apply=False)
    _deleted(ctx, ["/Docs/a.txt"])
    result = p50_trash.run(ctx)
    assert result.status == "PLANNED" and result.outputs["planned"] == 1


def test_trash_stops_at_the_budget_and_chains(state_context, monkeypatch, plain_crypt):
    ctx = _ctx(state_context)
    _deleted(ctx, ["/Docs/Docs.txt", "/Other/Other.txt"])
    proton = FakeProton(_folders(["Docs", "Other"]))
    proton.trashed = []
    proton.trash = lambda paths, phase: proton.trashed.extend(paths)
    _wire(monkeypatch, proton)
    clock = iter(range(1, 10_000, 40))  # 40 s per look at the clock, 60 s budget
    monkeypatch.setattr(p50_trash, "now", lambda: float(next(clock)), raising=False)
    result = p50_trash.run(ctx)
    assert proton.trashed == ["/my-files/Dropbox/Docs/Docs.txt"]
    assert (
        result.outputs["folders"] == 1
        and result.outputs["remaining"] == 1
        and result.outputs["chain"] is True
    )
    assert ctx.state.current_run()["chain"] == 1
    assert [
        r[0]
        for r in ctx.state.connection.execute("SELECT path_lower FROM mirror_objects")
    ] == ["/other/other.txt"]


def test_trash_checkpoints_every_few_folders(state_context, monkeypatch, plain_crypt):
    ctx = _ctx(state_context)
    _deleted(ctx, ["/A/A.txt", "/B/B.txt", "/C/C.txt"])
    proton = FakeProton(_folders(["A", "B", "C"]))
    proton.trash = lambda paths, phase: None
    _wire(monkeypatch, proton)
    pushes = []
    monkeypatch.setattr(
        p50_trash.statefile,
        "push",
        lambda state, runtime, paths, store, label: pushes.append(label),
    )
    monkeypatch.setattr(p50_trash, "CHECKPOINT_EVERY", 2, raising=False)
    result = p50_trash.run(ctx)
    assert result.outputs["folders"] == 3
    assert pushes == ["1-trash-2", "1-trash"]

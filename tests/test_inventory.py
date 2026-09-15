from __future__ import annotations

from conftest import seed_api_inventory

from migrator.phases import p10_inventory
from migrator.phases.base import PhaseContext


def _ctx(state_context):
    cfg, paths, state, logger, runtime = state_context
    run_id = state.start_run(
        start_epoch=1,
        hour_utc=0,
        weekday=0,
        budget_minutes=1,
        host="t",
        reconcile=False,
    )
    phase_run_id = state.start_phase(10, "10_inventory", apply=False, inputs={})
    return PhaseContext(cfg, paths, state, logger, False, phase_run_id, run_id, runtime)


def test_inventory_records_counts_and_run_link(state_context, monkeypatch):
    ctx = _ctx(state_context)
    rows = [
        ("/A/one.txt", 3, "h1", 1, "file"),
        ("/A", None, None, 1, "folder"),
        ("/notes.paper", 0, None, 0, "file"),
    ]
    inventory_id = seed_api_inventory(ctx.state, "run:1", rows)
    monkeypatch.setattr(p10_inventory, "access_token", lambda cfg, runtime: "tok")
    monkeypatch.setattr(
        p10_inventory.DropboxAPIProvider,
        "inventory",
        lambda self, purpose, reuse_complete=True: inventory_id,
    )
    result = p10_inventory.run(ctx)
    assert result.outputs == {
        "inventory_id": inventory_id,
        "files": 1,
        "folders": 1,
        "bytes": 3,
        "non_downloadable": 1,
        "unhashed": 0,
        "recased": 0,
        "pruned_inventories": 0,
        "pruned_history": 0,
    }
    assert ctx.state.current_run()["inventory_id"] == inventory_id


def test_unhashed_files_become_non_downloadable(state_context, monkeypatch):
    ctx = _ctx(state_context)
    inventory_id = seed_api_inventory(
        ctx.state,
        "run:1",
        [("/cloud.gdoc", 0, None, 1, "file"), ("/real.txt", 2, "h", 1, "file")],
    )
    monkeypatch.setattr(p10_inventory, "access_token", lambda cfg, runtime: "tok")
    monkeypatch.setattr(
        p10_inventory.DropboxAPIProvider,
        "inventory",
        lambda self, purpose, reuse_complete=True: inventory_id,
    )
    outputs = p10_inventory.run(ctx).outputs
    assert (
        outputs["files"] == 1
        and outputs["non_downloadable"] == 1
        and outputs["unhashed"] == 1
    )


def test_prune_keeps_newest_inventories(state_context):
    _, _, state, _, _ = state_context
    ids = [
        seed_api_inventory(state, f"run:{n}", [("/a.txt", 1, "h", 1, "file")])
        for n in range(4)
    ]
    assert p10_inventory.prune_inventories(state.connection, keep=2) == 2
    left = {
        r["inventory_id"]
        for r in state.connection.execute("SELECT inventory_id FROM dropbox_objects")
    }
    assert left == set(ids[2:])
    assert p10_inventory.prune_inventories(state.connection) == 1  # default keeps one
    left = {
        r["inventory_id"]
        for r in state.connection.execute("SELECT inventory_id FROM dropbox_objects")
    }
    assert left == {ids[3]}


def test_prune_history_drops_old_log_rows_but_keeps_reconcile_figures(
    state_context,
):
    _, _, state, _, _ = state_context
    old, new = "2020-01-01T00:00:00Z", "2999-01-01T00:00:00Z"
    with state.connection:
        state.connection.executemany(
            "INSERT INTO events(timestamp, level, phase, operation, message, fields_json) VALUES (?, 'INFO', ?, ?, 'm', '{}')",
            [
                (old, "40_batches", "step"),
                (old, "60_reconcile", "figures"),
                (new, "40_batches", "step"),
            ],
        )
        state.connection.executemany(
            "INSERT INTO commands(provider, operation, safe_argv_json, started_at, attempt) VALUES ('proton', 'list', '[]', ?, 1)",
            [(old,), (new,)],
        )
    assert p10_inventory.prune_history(state.connection) == 2
    events = state.connection.execute(
        "SELECT timestamp, operation FROM events ORDER BY id"
    ).fetchall()
    assert [tuple(r) for r in events] == [(old, "figures"), (new, "step")]
    assert state.connection.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 1


def test_phase_registers_access_token_for_redaction(state_context, monkeypatch):
    ctx = _ctx(state_context)
    inventory_id = seed_api_inventory(
        ctx.state, "run:1", [("/a.txt", 1, "h", 1, "file")]
    )
    token = "live-dropbox-token-xyz"
    monkeypatch.setattr(p10_inventory, "access_token", lambda cfg, runtime: token)
    monkeypatch.setattr(
        p10_inventory.DropboxAPIProvider,
        "inventory",
        lambda self, purpose, reuse_complete=True: inventory_id,
    )
    p10_inventory.run(ctx)
    ctx.logger.info("10_inventory", "probe", f"leaked token {token}")
    human = (ctx.paths.logs / "migrate.log").read_text(encoding="utf-8")
    assert token not in human
    assert "[REDACTED]" in human


def test_inventory_recases_paths_from_folder_names(state_context, monkeypatch):
    ctx = _ctx(state_context)
    rows = [
        ("/Apps", None, None, 1, "folder"),
        ("/apps/Outlook", None, None, 1, "folder"),
        ("/apps/outlook/Report.pdf", 3, "h1", 1, "file"),
        ("/Apps/Outlook/other.pdf", 2, "h2", 1, "file"),
    ]
    inventory_id = seed_api_inventory(ctx.state, "run:1", rows)
    monkeypatch.setattr(p10_inventory, "access_token", lambda cfg, runtime: "tok")
    monkeypatch.setattr(
        p10_inventory.DropboxAPIProvider,
        "inventory",
        lambda self, purpose, reuse_complete=True: inventory_id,
    )
    result = p10_inventory.run(ctx)
    assert result.outputs["recased"] == 2
    displays = {
        r["path_lower"]: r["path_display"]
        for r in ctx.state.connection.execute(
            "SELECT path_lower, path_display FROM dropbox_objects WHERE inventory_id=?",
            (inventory_id,),
        )
    }
    assert displays == {
        "/apps": "/Apps",
        "/apps/outlook": "/Apps/Outlook",
        "/apps/outlook/report.pdf": "/Apps/Outlook/Report.pdf",
        "/apps/outlook/other.pdf": "/Apps/Outlook/other.pdf",
    }
    # a second pass finds nothing left to rewrite
    assert p10_inventory.recase_display_paths(ctx.state.connection, inventory_id) == 0

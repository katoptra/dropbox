from __future__ import annotations

import json

from conftest import FakeStore, seed_api_inventory

from migrator.filesystem import comparison_key
from migrator.phases import p60_reconcile
from migrator.phases.base import PhaseContext


def _ctx(state_context, reconcile=True, remaining=0, start_epoch=1, budget_minutes=1):
    cfg, paths, state, logger, runtime = state_context
    run_id = state.start_run(
        start_epoch=start_epoch,
        hour_utc=0,
        weekday=0,
        budget_minutes=budget_minutes,
        host="t",
        reconcile=reconcile,
    )
    state.update_run(run_id, planned_batches=0, remaining_batches=remaining)
    phase_run_id = state.start_phase(60, "60_reconcile", apply=True, inputs={})
    return PhaseContext(cfg, paths, state, logger, True, phase_run_id, run_id, runtime)


def _snapshot(state, nodes, status="COMPLETE"):
    """nodes: (relative, uid, size) or (relative, uid, size, sha1)."""
    with state.connection:
        cursor = state.connection.execute(
            """INSERT INTO proton_snapshots(purpose, started_at, completed_at, status, destination_root, cli_version)
               VALUES ('reconcile', 'now', 'now', ?, '/my-files/Dropbox', '0.8.0')""",
            (status,),
        )
        snapshot_id = int(cursor.lastrowid)
        for entry in nodes:
            relative, uid, size, *rest = entry
            sha1 = rest[0] if rest else None
            state.connection.execute(
                """INSERT INTO proton_nodes(snapshot_id, uid, parent_uid, visible_segments_json, relative_path, cli_path,
                   comparison_key, name, node_type, claimed_size, sha1, raw_json)
                   VALUES (?, ?, '__ROOT__', '[]', ?, ?, ?, ?, 'file', ?, ?, '{}')""",
                (
                    snapshot_id,
                    uid,
                    relative,
                    "/my-files/Dropbox/" + relative,
                    comparison_key(relative),
                    relative.rsplit("/", 1)[-1],
                    size,
                    sha1,
                ),
            )
    return snapshot_id


def _folders(state, snapshot_id, folders):
    """folders: (relative, uid, parent_uid) folder nodes of a snapshot."""
    with state.connection:
        for relative, uid, parent_uid in folders:
            state.connection.execute(
                """INSERT INTO proton_nodes(snapshot_id, uid, parent_uid, visible_segments_json, relative_path, cli_path,
                   comparison_key, name, node_type, raw_json)
                   VALUES (?, ?, ?, '[]', ?, ?, ?, ?, 'folder', '{}')""",
                (
                    snapshot_id,
                    uid,
                    parent_uid,
                    relative,
                    "/my-files/Dropbox/" + relative,
                    comparison_key(relative),
                    relative.rsplit("/", 1)[-1],
                ),
            )


def _mirror(state, rows):
    """rows: (display, size, uid) or (display, size, uid, sha1)."""
    with state.connection:
        for entry in rows:
            display, size, uid, *rest = entry
            sha1 = rest[0] if rest else "s"
            state.connection.execute(
                """INSERT INTO mirror_objects(path_lower, path_display, size, content_hash, sha1, sha256, proton_uid,
                   run_id, mirrored_at) VALUES (?, ?, ?, 'h', ?, 's', ?, 0, 'now')""",
                (display.lower(), display, size, sha1, uid),
            )


def _figures_event(state):
    row = state.connection.execute(
        "SELECT fields_json FROM events WHERE phase='60_reconcile' AND operation='figures'"
    ).fetchone()
    return json.loads(row["fields_json"])


def test_reconcile_drops_missing_missized_and_sha1_mismatched(
    state_context, monkeypatch, plain_crypt
):
    ctx = _ctx(state_context)
    inventory_id = seed_api_inventory(
        ctx.state,
        "run:1",
        [("/Keep/ok.txt", 3, "h", 1, "file"), ("/Keep/pending.txt", 7, "h", 1, "file")],
    )
    ctx.state.update_run(ctx.run_id, inventory_id=inventory_id)
    _mirror(
        ctx.state,
        [
            ("/Keep/ok.txt", 3, None, "sha-ok"),
            ("/Keep/lost.txt", 2, "u-lost", "sha-lost"),
            ("/Keep/bad.txt", 5, "u-bad", "sha-bad"),
            ("/Keep/hash.txt", 4, "u-hash", "sha-mirror"),
        ],
    )
    _snapshot(
        ctx.state, [("Old/x", "u-old", 1)]
    )  # last week's walk; pruned by this run
    snapshot_id = _snapshot(
        ctx.state,
        [
            ("Keep/ok.txt", "u-ok", 3, "sha-ok"),
            ("Keep/bad.txt", "u-bad", 99, "sha-bad"),
            ("Keep/pending.txt", "u-p", 7),
            ("Keep/hash.txt", "u-hash", 4, "sha-proton"),
            ("Stray/x.bin", "u-stray", 1),
        ],
    )
    trashed = []
    walked = []
    fake = type(
        "P",
        (),
        {
            "root_uid": lambda self, phase: "uid-destination",
            "inventory": lambda self, purpose, phase, reuse_complete=True, deadline=None: (
                walked.append((purpose, reuse_complete, deadline)) or snapshot_id
            ),
            "trash": lambda self, paths, phase: trashed.extend(paths),
        },
    )()
    monkeypatch.setattr(p60_reconcile, "ProtonCLIProvider", lambda *a, **k: fake)
    monkeypatch.setattr(p60_reconcile, "Store", lambda runtime, paths: FakeStore())
    monkeypatch.setattr(p60_reconcile.session, "writeback", lambda *a: False)
    result = p60_reconcile.run(ctx)
    # A stable purpose with reuse_complete=False resumes a killed walk across runs and
    # still refuses last week's COMPLETE one; the deadline leaves ten minutes for the
    # rest of the run.
    assert walked == [("reconcile", False, 1 + 1 * 60 - 600)]
    assert result.outputs["dropped"] == 3
    assert result.outputs["sha1_mismatch"] == 1
    assert result.outputs["matched"] == 1
    assert result.outputs["strays_trashed"] == 1
    assert result.outputs["uid_refreshed"] == 1
    assert trashed == ["/my-files/Dropbox/Stray/x.bin"]
    left = {
        r["path_lower"]: r["proton_uid"]
        for r in ctx.state.connection.execute("SELECT * FROM mirror_objects")
    }
    assert left == {"/keep/ok.txt": "u-ok"}
    assert (
        ctx.state.connection.execute(
            "SELECT COUNT(*) FROM proton_snapshots"
        ).fetchone()[0]
        == 1
    )
    assert ctx.paths.walked.exists()
    fields = _figures_event(ctx.state)
    assert fields["complete"] == 1
    assert fields["snapshot_id"] == snapshot_id
    assert fields["matched"] == 1
    assert fields["dropped"] == 3
    assert fields["uid_refreshed"] == 1
    assert fields["strays_trashed"] == 1
    assert fields["sha1_mismatch"] == 1


def test_reconcile_matches_a_digest_proton_reports_in_upper_case(
    state_context, monkeypatch, plain_crypt
):
    ctx = _ctx(state_context)
    inventory_id = seed_api_inventory(
        ctx.state, "run:1", [("/Keep/ok.txt", 3, "h", 1, "file")]
    )
    ctx.state.update_run(ctx.run_id, inventory_id=inventory_id)
    digest = "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"
    _mirror(ctx.state, [("/Keep/ok.txt", 3, "u-ok", digest)])
    snapshot_id = _snapshot(ctx.state, [("Keep/ok.txt", "u-ok", 3, digest.upper())])
    trashed = []
    fake = type(
        "P",
        (),
        {
            "root_uid": lambda self, phase: "uid-destination",
            "inventory": lambda self, purpose, phase, reuse_complete=True, deadline=None: (
                snapshot_id
            ),
            "trash": lambda self, paths, phase: trashed.extend(paths),
        },
    )()
    monkeypatch.setattr(p60_reconcile, "ProtonCLIProvider", lambda *a, **k: fake)
    monkeypatch.setattr(p60_reconcile, "Store", lambda runtime, paths: FakeStore())
    monkeypatch.setattr(p60_reconcile.session, "writeback", lambda *a: False)
    result = p60_reconcile.run(ctx)
    assert result.outputs["matched"] == 1
    assert result.outputs["dropped"] == 0
    assert result.outputs["sha1_mismatch"] == 0
    assert trashed == []


def test_reconcile_partial_walk_pushes_state_and_touches_nothing(
    state_context, monkeypatch, plain_crypt
):
    ctx = _ctx(state_context)
    inventory_id = seed_api_inventory(ctx.state, "run:1", [])
    ctx.state.update_run(ctx.run_id, inventory_id=inventory_id)
    _mirror(ctx.state, [("/Keep/ok.txt", 3, "u-ok")])
    with ctx.state.connection:
        cursor = ctx.state.connection.execute(
            """INSERT INTO proton_snapshots(purpose, started_at, status, destination_root, cli_version)
               VALUES ('reconcile', 'now', 'RUNNING', '/my-files/Dropbox', '0.8.0')"""
        )
        snapshot_id = int(cursor.lastrowid)
        ctx.state.connection.execute(
            """INSERT INTO proton_folders(snapshot_id, uid, parent_uid, visible_segments_json, cli_path, status)
               VALUES (?, '__ROOT__', NULL, '[]', '/my-files/Dropbox', 'COMPLETE')""",
            (snapshot_id,),
        )
        ctx.state.connection.execute(
            """INSERT INTO proton_folders(snapshot_id, uid, parent_uid, visible_segments_json, cli_path, status)
               VALUES (?, 'u-a', '__ROOT__', '["A"]', '/my-files/Dropbox/A', 'PENDING')""",
            (snapshot_id,),
        )

    def _refuse_trash(self, paths, phase):
        raise AssertionError("a partial walk must not trash anything")

    fake = type(
        "P",
        (),
        {
            "root_uid": lambda self, phase: "uid-destination",
            "inventory": lambda self, purpose, phase, reuse_complete=True, deadline=None: (
                snapshot_id
            ),
            "trash": _refuse_trash,
        },
    )()
    pushed = []
    monkeypatch.setattr(p60_reconcile, "ProtonCLIProvider", lambda *a, **k: fake)
    monkeypatch.setattr(p60_reconcile, "Store", lambda runtime, paths: FakeStore())
    monkeypatch.setattr(p60_reconcile.session, "writeback", lambda *a: False)
    monkeypatch.setattr(
        p60_reconcile.statefile,
        "push",
        lambda state, runtime, paths, store, label: pushed.append(label),
    )
    result = p60_reconcile.run(ctx)
    assert result.status == "PASS"
    assert result.outputs == {"partial": 1}
    assert pushed == ["1-reconcile"]
    assert not ctx.paths.walked.exists()
    left = {
        r["path_lower"]
        for r in ctx.state.connection.execute("SELECT * FROM mirror_objects")
    }
    assert left == {"/keep/ok.txt"}
    fields = _figures_event(ctx.state)
    assert fields["complete"] == 0
    assert fields["snapshot_id"] == snapshot_id
    assert fields["folders_listed"] == 1
    assert fields["folders_pending"] == 1
    assert fields["proton_files"] == 0
    assert fields["dropped"] == 0
    assert fields["strays_trashed"] == 0
    assert fields["sha1_mismatch"] == 0


def test_reconcile_skips_when_not_scheduled(state_context, monkeypatch, plain_crypt):
    ctx = _ctx(state_context, reconcile=False)
    assert p60_reconcile.run(ctx).outputs == {"skipped": "not a reconcile run"}


def test_reconcile_skips_while_batches_remain(state_context, monkeypatch, plain_crypt):
    ctx = _ctx(state_context, remaining=1)
    assert p60_reconcile.run(ctx).outputs == {"skipped": "batches remain"}


def test_reconcile_trashes_empty_folders_dropbox_no_longer_has(
    state_context, monkeypatch, plain_crypt
):
    ctx = _ctx(state_context)
    inventory_id = seed_api_inventory(
        ctx.state,
        "run:1",
        [("/Keep", 0, None, 1, "folder"), ("/Keep/ok.txt", 3, "h", 1, "file")],
    )
    ctx.state.update_run(ctx.run_id, inventory_id=inventory_id)
    _mirror(
        ctx.state,
        [
            ("/Keep/ok.txt", 3, "u-ok", "sha-ok"),
            ("/Held/late.txt", 2, "u-late", "sha-l"),
        ],
    )
    snapshot_id = _snapshot(
        ctx.state,
        [("Keep/ok.txt", "u-ok", 3, "sha-ok"), ("Held/late.txt", "u-late", 2, "sha-l")],
    )
    _folders(
        ctx.state,
        snapshot_id,
        [
            ("Keep", "u-keep", "__ROOT__"),
            ("Gone", "u-gone", "__ROOT__"),
            ("Gone/Sub", "u-sub", "u-gone"),
            ("Held", "u-held", "__ROOT__"),
        ],
    )
    trashed = []
    fake = type(
        "P",
        (),
        {
            "root_uid": lambda self, phase: "uid-destination",
            "inventory": lambda self, purpose, phase, reuse_complete=True, deadline=None: (
                snapshot_id
            ),
            "trash": lambda self, paths, phase: trashed.append(list(paths)),
        },
    )()
    monkeypatch.setattr(p60_reconcile, "ProtonCLIProvider", lambda *a, **k: fake)
    monkeypatch.setattr(p60_reconcile, "Store", lambda runtime, paths: FakeStore())
    monkeypatch.setattr(p60_reconcile.session, "writeback", lambda *a: False)
    result = p60_reconcile.run(ctx)
    # Gone and Gone/Sub are folders Dropbox no longer has, holding nothing the mirror
    # knows: the topmost one goes and takes Sub with it. Held is gone from Dropbox too
    # but still holds a file on record, so it stays until that file is trashed.
    assert trashed == [["/my-files/Dropbox/Gone"]]
    assert result.outputs["folders_trashed"] == 1
    assert _figures_event(ctx.state)["folders_trashed"] == 1

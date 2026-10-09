from __future__ import annotations

import json

from conftest import FakeStore

from migrator.phases import p70_report
from migrator.phases.base import PhaseContext


def _ctx(state_context):
    cfg, paths, state, logger, runtime = state_context
    run_id = state.start_run(
        start_epoch=1000,
        hour_utc=2,
        weekday=0,
        budget_minutes=165,
        host="github:1",
        reconcile=False,
    )
    phase_run_id = state.start_phase(70, "70_report", apply=True, inputs={})
    return PhaseContext(cfg, paths, state, logger, True, phase_run_id, run_id, runtime)


def _populate(ctx, chain=True):
    state = ctx.state
    with state.connection:
        cursor = state.connection.execute(
            """INSERT INTO dropbox_inventory_runs(started_at, completed_at, status, account_id, root_namespace_id, purpose)
               VALUES ('now','now','COMPLETE','dbid:test-account','ns','run:1')"""
        )
        inventory_id = int(cursor.lastrowid)
        state.connection.executemany(
            """INSERT INTO dropbox_objects(inventory_id, object_key, tag, name, path_display, path_lower, comparison_key,
               size, content_hash, is_downloadable, raw_json, first_page, last_page)
               VALUES (?, ?, 'file', 'n', ?, ?, ?, ?, 'h', ?, '{}', 1, 1)""",
            [
                (
                    inventory_id,
                    "/taxes/a.pdf",
                    "/Taxes/a.pdf",
                    "/taxes/a.pdf",
                    "/taxes/a.pdf",
                    100,
                    1,
                ),
                (inventory_id, "/b.txt", "/b.txt", "/b.txt", "/b.txt", 50, 1),
                (inventory_id, "/n.paper", "/n.paper", "/n.paper", "/n.paper", 0, 0),
            ],
        )
        state.connection.execute(
            """INSERT INTO mirror_objects(path_lower, path_display, size, content_hash, sha1, sha256, run_id, mirrored_at)
               VALUES ('/taxes/a.pdf', '/Taxes/a.pdf', 100, 'h', 's', 's', ?, 'now')""",
            (ctx.run_id,),
        )
        state.connection.execute(
            """INSERT INTO batches(run_id, number, bytes, file_count, status, started_at, completed_at, details_json)
               VALUES (?, 1, 100, 1, 'CHECKPOINTED', 'now', 'now', ?)""",
            (
                ctx.run_id,
                json.dumps(
                    {
                        "fetched": 1,
                        "vanished": 0,
                        "verified": 1,
                        "bytes": 100,
                        "uploaded_files": 1,
                        "uploaded_bytes": 100,
                        "confirmed": 1,
                        "skipped_identical": 0,
                        "confirm_failed": 0,
                        "checkpointed": 1,
                        "failed": 0,
                        "fetch_seconds": 10,
                        "upload_seconds": 20,
                        "seconds": 40,
                    }
                ),
            ),
        )
        state.connection.execute(
            """INSERT INTO batches(run_id, number, bytes, file_count, status) VALUES (?, 2, 50, 1, 'PLANNED')""",
            (ctx.run_id,),
        )
    state.update_run(
        ctx.run_id,
        inventory_id=inventory_id,
        planned_batches=2,
        remaining_batches=1,
        chain=int(chain),
    )
    ctx.logger.warning(
        "10_inventory",
        "files/list_folder",
        "Dropbox request will be retried",
        retry_count=1,
        provider_category="RATE_LIMIT",
        wait_seconds=15,
    )
    cid = state.record_command_start("proton", "upload", ["proton-drive"], 1)
    state.record_command_end(cid, 1, "RATE_LIMIT")
    for number, name in (
        (10, "10_inventory"),
        (20, "20_delta"),
        (30, "30_plan"),
        (40, "40_batches"),
    ):
        pid = state.start_phase(number, name, apply=True, inputs={"run_id": ctx.run_id})
        state.complete_phase(pid, "PASS")


def test_figures_and_markdown_carry_counts_never_names(
    state_context, monkeypatch, plain_crypt
):
    ctx = _ctx(state_context)
    _populate(ctx)
    ctx.logger.info(
        "60_reconcile",
        "figures",
        "reconcile figures",
        snapshot_id=1,
        complete=1,
        proton_files=1,
        matched=1,
        dropped=0,
        sha1_mismatch=0,
        uid_refreshed=0,
        strays_trashed=0,
        folders_trashed=0,
    )
    store = FakeStore()
    monkeypatch.setattr(p70_report, "Store", lambda runtime, paths: store)
    figures = p70_report.figures(ctx)
    assert figures["mirror"] == {
        "inventory_files": 2,
        "inventory_bytes": 150,
        "mirrored_files": 1,
        "mirrored_bytes": 100,
        "percent_mirrored": 66.7,
        "non_downloadable": 1,
        "oversized_files": 0,
        "oversized_bytes": 0,
        "batches_remaining": 1,
        "bytes_remaining": 50,
        "projected_runs_remaining": 1,
        "chain": True,
    }
    assert (
        figures["run"]["batches_completed"] == 1
        and figures["run"]["files_confirmed"] == 1
    )
    assert figures["throttling"]["dropbox"] == {
        "rate_limited": 1,
        "wait_seconds": 15.0,
        "longest_wait_seconds": 15.0,
    }
    assert figures["throttling"]["proton"]["rate_limited"] == 1
    assert figures["verification"] == {
        "confirmed_this_run": 1,
        "confirm_failed": 0,
        "files_confirmed_cumulative": 1,
        "reconcile_walk": "complete",
        "reconcile_matched": 1,
        "reconcile_dropped": 0,
        "reconcile_strays_trashed": 0,
        "reconcile_folders_trashed": 0,
        "mismatches": 0,
    }
    result = p70_report.run(ctx)
    text = ctx.paths.report.read_text(encoding="utf-8")
    assert "Taxes" not in text and "a.pdf" not in text
    assert "percent mirrored" in text.lower() and "66.7" in text
    assert ctx.paths.chain.exists()
    assert result.status == "PASS"
    assert "RUNNING" not in text  # the report does not list its own unfinished row
    assert result.outputs["status"] == "SUCCESS"
    row = ctx.state.connection.execute(
        "SELECT status FROM runs WHERE id=?", (ctx.run_id,)
    ).fetchone()
    assert row["status"] == "SUCCESS"
    assert sorted(store.objects) == [
        ".state/history/1000-report.sqlite.xz.age",
        ".state/state.sqlite.xz.age",
    ]
    event = ctx.state.connection.execute(
        "SELECT fields_json FROM events WHERE operation='figures' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert json.loads(event["fields_json"])["mirror"]["mirrored_files"] == 1


def test_report_marks_failed_run_and_writes_no_chain(
    state_context, monkeypatch, plain_crypt
):
    ctx = _ctx(state_context)
    _populate(ctx, chain=False)
    # The log has no 60_reconcile figures event: no walk ran.
    assert p70_report.figures(ctx)["verification"] == {
        "confirmed_this_run": 1,
        "confirm_failed": 0,
        "files_confirmed_cumulative": 1,
        "reconcile_walk": "n/a",
        "reconcile_matched": "n/a",
        "reconcile_dropped": "n/a",
        "reconcile_strays_trashed": "n/a",
        "reconcile_folders_trashed": "n/a",
        "mismatches": "n/a",
    }
    monkeypatch.setattr(p70_report, "Store", lambda runtime, paths: FakeStore())
    pid = ctx.state.start_phase(
        40, "40_batches", apply=True, inputs={"run_id": ctx.run_id}
    )
    ctx.state.complete_phase(pid, "FAIL", error_summary="batch 1 failed")
    result = p70_report.run(ctx)
    assert result.status == "FAIL" and result.outputs["status"] == "FAIL"
    assert not ctx.paths.chain.exists()
    assert "FAIL" in ctx.paths.report.read_text(encoding="utf-8")
    row = ctx.state.connection.execute(
        "SELECT status FROM runs WHERE id=?", (ctx.run_id,)
    ).fetchone()
    assert row["status"] == "FAIL"


def test_confirm_failures_surface_in_verification_and_errors(state_context):
    ctx = _ctx(state_context)
    with ctx.state.connection:
        ctx.state.connection.execute(
            """INSERT INTO batches(run_id, number, bytes, file_count, status, started_at, completed_at, details_json)
               VALUES (?, 1, 10, 1, 'FAILED', 'now', 'now', ?)""",
            (ctx.run_id, json.dumps({"confirmed": 0, "confirm_failed": 1})),
        )
    ctx.state.update_run(ctx.run_id, planned_batches=1, remaining_batches=0)
    ctx.logger.error(
        "40_batches",
        "gate",
        "a batch failed confirmation",
        provider_category="VERIFICATION_FAILURE",
        batch=1,
    )
    figures = p70_report.figures(ctx)
    assert figures["verification"]["confirm_failed"] == 1
    assert figures["errors"]["confirm failure"] == 1


def test_reconcile_walk_partial_never_reads_as_a_clean_bill_of_health(state_context):
    ctx = _ctx(state_context)
    ctx.logger.info(
        "60_reconcile",
        "figures",
        "reconcile figures",
        snapshot_id=1,
        complete=0,
        folders_pending=7,
    )
    verification = p70_report.figures(ctx)["verification"]
    assert verification["reconcile_walk"] == "partial, 7 folders pending"
    # A walk that did not complete writes only zero matched/dropped/strays/mismatch
    # values to the log. These values must not show a clean (zero) result until a walk
    # completes.
    assert verification["reconcile_matched"] == "n/a"
    assert verification["reconcile_dropped"] == "n/a"
    assert verification["reconcile_strays_trashed"] == "n/a"
    assert verification["mismatches"] == "n/a"


def test_reconcile_walk_reports_the_latest_complete_walk_after_a_partial_one(
    state_context,
):
    ctx = _ctx(state_context)
    ctx.logger.info(
        "60_reconcile",
        "figures",
        "reconcile figures",
        snapshot_id=1,
        complete=1,
        matched=5,
        dropped=1,
        sha1_mismatch=0,
        strays_trashed=2,
    )
    ctx.logger.info(
        "60_reconcile",
        "figures",
        "reconcile figures",
        snapshot_id=2,
        complete=0,
        folders_pending=3,
    )
    verification = p70_report.figures(ctx)["verification"]
    # The most recent walk did not complete, but the values of the last completed walk
    # stay.
    assert verification["reconcile_walk"] == "partial, 3 folders pending"
    assert verification["reconcile_matched"] == 5
    assert verification["reconcile_dropped"] == 1
    assert verification["reconcile_strays_trashed"] == 2
    assert verification["mismatches"] == 0

from __future__ import annotations

import json
import math
from typing import Any

from .. import statefile
from ..store import Store
from .base import PhaseContext, PhaseResult
from .batch import history_label

PHASE = "70_report"
ERROR_CLASSES = {
    "content-hash mismatch": "content hash mismatch",
    "upload failure": "upload failed",
    "confirm failure": "failed confirmation",
    "listing refused": "floor",
    "session trouble": "login",
}


def _sum(rows: list[dict[str, Any]], key: str) -> int:
    return int(sum(int(row.get(key, 0) or 0) for row in rows))


def _batch_details(ctx: PhaseContext) -> list[dict[str, Any]]:
    return [
        json.loads(row["details_json"] or "{}")
        for row in ctx.state.connection.execute(
            "SELECT details_json FROM batches WHERE run_id=? AND status IN ('CHECKPOINTED', 'FAILED') ORDER BY number",
            (ctx.run_id,),
        )
    ]


def _reconcile_figures(ctx: PhaseContext) -> dict[str, Any]:
    """Reads the figures event of the last weekly walk, as migrator.phases.p60_reconcile
    writes it. If a walk stops at the end of the budget, it writes `complete=0` to the
    log, and its matched/dropped/strays values are zero. These zero values are not data.
    Thus, this function reads matched/dropped/strays/mismatches only from the last
    COMPLETE walk. `reconcile_walk` tells if the most recent walk completed. If there is
    no event, no walk ran."""
    connection = ctx.state.connection
    latest = connection.execute(
        "SELECT fields_json FROM events WHERE phase='60_reconcile' AND operation='figures' "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    complete = connection.execute(
        "SELECT fields_json FROM events WHERE phase='60_reconcile' AND operation='figures' "
        "AND json_extract(fields_json, '$.complete')=1 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    latest_fields = json.loads(latest["fields_json"]) if latest else {}
    complete_fields = json.loads(complete["fields_json"]) if complete else {}
    if not latest:
        walk = "n/a"
    elif int(latest_fields.get("complete", 0) or 0):
        walk = "complete"
    else:
        walk = f"partial, {latest_fields.get('folders_pending', 'n/a')} folders pending"
    return {
        "reconcile_walk": walk,
        "reconcile_matched": complete_fields.get("matched", "n/a"),
        "reconcile_dropped": complete_fields.get("dropped", "n/a"),
        "reconcile_strays_trashed": complete_fields.get("strays_trashed", "n/a"),
        "reconcile_folders_trashed": complete_fields.get("folders_trashed", "n/a"),
        "mismatches": complete_fields.get("sha1_mismatch", "n/a"),
    }


def _throttling(
    ctx: PhaseContext,
    provider_phase_like: str,
    command_provider: str | None,
    since: str,
) -> dict[str, float]:
    """The figures are for this run only. `since` is runs.started_at. The events and
    commands tables have no run id. Thus, this function compares `commands.started_at`
    with `since`."""
    connection = ctx.state.connection
    waits = [
        float(json.loads(row["fields_json"] or "{}").get("wait_seconds") or 0)
        for row in connection.execute(
            "SELECT fields_json FROM events WHERE provider_category='RATE_LIMIT' AND operation LIKE ? "
            "AND timestamp >= ?",
            (provider_phase_like, since),
        )
    ]
    commands = 0
    if command_provider is not None:
        commands = int(
            connection.execute(
                "SELECT COUNT(*) FROM commands WHERE provider=? AND response_category='RATE_LIMIT' AND started_at >= ?",
                (command_provider, since),
            ).fetchone()[0]
        )
    return {
        "rate_limited": len(waits) + commands,
        "wait_seconds": round(sum(waits), 1),
        "longest_wait_seconds": round(max(waits, default=0.0), 1),
    }


def figures(ctx: PhaseContext) -> dict[str, Any]:
    connection = ctx.state.connection
    run = ctx.state.current_run()
    since = str(run["started_at"])
    inventory = connection.execute(
        """SELECT SUM(CASE WHEN is_downloadable=1 THEN 1 ELSE 0 END) AS files,
                  COALESCE(SUM(CASE WHEN is_downloadable=1 THEN size ELSE 0 END), 0) AS bytes,
                  SUM(CASE WHEN is_downloadable=0 THEN 1 ELSE 0 END) AS non_downloadable
           FROM dropbox_objects WHERE inventory_id=? AND tag='file'""",
        (run["inventory_id"],),
    ).fetchone()
    inventory_files = int(inventory["files"] or 0)
    inventory_bytes = int(inventory["bytes"] or 0)
    mirrored = connection.execute(
        """SELECT COUNT(*) AS files, COALESCE(SUM(m.size), 0) AS bytes FROM mirror_objects m
           JOIN dropbox_objects d ON d.inventory_id=? AND d.path_lower=m.path_lower
           WHERE d.tag='file' AND d.is_downloadable=1 AND d.size=m.size AND d.content_hash=m.content_hash""",
        (run["inventory_id"],),
    ).fetchone()
    remaining = connection.execute(
        "SELECT COUNT(*) AS n, COALESCE(SUM(bytes), 0) AS bytes FROM batches WHERE run_id=? AND status='PLANNED'",
        (ctx.run_id,),
    ).fetchone()
    details = _batch_details(ctx)
    moved = _sum(details, "checkpointed")
    moved_bytes = _sum(details, "uploaded_bytes")
    projected = (
        math.ceil(int(remaining["bytes"]) / moved_bytes)
        if moved_bytes and remaining["bytes"]
        else (0 if not remaining["bytes"] else None)
    )
    elapsed = sum(float(d.get("seconds", 0) or 0) for d in details)
    durations = [float(d.get("seconds", 0) or 0) for d in details]
    durations.sort()
    fetch_s = sum(float(d.get("fetch_seconds", 0) or 0) for d in details)
    upload_s = sum(float(d.get("upload_seconds", 0) or 0) for d in details)
    gb = 1024**3

    def rate(nbytes: int, seconds: float) -> float:
        return round(nbytes / gb / (seconds / 3600), 2) if seconds else 0.0

    phases = connection.execute(
        """SELECT phase_name, status, error_summary FROM phase_runs
           WHERE json_extract(inputs_json, '$.run_id') = ?
             AND id IN (SELECT MAX(id) FROM phase_runs GROUP BY phase_number)
             AND status != 'RUNNING'
           ORDER BY phase_number""",
        (ctx.run_id,),
    ).fetchall()
    failed_phases = [row["phase_name"] for row in phases if row["status"] == "FAIL"]
    errors = {
        label: int(
            connection.execute(
                "SELECT COUNT(*) FROM events WHERE level='ERROR' AND timestamp >= ? "
                "AND (message LIKE ? OR safe_raw_error LIKE ?)",
                (since, f"%{needle}%", f"%{needle}%"),
            ).fetchone()[0]
        )
        for label, needle in ERROR_CLASSES.items()
    }
    errors["command non-zero exit"] = int(
        connection.execute(
            "SELECT COUNT(*) FROM commands WHERE exit_code IS NOT NULL AND exit_code != 0 AND started_at >= ?",
            (since,),
        ).fetchone()[0]
    )
    deletions = connection.execute(
        "SELECT status, COUNT(*) AS n FROM deletions WHERE run_id=? GROUP BY status",
        (ctx.run_id,),
    ).fetchall()
    cumulative_verified = int(
        connection.execute("SELECT COUNT(*) FROM mirror_objects").fetchone()[0]
    )
    plan_row = connection.execute(
        """SELECT outputs_json FROM phase_runs WHERE phase_name='30_plan'
           AND json_extract(inputs_json, '$.run_id') = ? AND outputs_json IS NOT NULL
           ORDER BY id DESC LIMIT 1""",
        (ctx.run_id,),
    ).fetchone()
    plan = json.loads(plan_row["outputs_json"]) if plan_row else {}
    reconcile_fields = _reconcile_figures(ctx)
    return {
        "mirror": {
            "inventory_files": inventory_files,
            "inventory_bytes": inventory_bytes,
            "mirrored_files": int(mirrored["files"]),
            "mirrored_bytes": int(mirrored["bytes"]),
            "percent_mirrored": round(100 * int(mirrored["bytes"]) / inventory_bytes, 1)
            if inventory_bytes
            else 100.0,
            "non_downloadable": int(inventory["non_downloadable"] or 0),
            "oversized_files": int(plan.get("oversized_files", 0)),
            "oversized_bytes": int(plan.get("oversized_bytes", 0)),
            "batches_remaining": int(remaining["n"]),
            "bytes_remaining": int(remaining["bytes"]),
            "projected_runs_remaining": projected,
            "chain": bool(run["chain"]),
        },
        "run": {
            "host": run["host"],
            "reconcile": bool(run["reconcile"]),
            "budget_minutes": int(run["budget_minutes"]),
            "budget_used_minutes": round(elapsed / 60, 1),
            "batches_planned": int(run["planned_batches"] or 0),
            "batches_completed": len(details),
            "files_fetched": _sum(details, "fetched"),
            "files_vanished": _sum(details, "vanished"),
            "files_hash_mismatched": _sum(details, "hash_mismatch"),
            "files_uploaded": _sum(details, "uploaded_files"),
            "bytes_uploaded": moved_bytes,
            "files_skipped_identical": _sum(details, "skipped_identical"),
            "files_confirmed": _sum(details, "confirmed"),
            "files_checkpointed": moved,
            "files_trashed": next(
                (int(r["n"]) for r in deletions if r["status"] == "TRASHED"), 0
            ),
            "files_not_found_for_trash": next(
                (int(r["n"]) for r in deletions if r["status"] == "NOT_FOUND"), 0
            ),
        },
        "throughput": {
            "dropbox_down_gb_per_hour": rate(_sum(details, "bytes"), fetch_s),
            "proton_up_gb_per_hour": rate(moved_bytes, upload_s),
            "batch_seconds_min": durations[0] if durations else 0,
            "batch_seconds_median": durations[len(durations) // 2] if durations else 0,
            "batch_seconds_max": durations[-1] if durations else 0,
        },
        "throttling": {
            "dropbox": _throttling(ctx, "files/%", None, since),
            "proton": _throttling(ctx, "proton%", "proton", since),
        },
        "errors": errors,
        "verification": {
            "confirmed_this_run": _sum(details, "confirmed"),
            "confirm_failed": _sum(details, "confirm_failed"),
            "files_confirmed_cumulative": cumulative_verified,
            **reconcile_fields,
        },
        "phases": {row["phase_name"]: row["status"] for row in phases},
        "failed_phases": failed_phases,
    }


def _table(title: str, rows: dict[str, Any]) -> str:
    lines = [f"### {title}", "", "| Figure | Value |", "|---|---|"]
    for key, value in rows.items():
        lines.append(
            f"| {key.replace('_', ' ')} | {value if value is not None else 'n/a'} |"
        )
    return "\n".join(lines) + "\n\n"


def render(fig: dict[str, Any], status: str) -> str:
    out = f"## dropbox run: {status}\n\n"
    out += _table("Mirror status", fig["mirror"])
    out += _table("This run", fig["run"])
    out += _table("Throughput", fig["throughput"])
    throttling = {f"dropbox {k}": v for k, v in fig["throttling"]["dropbox"].items()}
    throttling.update(
        {f"proton {k}": v for k, v in fig["throttling"]["proton"].items()}
    )
    out += _table("Throttling", throttling)
    out += _table("Errors and issues", fig["errors"])
    out += _table("Verification", fig["verification"])
    out += _table("Phases", fig["phases"])
    if fig["failed_phases"]:
        out += f"Failed phases: {', '.join(fig['failed_phases'])}\n"
    return out


def run(ctx: PhaseContext) -> PhaseResult:
    # Before finish_run: current_run() must find the RUNNING row.
    label = f"{history_label(ctx)}-report"
    fig = figures(ctx)
    run_status = "FAIL" if fig["failed_phases"] else "SUCCESS"
    ctx.paths.report.write_text(render(fig, run_status), encoding="utf-8")
    ctx.paths.chain.unlink(missing_ok=True)
    if fig["mirror"]["chain"] and run_status == "SUCCESS":
        ctx.paths.chain.write_text("chain\n", encoding="utf-8")
    ctx.state.finish_run(ctx.run_id, run_status)
    ctx.logger.info(PHASE, "figures", "run figures", **fig)
    if ctx.apply:
        # Until this push, the run row, its figures event and the status of the run are
        # only in the local state.
        statefile.push(
            ctx.state,
            ctx.runtime,
            ctx.paths,
            Store(ctx.runtime, ctx.paths),
            label=label,
        )
    # PhaseResult.status is "PASS" if the run completed with SUCCESS, and "FAIL" if not.
    # These are the same status words as in all the other phases. outputs["status"] and
    # runs.status (through finish_run) keep "SUCCESS" or "FAIL". A FAIL phase status
    # stops `task pipeline` before `ping`.
    return PhaseResult(
        status="PASS" if run_status == "SUCCESS" else "FAIL",
        outputs={
            "status": run_status,
            "chain": fig["mirror"]["chain"],
            "report": str(ctx.paths.report),
        },
    )

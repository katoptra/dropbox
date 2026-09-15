from __future__ import annotations

import shutil
from typing import Any

from .base import PhaseContext, PhaseError, PhaseResult

PHASE = "30_plan"


def pack(rows: list[Any], batch_bytes: int, batch_files: int) -> list[list[Any]]:
    """Greedy first-fit in path order by bytes and by file count (budget.batch_files
    explains why); a file over batch_bytes is a batch by itself."""
    batches: list[list[Any]] = []
    current: list[Any] = []
    current_bytes = 0
    for row in rows:
        size = int(row["size"])
        if size > batch_bytes:
            if current:
                batches.append(current)
                current, current_bytes = [], 0
            batches.append([row])
            continue
        if current and (
            current_bytes + size > batch_bytes or len(current) >= batch_files
        ):
            batches.append(current)
            current, current_bytes = [], 0
        current.append(row)
        current_bytes += size
    if current:
        batches.append(current)
    return batches


def run(ctx: PhaseContext) -> PhaseResult:
    run = ctx.state.current_run()
    connection = ctx.state.connection
    tree_bytes = int(
        connection.execute(
            "SELECT COALESCE(SUM(size), 0) FROM dropbox_objects WHERE inventory_id=? AND tag='file' AND is_downloadable=1",
            (run["inventory_id"],),
        ).fetchone()[0]
    )
    budget = ctx.cfg.budget
    if tree_bytes > budget.ceiling_bytes:
        raise PhaseError(
            f"tree is {tree_bytes} bytes, over CEILING_GB ({budget.ceiling_bytes} bytes)"
        )
    rows = connection.execute(
        "SELECT * FROM delta_changed WHERE run_id=? ORDER BY path_lower", (ctx.run_id,)
    ).fetchall()
    free = shutil.disk_usage(ctx.paths.root).free
    # A file the disk cannot stage is left out of the plan rather than failing the run:
    # a failed batch stops the chain, and the same file would stop it every run after.
    file_cap = min(budget.max_file_bytes, free - budget.headroom_bytes)
    oversized = [r for r in rows if int(r["size"]) > file_cap]
    rows = [r for r in rows if int(r["size"]) <= file_cap]
    largest = max((int(r["size"]) for r in rows), default=0)
    needed = (
        max(largest, min(budget.batch_bytes, sum(int(r["size"]) for r in rows)))
        + budget.headroom_bytes
    )
    if largest and free < needed:
        raise PhaseError(f"disk cannot hold a batch: {free} free, {needed} needed")
    batches = pack(rows, budget.batch_bytes, budget.batch_files)
    with connection:
        # A PLANNED batch from any run was never executed; each run re-plans from the
        # state, so those rows are dead weight in every checkpoint that follows.
        connection.execute(
            "DELETE FROM batch_items WHERE batch_id IN (SELECT id FROM batches WHERE status='PLANNED')"
        )
        connection.execute("DELETE FROM batches WHERE status='PLANNED'")
        # A CHECKPOINTED item is already a mirror_objects row and nothing reads it
        # again; CONFIRM_FAILED rows stay, the operator reads their failure text.
        connection.execute("DELETE FROM batch_items WHERE status='CHECKPOINTED'")
        for number, batch in enumerate(batches, start=1):
            cursor = connection.execute(
                "INSERT INTO batches(run_id, number, bytes, file_count, status) VALUES (?, ?, ?, ?, 'PLANNED')",
                (ctx.run_id, number, sum(int(r["size"]) for r in batch), len(batch)),
            )
            connection.executemany(
                """INSERT INTO batch_items(batch_id, path_lower, path_display, size, content_hash, status)
                   VALUES (?, ?, ?, ?, ?, 'PLANNED')""",
                [
                    (
                        cursor.lastrowid,
                        r["path_lower"],
                        r["path_display"],
                        int(r["size"]),
                        r["content_hash"],
                    )
                    for r in batch
                ],
            )
    ctx.state.update_run(ctx.run_id, planned_batches=len(batches))
    outputs = {
        "batches": len(batches),
        "files": len(rows),
        "bytes": sum(int(r["size"]) for r in rows),
        "largest_file": largest,
        "tree_bytes": tree_bytes,
        "free_bytes": free,
        "file_cap_bytes": file_cap,
        "oversized_files": len(oversized),
        "oversized_bytes": sum(int(r["size"]) for r in oversized),
    }
    ctx.logger.info(PHASE, "gate", "batches planned", **outputs)
    return PhaseResult(outputs=outputs)

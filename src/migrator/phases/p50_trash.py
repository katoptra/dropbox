from __future__ import annotations

import time
from collections import Counter, defaultdict
from pathlib import PurePosixPath

from .. import session, statefile
from ..logging import utc_now
from ..providers.proton_cli import (
    ProtonCLIError,
    ProtonCLIProvider,
    child_cli_path,
    unwrap,
)
from ..store import Store
from .base import PhaseContext, PhaseResult
from .batch import history_label, parent_cli_path, resolve_children
from .p40_batches import should_start

PHASE = "50_trash"
now = time.time
# ponytail: a checkpoint snapshots, compresses and pushes the whole state, about a
# minute at today's size, so one every 50 folders (roughly twelve minutes of listing
# and trashing) keeps that under a tenth of the phase. A run cut off mid-phase loses
# the bookkeeping of at most 50 folders; their files are re-listed next run and come
# back NOT_FOUND.
CHECKPOINT_EVERY = 50


def run(ctx: PhaseContext) -> PhaseResult:
    run = ctx.state.current_run()
    connection = ctx.state.connection
    rows = connection.execute(
        "SELECT * FROM delta_deleted WHERE run_id=? ORDER BY path_lower", (ctx.run_id,)
    ).fetchall()
    if not ctx.apply:
        return PhaseResult(status="PLANNED", outputs={"planned": len(rows)})
    if run["remaining_batches"] is None or int(run["remaining_batches"]) > 0:
        ctx.logger.info(
            PHASE,
            "gate",
            "trash deferred until every batch has landed",
            planned=len(rows),
        )
        return PhaseResult(outputs={"skipped": "batches remain", "planned": len(rows)})
    if not rows:
        return PhaseResult(
            outputs={
                "planned": 0,
                "trashed": 0,
                "not_found": 0,
                "listing_failed": 0,
                "folders": 0,
            }
        )
    store = Store(ctx.runtime, ctx.paths)
    proton = ProtonCLIProvider(
        ctx.cfg,
        ctx.state,
        ctx.logger,
        after_call=lambda: session.writeback(ctx.runtime, ctx.paths, store),
    )
    proton.root_uid(PHASE)
    by_parent: dict[str, list] = defaultdict(list)
    for row in rows:
        by_parent[
            parent_cli_path(ctx.cfg.proton.destination, str(row["path_display"]))
        ].append(row)
    parents = sorted(by_parent.items())
    # A reorganized Dropbox can leave tens of thousands of files to trash, far more
    # than one run holds: the phase works folder by folder inside the run's budget,
    # drops each folder's mirror rows as it goes, and leaves the rest to the next run.
    budget = int(run["budget_minutes"]) * 60
    start_epoch = int(run["start_epoch"])
    label = history_label(ctx)
    counts: Counter[str] = Counter()
    durations: list[float] = []
    since_push = 0
    for index, (parent, group) in enumerate(parents, 1):
        if not should_start(
            elapsed=now() - start_epoch,
            longest=max(durations, default=0.0),
            budget=budget,
            completed=len(durations),
        ):
            break
        began = now()
        folder = _trash_folder(ctx, proton, parent, group)
        counts.update(folder)
        durations.append(now() - began)
        since_push += 1
        ctx.logger.info(
            PHASE,
            "folder",
            f"folder {index} of {len(parents)}: "
            + ", ".join(
                f"{v} {k.replace('_', ' ')}" for k, v in sorted(folder.items())
            ),
            parent=parent,
            **folder,
        )
        if since_push == CHECKPOINT_EVERY:
            statefile.push(
                ctx.state, ctx.runtime, ctx.paths, store, label=f"{label}-trash-{index}"
            )
            since_push = 0
    remaining = len(parents) - len(durations)
    chain = remaining > 0
    ctx.state.update_run(ctx.run_id, chain=int(chain))
    if since_push:
        statefile.push(ctx.state, ctx.runtime, ctx.paths, store, label=f"{label}-trash")
    outputs = {
        "planned": len(rows),
        "trashed": counts["trashed"],
        "not_found": counts["not_found"],
        "listing_failed": counts["listing_failed"],
        "folders": counts["folders"],
        "remaining": remaining,
        "chain": chain,
    }
    ctx.logger.info(PHASE, "gate", "deleted files trashed", **outputs)
    return PhaseResult(outputs=outputs)


def _trash_folder(ctx: PhaseContext, proton, parent: str, group: list) -> Counter[str]:
    """One folder: list it, trash the deleted files still there, record each row, and
    drop the mirror rows of the files trashed or already gone."""
    counts: Counter[str] = Counter()
    try:
        by_name = resolve_children(proton, parent, PHASE)
    except ProtonCLIError:
        # The files may well still be there: keep their state rows so tomorrow retries.
        for row in group:
            _record(ctx, row, "LISTING_FAILED", None)
        counts["listing_failed"] += len(group)
        return counts
    targets = []
    found = []
    gone = []
    for row in group:
        name = PurePosixPath(str(row["path_display"])).name
        candidates = by_name.get(name, [])
        files = [
            n for n in candidates if str(unwrap(n.get("type"))).casefold() == "file"
        ]
        # The UID recorded when the file was mirrored names the exact node. Under a
        # genuine Proton duplicate the name alone would pick either twin, and trashing
        # the wrong one loses the mirrored copy and leaves a stray behind.
        node = next(
            (n for n in files if str(unwrap(n.get("uid"))) == row["proton_uid"]),
            next(iter(files), None),
        )
        if node is None:
            _record(ctx, row, "NOT_FOUND", None)
            counts["not_found"] += 1
            gone.append(row)
            continue
        uid = str(unwrap(node["uid"]))
        targets.append(child_cli_path(parent, name, uid, len(candidates) > 1))
        found.append((row, uid))
    if targets:
        # ponytail: the trash call returning is taken as evidence; the folder is not
        # re-listed to prove each node is gone. The ceiling is one folder listing per
        # trash call saved, and reconcile is the backstop: a node still present comes
        # back as a stray on the next weekly walk.
        proton.trash(targets, PHASE)
        for row, uid in found:
            _record(ctx, row, "TRASHED", uid)
            counts["trashed"] += 1
            gone.append(row)
    with ctx.state.connection:
        ctx.state.connection.executemany(
            "DELETE FROM mirror_objects WHERE path_lower=?",
            [(row["path_lower"],) for row in gone],
        )
    counts["folders"] += 1
    return counts


def _record(ctx: PhaseContext, row, status: str, uid: str | None) -> None:
    with ctx.state.connection:
        ctx.state.connection.execute(
            """INSERT OR REPLACE INTO deletions(run_id, path_lower, path_display, proton_uid, status, trashed_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                ctx.run_id,
                row["path_lower"],
                row["path_display"],
                uid,
                status,
                utc_now() if status == "TRASHED" else None,
            ),
        )

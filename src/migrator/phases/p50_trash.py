from __future__ import annotations

import time
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from itertools import batched
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
# ponytail: one `filesystem trash` call resolves its paths one after another at about
# two seconds each against a 300 s command timeout that a mutation never retries, so
# a call carries at most 50 paths (about 100 s). The ceiling is the CLI's per-path
# cost; the upgrade path is trashing by node UID once the CLI accepts one.
TRASH_CHUNK = 50


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
                "subtrees": 0,
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
    units = _units(ctx, rows)
    # A reorganized Dropbox can leave tens of thousands of files to trash, far more
    # than one run holds: the phase works unit by unit inside the run's budget, drops
    # each unit's mirror rows as it goes, and leaves the rest to the next run.
    budget = int(run["budget_minutes"]) * 60
    start_epoch = int(run["start_epoch"])
    label = history_label(ctx)
    counts: Counter[str] = Counter()
    durations: list[float] = []
    since_push = 0
    for index, ((parent, name), group) in enumerate(units, 1):
        if not should_start(
            elapsed=now() - start_epoch,
            longest=max(durations, default=0.0),
            budget=budget,
            completed=len(durations),
        ):
            break
        began = now()
        folder = _trash_unit(ctx, proton, parent, name, group)
        counts.update(folder)
        durations.append(now() - began)
        since_push += 1
        ctx.logger.info(
            PHASE,
            "folder",
            f"folder {index} of {len(units)}: "
            + ", ".join(
                f"{v} {k.replace('_', ' ')}" for k, v in sorted(folder.items())
            ),
            parent=parent,
            folder=name,
            **folder,
        )
        if since_push == CHECKPOINT_EVERY:
            statefile.push(
                ctx.state, ctx.runtime, ctx.paths, store, label=f"{label}-trash-{index}"
            )
            since_push = 0
    remaining = len(units) - len(durations)
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
        "subtrees": counts["subtrees"],
        "remaining": remaining,
        "chain": chain,
    }
    ctx.logger.info(PHASE, "gate", "deleted files trashed", **outputs)
    return PhaseResult(outputs=outputs)


def _units(ctx: PhaseContext, rows: list) -> list[tuple[tuple[str, str | None], list]]:
    """The work, one unit per trash call sequence: a folder unit `(parent, name)` is a
    topmost directory the mirror holds nothing live under, so one call on the folder
    node takes every deleted file beneath it; a file unit `(parent, None)` holds the
    deleted files of a directory that still has live files. Folder units come first:
    a reorganization moves whole trees, and they are where the files are."""
    destination = ctx.cfg.proton.destination
    deleted = {str(row["path_lower"]): row for row in rows}
    live = sorted(
        path
        for (path,) in ctx.state.connection.execute(
            "SELECT path_lower FROM mirror_objects"
        )
        if path not in deleted
    )

    def occupied(prefix: str) -> bool:
        i = bisect_left(live, prefix)
        return i < len(live) and live[i].startswith(prefix)

    dirs: set[str] = set()
    for path in deleted:
        parent = PurePosixPath(path).parent
        while parent != parent.parent:  # never the destination itself
            dirs.add(f"{parent}/")
            parent = parent.parent
    gone = {d for d in dirs if not occupied(d)}
    tops = sorted(d for d in gone if f"{PurePosixPath(d).parent}/" not in gone)
    units: dict[tuple[str, str | None], list] = defaultdict(list)
    for path, row in deleted.items():
        display = str(row["path_display"])
        i = bisect_right(tops, path) - 1
        if i >= 0 and path.startswith(tops[i]):
            depth = tops[i].count("/") - 1
            parts = PurePosixPath(display.lstrip("/")).parts[:depth]
            key = (parent_cli_path(destination, "/" + "/".join(parts)), parts[-1])
        else:
            key = (parent_cli_path(destination, display), None)
        units[key].append(row)
    return sorted(units.items(), key=lambda unit: (unit[0][1] is None, unit[0]))


def _trash_unit(
    ctx: PhaseContext, proton, parent: str, name: str | None, group: list
) -> Counter[str]:
    """One unit: list its parent, trash what is still there call by call, record each
    row, and drop the mirror rows of the files trashed or already gone."""
    counts: Counter[str] = Counter()
    try:
        by_name = resolve_children(proton, parent, PHASE)
    except ProtonCLIError:
        # The files may well still be there: keep their state rows so tomorrow retries.
        for row in group:
            _record(ctx, row, "LISTING_FAILED", None)
        counts["listing_failed"] += len(group)
        return counts
    if name is None:
        calls, gone = _plan_files(by_name, parent, group)
    else:
        calls, gone = _plan_folder(by_name, parent, name, group)
        counts["subtrees"] += len(calls)
    for row in gone:
        _record(ctx, row, "NOT_FOUND", None)
    counts["not_found"] += len(gone)
    _drop(ctx, gone)
    for targets, hits in calls:
        # ponytail: the trash call returning is taken as evidence; the folder is not
        # re-listed to prove each node is gone. The ceiling is one folder listing per
        # trash call saved, and reconcile is the backstop: a node still present comes
        # back as a stray on the next weekly walk.
        proton.trash(targets, PHASE)
        for row, uid in hits:
            _record(ctx, row, "TRASHED", uid)
        counts["trashed"] += len(hits)
        _drop(ctx, [row for row, _ in hits])
    counts["folders"] += 1
    return counts


def _plan_files(by_name: dict, parent: str, group: list) -> tuple[list, list]:
    """Each deleted file by name, the recorded UID choosing among twins; the targets
    in chunks of TRASH_CHUNK, each with the rows it settles."""
    hits = []
    gone = []
    for row in group:
        name = PurePosixPath(str(row["path_display"])).name
        candidates = by_name.get(name, [])
        files = [n for n in candidates if _kind(n) == "file"]
        # The UID recorded when the file was mirrored names the exact node. Under a
        # genuine Proton duplicate the name alone would pick either twin, and trashing
        # the wrong one loses the mirrored copy and leaves a stray behind.
        node = next(
            (n for n in files if str(unwrap(n.get("uid"))) == row["proton_uid"]),
            next(iter(files), None),
        )
        if node is None:
            gone.append(row)
            continue
        uid = str(unwrap(node["uid"]))
        hits.append((child_cli_path(parent, name, uid, len(candidates) > 1), row, uid))
    calls = [
        ([target for target, _, _ in chunk], [(row, uid) for _, row, uid in chunk])
        for chunk in batched(hits, TRASH_CHUNK)
    ]
    return calls, gone


def _plan_folder(
    by_name: dict, parent: str, name: str, group: list
) -> tuple[list, list]:
    """The folder node itself, every twin of it: nothing live is on record beneath
    that name, so whatever any twin holds is a stray reconcile would trash anyway."""
    candidates = by_name.get(name, [])
    targets = [
        child_cli_path(parent, name, str(unwrap(n["uid"])), len(candidates) > 1)
        for n in candidates
        if _kind(n) == "folder"
    ]
    if not targets:
        return [], list(group)
    return [(targets, [(row, row["proton_uid"]) for row in group])], []


def _kind(node: dict) -> str:
    return str(unwrap(node.get("type"))).casefold()


def _drop(ctx: PhaseContext, rows: list) -> None:
    with ctx.state.connection:
        ctx.state.connection.executemany(
            "DELETE FROM mirror_objects WHERE path_lower=?",
            [(row["path_lower"],) for row in rows],
        )


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

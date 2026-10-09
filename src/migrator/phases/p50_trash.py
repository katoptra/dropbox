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
# ponytail: a checkpoint makes a snapshot of the full state, compresses it and pushes
# it. This takes about one minute, and the time increases with the size of the state.
# One checkpoint for each 50 folders (about twelve minutes of listings and trash calls)
# keeps this time below one tenth of the phase. If a run stops in this phase, it loses
# the records of at most 50 folders. The next run lists their files again and gets
# NOT_FOUND for them.
CHECKPOINT_EVERY = 50
# ponytail: one `filesystem trash` call finds its paths one after the other, at about
# two seconds for each path, against a command timeout of 300 s. Thus, a call has at
# most 50 paths (about 100 s). The ceiling is the time of the CLI for each path. The
# upgrade path: trash by node UID when the CLI accepts a UID.
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
    # If files in Dropbox move to new folders, tens of thousands of files can go to the
    # trash. That is much more than one run can do. Thus, the phase does one unit at a
    # time, in the budget of the run. It deletes the mirror rows of a unit one trash
    # call at a time, and the next run does the remaining units.
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
    """The work, as units. Each unit is a sequence of trash calls:

    - A folder unit `(parent, name)` is a topmost directory that has no remaining file
      of the mirror. One call on the folder node moves each deleted file in it to the
      trash.
    - A file unit `(parent, None)` holds the deleted files of a directory that has
      remaining files.

    The folder units are first: a reorganization moves full trees, and most of the files
    are in them."""
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
        while parent != parent.parent:  # the destination is not a unit
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
    """One unit: list its parent. Record the files that are not there, and delete their
    mirror rows. Then move the nodes that are there to the trash, one call at a time.
    After each call, record its rows and delete their mirror rows."""
    counts: Counter[str] = Counter()
    try:
        by_name = resolve_children(proton, parent, PHASE)
    except ProtonCLIError:
        # The files can be there. Thus, keep their state rows, and the next run tries
        # again.
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
        # ponytail: if the trash call completes, the phase accepts that as proof. It
        # does not list the folder again to make sure that each node is gone. The
        # ceiling: this saves one folder listing for each trash call, and reconcile is
        # the backstop. If a node stays there, the next weekly walk finds it as a stray.
        proton.trash(targets, PHASE)
        for row, uid in hits:
            _record(ctx, row, "TRASHED", uid)
        counts["trashed"] += len(hits)
        _drop(ctx, [row for row, _ in hits])
    counts["folders"] += 1
    return counts


def _plan_files(by_name: dict, parent: str, group: list) -> tuple[list, list]:
    """Each deleted file by name, and the recorded UID selects one of the twins. The
    targets are in chunks of TRASH_CHUNK, each chunk with the rows that it completes."""
    hits = []
    gone = []
    for row in group:
        name = PurePosixPath(str(row["path_display"])).name
        candidates = by_name.get(name, [])
        files = [n for n in candidates if _kind(n) == "file"]
        # The UID that the mirror recorded for the file names the correct node. If
        # Proton has a duplicate, the name alone can select the incorrect twin. A trash
        # call on the incorrect twin moves the copy of the mirror to the trash, and the
        # other twin stays as a stray.
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
    """The folder node, and each twin of it. No remaining file of the mirror is in a
    folder of that name. Thus, all that a twin holds is a stray, and reconcile also
    moves a stray to the trash."""
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

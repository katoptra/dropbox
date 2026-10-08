from __future__ import annotations

import sqlite3

from .. import session, statefile
from ..filesystem import comparison_key
from ..providers.proton_cli import ProtonCLIProvider
from ..store import Store
from .base import PhaseContext, PhaseResult
from .batch import history_label

PHASE = "60_reconcile"
SNAPSHOT_PURPOSE = "reconcile"


def _prune_other_snapshots(connection: sqlite3.Connection, snapshot_id: int) -> None:
    # One walk gives sufficient data. The previous walk only makes each checkpoint
    # larger. This function keeps the walk that this run completed or continued, also if
    # the walk is not complete.
    with connection:
        stale = [
            (int(r["id"]),)
            for r in connection.execute(
                "SELECT id FROM proton_snapshots WHERE id != ?", (snapshot_id,)
            )
        ]
        for table in ("proton_nodes", "proton_folders"):
            connection.executemany(f"DELETE FROM {table} WHERE snapshot_id=?", stale)
        connection.executemany("DELETE FROM proton_snapshots WHERE id=?", stale)


def _folder_counts(connection: sqlite3.Connection, snapshot_id: int) -> tuple[int, int]:
    listed = connection.execute(
        "SELECT COUNT(*) AS count FROM proton_folders WHERE snapshot_id=? AND status='COMPLETE'",
        (snapshot_id,),
    ).fetchone()["count"]
    pending = connection.execute(
        "SELECT COUNT(*) AS count FROM proton_folders WHERE snapshot_id=? AND status!='COMPLETE'",
        (snapshot_id,),
    ).fetchone()["count"]
    return int(listed), int(pending)


def _correct_mirror(
    connection: sqlite3.Connection, nodes: dict[str, sqlite3.Row]
) -> tuple[int, int, int, int, set[str]]:
    """Deletes the mirror_objects rows that Proton does not have, or that have an
    incorrect size or a different SHA-1. For the other rows, it updates the recorded
    UID. Returns (dropped, refreshed, matched, sha1_mismatch, the comparison keys of the
    rows that stay in the state)."""
    dropped = refreshed = matched = sha1_mismatch = 0
    known: set[str] = set()
    with connection:
        for row in connection.execute("SELECT * FROM mirror_objects").fetchall():
            # This line gets the key before the deletion below. A row that this
            # reconcile will correct is state that the mirror knows, not an unknown
            # stray.
            key = comparison_key(str(row["path_display"]))
            known.add(key)
            node = nodes.get(key)
            size_mismatch = (
                node is None
                or node["claimed_size"] is None
                or int(node["claimed_size"]) != int(row["size"])
            )
            # A node with no claimed digest (Proton did not calculate one) gives no
            # value to compare. Thus, this function does not compare the node, and the
            # node is not a mismatch.
            digest_mismatch = (
                not size_mismatch
                and node["sha1"] is not None
                and str(node["sha1"]).casefold() != str(row["sha1"]).casefold()
            )
            if size_mismatch or digest_mismatch:
                connection.execute(
                    "DELETE FROM mirror_objects WHERE path_lower=?",
                    (row["path_lower"],),
                )
                dropped += 1
                if digest_mismatch:
                    sha1_mismatch += 1
                continue
            matched += 1
            if row["proton_uid"] != node["uid"]:
                connection.execute(
                    "UPDATE mirror_objects SET proton_uid=? WHERE path_lower=?",
                    (node["uid"], row["path_lower"]),
                )
                refreshed += 1
    return dropped, refreshed, matched, sha1_mismatch, known


def _stray_folders(
    connection: sqlite3.Connection, snapshot_id: int, inventory_id: int, known: set[str]
) -> list[str]:
    """The Proton folders in the destination that are not in Dropbox and that hold no
    file that the mirror knows. Only the topmost folders: a trash call on a folder also
    moves its subtree to the trash. A folder that holds a file of the state waits for
    the trash call of that file."""
    dropbox_folders = {
        comparison_key(str(row["path_display"]))
        for row in connection.execute(
            "SELECT path_display FROM dropbox_objects WHERE inventory_id=? AND tag='folder'",
            (inventory_id,),
        )
    }
    occupied: set[str] = set()
    for key in known:
        while "/" in key:
            key = key.rsplit("/", 1)[0]
            if key in occupied:
                break
            occupied.add(key)
    folders = connection.execute(
        "SELECT uid, parent_uid, comparison_key, cli_path FROM proton_nodes "
        "WHERE snapshot_id=? AND LOWER(node_type)='folder'",
        (snapshot_id,),
    ).fetchall()
    strays = {
        str(row["uid"]): row
        for row in folders
        if row["comparison_key"] not in dropbox_folders
        and row["comparison_key"] not in occupied
    }
    return sorted(
        str(row["cli_path"])
        for row in strays.values()
        if str(row["parent_uid"]) not in strays
    )


def run(ctx: PhaseContext) -> PhaseResult:
    run = ctx.state.current_run()
    skipped = None
    if not run["reconcile"]:
        skipped = "not a reconcile run"
    elif run["remaining_batches"] is None or int(run["remaining_batches"]) > 0:
        skipped = "batches remain"
    if not ctx.apply:
        if skipped:
            return PhaseResult(outputs={"skipped": skipped})
        return PhaseResult(status="PLANNED", outputs={"planned": "full Proton walk"})
    store = Store(ctx.runtime, ctx.paths)
    proton = ProtonCLIProvider(
        ctx.cfg,
        ctx.state,
        ctx.logger,
        after_call=lambda: session.writeback(ctx.runtime, ctx.paths, store),
        session_dir=ctx.paths.session,
    )
    if skipped:
        return PhaseResult(outputs={"skipped": skipped})
    proton.root_uid(PHASE)
    deadline = int(run["start_epoch"]) + int(run["budget_minutes"]) * 60 - 600
    # ponytail: the walk is one CLI process for each folder, on proton.walk_workers
    # workers, and it addresses each folder by UID. Its cost is the number of folders
    # divided by the number of workers. With a stable purpose and reuse_complete=False,
    # the walk continues the RUNNING snapshot that the deadline or a stopped run left.
    # It does not start again at the root. It also does not use again a COMPLETE walk
    # from a previous reconcile.
    snapshot_id = proton.inventory(
        SNAPSHOT_PURPOSE, PHASE, reuse_complete=False, deadline=deadline
    )
    connection = ctx.state.connection
    _prune_other_snapshots(connection, snapshot_id)
    snapshot_status = str(
        connection.execute(
            "SELECT status FROM proton_snapshots WHERE id=?", (snapshot_id,)
        ).fetchone()["status"]
    )
    folders_listed, folders_pending = _folder_counts(connection, snapshot_id)
    label = f"{history_label(ctx)}-reconcile"
    if snapshot_status != "COMPLETE":
        statefile.push(ctx.state, ctx.runtime, ctx.paths, store, label=label)
        ctx.logger.info(
            PHASE,
            "figures",
            "reconcile figures",
            snapshot_id=snapshot_id,
            complete=0,
            folders_listed=folders_listed,
            folders_pending=folders_pending,
            proton_files=0,
            matched=0,
            dropped=0,
            uid_refreshed=0,
            strays_trashed=0,
            folders_trashed=0,
            sha1_mismatch=0,
        )
        return PhaseResult(outputs={"partial": folders_pending})
    # ponytail: this compares the snapshot with the mirror_objects of this run, also if
    # the walk started some weeks before. If a file changed or went to the trash after
    # the walk listed its folder, the result is a drop (a re-upload that the CLI skips
    # as the same content, at a low cost) or a trash call on a node that is in the
    # trash. The ceiling: a walk that continues for more than one reconcile interval.
    # The upgrade path: start a new snapshot if the walk started more than one interval
    # before.

    # proton_nodes.relative_path (and its comparison_key) does not start with a slash,
    # but a Dropbox display path does. comparison_key removes this slash from the two
    # before it makes the key.
    nodes = {
        str(row["comparison_key"]): row
        for row in connection.execute(
            "SELECT * FROM proton_nodes WHERE snapshot_id=? AND LOWER(node_type)='file'",
            (snapshot_id,),
        )
    }
    dropped, refreshed, matched, sha1_mismatch, known = _correct_mirror(
        connection, nodes
    )
    known |= {
        comparison_key(str(row["path_display"]))
        for row in connection.execute(
            "SELECT path_display FROM dropbox_objects "
            "WHERE inventory_id=? AND tag='file' AND is_downloadable=1",
            (run["inventory_id"],),
        )
    }
    strays = sorted(
        str(node["cli_path"]) for key, node in nodes.items() if key not in known
    )
    if strays:
        proton.trash(strays, PHASE)
    stray_folders = _stray_folders(connection, snapshot_id, run["inventory_id"], known)
    if stray_folders:
        proton.trash(stray_folders, PHASE)
    statefile.push(ctx.state, ctx.runtime, ctx.paths, store, label=label)
    ctx.paths.walked.touch()
    ctx.logger.info(
        PHASE,
        "figures",
        "reconcile figures",
        snapshot_id=snapshot_id,
        complete=1,
        folders_listed=folders_listed,
        folders_pending=folders_pending,
        proton_files=len(nodes),
        matched=matched,
        dropped=dropped,
        uid_refreshed=refreshed,
        strays_trashed=len(strays),
        folders_trashed=len(stray_folders),
        sha1_mismatch=sha1_mismatch,
    )
    outputs = {
        "snapshot_id": snapshot_id,
        "proton_files": len(nodes),
        "dropped": dropped,
        "uid_refreshed": refreshed,
        "strays_trashed": len(strays),
        "folders_trashed": len(stray_folders),
        "matched": matched,
        "sha1_mismatch": sha1_mismatch,
    }
    return PhaseResult(outputs=outputs)

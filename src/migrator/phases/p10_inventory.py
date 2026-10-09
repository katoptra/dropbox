from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

from ..providers.dropbox_api import DropboxAPIProvider
from ..providers.dropbox_auth import access_token
from .base import PhaseContext, PhaseResult

PHASE = "10_inventory"
# the table of inventory runs -> the tables that use its id as a key
_INVENTORY_TABLES = {
    "dropbox_inventory_runs": ("dropbox_objects", "dropbox_pages"),
}


def prune_inventories(connection: sqlite3.Connection, keep: int = 1) -> int:
    """Previous listings are the largest part of the state, and each checkpoint sends
    the state to R2. Only the run that made a listing reads it."""
    pruned = 0
    with connection:
        for runs_table, child_tables in _INVENTORY_TABLES.items():
            stale = [
                (int(r["id"]),)
                for r in connection.execute(
                    f"SELECT id FROM {runs_table} ORDER BY id DESC LIMIT -1 OFFSET ?",
                    (keep,),
                )
            ]
            for table in child_tables:
                connection.executemany(
                    f"DELETE FROM {table} WHERE inventory_id=?", stale
                )
            connection.executemany(f"DELETE FROM {runs_table} WHERE id=?", stale)
            pruned += len(stale)
    return pruned


# The bucket deletes the history copies of the state after this number of days. The log
# rows in the state use the same limit.
HISTORY_DAYS = 7


def prune_history(connection: sqlite3.Connection, days: int = HISTORY_DAYS) -> int:
    """Delete the events and the commands from before the last `days` days. Keep the
    reconcile figures, because the report reads those of the last completed walk. That
    walk can be some weeks before this run."""
    cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    with connection:
        events = connection.execute(
            "DELETE FROM events WHERE timestamp < ? "
            "AND NOT (phase='60_reconcile' AND operation='figures')",
            (cutoff,),
        ).rowcount
        commands = connection.execute(
            "DELETE FROM commands WHERE started_at < ?", (cutoff,)
        ).rowcount
    return events + commands


def recase_display_paths(connection: sqlite3.Connection, inventory_id: int) -> int:
    """Dropbox sets the uppercase and lowercase letters of path_display for each entry.
    Thus, in the entries of one folder, the names of the parents can have different
    letters. The name in the row of a folder is the correct name of that folder.

    This function makes the path_display of each entry again, from the names of its
    ancestors and from the name of the entry. Thus, the tree in staging and in Proton
    has one name for each folder."""
    rows = connection.execute(
        "SELECT rowid, path_lower, path_display, name, tag FROM dropbox_objects WHERE inventory_id=?",
        (inventory_id,),
    ).fetchall()
    folder_names = {
        str(r["path_lower"]): str(r["name"]) for r in rows if r["tag"] == "folder"
    }
    updates = []
    for row in rows:
        lower_parts = str(row["path_lower"]).lstrip("/").split("/")
        display_parts = str(row["path_display"]).lstrip("/").split("/")
        if len(display_parts) != len(lower_parts):
            display_parts = lower_parts
        prefix = ""
        rebuilt = []
        for lower, display in zip(lower_parts[:-1], display_parts[:-1], strict=True):
            prefix += "/" + lower
            rebuilt.append(folder_names.get(prefix, display))
        rebuilt.append(str(row["name"]))
        display_path = "/" + "/".join(rebuilt)
        if display_path != str(row["path_display"]):
            updates.append((display_path, int(row["rowid"])))
    with connection:
        connection.executemany(
            "UPDATE dropbox_objects SET path_display=? WHERE rowid=?", updates
        )
    return len(updates)


def run(ctx: PhaseContext) -> PhaseResult:
    purpose = f"run:{ctx.run_id}"
    token = access_token(ctx.cfg, ctx.runtime)
    ctx.logger.add_secret(token)
    api = DropboxAPIProvider(ctx.cfg, ctx.state, ctx.logger, token=token)
    inventory_id = api.inventory(purpose, reuse_complete=True)
    with ctx.state.connection:
        # If Dropbox lists a file as downloadable but without a content hash, verify
        # cannot examine it, and the mirror cannot copy it. Thus, this statement counts
        # it with the files that are not downloadable. If not, it keeps "percent
        # mirrored" below 100 for all time.
        unhashed = ctx.state.connection.execute(
            "UPDATE dropbox_objects SET is_downloadable=0 WHERE inventory_id=? AND tag='file' "
            "AND is_downloadable=1 AND (content_hash IS NULL OR size IS NULL)",
            (inventory_id,),
        ).rowcount
    recased = recase_display_paths(ctx.state.connection, inventory_id)
    summary = ctx.state.connection.execute(
        """
        SELECT
          SUM(CASE WHEN tag='file' AND is_downloadable=1 THEN 1 ELSE 0 END) AS files,
          SUM(CASE WHEN tag='folder' THEN 1 ELSE 0 END) AS folders,
          SUM(CASE WHEN tag='file' AND is_downloadable=1 THEN size ELSE 0 END) AS bytes,
          SUM(CASE WHEN tag='file' AND is_downloadable=0 THEN 1 ELSE 0 END) AS non_downloadable
        FROM dropbox_objects WHERE inventory_id=?
        """,
        (inventory_id,),
    ).fetchone()
    ctx.state.update_run(ctx.run_id, inventory_id=inventory_id)
    outputs = {
        "inventory_id": inventory_id,
        "files": int(summary["files"] or 0),
        "folders": int(summary["folders"] or 0),
        "bytes": int(summary["bytes"] or 0),
        "non_downloadable": int(summary["non_downloadable"] or 0),
        "unhashed": int(unhashed),
        "recased": recased,
        "pruned_inventories": prune_inventories(ctx.state.connection),
        "pruned_history": prune_history(ctx.state.connection),
    }
    ctx.logger.info(PHASE, "gate", "Dropbox inventory complete", **outputs)
    return PhaseResult(outputs=outputs)

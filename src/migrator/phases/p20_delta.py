from __future__ import annotations

import sqlite3
import unicodedata

from ..logging import utc_now
from .base import PhaseContext, PhaseError, PhaseResult

PHASE = "20_delta"


def display_paths(connection: sqlite3.Connection, inventory_id: int) -> dict[str, str]:
    """path_lower -> the NFC display path. In a recursive listing, the path_display of a
    file can have previous uppercase and lowercase letters in the name of a parent.
    Thus, this function gets the names of the parents from the folder entries."""
    folders = {
        str(row["path_lower"]): str(row["path_display"])
        for row in connection.execute(
            "SELECT path_lower, path_display FROM dropbox_objects WHERE inventory_id=? AND tag='folder'",
            (inventory_id,),
        )
    }
    memo: dict[str, str] = {"": ""}

    def resolve(lower: str, display: str) -> str:
        if lower in memo:
            return memo[lower]
        parent_lower, _, _ = lower.rpartition("/")
        name = display.rpartition("/")[2]
        if parent_lower in folders:
            parent_display = resolve(parent_lower, folders[parent_lower])
        else:
            parent_display = display.rpartition("/")[0]
        memo[lower] = parent_display + "/" + name
        return memo[lower]

    files = connection.execute(
        "SELECT path_lower, path_display FROM dropbox_objects "
        "WHERE inventory_id=? AND tag='file' AND is_downloadable=1",
        (inventory_id,),
    ).fetchall()
    return {
        str(row["path_lower"]): unicodedata.normalize(
            "NFC", resolve(str(row["path_lower"]), str(row["path_display"]))
        )
        for row in files
    }


def run(ctx: PhaseContext) -> PhaseResult:
    run = ctx.state.current_run()
    inventory_id = run["inventory_id"]
    if inventory_id is None:
        raise PhaseError("inventory has not run")
    connection = ctx.state.connection
    listed = int(
        connection.execute(
            "SELECT COUNT(*) FROM dropbox_objects WHERE inventory_id=? AND tag='file' AND is_downloadable=1",
            (inventory_id,),
        ).fetchone()[0]
    )
    mirrored, _ = ctx.state.mirror_totals()
    floor = max(1, int(mirrored * ctx.cfg.budget.listing_floor_ratio))
    if mirrored and listed < floor:
        raise PhaseError(
            f"listing has {listed} files, under the floor of {floor}; a truncated listing "
            "must never become a trash list"
        )
    display = display_paths(connection, inventory_id)
    with connection:
        # The delta of this run is the only delta. The plan and trash steps of the
        # previous runs used their rows. If these rows stay, each checkpoint sends them
        # to R2 again.
        connection.execute("DELETE FROM delta_changed")
        connection.execute("DELETE FROM delta_deleted")
        changed = connection.execute(
            """
            SELECT d.path_lower, d.size, d.content_hash
            FROM dropbox_objects d LEFT JOIN mirror_objects m ON m.path_lower = d.path_lower
            WHERE d.inventory_id=? AND d.tag='file' AND d.is_downloadable=1
              AND d.content_hash IS NOT NULL AND d.size IS NOT NULL
              AND (m.path_lower IS NULL OR m.size != d.size OR m.content_hash != d.content_hash)
            ORDER BY d.path_lower
            """,
            (inventory_id,),
        ).fetchall()
        connection.executemany(
            "INSERT INTO delta_changed(run_id, path_lower, path_display, size, content_hash) VALUES (?, ?, ?, ?, ?)",
            [
                (
                    ctx.run_id,
                    r["path_lower"],
                    display[str(r["path_lower"])],
                    int(r["size"]),
                    r["content_hash"],
                )
                for r in changed
            ],
        )
        connection.execute(
            """
            INSERT INTO delta_deleted(run_id, path_lower, path_display, proton_uid)
            SELECT ?, m.path_lower, m.path_display, m.proton_uid FROM mirror_objects m
            WHERE NOT EXISTS (
                SELECT 1 FROM dropbox_objects d
                WHERE d.inventory_id=? AND d.path_lower=m.path_lower AND d.tag='file' AND d.is_downloadable=1
            )
            """,
            (ctx.run_id, inventory_id),
        )
    deleted = int(
        connection.execute(
            "SELECT COUNT(*) FROM delta_deleted WHERE run_id=?", (ctx.run_id,)
        ).fetchone()[0]
    )
    outputs = {
        "changed_files": len(changed),
        "changed_bytes": sum(int(r["size"]) for r in changed),
        "deleted_files": deleted,
        "listed_files": listed,
        "mirrored_files": mirrored,
        "computed_at": utc_now(),
    }
    ctx.logger.info(PHASE, "gate", "delta computed", **outputs)
    return PhaseResult(outputs=outputs)

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
from typing import Any

from .. import statefile
from ..hashing import hash_file
from ..logging import utc_now
from ..providers.dropbox_api import DropboxNotFound, RetryEvent
from ..providers.proton_cli import escape_component, unwrap
from ..store import Store
from .base import PhaseContext, PhaseError

PHASE = "40_batches"


def items(
    ctx: PhaseContext, batch_id: int, status: str | None = None
) -> list[sqlite3.Row]:
    clause = " AND status=?" if status else ""
    params: tuple[Any, ...] = (batch_id, status) if status else (batch_id,)
    return ctx.state.connection.execute(
        f"SELECT * FROM batch_items WHERE batch_id=?{clause} ORDER BY path_lower",
        params,
    ).fetchall()


def _set_item(
    ctx: PhaseContext, batch_id: int, path_lower: str, status: str, **columns: Any
) -> None:
    # ponytail: one commit for each row. Thus, if a step stops before its end, it loses
    # at most the row in progress. The ceiling is one fsync for each file in each step.
    # The upgrade path: put all the status changes of a step in one transaction, and
    # commit it at the end of the step.
    assignments = ", ".join(["status=?", *(f"{name}=?" for name in columns)])
    with ctx.state.connection:
        ctx.state.connection.execute(
            f"UPDATE batch_items SET {assignments} WHERE batch_id=? AND path_lower=?",
            (status, *columns.values(), batch_id, path_lower),
        )


def _details(reason: str, **extra: Any) -> str:
    return json.dumps({"reason": reason, **extra}, sort_keys=True)


def local_path(paths: Any, path_display: str) -> Path:
    """The staging tree uses path_display. Thus, Proton gets the uppercase and lowercase
    letters of Dropbox. path_lower stays the key and the path for the API."""
    return paths.staging / path_display.lstrip("/")


def parent_cli_path(destination: str, path_display: str) -> str:
    parts = PurePosixPath(path_display.lstrip("/")).parent.parts
    if not parts:
        return destination.rstrip("/")
    return (
        destination.rstrip("/")
        + "/"
        + "/".join(escape_component(part) for part in parts)
    )


def _clear(directory: Path) -> None:
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True, exist_ok=True)


def history_label(ctx: PhaseContext) -> str:
    """The key of a history object starts with the start epoch of the run. This value is
    unique for each run, also after a rollback gives out the same run ids again. It is
    the <epoch> that the spec names in `.state/history/<epoch>-...`."""
    return str(int(ctx.state.current_run()["start_epoch"]))


def _prune_empty_dirs(root: Path) -> None:
    for current, dirs, _files in os.walk(root, topdown=False):
        for name in dirs:
            try:
                os.rmdir(Path(current) / name)
            except OSError:
                pass


def resolve_children(
    proton: Any, parent: str, phase: str
) -> dict[str, list[dict[str, Any]]]:
    """List one Proton folder and group the nodes of the listing by name. Two or more
    nodes with the same name are a duplicate in Proton. Each caller (trash) makes the
    path of a node with `child_cli_path(parent, name, uid, len(by_name[name]) > 1)`."""
    children = proton.list_folder(parent, phase)
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for node in children:
        by_name[str(unwrap(node.get("name")))].append(node)
    return dict(by_name)


def fetch(ctx: PhaseContext, dropbox: Any, batch_id: int) -> dict[str, int]:
    """One Dropbox API call for each file, in a thread pool. Each file goes to its
    path_display in staging.

    fetch clears staging, then fills it. If an item of the batch has a status after the
    PLANNED and VANISHED statuses of fetch, a second fetch can erase files in staging
    that verify and upload use. Thus, fetch refuses to run, and no file goes away
    without an error."""
    advanced = [
        r for r in items(ctx, batch_id) if r["status"] not in ("PLANNED", "VANISHED")
    ]
    if advanced:
        raise PhaseError(
            f"batch {batch_id} has {len(advanced)} item(s) past fetch; "
            "fetch runs once per batch and must not re-clear staging underneath them"
        )
    _clear(ctx.paths.staging)
    rows = items(ctx, batch_id, "PLANNED")

    def _download(row: sqlite3.Row) -> list[RetryEvent]:
        target = local_path(ctx.paths, str(row["path_display"]))
        target.parent.mkdir(parents=True, exist_ok=True)
        return dropbox.download(str(row["path_lower"]), target)

    counts: Counter[str] = Counter()
    error: BaseException | None = None
    pool = ThreadPoolExecutor(max_workers=ctx.cfg.dropbox.download_workers)
    try:
        futures = [(row, pool.submit(_download, row)) for row in rows]
        for row, future in futures:
            try:
                retries = future.result()
            except DropboxNotFound:
                _set_item(
                    ctx,
                    batch_id,
                    row["path_lower"],
                    "VANISHED",
                    details_json=_details("vanished"),
                )
                counts["vanished"] += 1
            except Exception as exc:  # noqa: BLE001 - the pool completes, then the batch stops
                if error is None:
                    error = exc
            else:
                # Only the thread that opened the state connection can use it. Thus,
                # this thread records the retries that a worker collected.
                for retry in retries:
                    ctx.logger.warning(
                        PHASE, "files/download", retry.message, **retry.fields
                    )
                _set_item(ctx, batch_id, row["path_lower"], "FETCHED")
                counts["fetched"] += 1
    finally:
        # An interrupt must not wait for files that no step will use.
        pool.shutdown(cancel_futures=True)
    if error is not None:
        raise error
    _prune_empty_dirs(ctx.paths.staging)
    ctx.logger.info(PHASE, "fetch", "batch fetched", batch=batch_id, **counts)
    return {"fetched": counts["fetched"], "vanished": counts["vanished"]}


def verify(ctx: PhaseContext, batch_id: int) -> dict[str, int]:
    """A mismatch is a file that changed after the listing and before the fetch. verify
    removes it from staging, and thus the upload does not see it. verify counts it, but
    does not record it. The next listing finds it again. If all the files are
    mismatches, the cause is corruption, not edits in Dropbox, and the batch stops with
    an error."""
    counts: Counter[str] = Counter()
    rows = items(ctx, batch_id, "FETCHED")
    for row in rows:
        staged = local_path(ctx.paths, str(row["path_display"]))
        hashes = hash_file(staged)
        if hashes.size != int(row["size"]) or hashes.dropbox_content_hash != str(
            row["content_hash"]
        ):
            staged.unlink()
            _set_item(
                ctx,
                batch_id,
                row["path_lower"],
                "HASH_MISMATCH",
                details_json=_details("content_hash"),
            )
            counts["hash_mismatch"] += 1
            continue
        _set_item(
            ctx,
            batch_id,
            row["path_lower"],
            "VERIFIED",
            sha1=hashes.sha1,
            sha256=hashes.sha256,
        )
        counts["verified"] += 1
        counts["bytes"] += hashes.size
    _prune_empty_dirs(ctx.paths.staging)
    if rows and counts["hash_mismatch"] == len(rows):
        raise PhaseError(
            "content hash mismatch on every staged file; the fetch path is corrupt"
        )
    ctx.logger.info(PHASE, "verify", "batch verified", batch=batch_id, **counts)
    return {
        "verified": counts["verified"],
        "bytes": counts["bytes"],
        "hash_mismatch": counts["hash_mismatch"],
    }


def upload(ctx: PhaseContext, proton: Any, batch_id: int) -> dict[str, int]:
    rows = items(ctx, batch_id, "VERIFIED")
    if not rows:
        return {"uploaded_files": 0, "uploaded_bytes": 0}
    sources = sorted(path for path in ctx.paths.staging.iterdir())
    stdout = proton.upload_tree(sources, ctx.cfg.proton.destination, PHASE)
    # confirm reads the transfer summary of the CLI to know the result of the upload.
    # Thus, the phase keeps this artifact.
    report = ctx.phase_dir(PHASE) / f"upload-{batch_id}.json"
    report.write_text(stdout or "", encoding="utf-8")
    ctx.state.record_artifact(ctx.phase_run_id, "upload_report", report, ctx.paths.root)
    total = sum(int(r["size"]) for r in rows)
    ctx.logger.info(
        PHASE, "upload", "batch uploaded", batch=batch_id, files=len(rows), bytes=total
    )
    return {"uploaded_files": len(rows), "uploaded_bytes": total}


def _last_summary(report: Path) -> dict[str, Any] | None:
    """The CLI writes its progress, and then its summary, as one JSON object on each
    line. The last line with `transferredItems` is the summary."""
    if not report.exists():
        return None
    summary: dict[str, Any] | None = None
    for line in report.read_text(encoding="utf-8").splitlines():
        try:
            candidate = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and "transferredItems" in candidate:
            summary = candidate
    return summary


def confirm(ctx: PhaseContext, batch_id: int) -> dict[str, int]:
    """The summary of the CLI tells the result of the upload, and confirm does not list
    Proton again. A batch confirms if these two conditions are correct:

    - The transferred, skipped and failed items are equal in number to the verified
      files plus each directory that holds them.
    - Each failure names a file in the batch.

    Only the files that the failures name stay for the next run."""
    rows = items(ctx, batch_id, "VERIFIED")
    files = len(rows)
    if not files:
        return {"confirmed": 0, "skipped_identical": 0, "confirm_failed": 0}
    summary = _last_summary(ctx.phase_dir(PHASE) / f"upload-{batch_id}.json")
    if summary is None:
        raise PhaseError("upload summary missing")
    folders = sum(1 for p in ctx.paths.staging.rglob("*") if p.is_dir())
    transferred = int(summary.get("transferredItems", 0))
    skipped = int(summary.get("skippedItems", 0))
    failed = int(summary.get("failedItems", 0))
    failures = [f for f in summary.get("failures") or [] if isinstance(f, dict)]
    # The CLI names a failure only by its basename. Thus, each verified item with that
    # name stays for the next run. If a twin gets this mark but its upload was correct,
    # the cost is one skip of the same content. If a failure names no file of the batch
    # (for example, a folder), the CLI did not try to upload some files. Then the phase
    # records no file of the batch.
    errors = {str(f.get("name") or ""): str(f.get("error") or "") for f in failures}
    errors.pop("", None)
    failed_rows = [
        r for r in rows if PurePosixPath(str(r["path_display"])).name in errors
    ]
    matched = {PurePosixPath(str(r["path_display"])).name for r in failed_rows}
    accounted = transferred + skipped + failed == files + folders
    confirmed = accounted and failed == len(failures) and matched == set(errors)
    good = files - len(failed_rows) if confirmed else 0
    with ctx.state.connection:
        if confirmed:
            for row in failed_rows:
                name = PurePosixPath(str(row["path_display"])).name
                _set_item(
                    ctx,
                    batch_id,
                    str(row["path_lower"]),
                    "CONFIRM_FAILED",
                    details_json=_details("upload failure", error=errors[name]),
                )
            ctx.state.connection.execute(
                "UPDATE batch_items SET status='CONFIRMED' WHERE batch_id=? AND status='VERIFIED'",
                (batch_id,),
            )
        else:
            ctx.state.connection.execute(
                "UPDATE batch_items SET status='CONFIRM_FAILED', details_json=? WHERE batch_id=? AND status='VERIFIED'",
                (
                    _details(
                        "upload summary mismatch",
                        files=files,
                        folders=folders,
                        transferred=transferred,
                        skipped=skipped,
                        failed=failed,
                    ),
                    batch_id,
                ),
            )
    for name in sorted(matched) if confirmed else ():
        # The class of the error goes to the log. The name stays in the encrypted state.
        ctx.logger.warning(
            PHASE,
            "confirm",
            "upload failure left for the next run",
            batch=batch_id,
            provider_category="UPLOAD_FAILURE",
            raw_error=errors[name],
        )
    ctx.logger.info(
        PHASE,
        "confirm",
        "batch confirmed by upload summary",
        batch=batch_id,
        confirmed=good,
        confirm_failed=files - good,
        skipped_identical=skipped if confirmed else 0,
    )
    return {
        "confirmed": good,
        "skipped_identical": skipped if confirmed else 0,
        "confirm_failed": files - good,
    }


def checkpoint(ctx: PhaseContext, store: Store, batch_id: int) -> dict[str, int]:
    """Merge the CONFIRMED rows, then push the state. This is always the last step."""
    connection = ctx.state.connection
    good = items(ctx, batch_id, "CONFIRMED")
    now = utc_now()
    with connection:
        connection.executemany(
            """
            INSERT INTO mirror_objects(path_lower, path_display, size, content_hash, sha1, sha256,
                                       run_id, mirrored_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path_lower) DO UPDATE SET
                path_display=excluded.path_display, size=excluded.size,
                content_hash=excluded.content_hash, sha1=excluded.sha1, sha256=excluded.sha256,
                run_id=excluded.run_id, mirrored_at=excluded.mirrored_at
            """,
            [
                (
                    r["path_lower"],
                    r["path_display"],
                    int(r["size"]),
                    r["content_hash"],
                    r["sha1"],
                    r["sha256"],
                    ctx.run_id,
                    now,
                )
                for r in good
            ],
        )
        connection.execute(
            "UPDATE batch_items SET status='CHECKPOINTED' WHERE batch_id=? AND status='CONFIRMED'",
            (batch_id,),
        )
        failed = int(
            connection.execute(
                "SELECT COUNT(*) FROM batch_items WHERE batch_id=? AND status='CONFIRM_FAILED'",
                (batch_id,),
            ).fetchone()[0]
        )
        # Only a batch that recorded no file gets the FAILED status. The named upload
        # failures stay with the batch, and the next delta finds them again.
        status = "FAILED" if failed and not good else "CHECKPOINTED"
        connection.execute(
            "UPDATE batches SET status=?, completed_at=? WHERE id=?",
            (status, now, batch_id),
        )
    number = int(
        connection.execute(
            "SELECT number FROM batches WHERE id=?", (batch_id,)
        ).fetchone()[0]
    )
    # After confirm counts the files in staging, the phase does not use them again. This
    # step removes them first. Thus, the snapshot, its xz and its age copy do not go on
    # a disk that holds a full batch.
    _clear(ctx.paths.staging)
    statefile.push(
        ctx.state, ctx.runtime, ctx.paths, store, label=f"{history_label(ctx)}-{number}"
    )
    ctx.logger.info(
        PHASE,
        "checkpoint",
        "batch checkpointed",
        batch=batch_id,
        checkpointed=len(good),
        failed=failed,
    )
    return {"checkpointed": len(good), "failed": failed}

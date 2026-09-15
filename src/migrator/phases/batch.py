from __future__ import annotations

import json
import os
import shutil
import sqlite3
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
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
    # ponytail: one commit per row, so a killed step loses at most the row in flight.
    # The ceiling is one fsync per file per step; the upgrade path is to batch the status
    # writes per step and commit once at its end.
    assignments = ", ".join(["status=?", *(f"{name}=?" for name in columns)])
    with ctx.state.connection:
        ctx.state.connection.execute(
            f"UPDATE batch_items SET {assignments} WHERE batch_id=? AND path_lower=?",
            (status, *columns.values(), batch_id, path_lower),
        )


def _details(reason: str, **extra: Any) -> str:
    return json.dumps({"reason": reason, **extra}, sort_keys=True)


def local_path(paths: Any, path_display: str) -> Path:
    """Staging is laid out by path_display so Proton receives Dropbox's own casing;
    path_lower stays the key and the API path."""
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
    """History objects are keyed by the run's start epoch: unique across runs even after
    a rollback re-issues run ids, and what the spec's `.state/history/<epoch>-...` names."""
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
    """List one Proton folder and group its visible nodes by name. More than one node
    sharing a name is a genuine Proton duplicate: `child_cli_path(parent, name, uid,
    len(by_name[name]) > 1)` is how every caller (trash) builds its path."""
    children = proton.list_folder(parent, phase)
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for node in children:
        by_name[str(unwrap(node.get("name")))].append(node)
    return dict(by_name)


def fetch(ctx: PhaseContext, dropbox: Any, batch_id: int) -> dict[str, int]:
    """One Dropbox API call per file, run through a thread pool; each lands at its
    path_display under staging. Fetch owns staging outright: once any item in the batch
    has moved past fetch's own PLANNED/VANISHED states, a second call would wipe staging
    out from under files verify/upload already depend on, so it refuses to run rather
    than silently losing them."""
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
            except Exception as exc:  # noqa: BLE001 - pool drains fully; batch fails after
                if error is None:
                    error = exc
            else:
                # The state is single-threaded, so the retries a worker collected are
                # recorded here, on the thread that opened the connection.
                for retry in retries:
                    ctx.logger.warning(
                        PHASE, "files/download", retry.message, **retry.fields
                    )
                _set_item(ctx, batch_id, row["path_lower"], "FETCHED")
                counts["fetched"] += 1
    finally:
        # An interrupt must not wait on files nobody will use.
        pool.shutdown(cancel_futures=True)
    if error is not None:
        raise error
    _prune_empty_dirs(ctx.paths.staging)
    ctx.logger.info(PHASE, "fetch", "batch fetched", batch=batch_id, **counts)
    return {"fetched": counts["fetched"], "vanished": counts["vanished"]}


def verify(ctx: PhaseContext, batch_id: int) -> dict[str, int]:
    """A mismatch is a file edited between listing and fetch: removed from staging so the
    upload never sees it, counted, never recorded; the next listing catches it. Every
    file mismatching is corruption, not editing, and fails the batch."""
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


# ponytail: a group's cost is its bytes at the CLI's 12.8 MB/s plus 0.2 s per file,
# so a file weighs as much as 2.7 MB when the groups are balanced. Measured once from
# batches.details_json; the upgrade path is fitting it from this run's batches.
FILE_WEIGHT = 2_700_000


def _top(row: Any) -> str:
    """The top-level staging entry a file lands under."""
    return PurePosixPath(str(row["path_display"]).lstrip("/")).parts[0]


def _groups(rows: list, workers: int) -> list[list[str]]:
    """Top-level staging entries spread over at most `workers` groups, heaviest first
    into the lightest group. Disjoint entries share no folder to create, so the
    groups can upload at once. One worker is one group holding every entry."""
    weight: Counter[str] = Counter()
    for row in rows:
        weight[_top(row)] += int(row["size"]) + FILE_WEIGHT
    groups: list[list[str]] = [[] for _ in range(min(workers, len(weight)))]
    load = [0] * len(groups)
    for top, cost in sorted(weight.items(), key=lambda item: (-item[1], item[0])):
        lightest = load.index(min(load))
        groups[lightest].append(top)
        load[lightest] += cost
    return [sorted(group) for group in groups]


def upload(ctx: PhaseContext, proton: Any, batch_id: int) -> dict[str, int]:
    rows = items(ctx, batch_id, "VERIFIED")
    if not rows:
        return {"uploaded_files": 0, "uploaded_bytes": 0}
    workers = ctx.runtime.upload_workers or ctx.cfg.proton.upload_workers
    groups = _groups(rows, workers)
    outputs = proton.upload_trees(
        [[ctx.paths.staging / top for top in group] for group in groups],
        ctx.cfg.proton.destination,
        PHASE,
    )
    for index, (group, stdout) in enumerate(zip(groups, outputs, strict=True)):
        # The CLI's own transfer summary is confirm's evidence, so this artifact is
        # kept; its first line names the entries the call was handed.
        report = ctx.phase_dir(PHASE) / f"upload-{batch_id}-{index}.json"
        report.write_text(
            json.dumps({"sources": group}) + "\n" + (stdout or ""), encoding="utf-8"
        )
        ctx.state.record_artifact(
            ctx.phase_run_id, "upload_report", report, ctx.paths.root
        )
    total = sum(int(r["size"]) for r in rows)
    ctx.logger.info(
        PHASE,
        "upload",
        "batch uploaded",
        batch=batch_id,
        files=len(rows),
        bytes=total,
        groups=len(groups),
    )
    return {"uploaded_files": len(rows), "uploaded_bytes": total}


def _read_report(report: Path) -> tuple[list[str], dict[str, Any] | None]:
    """The report is one JSON object per line: the sources header first, then the
    CLI's progress and its final summary; the last line carrying `transferredItems`
    is the summary."""
    sources: list[str] = []
    summary: dict[str, Any] | None = None
    for line in report.read_text(encoding="utf-8").splitlines():
        try:
            candidate = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(candidate, dict):
            continue
        if "sources" in candidate:
            sources = [str(top) for top in candidate["sources"]]
        elif "transferredItems" in candidate:
            summary = candidate
    return sources, summary


@dataclass(frozen=True)
class _Verdict:
    """One upload call's summary held against the files and folders it was handed."""

    confirmed: bool
    failed_rows: list
    errors: dict[str, str]
    counts: Counter[str]


def _verdict(rows: list, folders: int, summary: dict[str, Any]) -> _Verdict:
    transferred = int(summary.get("transferredItems", 0))
    skipped = int(summary.get("skippedItems", 0))
    failed = int(summary.get("failedItems", 0))
    failures = [f for f in summary.get("failures") or [] if isinstance(f, dict)]
    # The CLI names a failure by basename alone, so every verified item carrying that
    # name is left for the next run; an over-marked twin costs one content-identical
    # skip. A failure naming nothing in the call (a folder, say) means files were never
    # attempted, and the whole batch stays unrecorded as before.
    errors = {str(f.get("name") or ""): str(f.get("error") or "") for f in failures}
    errors.pop("", None)
    failed_rows = [
        r for r in rows if PurePosixPath(str(r["path_display"])).name in errors
    ]
    matched = {PurePosixPath(str(r["path_display"])).name for r in failed_rows}
    accounted = transferred + skipped + failed == len(rows) + folders
    confirmed = accounted and failed == len(failures) and matched == set(errors)
    counts = Counter(
        files=len(rows),
        folders=folders,
        transferred=transferred,
        skipped=skipped,
        failed=failed,
    )
    return _Verdict(confirmed, failed_rows, errors, counts)


def confirm(ctx: PhaseContext, batch_id: int) -> dict[str, int]:
    """The CLI's own summaries are the evidence: no re-listing Proton. Each upload call
    confirms when transferred, skipped and failed items account for every verified file
    plus every directory under the entries it was handed, and every failure names a
    file among them; the batch confirms when every call does, and the failed files
    alone are left for the next run."""
    rows = items(ctx, batch_id, "VERIFIED")
    files = len(rows)
    if not files:
        return {"confirmed": 0, "skipped_identical": 0, "confirm_failed": 0}
    reports = sorted(ctx.phase_dir(PHASE).glob(f"upload-{batch_id}-*.json"))
    by_top: dict[str, list] = defaultdict(list)
    for row in rows:
        by_top[_top(row)].append(row)
    verdicts = []
    covered = 0
    for report in reports:
        sources, summary = _read_report(report)
        if summary is None:
            raise PhaseError("upload summary missing")
        group = [row for top in sources for row in by_top.get(top, [])]
        covered += len(group)
        folders = sum(
            1
            for top in sources
            for path in (ctx.paths.staging / top, *(ctx.paths.staging / top).rglob("*"))
            if path.is_dir()
        )
        verdicts.append(_verdict(group, folders, summary))
    if not verdicts:
        raise PhaseError("upload summary missing")
    confirmed = covered == files and all(v.confirmed for v in verdicts)
    failed_rows = [row for v in verdicts for row in v.failed_rows]
    errors = {name: error for v in verdicts for name, error in v.errors.items()}
    counts: Counter[str] = Counter()
    for v in verdicts:
        counts.update(v.counts)
    skipped = counts["skipped"]
    matched = {PurePosixPath(str(r["path_display"])).name for r in failed_rows}
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
                (_details("upload summary mismatch", **counts), batch_id),
            )
    for name in sorted(matched) if confirmed else ():
        # The error class reaches the log; the name stays in the encrypted state.
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
    """Merge every CONFIRMED row, then push the state. Always the last step."""
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
        # Only a batch that recorded nothing fails; named upload failures ride along
        # and come back through the next delta.
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
    # Staging is dead once confirm has counted it; clearing it first keeps the
    # snapshot, its xz and its age copy off a disk still holding a whole batch.
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

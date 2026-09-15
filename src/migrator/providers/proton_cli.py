from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import zlib
from collections import Counter, deque
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

from ..config import Config
from ..filesystem import comparison_key
from ..logging import RunLogger, utc_now
from ..state import State


class ProtonCLIError(RuntimeError):
    """`category` is the attempt's response category when a mutation raised it, so a
    caller can tell an authentication failure, which no retry helps, from the rest."""

    def __init__(self, message: str, category: str = "") -> None:
        super().__init__(message)
        self.category = category


def unwrap(value: Any) -> Any:
    if isinstance(value, dict) and "ok" in value:
        return value.get("value") if value.get("ok") else None
    return value


def escape_component(name: str) -> str:
    return name.replace("\\", "\\\\").replace("/", "\\/")


def split_parent_path(path: str) -> tuple[str, str]:
    for index in range(len(path) - 1, 0, -1):
        if path[index] != "/":
            continue
        preceding_backslashes = 0
        cursor = index - 1
        while cursor >= 0 and path[cursor] == "\\":
            preceding_backslashes += 1
            cursor -= 1
        if preceding_backslashes % 2 == 0:
            parent = path[:index]
            component = path[index + 1 :]
            if parent and component:
                return parent, component
            break
    raise ProtonCLIError(
        "Proton destination must be a folder below a supported CLI root"
    )


def child_cli_path(parent_cli_path: str, name: str, uid: str, duplicate: bool) -> str:
    component = uid if duplicate else escape_component(name)
    return parent_cli_path.rstrip("/") + "/" + component


def _category(stderr: str, returncode: int) -> str:
    lowered = stderr.casefold()
    if "rate" in lowered or "429" in lowered:
        return "RATE_LIMIT"
    if "auth" in lowered or "login" in lowered:
        return "AUTH"
    if "timeout" in lowered:
        return "TIMEOUT"
    return f"EXIT_{returncode}"


def section_root(destination: str) -> str:
    """`/my-files/Dropbox/x` -> `/my-files`: the CLI section a node UID is resolved in."""
    return "/" + destination.strip("/").split("/", 1)[0]


@dataclass(frozen=True)
class Attempt:
    """One CLI invocation as the state records it."""

    argv: list[str]
    attempt: int
    returncode: int
    category: str
    stdout: str
    stderr: str
    started_at: str
    completed_at: str


SESSION_FILE = "auth-session.json"


class WorkerSessions:
    """One private copy of the CLI session directory per walk worker.

    The CLI refreshes its token only on a 401, rotates it, and a refresh the server
    rejects signs that copy out by deleting its session file. Workers therefore never
    share a session file: the copy a successful refresh rewrote is adopted afterwards
    (`promote`), and every worker is re-seeded from it before the losers retry. Each
    copy also carries the CLI's on-disk crypto and entity caches, so a worker starts
    warm with whatever the run has already decrypted."""

    def __init__(self, session_dir: Path | None, count: int) -> None:
        self.session_dir = session_dir
        self.count = count
        self.dirs: list[Path] = []

    def __enter__(self) -> Self:
        if self.session_dir is None or not (self.session_dir / SESSION_FILE).is_file():
            return self
        root = self.session_dir.parent / "walk-sessions"
        shutil.rmtree(root, ignore_errors=True)
        for index in range(self.count):
            target = root / f"w{index}"
            shutil.copytree(
                self.session_dir, target, ignore=shutil.ignore_patterns("*.log*")
            )
            target.chmod(0o700)
            self.dirs.append(target)
        return self

    def __exit__(self, *exc: object) -> None:
        if self.dirs:
            shutil.rmtree(self.dirs[0].parent, ignore_errors=True)

    def env(self, worker: int) -> dict[str, str] | None:
        if not self.dirs:
            return None
        return {**os.environ, "PROTON_DRIVE_CACHE_DIR": str(self.dirs[worker])}

    def promote(self) -> bool:
        """Adopt the newest surviving session copy into the main directory."""
        if self.session_dir is None:
            return False
        candidates = [
            d / SESSION_FILE for d in self.dirs if (d / SESSION_FILE).is_file()
        ]
        if not candidates:
            return False
        # ponytail: newest write wins. Two workers refreshing in the same instant would
        # both hold valid tokens and the older copy's is silently dropped; the CLI does
        # not say which token the server considers current, so there is nothing better
        # to compare until it does.
        newest = max(candidates, key=lambda f: f.stat().st_mtime_ns)
        main = self.session_dir / SESSION_FILE
        content = newest.read_bytes()
        if main.is_file() and main.read_bytes() == content:
            return False
        main.write_bytes(content)
        main.chmod(0o600)
        return True

    def reseed(self) -> None:
        if self.session_dir is None or not (self.session_dir / SESSION_FILE).is_file():
            return
        content = (self.session_dir / SESSION_FILE).read_bytes()
        for directory in self.dirs:
            (directory / SESSION_FILE).write_bytes(content)


class ProtonCLIProvider:
    def __init__(
        self,
        cfg: Config,
        state: State,
        logger: RunLogger,
        *,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        sleep: Callable[[float], None] = time.sleep,
        after_call: Callable[[], None] | None = None,
        session_dir: Path | None = None,
    ) -> None:
        self.cfg = cfg
        self.state = state
        self.logger = logger
        self.run = run
        self.sleep = sleep
        self.after_call = after_call
        self.session_dir = session_dir

    def _after(self) -> None:
        if self.after_call is not None:
            self.after_call()

    def version(self) -> str:
        try:
            result = self.run(
                [self.cfg.proton.executable, "version"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=self.cfg.proton.command_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise ProtonCLIError("Proton version command timed out") from exc
        if result.returncode:
            raise ProtonCLIError(
                f"cannot execute official Proton Drive CLI: {result.stderr[-2000:]}"
            )
        return result.stdout.strip()

    def _invoke(
        self,
        argv: list[str],
        *,
        attempts: int,
        env: dict[str, str] | None = None,
        writeback: bool = True,
        on_start: Callable[[list[str], int], int] | None = None,
        on_end: Callable[[int, Attempt], None] | None = None,
    ) -> tuple[str | None, list[Attempt]]:
        """Runs argv under the provider's retry policy: exponential backoff, no retry
        after an authentication failure. Returns (stdout, attempts); stdout is None when
        the attempts were exhausted. `on_start`/`on_end` observe each attempt as it
        happens; a worker thread passes neither and the caller records afterwards.
        `writeback=False` skips the shared session's write-back after each call: the
        walk owns it, and only the main thread may push the session."""
        delay = self.cfg.proton.initial_backoff_seconds
        made: list[Attempt] = []
        for attempt in range(1, attempts + 1):
            token = on_start(argv, attempt) if on_start else 0
            started_at = utc_now()
            try:
                try:
                    result = self.run(
                        argv,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                        timeout=self.cfg.proton.command_timeout_seconds,
                        env=env,
                    )
                finally:
                    if writeback:
                        self._after()
            except subprocess.TimeoutExpired:
                record = Attempt(
                    argv, attempt, -1, "TIMEOUT", "", "", started_at, utc_now()
                )
            else:
                category = (
                    "SUCCESS"
                    if result.returncode == 0
                    else _category(result.stderr, result.returncode)
                )
                record = Attempt(
                    argv,
                    attempt,
                    result.returncode,
                    category,
                    result.stdout,
                    result.stderr[-4000:],
                    started_at,
                    utc_now(),
                )
            made.append(record)
            if on_end:
                on_end(token, record)
            if record.category == "SUCCESS":
                return record.stdout, made
            if record.category == "AUTH" or attempt == attempts:
                return None, made
            self.sleep(delay)
            delay = min(delay * 2, self.cfg.proton.maximum_backoff_seconds)
        return None, made

    def _log_attempt(self, phase: str, operation: str, record: Attempt) -> None:
        if record.category == "SUCCESS":
            return
        if record.category == "TIMEOUT":
            self.logger.warning(
                phase,
                operation,
                "Proton CLI operation timed out and will be retried",
                retry_count=record.attempt,
                provider_category="TIMEOUT",
            )
        elif record.category == "AUTH":
            # A dead session is not a transient failure: retrying it only delays the
            # loud stop the operator has to act on.
            self.logger.error(
                phase,
                operation,
                "Proton CLI operation failed on authentication",
                retry_count=record.attempt,
                provider_category="AUTH",
                raw_error=record.stderr,
            )
        else:
            self.logger.warning(
                phase,
                operation,
                "Proton CLI operation failed and will be retried",
                retry_count=record.attempt,
                provider_category=record.category,
                raw_error=record.stderr,
            )

    def _record(self, phase: str, operation: str, records: list[Attempt]) -> None:
        """Main-thread bookkeeping for attempts a worker made without observers."""
        for record in records:
            command_id = self.state.record_command_start(
                "proton",
                operation,
                record.argv,
                record.attempt,
                started_at=record.started_at,
            )
            self.state.record_command_end(
                command_id,
                record.returncode,
                record.category,
                completed_at=record.completed_at,
            )
            self._log_attempt(phase, operation, record)

    @staticmethod
    def _parse(operation: str, stdout: str | None, made: list[Attempt]) -> Any:
        if stdout is None:
            if made and made[-1].category == "AUTH":
                raise ProtonCLIError(f"Proton {operation} failed (AUTH)")
            raise ProtonCLIError(f"Proton {operation} exhausted retries")
        try:
            return json.loads(stdout)
        except ValueError as exc:
            raise ProtonCLIError(f"Proton {operation} returned invalid JSON") from exc

    def _json_command(
        self, operation: str, argv: list[str], *, phase: str, attempts: int
    ) -> Any:
        def start(safe_argv: list[str], attempt: int) -> int:
            return self.state.record_command_start(
                "proton", operation, safe_argv, attempt
            )

        def end(command_id: int, record: Attempt) -> None:
            self.state.record_command_end(
                command_id, record.returncode, record.category
            )
            self._log_attempt(phase, operation, record)

        stdout, made = self._invoke(argv, attempts=attempts, on_start=start, on_end=end)
        return self._parse(operation, stdout, made)

    def root_uid(self, phase: str) -> str:
        parent_path, escaped_name = split_parent_path(self.cfg.proton.destination)
        listing = self._json_command(
            "filesystem_list_destination_parent",
            [self.cfg.proton.executable, "filesystem", "list", "-j", parent_path],
            phase=phase,
            attempts=self.cfg.proton.list_max_attempts,
        )
        if not isinstance(listing, list):
            raise ProtonCLIError(
                "Proton destination parent listing was not a JSON array"
            )
        matches = [
            node
            for node in listing
            if isinstance(node, dict)
            and isinstance(unwrap(node.get("name")), str)
            and escape_component(str(unwrap(node["name"]))) == escaped_name
        ]
        if len(matches) != 1 or unwrap(matches[0].get("type")) != "folder":
            raise ProtonCLIError(
                f"configured Proton destination did not resolve to exactly one folder: "
                f"{len(matches)} name match(es) among {len(listing)} entries of the parent"
            )
        observed = str(unwrap(matches[0].get("uid")) or "")
        expected = self.cfg.proton.expected_destination_uid
        matched = observed == expected
        self.state.record_identity_observation(
            "proton",
            phase,
            expected,
            observed,
            matched=matched,
            details={"destination": self.cfg.proton.destination},
        )
        if not matched:
            raise ProtonCLIError(
                "configured Proton destination UID did not exactly match the listing"
            )
        return observed

    def list_folder(self, path: str, phase: str) -> list[dict[str, Any]]:
        result = self._json_command(
            "filesystem_list",
            [self.cfg.proton.executable, "filesystem", "list", "-j", path],
            phase=phase,
            attempts=self.cfg.proton.list_max_attempts,
        )
        if not isinstance(result, list):
            raise ProtonCLIError("Proton filesystem list was not a JSON array")
        return result

    def inventory(
        self,
        purpose: str,
        phase: str,
        *,
        reuse_complete: bool = True,
        deadline: float | None = None,
    ) -> int:
        """Walks the destination folder by folder into a proton_snapshots row. The queue
        of folders lives in proton_folders, so a walk the deadline or a killed run cut
        short resumes where it stopped. Returns the snapshot id; its status says whether
        the walk completed."""
        snapshot_id = self._open_snapshot(purpose, reuse_complete)
        if snapshot_id is None:
            return self._latest_complete(purpose)
        finished = _Walk(self, phase, snapshot_id, deadline).run()
        if finished:
            self._close_snapshot(snapshot_id)
        return snapshot_id

    def _latest_complete(self, purpose: str) -> int:
        row = self.state.connection.execute(
            """
            SELECT id FROM proton_snapshots
            WHERE purpose=? AND destination_root=? AND status='COMPLETE'
            ORDER BY id DESC LIMIT 1
            """,
            (purpose, self.cfg.proton.destination),
        ).fetchone()
        return int(row["id"])

    def _open_snapshot(self, purpose: str, reuse_complete: bool) -> int | None:
        """The RUNNING snapshot to resume, a new one, or None when a COMPLETE walk may
        be reused."""
        connection = self.state.connection
        version = self.version()
        row = connection.execute(
            """
            SELECT * FROM proton_snapshots
            WHERE purpose=? AND destination_root=?
              AND status IN ('RUNNING', 'COMPLETE')
            ORDER BY id DESC LIMIT 1
            """,
            (purpose, self.cfg.proton.destination),
        ).fetchone()
        if row and row["status"] == "COMPLETE":
            return None if reuse_complete else self._new_snapshot(purpose, version)
        if row:
            return int(row["id"])
        return self._new_snapshot(purpose, version)

    def _new_snapshot(self, purpose: str, version: str) -> int:
        connection = self.state.connection
        with connection:
            cursor = connection.execute(
                """
                INSERT INTO proton_snapshots(
                    purpose, started_at, status, destination_root, cli_version
                ) VALUES (?, ?, 'RUNNING', ?, ?)
                """,
                (purpose, utc_now(), self.cfg.proton.destination, version),
            )
            connection.execute(
                """
                INSERT INTO proton_folders(
                    snapshot_id, uid, parent_uid, visible_segments_json,
                    cli_path, status
                ) VALUES (?, '__ROOT__', NULL, '[]', ?, 'PENDING')
                """,
                (cursor.lastrowid, self.cfg.proton.destination),
            )
        return int(cursor.lastrowid)

    def _close_snapshot(self, snapshot_id: int) -> None:
        connection = self.state.connection
        remaining = connection.execute(
            """
            SELECT COUNT(*) AS count FROM proton_folders
            WHERE snapshot_id=? AND status!='COMPLETE'
            """,
            (snapshot_id,),
        ).fetchone()["count"]
        if remaining:
            raise ProtonCLIError("Proton folder queue is incomplete")
        with connection:
            connection.execute(
                "UPDATE proton_snapshots SET status='COMPLETE', completed_at=? WHERE id=?",
                (utc_now(), snapshot_id),
            )

    def list_path(self, folder: Any) -> str:
        """The root is addressed by its configured name path; every folder below it by
        UID, which the CLI resolves in one lookup instead of listing each ancestor."""
        if folder["uid"] == "__ROOT__":
            return str(folder["cli_path"])
        return f"{section_root(self.cfg.proton.destination)}/{folder['uid']}"

    def _commit_listing(
        self, snapshot_id: int, folder: Any, children: list[dict[str, Any]]
    ) -> list[str]:
        """Records a folder's children and marks it COMPLETE. Returns the UIDs of the
        subfolders this listing added to the queue."""
        connection = self.state.connection
        cli_path = str(folder["cli_path"])
        parent_uid = str(folder["uid"])
        parent_segments = json.loads(folder["visible_segments_json"])
        names = [str(unwrap(node.get("name"))) for node in children]
        duplicates = {name for name, count in Counter(names).items() if count > 1}
        queued: list[str] = []
        with connection:
            for node in children:
                uid = unwrap(node.get("uid"))
                name_value = unwrap(node.get("name"))
                node_type = str(unwrap(node.get("type")) or "")
                if not uid:
                    raise ProtonCLIError(
                        f"Proton node in {cli_path!r} had no stable UID"
                    )
                if name_value is None:
                    raise ProtonCLIError(f"Proton node {uid!r} had no visible name")
                uid = str(uid)
                name = str(name_value)
                segments = [*parent_segments, name]
                relative = "/".join(segments)
                node_cli_path = child_cli_path(cli_path, name, uid, name in duplicates)
                active = unwrap(node.get("activeRevision"))
                claimed_size = claimed_mtime = sha1 = sha1_verified = None
                if isinstance(active, dict):
                    claimed_size = unwrap(active.get("claimedSize"))
                    claimed_mtime = unwrap(active.get("claimedModificationTime"))
                    digests = unwrap(active.get("claimedDigests"))
                    if isinstance(digests, dict):
                        sha1 = unwrap(digests.get("sha1"))
                        sha1_verified = unwrap(digests.get("sha1Verified"))
                connection.execute(
                    """
                    INSERT OR REPLACE INTO proton_nodes(
                        snapshot_id, uid, parent_uid, visible_segments_json,
                        relative_path, cli_path, comparison_key, name, node_type,
                        creation_time, modification_time, claimed_size,
                        claimed_modification_time, sha1, sha1_verified, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        uid,
                        parent_uid,
                        json.dumps(segments, ensure_ascii=False),
                        relative,
                        node_cli_path,
                        comparison_key(relative),
                        name,
                        node_type,
                        unwrap(node.get("creationTime")),
                        unwrap(node.get("modificationTime")),
                        claimed_size,
                        claimed_mtime,
                        sha1,
                        int(bool(sha1_verified)) if sha1_verified is not None else None,
                        # Every field reconcile reads is its own column; the node's raw
                        # JSON would be half the state and nothing reads it back.
                        "{}",
                    ),
                )
                if node_type == "folder":
                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO proton_folders(
                            snapshot_id, uid, parent_uid, visible_segments_json,
                            cli_path, status
                        ) VALUES (?, ?, ?, ?, ?, 'PENDING')
                        """,
                        (
                            snapshot_id,
                            uid,
                            parent_uid,
                            json.dumps(segments, ensure_ascii=False),
                            node_cli_path,
                        ),
                    )
                    if cursor.rowcount:
                        queued.append(uid)
            connection.execute(
                """
                UPDATE proton_folders SET
                    status='COMPLETE', attempt_count=attempt_count+1
                WHERE snapshot_id=? AND uid=?
                """,
                (snapshot_id, parent_uid),
            )
        return queued

    def _note_failure(self, snapshot_id: int, folder: Any, category: str) -> None:
        with self.state.connection:
            self.state.connection.execute(
                """
                UPDATE proton_folders SET
                    attempt_count=attempt_count+1, last_error_category=?
                WHERE snapshot_id=? AND uid=?
                """,
                (category, snapshot_id, str(folder["uid"])),
            )

    def upload_tree(self, sources: list[Path], destination: str, phase: str) -> str:
        argv = [
            self.cfg.proton.executable,
            "filesystem",
            "upload",
            "-f",
            "create-new-revision",
            "-d",
            "merge",
            "--json",
            "--skip-thumbnails",
            *(str(source) for source in sources),
            destination,
        ]
        return self._mutation("upload", argv, phase, accepted=frozenset({0, 1}))

    def trash(self, cli_paths: list[str], phase: str) -> None:
        if not cli_paths:
            return
        self._mutation(
            "trash",
            [self.cfg.proton.executable, "filesystem", "trash", *cli_paths],
            phase,
        )

    def empty_trash(self, phase: str) -> None:
        self._mutation(
            "empty_trash",
            [self.cfg.proton.executable, "filesystem", "empty-trash"],
            phase,
        )

    def _mutation(
        self,
        operation: str,
        argv: list[str],
        phase: str,
        *,
        accepted: frozenset[int] = frozenset({0}),
    ) -> str:
        """Runs a mutating CLI call under a bounded retry: a timeout or a failure that
        is not an authentication failure is tried again after a backoff, up to
        `mutation_max_attempts`; the last attempt's failure is the one raised."""
        attempts = self.cfg.proton.mutation_max_attempts
        delay = self.cfg.proton.initial_backoff_seconds
        for attempt in range(1, attempts + 1):
            last = attempt == attempts
            try:
                return self._mutate_once(
                    operation, argv, phase, attempt, accepted, last
                )
            except ProtonCLIError as exc:
                if last or exc.category == "AUTH":
                    raise
            self.sleep(delay)
            delay = min(delay * 2, self.cfg.proton.maximum_backoff_seconds)
        raise AssertionError("unreachable: the last attempt raises or returns")

    def _mutate_once(
        self,
        operation: str,
        argv: list[str],
        phase: str,
        attempt: int,
        accepted: frozenset[int],
        last: bool,
    ) -> str:
        command_id = self.state.record_command_start("proton", operation, argv, attempt)
        timeout = (
            self.cfg.proton.transfer_timeout_seconds
            if operation == "upload"
            else self.cfg.proton.command_timeout_seconds
        )
        log = self.logger.error if last else self.logger.warning
        outcome = "" if last else " and will be retried"
        try:
            try:
                result = self.run(
                    argv,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                    timeout=timeout,
                )
            finally:
                self._after()
        except subprocess.TimeoutExpired as exc:
            self.state.record_command_end(command_id, -1, "TIMEOUT")
            # The CLI prints nothing on stdout until it finishes, so its stderr tail
            # is the only account of a stalled transfer; the state carries it to R2.
            stderr = exc.stderr or ""
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", "replace")
            log(
                phase,
                operation,
                f"official Proton CLI mutation timed out after {int(timeout)} s{outcome}",
                retry_count=attempt,
                provider_category="TIMEOUT",
                raw_error=stderr[-4000:],
            )
            raise ProtonCLIError(f"Proton {operation} timed out", "TIMEOUT") from exc
        category = (
            "SUCCESS"
            if result.returncode == 0
            else _category(result.stderr, result.returncode)
        )
        self.state.record_command_end(command_id, result.returncode, category)
        if result.returncode not in accepted or category == "AUTH":
            final = last or category == "AUTH"
            (self.logger.error if final else self.logger.warning)(
                phase,
                operation,
                "official Proton CLI mutation failed" + ("" if final else outcome),
                retry_count=attempt,
                provider_category=category,
                raw_error=result.stderr[-4000:],
            )
            raise ProtonCLIError(
                f"Proton {operation} failed ({category}): {result.stderr[-4000:]}",
                category,
            )
        if result.returncode != 0:
            # An accepted non-zero exit means the CLI handled some items and refused
            # others; confirm reads the upload summary's counts as one batch-wide
            # verdict, and this log line is the only account of why the exit was
            # non-zero.
            self.logger.warning(
                phase,
                operation,
                f"official Proton CLI mutation exited {result.returncode} and was accepted",
                provider_category=category,
                raw_error=result.stderr[-4000:],
            )
        return result.stdout


class _Walk:
    """The folder walk behind `ProtonCLIProvider.inventory`: N workers, each listing
    one folder per CLI process from its own session copy.

    A child folder is queued to the worker that listed its parent, so that worker
    already holds the parent's decrypted keys in its cache; a worker with an empty queue
    steals from the longest one. ponytail: queues are rebuilt from PENDING rows at
    start, spread by the top-level folder name, so a resumed walk starts with cold
    locality that inheritance restores as it goes."""

    RESCUES = 3

    def __init__(
        self,
        provider: ProtonCLIProvider,
        phase: str,
        snapshot_id: int,
        deadline: float | None,
    ) -> None:
        self.p = provider
        self.phase = phase
        self.snapshot_id = snapshot_id
        self.deadline = deadline
        self.workers = max(1, int(provider.cfg.proton.walk_workers))
        self.queues: list[deque[str]] = [deque() for _ in range(self.workers)]
        self.in_flight: dict[Future[Any], tuple[int, Any]] = {}
        self.listed = 0
        self.rescues = 0

    def run(self) -> bool:
        """Returns True when every folder is COMPLETE, False when the deadline stopped
        the walk with folders still pending."""
        self._load_pending()
        stopped = False
        with (
            WorkerSessions(self.p.session_dir, self.workers) as sessions,
            ThreadPoolExecutor(max_workers=self.workers) as pool,
        ):
            while True:
                self._dispatch(pool, sessions)
                if not self.in_flight:
                    break
                done, _ = wait(self.in_flight, return_when=FIRST_COMPLETED)
                outcome = self._collect(done)
                if outcome != "ok" or self._past_deadline():
                    # Let the listings under way land before deciding anything.
                    drained = self._collect(set(self.in_flight))
                    outcome = outcome if outcome != "ok" else drained
                if outcome == "failed":
                    raise ProtonCLIError("Proton filesystem_list exhausted retries")
                if outcome == "auth":
                    self._rescue(sessions)
                if self._past_deadline():
                    stopped = True
                    break
            # Without copies the workers shared the real session; one main-thread
            # write-back covers whatever a refresh changed.
            if sessions.promote() or not sessions.dirs:
                self.p._after()
        return not stopped

    def _past_deadline(self) -> bool:
        return self.deadline is not None and time.time() >= self.deadline

    def _load_pending(self) -> None:
        rows = self.p.state.connection.execute(
            """
            SELECT uid, visible_segments_json FROM proton_folders
            WHERE snapshot_id=? AND status='PENDING'
            ORDER BY visible_segments_json, uid
            """,
            (self.snapshot_id,),
        ).fetchall()
        for row in rows:
            segments = json.loads(row["visible_segments_json"])
            top = segments[0] if segments else ""
            self.queues[zlib.crc32(top.encode()) % self.workers].append(row["uid"])

    def _dispatch(self, pool: ThreadPoolExecutor, sessions: WorkerSessions) -> None:
        busy = {worker for worker, _ in self.in_flight.values()}
        for worker in range(self.workers):
            if worker in busy:
                continue
            queue = self.queues[worker]
            if not queue:
                queue = max(self.queues, key=len)
                if not queue:
                    continue
            uid = queue.popleft()
            folder = self.p.state.connection.execute(
                "SELECT * FROM proton_folders WHERE snapshot_id=? AND uid=?",
                (self.snapshot_id, uid),
            ).fetchone()
            argv = [
                self.p.cfg.proton.executable,
                "filesystem",
                "list",
                "-j",
                self.p.list_path(folder),
            ]
            future = pool.submit(
                self.p._invoke,
                argv,
                attempts=self.p.cfg.proton.list_max_attempts,
                env=sessions.env(worker),
                writeback=False,
            )
            self.in_flight[future] = (worker, folder)

    def _collect(self, done: set[Future[Any]]) -> str:
        """Commits finished listings. Returns "ok", "auth" (a worker's session died) or
        "failed" (a listing exhausted its retries)."""
        outcome = "ok"
        for future in done:
            worker, folder = self.in_flight.pop(future)
            try:
                stdout, made = future.result()
            except Exception as exc:
                self.p._note_failure(self.snapshot_id, folder, type(exc).__name__)
                raise
            self.p._record(self.phase, "filesystem_list", made)
            if stdout is None:
                category = made[-1].category if made else "EXIT_?"
                self.p._note_failure(self.snapshot_id, folder, category)
                self.queues[worker].appendleft(str(folder["uid"]))
                outcome = (
                    "auth"
                    if category == "AUTH" and outcome != "failed"
                    else ("failed" if category != "AUTH" else outcome)
                )
                continue
            children = self.p._parse("filesystem_list", stdout, made)
            if not isinstance(children, list):
                raise ProtonCLIError("Proton filesystem list was not a JSON array")
            self.queues[worker].extend(
                self.p._commit_listing(self.snapshot_id, folder, children)
            )
            self.listed += 1
            pending = sum(len(q) for q in self.queues) + len(self.in_flight)
            self.p.logger.info(
                self.phase,
                "proton_folder",
                f"listed folder {self.listed}, {pending} pending",
                object_identifier=str(folder["cli_path"]),
                entries=len(children),
                worker=worker,
            )
        return outcome

    def _rescue(self, sessions: WorkerSessions) -> None:
        """A worker's refresh lost the race: adopt the copy that won, re-seed the rest."""
        self.rescues += 1
        if self.rescues > self.RESCUES:
            raise ProtonCLIError("Proton session could not be recovered for the walk")
        if sessions.promote():
            self.p._after()
        sessions.reseed()
        self.p.logger.warning(
            self.phase,
            "session",
            "adopted the refreshed Proton session and re-seeded the walk workers",
            retry_count=self.rescues,
            provider_category="AUTH",
        )

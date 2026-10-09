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
    """`category` is the response category of the attempt, if a mutation raised this
    error. Thus, a caller can know the difference between an authentication failure,
    which a retry cannot correct, and the other errors."""

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
    """`/my-files/Dropbox/x` -> `/my-files`: the CLI section in which the CLI finds a
    node UID."""
    return "/" + destination.strip("/").split("/", 1)[0]


@dataclass(frozen=True)
class Attempt:
    """One CLI call, as the state records it."""

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
    """One private copy of the CLI session directory for each walk worker.

    The CLI refreshes its token only after a 401, and the refresh changes the token. If
    the server rejects a refresh, the CLI signs out of that copy and deletes its session
    file. Thus, two workers do not use the same session file. After a refresh that the
    server accepted, `promote` puts the copy that the refresh wrote in the session
    directory. Then `reseed` writes that copy to each worker before the losers try
    again.

    Each copy also has the crypto and entity caches of the CLI on disk. Thus, a worker
    starts with all the data that the run decrypted before the walk."""

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
        """Put the session file of the newest copy that has one in the session
        directory."""
        if self.session_dir is None:
            return False
        candidates = [
            d / SESSION_FILE for d in self.dirs if (d / SESSION_FILE).is_file()
        ]
        if not candidates:
            return False
        # ponytail: the newest file wins. If two workers do a refresh at the same time,
        # each copy can hold a token that the server accepts. This method then ignores
        # the token of the other copy, and writes no message. The CLI does not tell
        # which token the server accepts. Thus, there is no better value to compare. The
        # upgrade path: when the CLI gives this data, compare the tokens.
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
        """Runs argv with the retry policy of the provider: an exponential backoff, and
        no retry after an authentication failure. Returns (stdout, attempts). stdout is
        None if no attempt got a SUCCESS result.

        `on_start` and `on_end` see each attempt when it occurs. A worker thread gives
        no observer, and the caller records the attempts after the call. With
        `writeback=False`, this method does not write the shared session back after each
        call. The walk does that, and only the main thread can push the session."""
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
            # A session that the server rejects is not a temporary failure. A retry only
            # adds time before the run stops, and the operator must correct the session.
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
        """Records, on the main thread, the attempts of a worker without observers."""
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
        """Walks the destination, one folder at a time, into a proton_snapshots row. The
        queue of folders is in proton_folders. Thus, if the deadline or a stopped run
        ends a walk, the next walk continues from the folder where it stopped. Returns
        the snapshot id. The status of the snapshot tells if the walk completed."""
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
        """The RUNNING snapshot to continue, a new snapshot, or None if the caller can
        use a COMPLETE walk again."""
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
        """The root has its configured name path as its address, and each folder below
        the root has its UID. The CLI finds a UID in one lookup, and does not list each
        ancestor."""
        if folder["uid"] == "__ROOT__":
            return str(folder["cli_path"])
        return f"{section_root(self.cfg.proton.destination)}/{folder['uid']}"

    def _commit_listing(
        self, snapshot_id: int, folder: Any, children: list[dict[str, Any]]
    ) -> list[str]:
        """Records the children of a folder and sets the status of the folder to
        COMPLETE. Returns the UIDs of the subfolders that this listing added to the
        queue."""
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
                        # Each field that reconcile reads has a column. The raw JSON of
                        # the node can be half of the state, and no step reads it.
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
        """Runs a CLI call that changes Proton. After a timeout, or after a failure that
        is not an authentication failure, it runs the call again after a backoff. It
        runs the call a maximum of `mutation_max_attempts` times, and raises the failure
        of the last attempt."""
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
            # The CLI writes nothing to stdout before it completes. Thus, the end of its
            # stderr is the only data about a transfer that stopped. The state keeps
            # this text and sends it to R2.
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
            # An accepted exit code that is not zero tells that the CLI did some items
            # and refused others. confirm reads the numbers in the upload summary as one
            # result for the full batch. Only this log line tells the cause of that exit
            # code.
            self.logger.warning(
                phase,
                operation,
                f"official Proton CLI mutation exited {result.returncode} and was accepted",
                provider_category=category,
                raw_error=result.stderr[-4000:],
            )
        return result.stdout


class _Walk:
    """The folder walk of `ProtonCLIProvider.inventory`: N workers. Each worker lists
    one folder in each CLI process, from a private session copy.

    A child folder goes into the queue of the worker that listed its parent. Thus, that
    worker has the decrypted keys of the parent in its cache. A worker with an empty
    queue gets folders from the longest queue.

    ponytail: at the start, the walk makes the queues again from the PENDING rows. The
    name of the top-level folder of a row selects its queue. Thus, a walk that continues
    starts without these keys in the caches. The rule for child folders fills the caches
    again during the walk."""

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
        """Returns True if all the folders are COMPLETE. Returns False if the deadline
        stopped the walk with folders in the PENDING status."""
        self._load_pending()
        with WorkerSessions(self.p.session_dir, self.workers) as sessions:
            try:
                with ThreadPoolExecutor(max_workers=self.workers) as pool:
                    return self._loop(pool, sessions)
            finally:
                # The pool stopped all its workers before this line. Thus, no worker can
                # change a token after it. promote puts the newest session copy in the
                # session directory before WorkerSessions deletes the copies. Thus, if
                # the walk stops with an error, the changed token stays. Without copies,
                # all the workers used the one session directory. Then one call on the
                # main thread sends the session to the bucket if a refresh changed it.
                if sessions.promote() or not sessions.dirs:
                    self.p._after()

    def _loop(self, pool: ThreadPoolExecutor, sessions: WorkerSessions) -> bool:
        while True:
            self._dispatch(pool, sessions)
            if not self.in_flight:
                return True
            done, _ = wait(self.in_flight, return_when=FIRST_COMPLETED)
            outcome = self._collect(done)
            if outcome != "ok" or self._past_deadline():
                # Before the decisions that follow, collect the listings that the
                # workers did not complete.
                drained = self._collect(set(self.in_flight))
                outcome = outcome if outcome != "ok" else drained
            if outcome == "failed":
                raise ProtonCLIError("Proton filesystem_list exhausted retries")
            if outcome == "auth":
                self._rescue(sessions)
            if self._past_deadline():
                return False

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
        """Commits the listings that the workers completed. Returns "ok", "auth" (the
        server rejected the session of a worker) or "failed" (a listing used all its
        retries)."""
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
        """The refresh of a worker did not win the race. Put the copy that won in the
        session directory, and write it to the other copies."""
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

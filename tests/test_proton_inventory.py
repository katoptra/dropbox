from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tarfile
import threading
import time
from datetime import datetime
from pathlib import Path

import pytest
from conftest import FakeStore

from migrator import session
from migrator.providers.proton_cli import (
    ProtonCLIError,
    ProtonCLIProvider,
    escape_component,
    unwrap,
)


class ProtonRunner:
    def __init__(self, root):
        self.root = root
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        if argv[-1] == "version":
            return subprocess.CompletedProcess(
                argv, 0, "Proton Drive CLI cli-drive@0.8.0+test\n", ""
            )
        path = argv[-1]
        if path == self.root:
            payload = [
                {
                    "uid": "uid-1",
                    "name": {"ok": True, "value": "Duplicate"},
                    "type": "folder",
                },
                {
                    "uid": "uid-2",
                    "name": {"ok": True, "value": "Duplicate"},
                    "type": "folder",
                },
            ]
        else:
            payload = []
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")


def test_proton_duplicate_names_are_traversed_by_uid(state_context):
    cfg, _, state, logger, _ = state_context
    runner = ProtonRunner(cfg.proton.destination)
    provider = ProtonCLIProvider(cfg, state, logger, run=runner, sleep=lambda _: None)
    snapshot = provider.inventory("test", "80_proton_inventory")
    status = state.connection.execute(
        "SELECT status FROM proton_snapshots WHERE id=?", (snapshot,)
    ).fetchone()["status"]
    assert status == "COMPLETE"
    paths = [call[-1] for call in runner.calls if "list" in call]
    assert "/my-files/uid-1" in paths and "/my-files/uid-2" in paths
    stored = {
        r["uid"]: r["cli_path"]
        for r in state.connection.execute(
            "SELECT uid, cli_path FROM proton_folders WHERE snapshot_id=?", (snapshot,)
        )
    }
    assert stored["uid-1"] == cfg.proton.destination + "/uid-1"
    assert stored["uid-2"] == cfg.proton.destination + "/uid-2"
    duplicates = state.connection.execute(
        """
        SELECT COUNT(*) AS count FROM proton_nodes
        WHERE snapshot_id=? AND relative_path='Duplicate'
        """,
        (snapshot,),
    ).fetchone()["count"]
    assert duplicates == 2


def test_verification_inventory_does_not_reuse_complete_snapshot(state_context):
    cfg, _, state, logger, _ = state_context
    runner = ProtonRunner(cfg.proton.destination)
    provider = ProtonCLIProvider(cfg, state, logger, run=runner, sleep=lambda _: None)
    first = provider.inventory("verification", "110_destination_verification")
    second = provider.inventory(
        "verification",
        "110_destination_verification",
        reuse_complete=False,
    )
    assert second != first


def test_incomplete_upload_inventory_resumes_existing_folder_queue(state_context):
    cfg, _, state, logger, _ = state_context
    child_path = "/my-files/uid-folder"

    def interrupted(argv, **kwargs):
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, "cli-drive@0.8.0\n", "")
        if argv[-1] == cfg.proton.destination:
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps([{"uid": "uid-folder", "name": "Folder", "type": "folder"}]),
                "",
            )
        return subprocess.CompletedProcess(argv, 1, "", "synthetic interruption")

    provider = ProtonCLIProvider(
        cfg, state, logger, run=interrupted, sleep=lambda _: None
    )
    with pytest.raises(ProtonCLIError, match="exhausted retries"):
        provider.inventory("upload_apply", "100_upload", reuse_complete=False)

    running = state.connection.execute(
        """
        SELECT id, status FROM proton_snapshots
        WHERE purpose='upload_apply' ORDER BY id DESC LIMIT 1
        """
    ).fetchone()
    assert running["status"] == "RUNNING"
    pending = state.connection.execute(
        """
        SELECT status FROM proton_folders
        WHERE snapshot_id=? AND uid='uid-folder'
        """,
        (running["id"],),
    ).fetchone()
    assert pending["status"] == "PENDING"

    calls = []

    def resumed(argv, **kwargs):
        calls.append(argv)
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, "cli-drive@0.8.0\n", "")
        assert argv[-1] == child_path
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                [
                    {
                        "uid": "uid-file",
                        "name": "file.txt",
                        "type": "file",
                        "size": 1,
                    }
                ]
            ),
            "",
        )

    provider = ProtonCLIProvider(cfg, state, logger, run=resumed, sleep=lambda _: None)
    resumed_id = provider.inventory("upload_apply", "100_upload", reuse_complete=False)

    assert resumed_id == running["id"]
    assert cfg.proton.destination not in [call[-1] for call in calls if "list" in call]
    assert (
        state.connection.execute(
            "SELECT status FROM proton_snapshots WHERE id=?", (resumed_id,)
        ).fetchone()["status"]
        == "COMPLETE"
    )
    assert (
        state.connection.execute(
            "SELECT COUNT(*) AS count FROM proton_nodes WHERE snapshot_id=?",
            (resumed_id,),
        ).fetchone()["count"]
        == 2
    )


def test_deadline_stops_the_walk_between_folders_and_resumes(state_context):
    cfg, _, state, logger, _ = state_context
    root = cfg.proton.destination

    def runner(argv, **kwargs):
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, "cli-drive@0.8.0\n", "")
        if argv[-1] == root:
            payload = [
                {"uid": "uid-a", "name": "A", "type": "folder"},
                {"uid": "uid-b", "name": "B", "type": "folder"},
            ]
        else:
            payload = []
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    provider = ProtonCLIProvider(cfg, state, logger, run=runner, sleep=lambda _: None)
    first = provider.inventory(
        "reconcile", "60_reconcile", reuse_complete=False, deadline=0
    )
    row = state.connection.execute(
        "SELECT status FROM proton_snapshots WHERE id=?", (first,)
    ).fetchone()
    assert row["status"] == "RUNNING"
    pending = {
        r["uid"]
        for r in state.connection.execute(
            "SELECT uid FROM proton_folders WHERE snapshot_id=? AND status='PENDING'",
            (first,),
        )
    }
    assert pending == {"uid-a", "uid-b"}

    second = provider.inventory("reconcile", "60_reconcile", reuse_complete=False)
    assert second == first
    status = state.connection.execute(
        "SELECT status FROM proton_snapshots WHERE id=?", (second,)
    ).fetchone()["status"]
    assert status == "COMPLETE"


def test_wrapped_fields_and_path_escaping():
    assert unwrap({"ok": True, "value": 42}) == 42
    assert unwrap({"ok": False, "error": "x"}) is None
    assert escape_component(r"a\b/c") == r"a\\b\/c"


def test_proton_list_timeout_fails_closed(state_context):
    cfg, _, state, logger, _ = state_context

    def timeout(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs.get("timeout", 1))

    provider = ProtonCLIProvider(cfg, state, logger, run=timeout, sleep=lambda _: None)
    with pytest.raises(ProtonCLIError, match="exhausted retries"):
        provider.list_folder(cfg.proton.destination, "test")


def _listing(entries):
    return json.dumps(entries)


def _seed_session(session_dir, content=b"old"):
    session_dir.mkdir(parents=True, exist_ok=True)
    (session_dir / "auth-session.json").write_bytes(content)
    (session_dir / "clientUid.json").write_bytes(b"{}")


def test_walk_lists_every_folder_below_the_root_by_uid(state_context):
    cfg, _, state, logger, _ = state_context
    root = cfg.proton.destination
    calls = []

    def runner(argv, **kwargs):
        calls.append(argv[-1])
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, "cli-drive@0.8.0\n", "")
        if argv[-1] == root:
            payload = [{"uid": "uid-a", "name": "A", "type": "folder"}]
        elif argv[-1] == "/my-files/uid-a":
            payload = [{"uid": "uid-b", "name": "B", "type": "folder"}]
        else:
            payload = []
        return subprocess.CompletedProcess(argv, 0, _listing(payload), "")

    provider = ProtonCLIProvider(cfg, state, logger, run=runner, sleep=lambda _: None)
    snapshot = provider.inventory("reconcile", "60_reconcile", reuse_complete=False)
    listed = [c for c in calls if c != "version"]
    # The root is the one folder addressed by name; every descendant is addressed by
    # UID so the CLI resolves it in one lookup instead of listing every ancestor.
    assert sorted(listed) == sorted([root, "/my-files/uid-a", "/my-files/uid-b"])
    stored = {
        r["uid"]: r["cli_path"]
        for r in state.connection.execute(
            "SELECT uid, cli_path FROM proton_folders WHERE snapshot_id=?", (snapshot,)
        )
    }
    assert stored["uid-b"] == root + "/A/B"


def test_walk_runs_folders_in_parallel_from_forked_session_copies(state_context):
    cfg, paths, state, logger, _ = state_context
    _seed_session(paths.session)
    root = cfg.proton.destination
    seen = []

    def runner(argv, env=None, **kwargs):
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, "cli-drive@0.8.0\n", "")
        if argv[-1] == root:
            payload = [
                {"uid": f"uid-{i}", "name": f"F{i}", "type": "folder"} for i in range(8)
            ]
        else:
            payload = []
            worker_dir = Path(env["PROTON_DRIVE_CACHE_DIR"])
            seen.append(worker_dir)
            assert (worker_dir / "auth-session.json").read_bytes() == b"old"
            time.sleep(0.05)
        return subprocess.CompletedProcess(argv, 0, _listing(payload), "")

    provider = ProtonCLIProvider(
        cfg, state, logger, run=runner, sleep=lambda _: None, session_dir=paths.session
    )
    started = time.time()
    snapshot = provider.inventory("reconcile", "60_reconcile", reuse_complete=False)
    elapsed = time.time() - started
    assert len(seen) == 8 and len(set(seen)) > 1
    assert all(d != paths.session for d in seen)
    assert elapsed < 0.3, "eight 50 ms listings ran serially"
    assert not any(d.exists() for d in seen), "worker session copies must be removed"
    assert (paths.session / "auth-session.json").read_bytes() == b"old"
    status = state.connection.execute(
        "SELECT status FROM proton_snapshots WHERE id=?", (snapshot,)
    ).fetchone()["status"]
    assert status == "COMPLETE"


def test_walk_adopts_the_session_a_worker_refreshed_and_retries_the_loser(
    state_context,
):
    cfg, paths, state, logger, _ = state_context
    _seed_session(paths.session)
    root = cfg.proton.destination
    pushes = []

    def runner(argv, env=None, **kwargs):
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, "cli-drive@0.8.0\n", "")
        if argv[-1] == root:
            payload = [
                {"uid": "uid-a", "name": "A", "type": "folder"},
                {"uid": "uid-b", "name": "B", "type": "folder"},
            ]
            return subprocess.CompletedProcess(argv, 0, _listing(payload), "")
        session = Path(env["PROTON_DRIVE_CACHE_DIR"]) / "auth-session.json"
        if argv[-1] == "/my-files/uid-a":
            # The winner: the CLI refreshed its token and rewrote its own copy.
            session.write_bytes(b"refreshed")
            return subprocess.CompletedProcess(argv, 0, _listing([]), "")
        if session.read_bytes() == b"old":
            # The loser: its refresh was rejected, so the CLI signed this copy out.
            session.unlink()
            return subprocess.CompletedProcess(argv, 1, "", "auth: session invalid")
        return subprocess.CompletedProcess(argv, 0, _listing([]), "")

    provider = ProtonCLIProvider(
        cfg,
        state,
        logger,
        run=runner,
        sleep=lambda _: None,
        after_call=lambda: pushes.append(1),
        session_dir=paths.session,
    )
    snapshot = provider.inventory("reconcile", "60_reconcile", reuse_complete=False)
    assert (paths.session / "auth-session.json").read_bytes() == b"refreshed"
    assert pushes, "the adopted session must be written back"
    statuses = {
        r["uid"]: r["status"]
        for r in state.connection.execute(
            "SELECT uid, status FROM proton_folders WHERE snapshot_id=?", (snapshot,)
        )
    }
    assert statuses == {
        "__ROOT__": "COMPLETE",
        "uid-a": "COMPLETE",
        "uid-b": "COMPLETE",
    }


def test_a_walk_that_raises_still_sends_the_token_a_worker_rotated(
    state_context, plain_crypt
):
    cfg, paths, state, logger, runtime = state_context
    _seed_session(paths.session)
    digest = hashlib.sha256(b"old").hexdigest()
    auth = paths.session / "auth-session.json"
    paths.session_sha.write_text(f"{digest}  {auth}\n", encoding="utf-8")
    root = cfg.proton.destination
    store = FakeStore()

    def runner(argv, env=None, **kwargs):
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, "cli-drive@0.8.0\n", "")
        if argv[-1] == root:
            payload = [
                {"uid": "uid-a", "name": "A", "type": "folder"},
                {"uid": "uid-b", "name": "B", "type": "folder"},
            ]
            return subprocess.CompletedProcess(argv, 0, _listing(payload), "")
        if argv[-1] == "/my-files/uid-a":
            # The CLI refreshed the token: the server accepts only the new one.
            copy = Path(env["PROTON_DRIVE_CACHE_DIR"]) / "auth-session.json"
            copy.write_bytes(b"refreshed")
            return subprocess.CompletedProcess(argv, 0, _listing([]), "")
        return subprocess.CompletedProcess(argv, 1, "", "synthetic failure")

    provider = ProtonCLIProvider(
        cfg,
        state,
        logger,
        run=runner,
        sleep=lambda _: None,
        after_call=lambda: session.writeback(runtime, paths, store),
        session_dir=paths.session,
    )
    with pytest.raises(ProtonCLIError, match="exhausted retries"):
        provider.inventory("reconcile", "60_reconcile", reuse_complete=False)
    assert auth.read_bytes() == b"refreshed"
    assert _sent(store) == b"refreshed", "the bucket must hold the rotated token"


def test_a_walk_that_raises_waits_for_the_listings_in_flight(
    state_context, plain_crypt
):
    cfg, paths, state, logger, runtime = state_context
    _seed_session(paths.session)
    root = cfg.proton.destination
    store = FakeStore()

    def runner(argv, env=None, **kwargs):
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, "cli-drive@0.8.0\n", "")
        if argv[-1] == root:
            payload = [
                {"uid": "uid-a", "name": "A", "type": "folder"},
                {"uid": "uid-b", "name": "B", "type": "folder"},
            ]
            return subprocess.CompletedProcess(argv, 0, _listing(payload), "")
        if argv[-1] == "/my-files/uid-a":
            raise OSError("synthetic: the CLI did not start")
        # B is still in flight when A stops the walk, and its refresh comes after that.
        time.sleep(0.2)
        copy = Path(env["PROTON_DRIVE_CACHE_DIR"]) / "auth-session.json"
        copy.write_bytes(b"refreshed")
        return subprocess.CompletedProcess(argv, 0, _listing([]), "")

    provider = ProtonCLIProvider(
        cfg,
        state,
        logger,
        run=runner,
        sleep=lambda _: None,
        after_call=lambda: session.writeback(runtime, paths, store),
        session_dir=paths.session,
    )
    with pytest.raises(OSError, match="synthetic"):
        provider.inventory("reconcile", "60_reconcile", reuse_complete=False)
    assert _sent(store) == b"refreshed", "the bucket must hold the rotated token"


def _sent(store) -> bytes:
    """auth-session.json from the session bundle in the bucket (plain_crypt: no age)."""
    with tarfile.open(fileobj=io.BytesIO(store.objects[session.SESSION_KEY])) as tar:
        return tar.extractfile("auth-session.json").read()


def test_worker_listings_never_write_the_shared_session_back_from_a_thread(
    state_context,
):
    cfg, _, state, logger, _ = state_context
    root = cfg.proton.destination
    callers = []

    def runner(argv, **kwargs):
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, "cli-drive@0.8.0\n", "")
        payload = (
            [{"uid": f"uid-{i}", "name": f"F{i}", "type": "folder"} for i in range(8)]
            if argv[-1] == root
            else []
        )
        return subprocess.CompletedProcess(argv, 0, _listing(payload), "")

    provider = ProtonCLIProvider(
        cfg,
        state,
        logger,
        run=runner,
        sleep=lambda _: None,
        after_call=lambda: callers.append(threading.current_thread()),
    )
    # No session directory: workers share the real session, and only the main thread
    # may push it to R2.
    provider.inventory("reconcile", "60_reconcile", reuse_complete=False)
    assert callers and set(callers) == {threading.main_thread()}


def test_worker_command_rows_keep_their_real_start_and_end_times(state_context):
    cfg, _, state, logger, _ = state_context
    root = cfg.proton.destination

    def runner(argv, **kwargs):
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, "cli-drive@0.8.0\n", "")
        if argv[-1] == root:
            payload = [{"uid": "uid-a", "name": "A", "type": "folder"}]
        else:
            time.sleep(0.05)
            payload = []
        return subprocess.CompletedProcess(argv, 0, _listing(payload), "")

    provider = ProtonCLIProvider(cfg, state, logger, run=runner, sleep=lambda _: None)
    provider.inventory("reconcile", "60_reconcile", reuse_complete=False)
    row = state.connection.execute(
        "SELECT started_at, completed_at FROM commands "
        "WHERE operation='filesystem_list' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    started = datetime.fromisoformat(row["started_at"])
    completed = datetime.fromisoformat(row["completed_at"])
    assert (completed - started).total_seconds() >= 0.04

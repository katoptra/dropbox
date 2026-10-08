from __future__ import annotations

import hashlib
import io
import shutil
import subprocess
import tarfile

import pytest
from conftest import FakeStore

from migrator import session
from migrator.paths import WorkPaths
from migrator.providers.proton_cli import ProtonCLIProvider


def _ready(runtime_factory, tmp_path):
    runtime = runtime_factory(tmp_path)
    paths = WorkPaths.from_runtime(runtime)
    paths.ensure()
    return runtime, paths


def _as_restored(paths, auth: bytes) -> None:
    """The two files and .run/session.sha, as the proton engine's `session` writes
    them. It writes .run/session.sha with `sha256sum <auth file>`."""
    (paths.session / "auth-session.json").write_bytes(auth)
    (paths.session / "clientUid.json").write_bytes(b"c")
    digest = hashlib.sha256(auth).hexdigest()
    paths.session_sha.write_text(
        f"{digest}  {paths.session / 'auth-session.json'}\n", encoding="utf-8"
    )


def test_a_rotated_session_reaches_the_bucket_after_the_call_that_rotated_it(
    state_context, plain_crypt, capsys
):
    cfg, paths, state, logger, runtime = state_context
    _as_restored(paths, b"v1")
    store = FakeStore()
    held = []  # what the bucket holds as each CLI call starts

    def run(argv, **kwargs):
        held.append(store.objects.get(session.SESSION_KEY))
        if len(held) == 2:  # this call changes the token in the file
            (paths.session / "auth-session.json").write_bytes(b"v2")
        return subprocess.CompletedProcess(argv, 0, "[]", "")

    # Each phase that uses the CLI makes its provider this way.
    provider = ProtonCLIProvider(
        cfg,
        state,
        logger,
        run=run,
        after_call=lambda: session.writeback(runtime, paths, store),
    )
    for _ in range(3):
        provider.list_folder("/my-files/Dropbox", "40_batches")
    # The session from the engine is not sent again. The changed session is sent
    # before the next call starts, and only one time.
    assert held[:2] == [None, None]
    with tarfile.open(fileobj=io.BytesIO(held[2])) as archive:
        members = {m.name: archive.extractfile(m).read() for m in archive.getmembers()}
    assert members == {"auth-session.json": b"v2", "clientUid.json": b"c"}
    assert session.writeback(runtime, paths, store) is False
    assert capsys.readouterr().out.count("session: token rotated") == 1


def test_writeback_sends_when_no_digest_is_recorded(
    runtime_factory, tmp_path, plain_crypt
):
    runtime, paths = _ready(runtime_factory, tmp_path)
    _as_restored(paths, b"v1")
    paths.session_sha.unlink()
    store = FakeStore()
    assert session.writeback(runtime, paths, store) is True
    assert session.SESSION_KEY in store.objects


def test_writeback_with_missing_session_file_is_noop(
    runtime_factory, tmp_path, plain_crypt
):
    runtime, paths = _ready(runtime_factory, tmp_path)
    assert session.writeback(runtime, paths, FakeStore()) is False


@pytest.mark.skipif(
    shutil.which("sha256sum") is None, reason="the toolbox image has it"
)
def test_session_sha_reads_and_writes_as_sha256sum_does(
    runtime_factory, tmp_path, plain_crypt
):
    # The engine's `session` writes .run/session.sha with sha256sum, and its
    # session-push compares with sha256sum -c.
    runtime, paths = _ready(runtime_factory, tmp_path)
    auth = paths.session / "auth-session.json"
    auth.write_bytes(b"v1")
    (paths.session / "clientUid.json").write_bytes(b"c")
    with paths.session_sha.open("w", encoding="utf-8") as out:
        subprocess.run(["sha256sum", str(auth)], stdout=out, check=True)
    assert session.writeback(runtime, paths, FakeStore()) is False
    auth.write_bytes(b"v2")
    assert session.writeback(runtime, paths, FakeStore()) is True
    check = ["sha256sum", "--status", "-c", str(paths.session_sha)]
    assert subprocess.run(check, check=False).returncode == 0

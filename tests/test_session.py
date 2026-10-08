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
from migrator.store import StoreError


def _ready(runtime_factory, tmp_path):
    runtime = runtime_factory(tmp_path)
    paths = WorkPaths.from_runtime(runtime)
    paths.ensure()
    return runtime, paths


def _as_restored(paths, auth: bytes) -> None:
    """Write the two files and .run/session.sha, as the session verb of the proton
    engine writes them. That verb writes .run/session.sha with `sha256sum <auth file>`.
    """
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
    held = []  # the bucket content at the start of each CLI call

    def run(argv, **kwargs):
        held.append(store.objects.get(session.SESSION_KEY))
        if len(held) == 2:  # this call changes the token in the file
            (paths.session / "auth-session.json").write_bytes(b"v2")
        return subprocess.CompletedProcess(argv, 0, "[]", "")

    # Each phase that uses the CLI makes its provider as this test does.
    provider = ProtonCLIProvider(
        cfg,
        state,
        logger,
        run=run,
        after_call=lambda: session.writeback(runtime, paths, store),
    )
    for _ in range(3):
        provider.list_folder("/my-files/Dropbox", "40_batches")
    # The phase does not send the session from the engine again. It sends the changed
    # session before the next call starts, and only one time.
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


class _FailingStore(FakeStore):
    def put(self, source, key):
        raise StoreError("R2 upload failed")


def test_a_failed_send_keeps_the_old_record_so_the_next_call_sends(
    runtime_factory, tmp_path, plain_crypt
):
    runtime, paths = _ready(runtime_factory, tmp_path)
    _as_restored(paths, b"v1")
    (paths.session / "auth-session.json").write_bytes(b"v2")
    with pytest.raises(StoreError):
        session.writeback(runtime, paths, _FailingStore())
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
    # The session verb of the engine writes .run/session.sha with sha256sum, and its
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


@pytest.mark.skipif(shutil.which("tar") is None, reason="the toolbox image has it")
def test_the_bundle_opens_with_the_engine_tar_command(
    runtime_factory, tmp_path, plain_crypt
):
    runtime, paths = _ready(runtime_factory, tmp_path)
    _as_restored(paths, b"v1")
    (paths.session / "auth-session.json").write_bytes(b"v2")
    (paths.session / "proton-drive.log").write_bytes(b"noise")
    store = FakeStore()
    assert session.writeback(runtime, paths, store) is True
    bundle = tmp_path / "session.tar"
    bundle.write_bytes(store.objects[session.SESSION_KEY])  # plain_crypt: no age layer
    out = tmp_path / "out"
    out.mkdir()
    # The session step of the engine, with the paths as arguments.
    engine = (
        'tar -xf "$1" -C "$2" auth-session.json clientUid.json && chmod 600 "$2"/*.json'
    )
    subprocess.run(["sh", "-c", engine, "sh", str(bundle), str(out)], check=True)
    assert {f.name: f.read_bytes() for f in out.iterdir()} == {
        "auth-session.json": b"v2",
        "clientUid.json": b"c",
    }
    assert (out / "auth-session.json").stat().st_mode & 0o777 == 0o600

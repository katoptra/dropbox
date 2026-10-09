from __future__ import annotations

import hashlib
import tarfile
from pathlib import Path

from . import crypt
from .env import Runtime
from .paths import WorkPaths
from .store import Store

SESSION_KEY = ".state/session.tar.age"
SESSION_FILES = ("auth-session.json", "clientUid.json")


def _digest(directory: Path) -> str | None:
    auth = directory / SESSION_FILES[0]
    if not auth.is_file():
        return None
    return hashlib.sha256(auth.read_bytes()).hexdigest()


def _recorded(paths: WorkPaths) -> str | None:
    """The first word of .run/session.sha: a digest, as sha256sum writes it."""
    try:
        return paths.session_sha.read_text(encoding="utf-8").split()[0]
    except (OSError, IndexError):
        return None


def _bundle(source_dir: Path, paths: WorkPaths, runtime: Runtime, store: Store) -> None:
    tar_path = paths.root / "session.tar"
    with tarfile.open(tar_path, "w") as archive:
        for name in SESSION_FILES:
            member = source_dir / name
            if member.is_file():
                archive.add(member, arcname=name)
    encrypted = paths.root / "session.tar.age"
    try:
        crypt.encrypt(runtime.age_identity, paths.age_key, tar_path, encrypted)
        store.put(encrypted, SESSION_KEY)
    finally:
        tar_path.unlink(missing_ok=True)
        encrypted.unlink(missing_ok=True)


def writeback(runtime: Runtime, paths: WorkPaths, store: Store) -> bool:
    """Send the session to R2 if auth-session.json is different from the digest in
    .run/session.sha. Then write the new digest to that file, as the proton engine's
    session-push does."""
    current = _digest(paths.session)
    if current is None or current == _recorded(paths):
        return False
    print("session: token rotated, sealing it back to the bucket")
    _bundle(paths.session, paths, runtime, store)
    auth = paths.session / SESSION_FILES[0]
    paths.session_sha.write_text(f"{current}  {auth}\n", encoding="utf-8")
    return True

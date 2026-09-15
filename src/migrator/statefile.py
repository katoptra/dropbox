from __future__ import annotations

import lzma
import os
import shutil
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from . import crypt
from .env import Runtime
from .paths import WorkPaths
from .phases.base import PhaseError
from .state import State
from .store import Store

STATE_KEY = ".state/state.sqlite.xz.age"
HISTORY_PREFIX = ".state/history/"
# The snapshot compresses as independent slices, one xz stream each, across every
# core; xz permits concatenated streams and lzma reads them back as one file, so the
# object's format is unchanged and every history copy stays readable either way.
CHUNK_BYTES = 64 * 1024 * 1024


def fetch(runtime: Runtime, paths: WorkPaths, store: Store) -> str:
    """Restore state.sqlite from R2. A missing object is an empty mirror only on the
    first run ever, which is when the history prefix is empty as well."""
    encrypted = paths.root / "state.sqlite.xz.age"
    try:
        if not store.get(STATE_KEY, encrypted):
            if store.list(HISTORY_PREFIX):
                raise PhaseError(
                    "state object is missing but history exists; a lost state must never "
                    "be mistaken for an empty mirror. Roll back with `task state-rollback`."
                )
            store.probe()  # "fresh" is only believable from a bucket that answers
            return "fresh"
        compressed = paths.root / "state.sqlite.xz"
        try:
            crypt.decrypt(runtime.age_identity, paths.age_key, encrypted, compressed)
            with (
                lzma.open(compressed, "rb") as source,
                open(paths.state_db, "wb") as target,
            ):
                shutil.copyfileobj(source, target)
        finally:
            compressed.unlink(missing_ok=True)
        return "restored"
    finally:
        encrypted.unlink(missing_ok=True)


def push(
    state: State, runtime: Runtime, paths: WorkPaths, store: Store, label: str
) -> None:
    snapshot = paths.root / "state.snapshot.sqlite"
    compressed = paths.root / "state.sqlite.xz"
    encrypted = paths.root / "state.sqlite.xz.age"
    try:
        state.snapshot_to(snapshot)
        _compress(snapshot, compressed)
        crypt.encrypt(runtime.age_identity, paths.age_key, compressed, encrypted)
        history_key = f"{HISTORY_PREFIX}{label}.sqlite.xz.age"
        store.put(encrypted, history_key)
        store.copy(
            history_key, STATE_KEY
        )  # server-side; the blob crosses the wire once
    finally:
        for path in (snapshot, compressed, encrypted):
            path.unlink(missing_ok=True)


def _compress(source_path: Path, target_path: Path) -> None:
    """One xz stream per CHUNK_BYTES slice, compressed in parallel, written in order.
    preset 1 compresses the state in seconds where 6 takes minutes, for objects about a
    third larger; every checkpoint pays this once. At most a core's worth of slices is
    in flight, so memory stays a few slices rather than the whole state."""
    window = os.cpu_count() or 1
    pending: deque = deque()
    with (
        open(source_path, "rb") as source,
        open(target_path, "wb") as target,
        ProcessPoolExecutor(max_workers=window) as pool,
    ):
        for block in iter(lambda: source.read(CHUNK_BYTES), b""):
            pending.append(pool.submit(lzma.compress, block, preset=1))
            if len(pending) > window:
                target.write(pending.popleft().result())
        while pending:
            target.write(pending.popleft().result())


def rollback(store: Store, history_key: str) -> None:
    store.copy(history_key, STATE_KEY)

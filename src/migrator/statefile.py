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
# The snapshot compresses as slices, one xz stream for each slice, independently on all
# the cores. xz lets a file contain a series of streams, and lzma reads them back as one
# file. Thus, the object keeps its format, and lzma can read each history copy, with one
# stream or with many streams.
CHUNK_BYTES = 64 * 1024 * 1024


def fetch(runtime: Runtime, paths: WorkPaths, store: Store) -> str:
    """Download and decrypt state.sqlite from R2. A missing object is an empty mirror
    only on the first run of the mirror, when the history prefix is also empty."""
    encrypted = paths.root / "state.sqlite.xz.age"
    try:
        if not store.get(STATE_KEY, encrypted):
            if store.list(HISTORY_PREFIX):
                raise PhaseError(
                    "state object is missing but history exists; a lost state must never "
                    "be mistaken for an empty mirror. Roll back with `task state-rollback`."
                )
            # The result "fresh" is correct only if the bucket answers.
            store.probe()
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
        # A copy on the server: the object goes through the network only one time.
        store.copy(history_key, STATE_KEY)
    finally:
        for path in (snapshot, compressed, encrypted):
            path.unlink(missing_ok=True)


def _compress(source_path: Path, target_path: Path) -> None:
    """One xz stream for each slice of CHUNK_BYTES. The slices compress in parallel, and
    this function writes them in sequence. With preset 1, the state compresses in
    seconds, but with preset 6, it compresses in minutes. The objects of preset 1 are
    about one third larger. Each checkpoint compresses the state one time.

    At most about one slice for each core is in progress. Thus, the memory holds some
    slices, not the full state."""
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

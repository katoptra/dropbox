from __future__ import annotations

import lzma

import pytest
from conftest import FakeStore

from migrator import statefile
from migrator.phases.base import PhaseError
from migrator.state import State


def test_fresh_when_bucket_has_no_state_and_no_history(state_context, plain_crypt):
    _, paths, state, _, runtime = state_context
    state.close()
    paths.state_db.unlink()
    assert statefile.fetch(runtime, paths, FakeStore()) == "fresh"
    assert not paths.state_db.exists()


def test_missing_state_beside_history_fails(state_context, plain_crypt):
    _, paths, state, _, runtime = state_context
    state.close()
    store = FakeStore()
    store.objects[statefile.HISTORY_PREFIX + "1-1.sqlite.xz.age"] = b"x"
    with pytest.raises(PhaseError, match="history"):
        statefile.fetch(runtime, paths, store)


def test_push_writes_history_then_canonical_and_fetch_restores(
    state_context, plain_crypt
):
    cfg, paths, state, _, runtime = state_context
    with state.connection:
        state.connection.execute(
            """INSERT INTO mirror_objects(path_lower, path_display, size, content_hash, sha1, sha256,
               run_id, mirrored_at) VALUES ('/a','/a',1,'h','s1','s2',1,'now')"""
        )
    store = FakeStore()
    statefile.push(state, runtime, paths, store, label="7-2")
    assert set(store.objects) == {
        statefile.STATE_KEY,
        statefile.HISTORY_PREFIX + "7-2.sqlite.xz.age",
    }
    raw = lzma.decompress(store.objects[statefile.STATE_KEY])
    state.close()
    paths.state_db.unlink()
    assert statefile.fetch(runtime, paths, store) == "restored"
    assert paths.state_db.read_bytes() == raw
    restored = State(paths.state_db, cfg.mirror.id)
    assert restored.mirror_totals() == (1, 1)
    restored.close()


def test_push_compresses_in_slices_that_restore_byte_for_byte(
    state_context, plain_crypt, monkeypatch
):
    """A 4 KB slice turns the smallest state into several xz streams; one lzma read
    must give back exactly the snapshot."""
    _, paths, state, _, runtime = state_context
    monkeypatch.setattr(statefile, "CHUNK_BYTES", 4096)
    store = FakeStore()
    statefile.push(state, runtime, paths, store, label="1-1")
    state.snapshot_to(paths.root / "expected.sqlite")
    expected = (paths.root / "expected.sqlite").read_bytes()
    assert len(expected) > 3 * 4096
    assert lzma.decompress(store.objects[statefile.STATE_KEY]) == expected
    state.close()
    paths.state_db.unlink()
    assert statefile.fetch(runtime, paths, store) == "restored"
    assert paths.state_db.read_bytes() == expected


def test_rollback_copies_history_over_canonical():
    store = FakeStore()
    store.objects[statefile.HISTORY_PREFIX + "1-1.sqlite.xz.age"] = b"old"
    store.objects[statefile.STATE_KEY] = b"bad"
    statefile.rollback(store, statefile.HISTORY_PREFIX + "1-1.sqlite.xz.age")
    assert store.objects[statefile.STATE_KEY] == b"old"

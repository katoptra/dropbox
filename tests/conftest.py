from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from migrator import crypt, session
from migrator.config import Budget, Config, Dropbox, Mirror, Proton, Reconcile
from migrator.env import Runtime
from migrator.logging import RunLogger
from migrator.paths import WorkPaths
from migrator.providers.dropbox_api import DropboxNotFound
from migrator.providers.proton_cli import ProtonCLIError
from migrator.state import State

TEST_ENV = {
    "MIRROR_R2_BUCKET": "test-bucket",
    "MIRROR_AGE_IDENTITY": "AGE-SECRET-KEY-1TEST",
    "MIRROR_DROPBOX_APP_KEY": "app-key",
    "MIRROR_DROPBOX_APP_SECRET": "app-secret",
    "MIRROR_DROPBOX_REFRESH_TOKEN": "refresh-token",
    "AWS_ACCESS_KEY_ID": "test-access-key",
    "AWS_SECRET_ACCESS_KEY": "test-secret-key",
    "AWS_ENDPOINT_URL_S3": "https://s3.example",
}


@pytest.fixture
def runtime_factory():
    def make(tmp_path: Path, **overrides) -> Runtime:
        env = {
            **TEST_ENV,
            "MIRROR_WORK_DIR": str(tmp_path / "run"),
            "MIRROR_CONFIG": str(tmp_path / "mirror.toml"),
            "MIRROR_RUN_EPOCH": "1700000000",
        }
        env.update({k: str(v) for k, v in overrides.items()})
        return Runtime.from_environ(env)

    return make


@pytest.fixture
def config_factory():
    def make(tmp_path: Path, **overrides) -> Config:
        source = tmp_path / "mirror.toml"
        source.write_text("# test\n", encoding="utf-8")
        return Config(
            mirror=Mirror("test"),
            dropbox=overrides.get(
                "dropbox",
                Dropbox(
                    expected_account_id="dbid:test-account",
                    minimum_call_interval_seconds=0,
                    max_attempts=3,
                    initial_backoff_seconds=0,
                ),
            ),
            proton=overrides.get(
                "proton",
                Proton(
                    expected_destination_uid="uid-destination",
                    initial_backoff_seconds=0,
                ),
            ),
            budget=overrides.get("budget", Budget()),
            reconcile=overrides.get("reconcile", Reconcile()),
            source_file=source,
            source_sha256="test-config-hash",
        )

    return make


@pytest.fixture
def state_context(tmp_path, config_factory, runtime_factory):
    cfg = config_factory(tmp_path)
    runtime = runtime_factory(tmp_path)
    paths = WorkPaths.from_runtime(runtime)
    paths.ensure()
    state = State(paths.state_db, cfg.mirror.id)
    state.initialize_migration(cfg.source_file, cfg.source_sha256)
    logger = RunLogger(
        paths.logs, secrets=runtime.secrets(), console=False, sink=state.record_event
    )
    try:
        yield cfg, paths, state, logger, runtime
    finally:
        state.close()


class FakeStore:
    """In-memory stand-in for migrator.store.Store."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def get(self, key, target: Path) -> bool:
        if key not in self.objects:
            return False
        target.write_bytes(self.objects[key])
        return True

    def put(self, source: Path, key) -> None:
        self.objects[key] = source.read_bytes()

    def copy(self, s, t):
        self.objects[t] = self.objects[s]

    def list(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix))

    def probe(self):
        pass


class FakeDropbox:
    """Stand-in for DropboxAPIProvider.download: canned bytes keyed by path_lower."""

    def __init__(self, files: dict[str, bytes], missing=()):
        self.files = {k.lower(): v for k, v in files.items()}
        self.missing = {m.lower() for m in missing}
        self.downloaded: list[str] = []

    def download(self, path_lower: str, target: Path) -> list:
        self.downloaded.append(path_lower)
        key = path_lower.lower()
        if key in self.missing:
            raise DropboxNotFound(path_lower)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.files[key])
        return []


@pytest.fixture
def plain_crypt(monkeypatch):
    # age is exercised in the image build; here encryption is identity so tar bytes are inspectable.
    monkeypatch.setattr(
        crypt,
        "encrypt",
        lambda identity, key_file, source, target, run=None: target.write_bytes(
            source.read_bytes()
        ),
    )
    monkeypatch.setattr(
        crypt,
        "decrypt",
        lambda identity, key_file, source, target, run=None: target.write_bytes(
            source.read_bytes()
        ),
    )
    monkeypatch.setattr(session, "_last_digest", None)


def seed_api_inventory(state, purpose, rows):
    """rows: (path_display, size, content_hash, is_downloadable, tag). Returns the inventory id."""
    with state.connection:
        cursor = state.connection.execute(
            """INSERT INTO dropbox_inventory_runs(started_at, completed_at, status, account_id,
               root_namespace_id, purpose) VALUES ('now','now','COMPLETE','dbid:test-account','ns',?)""",
            (purpose,),
        )
        inventory_id = int(cursor.lastrowid)
        for path, size, content_hash, downloadable, tag in rows:
            state.connection.execute(
                """INSERT INTO dropbox_objects(inventory_id, object_key, tag, name, path_display, path_lower,
                   comparison_key, size, content_hash, is_downloadable, raw_json, first_page, last_page)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', 1, 1)""",
                (
                    inventory_id,
                    path.lower(),
                    tag,
                    path.rsplit("/", 1)[-1],
                    path,
                    path.lower(),
                    path.lower(),
                    size,
                    content_hash,
                    downloadable,
                ),
            )
    return inventory_id


class FakeProton:
    """Stand-in for ProtonCLIProvider: canned listings, an `upload_tree` that reports on
    what it was actually handed. `skip` and `fail` are file names the CLI would report as
    content-identical or refused; set them on the instance before calling `upload_tree`."""

    def __init__(self, listings: dict[str, list[dict]], fail_list=()):
        self.listings = listings
        self.fail_list = set(fail_list)
        self.skip: set[str] = set()
        self.fail: set[str] = set()
        self.uploads = []

    def root_uid(self, phase):
        return "uid-destination"

    def upload_trees(self, groups, destination, phase):
        return [self.upload_tree(group, destination, phase) for group in groups]

    def upload_tree(self, sources, destination, phase, env=None, writeback=True):
        self.uploads.append(([str(s) for s in sources], destination))
        counts = Counter()
        failures = []

        def walk(path: Path) -> None:
            if path.is_dir():
                counts["transferredItems"] += 1
                for child in sorted(path.iterdir()):
                    walk(child)
                return
            if path.name in self.fail:
                counts["failedItems"] += 1
                failures.append(
                    {
                        "name": path.name,
                        "error": "ServerError: refused",
                        "nodeUid": None,
                    }
                )
            elif path.name in self.skip:
                counts["skippedItems"] += 1
            else:
                counts["transferredItems"] += 1
                counts["transferredBytes"] += path.stat().st_size

        for source in sources:
            walk(source)
        return json.dumps(
            {
                "transferredItems": counts["transferredItems"],
                "transferredBytes": counts["transferredBytes"],
                "skippedItems": counts["skippedItems"],
                "failedItems": counts["failedItems"],
                "failures": failures,
            }
        )

    def list_folder(self, path, phase):
        if path in self.fail_list:
            raise ProtonCLIError("EXIT_1")
        return self.listings[path]


def proton_node(uid, name, size, sha1, kind="file"):
    """One entry as `proton-drive filesystem list -j` returns it."""
    return {
        "uid": uid,
        "name": {"ok": True, "value": name},
        "type": kind,
        "activeRevision": {
            "claimedSize": size,
            "claimedDigests": {"sha1": sha1, "sha1Verified": True},
        },
    }

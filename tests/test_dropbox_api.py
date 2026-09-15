from __future__ import annotations

import json
from dataclasses import replace

import pytest
import requests

from migrator.logging import RunLogger
from migrator.providers.dropbox_api import (
    DropboxAPIError,
    DropboxAPIProvider,
    DropboxNotFound,
)
from migrator.state import State


class Response:
    def __init__(self, status, payload=None, text="", headers=None, content=b""):
        self.status_code = status
        self._payload = payload
        self.text = text
        self.headers = headers or {}
        self._content = content

    def json(self):
        return self._payload

    def iter_content(self, chunk_size):
        for offset in range(0, len(self._content), chunk_size):
            yield self._content[offset : offset + chunk_size]


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []

    def post(self, url, **kwargs):
        self.urls.append(url)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def test_api_retries_429_and_504(state_context):
    cfg, _paths, state, logger, _runtime = state_context
    cfg = replace(
        cfg,
        dropbox=replace(
            cfg.dropbox,
            max_attempts=4,
            initial_backoff_seconds=2,
            maximum_backoff_seconds=10,
        ),
    )
    session = Session(
        [
            Response(429, text="limited", headers={"Retry-After": "7"}),
            Response(504, text="gateway timeout"),
            Response(200, {"ok": True}),
        ]
    )
    sleeps = []
    provider = DropboxAPIProvider(
        cfg, state, logger, token="test-token", session=session, sleep=sleeps.append
    )
    assert provider._call("test", {}) == {"ok": True}
    assert sleeps == [7.0, 2]


def test_interrupted_inventory_resumes_from_committed_cursor(tmp_path, config_factory):
    cfg = config_factory(
        tmp_path,
        dropbox=replace(
            config_factory(tmp_path).dropbox,
            max_attempts=1,
            initial_backoff_seconds=0,
        ),
    )
    state = State(tmp_path / "state.sqlite", cfg.mirror.id)
    state.initialize_migration(cfg.source_file, cfg.source_sha256)
    token = "test-token"
    logger = RunLogger(tmp_path / "logs", secrets=[token], console=False)
    account = {
        "account_id": "dbid:test-account",
        "name": {"display_name": "Test"},
        "root_info": {"root_namespace_id": "root:test"},
    }
    first_page = {
        "entries": [
            {
                ".tag": "file",
                "name": "one",
                "path_display": "/one",
                "path_lower": "/one",
                "id": "id:one",
                "size": 1,
                "content_hash": "hash-one",
            }
        ],
        "cursor": "cursor-1",
        "has_more": True,
    }
    first_session = Session(
        [
            Response(200, account),
            Response(200, first_page),
            requests.ConnectionError("interrupted"),
        ]
    )
    with pytest.raises(DropboxAPIError):
        DropboxAPIProvider(
            cfg, state, logger, token=token, session=first_session, sleep=lambda _: None
        ).inventory()
    run = state.connection.execute("SELECT * FROM dropbox_inventory_runs").fetchone()
    assert run["cursor"] == "cursor-1"
    assert run["page_count"] == 1

    second_page = {
        "entries": [
            {
                ".tag": "file",
                "name": "two",
                "path_display": "/two",
                "path_lower": "/two",
                "id": "id:two",
                "size": 2,
                "content_hash": "hash-two",
            }
        ],
        "cursor": "cursor-2",
        "has_more": False,
    }
    second_session = Session([Response(200, account), Response(200, second_page)])
    inventory_id = DropboxAPIProvider(
        cfg, state, logger, token=token, session=second_session, sleep=lambda _: None
    ).inventory()
    complete = state.connection.execute(
        "SELECT * FROM dropbox_inventory_runs WHERE id=?", (inventory_id,)
    ).fetchone()
    count = state.connection.execute(
        "SELECT COUNT(*) AS count FROM dropbox_objects WHERE inventory_id=?",
        (inventory_id,),
    ).fetchone()["count"]
    assert complete["status"] == "COMPLETE"
    assert count == 2
    raw = {
        r[0]
        for r in state.connection.execute(
            "SELECT raw_json FROM dropbox_objects WHERE inventory_id=?", (inventory_id,)
        )
    }
    assert raw == {"{}"}  # nothing reads the entry's raw JSON back
    assert second_session.urls[-1].endswith("/files/list_folder/continue")
    state.close()


def test_download_writes_the_response_body_to_the_target(state_context, tmp_path):
    cfg, _paths, state, logger, _runtime = state_context
    session = Session([Response(200, content=b"hello world")])
    provider = DropboxAPIProvider(
        cfg, state, logger, token="test-token", session=session, sleep=lambda _: None
    )
    target = tmp_path / "out" / "a.txt"
    provider.download("/a.txt", target)
    assert target.read_bytes() == b"hello world"
    assert not target.with_name("a.txt.part").exists()
    assert session.urls[-1].endswith("/files/download")


def test_download_raises_not_found_for_a_missing_path(state_context, tmp_path):
    cfg, _paths, state, logger, _runtime = state_context
    body = {
        "error_summary": "path/not_found/",
        "error": {".tag": "path", "path": {".tag": "not_found"}},
    }
    session = Session([Response(409, body, text=json.dumps(body))])
    provider = DropboxAPIProvider(
        cfg, state, logger, token="test-token", session=session, sleep=lambda _: None
    )
    with pytest.raises(DropboxNotFound):
        provider.download("/gone.txt", tmp_path / "gone.txt")


def test_download_retries_429_then_succeeds(state_context, tmp_path):
    cfg, _paths, state, logger, _runtime = state_context
    cfg = replace(
        cfg, dropbox=replace(cfg.dropbox, max_attempts=3, initial_backoff_seconds=1)
    )
    session = Session(
        [
            Response(429, text="limited", headers={"Retry-After": "5"}),
            Response(200, content=b"payload"),
        ]
    )
    sleeps = []
    provider = DropboxAPIProvider(
        cfg, state, logger, token="test-token", session=session, sleep=sleeps.append
    )
    target = tmp_path / "a.txt"
    provider.download("/a.txt", target)
    assert target.read_bytes() == b"payload"
    assert sleeps == [5.0]


def test_download_retries_when_the_body_drops_mid_stream(state_context, tmp_path):
    cfg, _paths, state, logger, _runtime = state_context
    cfg = replace(
        cfg, dropbox=replace(cfg.dropbox, max_attempts=3, initial_backoff_seconds=1)
    )

    class Dropping(Response):
        def iter_content(self, chunk_size):
            yield b"half"
            raise requests.ConnectionError("connection reset")

    session = Session([Dropping(200), Response(200, content=b"payload")])
    provider = DropboxAPIProvider(
        cfg, state, logger, token="test-token", session=session, sleep=lambda _: None
    )
    target = tmp_path / "a.txt"
    retries = provider.download("/a.txt", target)
    assert target.read_bytes() == b"payload"
    assert not target.with_name("a.txt.part").exists()
    assert [r.fields["provider_category"] for r in retries] == ["NETWORK"]

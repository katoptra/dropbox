from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from migrator.providers.proton_cli import (
    ProtonCLIError,
    ProtonCLIProvider,
    child_cli_path,
)


def _fake_run(responses):
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        code, out, err = responses.pop(0)
        return subprocess.CompletedProcess(argv, code, stdout=out, stderr=err)

    return run, calls


def test_upload_tree_argv_and_hook(state_context, tmp_path):
    cfg, _, state, logger, _ = state_context
    run, calls = _fake_run([(0, '{"uploaded":1}\n', "")])
    hooks = []
    provider = ProtonCLIProvider(
        cfg, state, logger, run=run, after_call=lambda: hooks.append(1)
    )
    out = provider.upload_tree(
        [tmp_path / "A", tmp_path / "B"], "/my-files/Dropbox", "40_batches"
    )
    assert out.startswith('{"uploaded"')
    argv = calls[0]
    assert argv[:3] == ["proton-drive", "filesystem", "upload"]
    assert argv[argv.index("-f") + 1] == "create-new-revision"
    assert argv[argv.index("-d") + 1] == "merge"
    assert "--json" in argv and "--skip-thumbnails" in argv
    assert argv[-3:] == [str(tmp_path / "A"), str(tmp_path / "B"), "/my-files/Dropbox"]
    assert hooks == [1]


def test_upload_tree_partial_failure_exit_code_is_accepted(state_context, tmp_path):
    cfg, _, state, logger, _ = state_context
    run, _ = _fake_run(
        [(1, '{"uploaded":1,"failed":1}\n', "one item could not be uploaded")]
    )
    provider = ProtonCLIProvider(cfg, state, logger, run=run)
    out = provider.upload_tree([tmp_path / "A"], "/my-files/Dropbox", "40_batches")
    assert out.startswith('{"uploaded"')


def test_upload_tree_failure_raises_and_still_hooks(state_context, tmp_path):
    cfg, _, state, logger, _ = state_context
    run, calls = _fake_run([(1, "", "You need to login first")])
    hooks = []
    provider = ProtonCLIProvider(
        cfg, state, logger, run=run, after_call=lambda: hooks.append(1)
    )
    with pytest.raises(ProtonCLIError, match="AUTH"):
        provider.upload_tree([tmp_path / "A"], "/my-files/Dropbox", "40_batches")
    assert hooks == [1] and len(calls) == 1  # a dead session is never retried


def test_mutation_retries_a_transient_failure_then_gives_up(state_context, tmp_path):
    cfg, _, state, logger, _ = state_context
    run, calls = _fake_run([(2, "", "ServerError: 503"), (0, '{"uploaded":1}\n', "")])
    slept = []
    provider = ProtonCLIProvider(cfg, state, logger, run=run, sleep=slept.append)
    out = provider.upload_tree([tmp_path / "A"], "/my-files/Dropbox", "40_batches")
    assert out.startswith('{"uploaded"') and len(calls) == 2
    assert slept == [cfg.proton.initial_backoff_seconds]
    rows = state.connection.execute(
        "SELECT attempt, response_category FROM commands ORDER BY id"
    ).fetchall()
    assert [tuple(r) for r in rows] == [(1, "EXIT_2"), (2, "SUCCESS")]
    run, calls = _fake_run([(2, "", "ServerError: 503")] * 5)
    provider = ProtonCLIProvider(cfg, state, logger, run=run, sleep=slept.append)
    with pytest.raises(ProtonCLIError, match="EXIT_2"):
        provider.trash(["/my-files/Dropbox/x"], "50_trash")
    assert len(calls) == cfg.proton.mutation_max_attempts


def test_list_stops_retrying_on_an_auth_failure(state_context):
    cfg, _, state, logger, _ = state_context
    run, calls = _fake_run([(1, "", "You need to login first")] * 8)
    provider = ProtonCLIProvider(cfg, state, logger, run=run, sleep=lambda _: None)
    with pytest.raises(ProtonCLIError, match="AUTH"):
        provider.list_folder("/my-files/Dropbox", "40_batches")
    assert len(calls) == 1


def test_upload_trees_runs_groups_at_once_and_rescues_a_signed_out_one(
    state_context, tmp_path
):
    """Group B's refresh loses the race: the CLI signs its copy out. Group A's copy
    holds the refreshed token, so it is adopted, every copy re-seeded, B re-run, and
    the main session ends up holding the refreshed token."""
    cfg, _, state, logger, _ = state_context
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    (session_dir / "auth-session.json").write_text("old")
    seen = []

    def run(argv, env=None, **kwargs):
        copy = Path(env["PROTON_DRIVE_CACHE_DIR"])
        group = argv[-2].rsplit("/", 1)[-1]
        seen.append((group, (copy / "auth-session.json").read_text()))
        if group == "A":
            (copy / "auth-session.json").write_text("refreshed")
            return subprocess.CompletedProcess(argv, 0, stdout='{"a":1}\n', stderr="")
        if (copy / "auth-session.json").read_text() == "old":
            (copy / "auth-session.json").unlink()
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr="login first")
        return subprocess.CompletedProcess(argv, 0, stdout='{"b":1}\n', stderr="")

    hooks = []
    provider = ProtonCLIProvider(
        cfg,
        state,
        logger,
        run=run,
        sleep=lambda _: None,
        after_call=lambda: hooks.append(1),
        session_dir=session_dir,
    )
    out = provider.upload_trees(
        [[tmp_path / "A"], [tmp_path / "B"]], "/my-files/Dropbox", "40_batches"
    )
    assert out == ['{"a":1}\n', '{"b":1}\n']
    assert sorted(seen) == [("A", "old"), ("B", "old"), ("B", "refreshed")]
    assert (session_dir / "auth-session.json").read_text() == "refreshed"
    assert hooks == [1]  # written back once, on the calling thread, after adoption
    rows = state.connection.execute(
        "SELECT response_category FROM commands ORDER BY id"
    ).fetchall()
    assert sorted(r[0] for r in rows) == ["AUTH", "SUCCESS", "SUCCESS"]


def test_upload_trees_raises_when_a_group_fails_for_good(state_context, tmp_path):
    cfg, _, state, logger, _ = state_context

    def run(argv, **kwargs):  # A lands, B refuses every attempt
        if argv[-2].endswith("/A"):
            return subprocess.CompletedProcess(argv, 0, stdout="{}\n", stderr="")
        return subprocess.CompletedProcess(argv, 2, stdout="", stderr="ServerError")

    provider = ProtonCLIProvider(cfg, state, logger, run=run, sleep=lambda _: None)
    with pytest.raises(ProtonCLIError, match="EXIT_2"):
        provider.upload_trees(
            [[tmp_path / "A"], [tmp_path / "B"]], "/my-files/Dropbox", "40_batches"
        )


def test_trash_passes_every_path_in_one_call(state_context):
    cfg, _, state, logger, _ = state_context
    run, calls = _fake_run([(0, "", "")])
    provider = ProtonCLIProvider(cfg, state, logger, run=run)
    provider.trash(["/my-files/Dropbox/a", "/my-files/Dropbox/b"], "50_trash")
    assert calls[0] == [
        "proton-drive",
        "filesystem",
        "trash",
        "/my-files/Dropbox/a",
        "/my-files/Dropbox/b",
    ]


def test_trash_with_no_paths_does_not_call_run(state_context):
    cfg, _, state, logger, _ = state_context
    run, calls = _fake_run([])
    provider = ProtonCLIProvider(cfg, state, logger, run=run)
    provider.trash([], "50_trash")
    assert calls == []


def test_empty_trash_argv(state_context):
    cfg, _, state, logger, _ = state_context
    run, calls = _fake_run([(0, "", "")])
    provider = ProtonCLIProvider(cfg, state, logger, run=run)
    provider.empty_trash("60_empty_trash")
    assert calls[0] == ["proton-drive", "filesystem", "empty-trash"]


def test_root_uid_matches_expected(state_context):
    cfg, _, state, logger, _ = state_context
    listing = json.dumps(
        [
            {
                "uid": "uid-destination",
                "name": {"ok": True, "value": "Dropbox"},
                "type": "folder",
            }
        ]
    )
    run, calls = _fake_run([(0, listing, "")])
    provider = ProtonCLIProvider(cfg, state, logger, run=run)
    assert provider.root_uid("00_probe") == "uid-destination"
    assert calls[0][-1] == "/my-files"


def test_root_uid_mismatch_raises(state_context):
    cfg, _, state, logger, _ = state_context
    listing = json.dumps(
        [{"uid": "other", "name": {"ok": True, "value": "Dropbox"}, "type": "folder"}]
    )
    run, _ = _fake_run([(0, listing, "")])
    provider = ProtonCLIProvider(cfg, state, logger, run=run)
    with pytest.raises(ProtonCLIError, match="did not exactly match"):
        provider.root_uid("00_probe")


def test_child_cli_path_escapes_or_uses_uid():
    assert (
        child_cli_path("/my-files/Dropbox", "a/b.txt", "u1", duplicate=False)
        == "/my-files/Dropbox/a\\/b.txt"
    )
    assert (
        child_cli_path("/my-files/Dropbox/", "x", "u1", duplicate=True)
        == "/my-files/Dropbox/u1"
    )


def test_upload_timeout_keeps_the_stderr_tail_as_evidence(state_context, tmp_path):
    cfg, _, state, logger, _ = state_context
    hooks = []

    def run(argv, **kwargs):
        raise subprocess.TimeoutExpired(
            argv, kwargs["timeout"], stderr="retrying after 429\nstill waiting\n"
        )

    provider = ProtonCLIProvider(
        cfg,
        state,
        logger,
        run=run,
        sleep=lambda _: None,
        after_call=lambda: hooks.append(1),
    )
    with pytest.raises(ProtonCLIError, match="timed out"):
        provider.upload_tree([tmp_path / "A"], "/my-files/Dropbox", "40_batches")
    # the session is written back after every attempt, even when the CLI is killed
    assert hooks == [1] * cfg.proton.mutation_max_attempts
    row = state.connection.execute(
        "SELECT message, safe_raw_error FROM events WHERE level='ERROR' ORDER BY id DESC"
    ).fetchone()
    assert "timed out after" in row["message"]
    assert "429" in row["safe_raw_error"]
    command = state.connection.execute(
        "SELECT exit_code, response_category FROM commands ORDER BY id DESC"
    ).fetchone()
    assert (command["exit_code"], command["response_category"]) == (-1, "TIMEOUT")

from __future__ import annotations

from pathlib import Path

import pytest

from migrator.env import Runtime

BASE = {
    "MIRROR_R2_BUCKET": "b",
    "MIRROR_AGE_IDENTITY": "AGE-SECRET-KEY-1TEST",
    "MIRROR_DROPBOX_APP_KEY": "k",
    "MIRROR_DROPBOX_APP_SECRET": "s",
    "MIRROR_DROPBOX_REFRESH_TOKEN": "r",
    "AWS_ACCESS_KEY_ID": "ak",
    "AWS_SECRET_ACCESS_KEY": "r2s",
    "AWS_ENDPOINT_URL_S3": "https://accountid.r2.cloudflarestorage.com",
}


def test_defaults_and_secrets():
    runtime = Runtime.from_environ(BASE)
    assert runtime.work_dir == Path(".run")
    assert runtime.config_path == Path("config/mirror.toml")
    assert runtime.run_epoch is None
    assert runtime.budget_override is None
    assert set(runtime.secrets()) == {
        "AGE-SECRET-KEY-1TEST",
        "s",
        "r",
        "ak",
        "r2s",
        "https://accountid.r2.cloudflarestorage.com",
    }


def test_account_identifiers_are_secrets_and_redacted():
    env = {
        "MIRROR_DROPBOX_ACCOUNT_ID": "dbid:real-account",
        "MIRROR_PROTON_DESTINATION_UID": "uid-real-folder",
        "AWS_ACCESS_KEY_ID": "AKIAREALKEY",
    }
    runtime = Runtime.from_environ(env)
    assert {"dbid:real-account", "uid-real-folder"} <= set(runtime.secrets())
    text = "expected dbid:real-account under uid-real-folder at AKIAREALKEY"
    assert runtime.redact(text) == "expected [redacted] under [redacted] at [redacted]"


def test_overrides_and_host():
    env = {
        **BASE,
        "MIRROR_RUN_EPOCH": "1700000000",
        "RUN_BUDGET_MIN": "30",
        "GITHUB_RUN_ID": "42",
        "MIRROR_WORK_DIR": "/tmp/w",
    }
    runtime = Runtime.from_environ(env)
    assert runtime.run_epoch == 1700000000
    assert runtime.budget_override == 30
    assert runtime.host == "github:42"
    assert runtime.work_dir == Path("/tmp/w")


def test_missing_secret_is_empty_not_error():
    runtime = Runtime.from_environ({})
    assert runtime.r2_bucket == ""
    assert runtime.secrets() == []


def test_bad_epoch_rejected():
    with pytest.raises(ValueError, match="MIRROR_RUN_EPOCH"):
        Runtime.from_environ({**BASE, "MIRROR_RUN_EPOCH": "noon"})

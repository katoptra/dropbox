from __future__ import annotations

import pytest

from migrator.config import ConfigError, load_config

GOOD = """
[mirror]
id = "test"
[dropbox]
expected_account_id = "dbid:abc"
[proton]
expected_destination_uid = "uid-12345678"
"""


def _write(tmp_path, text):
    path = tmp_path / "mirror.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_defaults_and_derived_bytes(tmp_path):
    cfg = load_config(_write(tmp_path, GOOD))
    assert cfg.dropbox.content_base_url == "https://content.dropboxapi.com/2"
    assert cfg.dropbox.download_workers == 4
    assert cfg.budget.batch_gb == 4
    assert cfg.budget.batch_files == 2000
    assert cfg.budget.batch_bytes == 4 * 1024**3
    assert cfg.budget.run_budget_minutes == 165
    assert cfg.budget.ceiling_gb == 4000
    assert cfg.proton.destination == "/my-files/Dropbox"
    assert cfg.proton.walk_workers == 8


def test_unknown_key_rejected(tmp_path):
    with pytest.raises(ConfigError, match="unknown keys"):
        load_config(_write(tmp_path, GOOD + "\n[budget]\nmax_batches = 4\n"))


def test_unknown_table_rejected(tmp_path):
    with pytest.raises(ConfigError, match="unknown top-level"):
        load_config(_write(tmp_path, GOOD + "\n[safety]\nx = 1\n"))


def test_required_identity_guards(tmp_path):
    with pytest.raises(ConfigError, match="expected_account_id"):
        load_config(_write(tmp_path, GOOD.replace('"dbid:abc"', '""')))
    with pytest.raises(ConfigError, match="expected_destination_uid"):
        load_config(_write(tmp_path, GOOD.replace('"uid-12345678"', '"short"')))


def test_numeric_floors(tmp_path):
    with pytest.raises(ConfigError, match="batch_gb"):
        load_config(_write(tmp_path, GOOD + "\n[budget]\nbatch_gb = 0\n"))
    with pytest.raises(ConfigError, match="listing_floor_ratio"):
        load_config(_write(tmp_path, GOOD + "\n[budget]\nlisting_floor_ratio = 1.5\n"))
    with pytest.raises(ConfigError, match="walk_workers"):
        load_config(_write(tmp_path, GOOD + "\nwalk_workers = 0\n"))
    with pytest.raises(ConfigError, match="walk_workers"):
        load_config(_write(tmp_path, GOOD + "\nwalk_workers = 33\n"))


BARE = """
[mirror]
id = "test"
"""

ACCOUNT_ENV = {
    "MIRROR_DROPBOX_ACCOUNT_ID": "dbid:from-vault",
    "MIRROR_PROTON_DESTINATION": "/my-files/Elsewhere",
    "MIRROR_PROTON_DESTINATION_UID": "uid-from-vault",
}


def test_account_values_come_from_the_environment(tmp_path):
    cfg = load_config(_write(tmp_path, BARE), environ=ACCOUNT_ENV)
    assert cfg.dropbox.expected_account_id == "dbid:from-vault"
    assert cfg.proton.destination == "/my-files/Elsewhere"
    assert cfg.proton.expected_destination_uid == "uid-from-vault"


def test_environment_overrides_the_file(tmp_path):
    cfg = load_config(_write(tmp_path, GOOD), environ=ACCOUNT_ENV)
    assert cfg.dropbox.expected_account_id == "dbid:from-vault"
    assert cfg.proton.expected_destination_uid == "uid-from-vault"


def test_missing_account_id_names_the_variable(tmp_path):
    with pytest.raises(ConfigError, match="MIRROR_DROPBOX_ACCOUNT_ID"):
        load_config(_write(tmp_path, BARE), environ={})


def test_missing_destination_uid_names_the_variable(tmp_path):
    env = {"MIRROR_DROPBOX_ACCOUNT_ID": "dbid:from-vault"}
    with pytest.raises(ConfigError, match="MIRROR_PROTON_DESTINATION_UID"):
        load_config(_write(tmp_path, BARE), environ=env)

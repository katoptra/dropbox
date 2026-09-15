from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


def _int(environ: Mapping[str, str], name: str) -> int | None:
    raw = environ.get(name, "")
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Runtime:
    work_dir: Path
    config_path: Path
    run_epoch: int | None
    budget_override: int | None
    upload_workers: int | None
    reconcile: bool
    verbose: bool
    r2_bucket: str
    age_identity: str
    dropbox_app_key: str
    dropbox_app_secret: str
    dropbox_refresh_token: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_endpoint_url: str
    dropbox_account_id: str
    proton_destination_uid: str
    host: str

    @classmethod
    def from_environ(cls, environ: Mapping[str, str] | None = None) -> Runtime:
        env = os.environ if environ is None else environ
        run_id = env.get("GITHUB_RUN_ID", "")
        return cls(
            work_dir=Path(env.get("MIRROR_WORK_DIR", ".run")),
            config_path=Path(env.get("MIRROR_CONFIG", "config/mirror.toml")),
            run_epoch=_int(env, "MIRROR_RUN_EPOCH"),
            budget_override=_int(env, "RUN_BUDGET_MIN"),
            upload_workers=_int(env, "UPLOAD_WORKERS"),
            reconcile=env.get("RECONCILE", "").lower() == "true",
            verbose=env.get("MIRROR_VERBOSE", "") == "1",
            r2_bucket=env.get("MIRROR_R2_BUCKET", ""),
            age_identity=env.get("MIRROR_AGE_IDENTITY", ""),
            dropbox_app_key=env.get("MIRROR_DROPBOX_APP_KEY", ""),
            dropbox_app_secret=env.get("MIRROR_DROPBOX_APP_SECRET", ""),
            dropbox_refresh_token=env.get("MIRROR_DROPBOX_REFRESH_TOKEN", ""),
            aws_access_key_id=env.get("AWS_ACCESS_KEY_ID", ""),
            aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY", ""),
            aws_endpoint_url=env.get("AWS_ENDPOINT_URL_S3", ""),
            dropbox_account_id=env.get("MIRROR_DROPBOX_ACCOUNT_ID", ""),
            proton_destination_uid=env.get("MIRROR_PROTON_DESTINATION_UID", ""),
            host=f"github:{run_id}" if run_id else socket.gethostname(),
        )

    def secrets(self) -> list[str]:
        """Every value that must never reach a log: credentials, and the
        identifiers that name the accounts (the repo and its logs are public)."""
        values = (
            self.age_identity,
            self.dropbox_app_secret,
            self.dropbox_refresh_token,
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.aws_endpoint_url,
            self.dropbox_account_id,
            self.proton_destination_uid,
        )
        return [value for value in values if value]

    def redact(self, text: str) -> str:
        for value in sorted(self.secrets(), key=len, reverse=True):
            text = text.replace(value, "[redacted]")
        return text

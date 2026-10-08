from __future__ import annotations

import os
import subprocess
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path


class CryptError(RuntimeError):
    pass


@contextmanager
def _identity_file(identity: str, key_file: Path):
    # ponytail: age reads an identity only from a file. This file is on the temporary
    # disk of the runner, with mode 0600, during one call. Then this function removes
    # it.
    if not identity:
        raise CryptError("required secret is unset: MIRROR_AGE_IDENTITY")
    fd = os.open(key_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(identity.strip() + "\n")
    try:
        yield key_file
    finally:
        key_file.unlink(missing_ok=True)


def _check(result: subprocess.CompletedProcess[str], what: str) -> None:
    if result.returncode:
        raise CryptError(f"age {what} failed with exit {result.returncode}")


def recipient(
    identity: str,
    key_file: Path,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str:
    with _identity_file(identity, key_file) as path:
        result = run(
            ["age-keygen", "-y", str(path)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=60,
        )
    _check(result, "keygen")
    return result.stdout.strip()


def encrypt(
    identity: str,
    key_file: Path,
    source: Path,
    target: Path,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> None:
    public = recipient(identity, key_file, run=run)
    result = run(
        ["age", "-r", public, "-o", str(target), str(source)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=600,
    )
    _check(result, "encrypt")


def decrypt(
    identity: str,
    key_file: Path,
    source: Path,
    target: Path,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> None:
    with _identity_file(identity, key_file) as path:
        result = run(
            ["age", "-d", "-i", str(path), "-o", str(target), str(source)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=600,
        )
    _check(result, "decrypt")

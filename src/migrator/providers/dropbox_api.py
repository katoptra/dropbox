from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from ..config import Config, Dropbox
from ..filesystem import comparison_key
from ..guards import dropbox_api_scope
from ..logging import RunLogger, utc_now
from ..state import State

DOWNLOAD_CHUNK_SIZE = 1024 * 1024


class DropboxAPIError(RuntimeError):
    pass


class DropboxNotFound(DropboxAPIError):
    pass


@dataclass(frozen=True)
class DropboxIdentity:
    account_id: str
    display_name: str
    root_namespace_id: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class RetryEvent:
    """A download retry, carrying the logger keywords the caller passes on."""

    message: str
    fields: dict[str, Any]


class DropboxAPIProvider:
    def __init__(
        self,
        cfg: Config,
        state: State,
        logger: RunLogger,
        *,
        token: str,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.cfg = cfg
        self.state = state
        self.logger = logger
        self.session = session or requests.Session()
        self.sleep = sleep
        self.token = token
        self._rate_lock = threading.Lock()

    def _throttle(self) -> None:
        """Serializes the pacing sleep so concurrent downloads share one call rate."""
        interval = self.cfg.dropbox.minimum_call_interval_seconds
        if interval:
            with self._rate_lock:
                self.sleep(interval)

    def _call(
        self,
        endpoint: str,
        payload: dict[str, Any] | None,
        *,
        root_namespace_id: str | None = None,
    ) -> dict[str, Any]:
        settings = self.cfg.dropbox
        url = settings.api_base_url.rstrip("/") + "/" + endpoint
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        if root_namespace_id:
            headers["Dropbox-API-Path-Root"] = json.dumps(
                {".tag": "root", "root": root_namespace_id}
            )

        delay = settings.initial_backoff_seconds
        for attempt in range(1, settings.max_attempts + 1):
            try:
                response = self.session.post(
                    url,
                    headers=headers,
                    data=json.dumps(payload) if payload is not None else "null",
                    timeout=settings.timeout_seconds,
                )
            except requests.RequestException as exc:
                category = "NETWORK"
                self.logger.warning(
                    "10_inventory",
                    endpoint,
                    "Dropbox network request failed",
                    retry_count=attempt,
                    provider_category=category,
                    raw_error=str(exc),
                )
                if attempt == settings.max_attempts:
                    raise DropboxAPIError(
                        f"Dropbox {endpoint} exhausted network retries"
                    ) from exc
                self.sleep(delay)
                delay = min(delay * 2, settings.maximum_backoff_seconds)
                continue

            if response.status_code == 200:
                try:
                    result = response.json()
                except ValueError as exc:
                    raise DropboxAPIError(
                        f"Dropbox {endpoint} returned invalid JSON"
                    ) from exc
                self._throttle()
                return result

            safe_error = response.text[-4000:]
            if response.status_code == 429:
                category = "RATE_LIMIT"
                raw_retry = response.headers.get("Retry-After", "60")
                try:
                    wait = max(0.0, float(raw_retry))
                except ValueError:
                    wait = 60.0
            elif response.status_code in {500, 502, 503, 504}:
                category = f"HTTP_{response.status_code}"
                wait = delay
                delay = min(delay * 2, settings.maximum_backoff_seconds)
            else:
                raise DropboxAPIError(
                    f"Dropbox {endpoint} failed with HTTP {response.status_code}: "
                    f"{safe_error}"
                )

            self.logger.warning(
                "10_inventory",
                endpoint,
                "Dropbox request will be retried",
                retry_count=attempt,
                provider_category=category,
                raw_error=safe_error,
                wait_seconds=wait,
            )
            if attempt == settings.max_attempts:
                raise DropboxAPIError(
                    f"Dropbox {endpoint} exhausted retries after {category}"
                )
            self.sleep(wait)

        raise AssertionError("unreachable Dropbox retry loop")

    def download(self, path_lower: str, target: Path) -> list[RetryEvent]:
        """Returns the retries it took, for the caller to log. Downloads run in a thread
        pool and the logger's sink writes SQLite from the thread that opened it, so a
        pooled download records nothing itself."""
        settings = self.cfg.dropbox
        url = settings.content_base_url.rstrip("/") + "/files/download"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Dropbox-API-Arg": json.dumps({"path": path_lower}),
            "Content-Type": "text/plain",
        }
        events: list[RetryEvent] = []
        delay = settings.initial_backoff_seconds
        for attempt in range(1, settings.max_attempts + 1):
            try:
                response = self.session.post(
                    url, headers=headers, stream=True, timeout=settings.timeout_seconds
                )
                if response.status_code == 200:
                    _write_download(response, target)
                    self._throttle()
                    return events
            except requests.RequestException as exc:
                # The body streams inside this try, so a drop mid-file retries too; the
                # half-written part file goes before the next attempt reopens it.
                _part_file(target).unlink(missing_ok=True)
                events.append(
                    RetryEvent(
                        "Dropbox download request failed",
                        {
                            "retry_count": attempt,
                            "provider_category": "NETWORK",
                            "raw_error": str(exc),
                        },
                    )
                )
                if attempt == settings.max_attempts:
                    raise DropboxAPIError(
                        "Dropbox download exhausted network retries"
                    ) from exc
                self.sleep(delay)
                delay = min(delay * 2, settings.maximum_backoff_seconds)
                continue

            category, wait, delay = _download_wait(response, delay, settings)
            events.append(
                RetryEvent(
                    "Dropbox download will be retried",
                    {
                        "retry_count": attempt,
                        "provider_category": category,
                        "raw_error": response.text[-4000:],
                        "wait_seconds": wait,
                    },
                )
            )
            if attempt == settings.max_attempts:
                raise DropboxAPIError(
                    f"Dropbox download exhausted retries after {category}"
                )
            self.sleep(wait)

        raise AssertionError("unreachable Dropbox download retry loop")

    def identity(self, operation: str = "identity") -> DropboxIdentity:
        raw = self._call("users/get_current_account", None)
        observed_root_namespace = str(
            raw.get("root_info", {}).get("root_namespace_id", "")
        )
        root_namespace = observed_root_namespace
        account_id = str(raw.get("account_id", ""))
        display_name = str(raw.get("name", {}).get("display_name", ""))
        if not account_id or not observed_root_namespace or not root_namespace:
            raise DropboxAPIError("Dropbox account response lacked stable identifiers")
        expected = self.cfg.dropbox.expected_account_id
        matched = expected == account_id
        observed_at = self.state.record_identity_observation(
            "dropbox",
            operation,
            expected,
            account_id,
            matched=matched,
            details={
                "display_name": display_name,
                "observed_root_namespace_id": observed_root_namespace,
                "target_root_namespace_id": root_namespace,
            },
        )
        if not matched:
            raise DropboxAPIError(
                f"Dropbox account guard failed: expected {expected}, got {account_id}"
            )
        with self.state.connection:
            self.state.connection.execute(
                """
                UPDATE migrations SET
                    source_account_id=?, source_account_display=?,
                    source_root_namespace_id=?, source_account_observed_at=?
                WHERE migration_id=?
                """,
                (
                    account_id,
                    display_name,
                    root_namespace,
                    observed_at,
                    self.state.migration_id,
                ),
            )
        return DropboxIdentity(account_id, display_name, root_namespace, raw)

    def inventory(
        self, purpose: str = "baseline", *, reuse_complete: bool = True
    ) -> int:
        identity = self.identity(f"inventory:{purpose}")
        connection = self.state.connection
        row = connection.execute(
            """
            SELECT * FROM dropbox_inventory_runs
            WHERE account_id=? AND root_namespace_id=?
              AND purpose=?
              AND status IN ('RUNNING', 'COMPLETE')
            ORDER BY id DESC LIMIT 1
            """,
            (identity.account_id, identity.root_namespace_id, purpose),
        ).fetchone()
        if row and row["status"] == "COMPLETE" and reuse_complete:
            return int(row["id"])
        if row and row["status"] == "COMPLETE":
            row = None
        if row:
            inventory_id = int(row["id"])
        else:
            with connection:
                cursor = connection.execute(
                    """
                    INSERT INTO dropbox_inventory_runs(
                        started_at, status, account_id, root_namespace_id, purpose
                    ) VALUES (?, 'RUNNING', ?, ?, ?)
                    """,
                    (
                        utc_now(),
                        identity.account_id,
                        identity.root_namespace_id,
                        purpose,
                    ),
                )
            inventory_id = int(cursor.lastrowid)

        while True:
            run = connection.execute(
                "SELECT * FROM dropbox_inventory_runs WHERE id=?", (inventory_id,)
            ).fetchone()
            page_number = int(run["page_count"]) + 1
            cursor_in = run["cursor"]
            has_more = run["has_more"]

            if cursor_in and has_more == 0:
                with connection:
                    connection.execute(
                        """
                        UPDATE dropbox_inventory_runs
                        SET status='COMPLETE', completed_at=?
                        WHERE id=?
                        """,
                        (utc_now(), inventory_id),
                    )
                break

            if cursor_in:
                result = self._call(
                    "files/list_folder/continue",
                    {"cursor": cursor_in},
                    root_namespace_id=identity.root_namespace_id,
                )
            else:
                result = self._call(
                    "files/list_folder",
                    {
                        "path": dropbox_api_scope(self.cfg.dropbox.root),
                        "recursive": True,
                        "include_deleted": False,
                        "include_mounted_folders": True,
                        "include_non_downloadable_files": True,
                        "limit": self.cfg.dropbox.page_limit,
                    },
                    root_namespace_id=identity.root_namespace_id,
                )

            entries = result.get("entries")
            cursor_out = result.get("cursor")
            if not isinstance(entries, list) or not isinstance(cursor_out, str):
                raise DropboxAPIError("Dropbox list response lacked entries or cursor")
            with connection:
                for entry in entries:
                    self._upsert_entry(inventory_id, page_number, entry)
                connection.execute(
                    """
                    INSERT INTO dropbox_pages(
                        inventory_id, page_number, cursor_in_sha256, cursor_out,
                        entry_count, committed_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        inventory_id,
                        page_number,
                        (
                            hashlib.sha256(cursor_in.encode()).hexdigest()
                            if cursor_in
                            else None
                        ),
                        cursor_out,
                        len(entries),
                        utc_now(),
                    ),
                )
                connection.execute(
                    """
                    UPDATE dropbox_inventory_runs
                    SET cursor=?, has_more=?, page_count=?
                    WHERE id=?
                    """,
                    (
                        cursor_out,
                        int(bool(result.get("has_more"))),
                        page_number,
                        inventory_id,
                    ),
                )
            self.logger.info(
                "10_inventory",
                "dropbox_api_page",
                f"committed Dropbox API page {page_number}",
                object_identifier=str(inventory_id),
                entries=len(entries),
            )

        return inventory_id

    def _upsert_entry(
        self, inventory_id: int, page_number: int, entry: dict[str, Any]
    ) -> None:
        path_display = self._scope_relative(entry.get("path_display"))
        path_lower = self._scope_relative(entry.get("path_lower"))
        object_id = entry.get("id")
        object_key = str(path_lower or f"@id:{object_id}")
        symlink = entry.get("symlink_info") or {}
        self.state.connection.execute(
            """
            INSERT INTO dropbox_objects(
                inventory_id, object_key, tag, name, path_display, path_lower,
                comparison_key, dropbox_id, revision, size, client_modified,
                server_modified, content_hash, is_downloadable, symlink_target,
                export_info_json, raw_json, first_page, last_page
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(inventory_id, object_key) DO UPDATE SET
                tag=excluded.tag,
                name=excluded.name,
                path_display=excluded.path_display,
                path_lower=excluded.path_lower,
                comparison_key=excluded.comparison_key,
                dropbox_id=excluded.dropbox_id,
                revision=excluded.revision,
                size=excluded.size,
                client_modified=excluded.client_modified,
                server_modified=excluded.server_modified,
                content_hash=excluded.content_hash,
                is_downloadable=excluded.is_downloadable,
                symlink_target=excluded.symlink_target,
                export_info_json=excluded.export_info_json,
                raw_json=excluded.raw_json,
                last_page=excluded.last_page
            """,
            (
                inventory_id,
                object_key,
                str(entry.get(".tag", "")),
                str(entry.get("name", "")),
                path_display,
                path_lower,
                comparison_key(str(path_lower or path_display or object_key)),
                object_id,
                entry.get("rev"),
                entry.get("size"),
                entry.get("client_modified"),
                entry.get("server_modified"),
                entry.get("content_hash"),
                int(bool(entry.get("is_downloadable", True))),
                symlink.get("target"),
                json.dumps(entry.get("export_info"), ensure_ascii=False),
                # Every column the pipeline reads is already its own column; the
                # entry's raw API JSON would be half the listing and nothing reads it.
                "{}",
                page_number,
                page_number,
            ),
        )

    def _scope_relative(self, value: Any) -> str | None:
        if value is None:
            return None
        path = str(value)
        scope = self.cfg.dropbox.root
        if not scope:
            return path
        actual = path.lstrip("/").split("/")
        expected = scope.split("/")
        if len(actual) <= len(expected) or any(
            left.casefold() != right.casefold()
            for left, right in zip(actual[: len(expected)], expected, strict=True)
        ):
            raise DropboxAPIError(
                f"Dropbox returned a path outside configured scope: {path!r}"
            )
        return "/" + "/".join(actual[len(expected) :])


def _part_file(target: Path) -> Path:
    return target.with_name(f"{target.name}.part")


def _write_download(response: requests.Response, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _part_file(target)
    with tmp.open("wb") as handle:
        for chunk in response.iter_content(DOWNLOAD_CHUNK_SIZE):
            if chunk:
                handle.write(chunk)
    os.replace(tmp, target)


def _download_wait(
    response: requests.Response, delay: float, settings: Dropbox
) -> tuple[str, float, float]:
    """A retryable status returns (category, wait_seconds, next_delay); anything else
    raises DropboxNotFound or DropboxAPIError directly."""
    safe_error = response.text[-4000:]
    if response.status_code == 409:
        if _path_not_found(response):
            raise DropboxNotFound("Dropbox object not found")
        raise DropboxAPIError(f"Dropbox download failed with HTTP 409: {safe_error}")
    if response.status_code == 429:
        raw_retry = response.headers.get("Retry-After", "60")
        try:
            wait = max(0.0, float(raw_retry))
        except ValueError:
            wait = 60.0
        return "RATE_LIMIT", wait, delay
    if response.status_code in {500, 502, 503, 504}:
        category = f"HTTP_{response.status_code}"
        return category, delay, min(delay * 2, settings.maximum_backoff_seconds)
    raise DropboxAPIError(
        f"Dropbox download failed with HTTP {response.status_code}: {safe_error}"
    )


def _path_not_found(response: requests.Response) -> bool:
    try:
        body = response.json()
    except ValueError:
        return False
    if not isinstance(body, dict):
        return False
    error = body.get("error")
    if not isinstance(error, dict):
        return False
    path = error.get("path")
    return isinstance(path, dict) and str(path.get(".tag", "")) == "not_found"

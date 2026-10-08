from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Event:
    timestamp: str
    level: str
    phase: str
    operation: str
    message: str
    object_identifier: str | None
    retry_count: int | None
    provider_category: str | None
    safe_raw_error: str | None
    fields: dict[str, Any]


class RunLogger:
    def __init__(
        self,
        logs_dir: Path,
        *,
        secrets: list[str] | None = None,
        console: bool = True,
        sink: Callable[[Event], None] | None = None,
    ) -> None:
        self.logs_dir = logs_dir
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        (self.logs_dir / "phases").mkdir(exist_ok=True)
        self.human_path = self.logs_dir / "migrate.log"
        self.jsonl_path = self.logs_dir / "migrate.jsonl"
        self.secrets = sorted(
            {value for value in (secrets or []) if value}, key=len, reverse=True
        )
        self.console = console
        self.sink = sink

    def add_secret(self, value: str) -> None:
        if not value or value in self.secrets:
            return
        self.secrets.append(value)
        self.secrets.sort(key=len, reverse=True)

    def redact(self, value: str | None) -> str | None:
        if value is None:
            return None
        safe = value
        for secret in self.secrets:
            safe = safe.replace(secret, "[REDACTED]")
        safe = re.sub(
            r"(?i)(authorization\s*:\s*bearer\s+)[^\s\"']+",
            r"\1[REDACTED]",
            safe,
        )
        return safe

    def event(
        self,
        level: str,
        phase: str,
        operation: str,
        message: str,
        *,
        object_identifier: str | None = None,
        retry_count: int | None = None,
        provider_category: str | None = None,
        raw_error: str | None = None,
        **fields: Any,
    ) -> None:
        event = Event(
            timestamp=utc_now(),
            level=level.upper(),
            phase=phase,
            operation=operation,
            message=self.redact(message) or "",
            object_identifier=self.redact(object_identifier),
            retry_count=retry_count,
            provider_category=provider_category,
            safe_raw_error=self.redact(raw_error),
            fields={
                key: self.redact(value) if isinstance(value, str) else value
                for key, value in fields.items()
            },
        )
        record = {
            "timestamp": event.timestamp,
            "level": event.level,
            "phase": event.phase,
            "operation": event.operation,
            "message": event.message,
            "object_identifier": event.object_identifier,
            "retry_count": event.retry_count,
            "provider_category": event.provider_category,
            "safe_raw_error": event.safe_raw_error,
            **event.fields,
        }
        human = (
            f"{event.timestamp} {event.level:<7} "
            f"[{event.phase}:{event.operation}] {event.message}"
        )
        if event.retry_count is not None:
            human += f" retry={event.retry_count}"
        detailed = human
        if event.object_identifier:
            detailed += f" object={event.object_identifier}"
        self._append(self.human_path, detailed + "\n")
        self._append(
            self.jsonl_path,
            json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n",
        )
        phase_name = re.sub(r"[^A-Za-z0-9_.-]", "_", phase)
        self._append(self.logs_dir / "phases" / f"{phase_name}.log", detailed + "\n")
        if self.console:
            # The console output goes to the CI log. The identifiers are the path names
            # of the mirror.
            print(human)
        if self.sink:
            self.sink(event)

    @staticmethod
    def _append(path: Path, text: str) -> None:
        with path.open("a", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            if " ERROR " in text or " WARNING " in text:
                os.fsync(stream.fileno())

    def info(self, phase: str, operation: str, message: str, **kwargs: Any) -> None:
        self.event("INFO", phase, operation, message, **kwargs)

    def warning(self, phase: str, operation: str, message: str, **kwargs: Any) -> None:
        self.event("WARNING", phase, operation, message, **kwargs)

    def error(self, phase: str, operation: str, message: str, **kwargs: Any) -> None:
        self.event("ERROR", phase, operation, message, **kwargs)

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..config import Config
from ..env import Runtime
from ..logging import RunLogger
from ..paths import WorkPaths
from ..state import State


class PhaseError(RuntimeError):
    """The error of a necessary gate that stops the phase."""


@dataclass
class PhaseResult:
    status: str = "PASS"
    outputs: dict[str, Any] = field(default_factory=dict)
    artifacts: list[tuple[str, Path, int | None]] = field(default_factory=list)

    def artifact(
        self, role: str, path: Path, row_count: int | None = None
    ) -> PhaseResult:
        self.artifacts.append((role, path, row_count))
        return self


@dataclass
class PhaseContext:
    cfg: Config
    paths: WorkPaths
    state: State
    logger: RunLogger
    apply: bool
    phase_run_id: int
    run_id: int
    runtime: Runtime

    def phase_dir(self, name: str) -> Path:
        directory = self.paths.logs / "phases" / name
        directory.mkdir(parents=True, exist_ok=True)
        return directory

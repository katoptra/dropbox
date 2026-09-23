from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .env import Runtime


@dataclass(frozen=True)
class WorkPaths:
    root: Path

    @classmethod
    def from_runtime(cls, runtime: Runtime) -> WorkPaths:
        return cls(root=runtime.work_dir.resolve())

    @property
    def state_db(self) -> Path:
        return self.root / "state.sqlite"

    @property
    def clock(self) -> Path:
        return self.root / "clock.json"

    @property
    def session(self) -> Path:
        return self.root / "session"

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    @property
    def report(self) -> Path:
        return self.root / "report.md"

    @property
    def chain(self) -> Path:
        return self.root / "chain"

    @property
    def reconcile(self) -> Path:
        # lib's toolbox `due` leaves it when this run is to walk Proton.
        return self.root / "reconcile"

    @property
    def walked(self) -> Path:
        # A complete walk leaves it, and the Taskfile then records the reconcile in R2.
        return self.root / "walked"

    @property
    def age_key(self) -> Path:
        return self.root / "age.key"

    def ensure(self) -> None:
        for directory in (
            self.root,
            self.session,
            self.staging,
            self.logs,
            self.logs / "phases",
        ):
            directory.mkdir(parents=True, exist_ok=True)
        self.session.chmod(0o700)

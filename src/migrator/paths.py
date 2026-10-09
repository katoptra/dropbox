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
    def start(self) -> Path:
        # The clock of the toolbox writes the start epoch of the run here.
        return self.root / "start.txt"

    @property
    def clock(self) -> Path:
        return self.root / "clock.json"

    @property
    def session(self) -> Path:
        # The session verb of the proton engine puts the CLI session here, in
        # ROOT_DIR/.run. Thus, the work directory must stay .run.
        return self.root / "session"

    @property
    def session_sha(self) -> Path:
        # The session verb of the proton engine writes this file. `session.writeback`
        # writes it again when it sends a changed session to R2.
        return self.root / "session.sha"

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
        # The due verb of the toolbox writes this file if this run must walk Proton.
        return self.root / "reconcile"

    @property
    def walked(self) -> Path:
        # A completed walk writes it. Then the Taskfile records the reconcile in R2.
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

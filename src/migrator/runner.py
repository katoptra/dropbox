from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass

from .config import load_config
from .env import Runtime
from .logging import RunLogger
from .paths import WorkPaths
from .phases import (
    p10_inventory,
    p20_delta,
    p30_plan,
    p40_batches,
    p50_trash,
    p60_reconcile,
    p70_report,
    p80_empty_trash,
)
from .phases.base import PhaseContext, PhaseError, PhaseResult
from .state import State


@dataclass(frozen=True)
class PhaseDefinition:
    number: int
    command: str
    name: str
    run: Callable[[PhaseContext], PhaseResult]


PHASES: dict[str, PhaseDefinition] = {
    "inventory": PhaseDefinition(10, "inventory", "10_inventory", p10_inventory.run),
    "delta": PhaseDefinition(20, "delta", "20_delta", p20_delta.run),
    "plan": PhaseDefinition(30, "plan", "30_plan", p30_plan.run),
    "batches": PhaseDefinition(40, "batches", "40_batches", p40_batches.run),
    "trash": PhaseDefinition(50, "trash", "50_trash", p50_trash.run),
    "reconcile": PhaseDefinition(60, "reconcile", "60_reconcile", p60_reconcile.run),
    "report": PhaseDefinition(70, "report", "70_report", p70_report.run),
    "empty-trash": PhaseDefinition(
        80, "empty-trash", "80_empty_trash", p80_empty_trash.run
    ),
}


def run_phase(command: str, *, apply: bool, runtime: Runtime) -> str:
    phase = PHASES[command]
    cfg = load_config(runtime.config_path)
    paths = WorkPaths.from_runtime(runtime)
    paths.ensure()
    # The CLI's cache dir must follow MIRROR_WORK_DIR, not the image default.
    os.environ["PROTON_DRIVE_CACHE_DIR"] = str(paths.session)
    state = State(paths.state_db, cfg.mirror.id)
    try:
        state.initialize_migration(cfg.source_file, cfg.source_sha256)
        run_id = int(state.current_run()["id"])
        logger = RunLogger(
            paths.logs, secrets=runtime.secrets(), console=True, sink=state.record_event
        )
        phase_run_id = state.start_phase(
            phase.number,
            phase.name,
            apply=apply,
            inputs={"run_id": run_id, "config_sha256": cfg.source_sha256},
            command_parameters={"apply": apply},
        )
        ctx = PhaseContext(
            cfg, paths, state, logger, apply, phase_run_id, run_id, runtime
        )
        logger.info(phase.name, "start", f"starting phase {phase.command}")
        try:
            result = phase.run(ctx)
        except BaseException as exc:
            logger.error(
                phase.name,
                "failure",
                f"phase {phase.command} failed",
                provider_category=type(exc).__name__,
                raw_error=str(exc),
            )
            state.complete_phase(
                phase_run_id,
                "FAIL",
                outputs={"error_type": type(exc).__name__},
                error_summary=logger.redact(str(exc)),
            )
            if isinstance(exc, PhaseError):
                raise
            raise PhaseError(f"{phase.command} failed: {type(exc).__name__}") from exc
        for role, path, row_count in result.artifacts:
            state.record_artifact(
                phase_run_id, role, path, paths.root, row_count=row_count
            )
        state.complete_phase(phase_run_id, result.status, outputs=result.outputs)
        logger.info(
            phase.name,
            "complete",
            f"phase {phase.command} completed with {result.status}",
        )
        return result.status
    finally:
        state.close()

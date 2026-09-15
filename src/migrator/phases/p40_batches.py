from __future__ import annotations

import json
import time
from collections import Counter

from .. import session
from ..logging import utc_now
from ..providers.dropbox_api import DropboxAPIProvider
from ..providers.dropbox_auth import access_token
from ..providers.proton_cli import ProtonCLIProvider
from ..store import Store
from . import batch
from .base import PhaseContext, PhaseError, PhaseResult

PHASE = "40_batches"
now = time.time


def should_start(
    *, elapsed: float, longest: float, budget: float, completed: int
) -> bool:
    """The first batch always runs; afterwards a batch starts only if the longest
    batch so far would still finish inside the budget."""
    return completed == 0 or elapsed + longest <= budget


def run(ctx: PhaseContext) -> PhaseResult:
    run = ctx.state.current_run()
    planned = ctx.state.connection.execute(
        "SELECT * FROM batches WHERE run_id=? AND status='PLANNED' ORDER BY number",
        (ctx.run_id,),
    ).fetchall()
    if not ctx.apply:
        return PhaseResult(status="PLANNED", outputs={"planned": len(planned)})
    store = Store(ctx.runtime, ctx.paths)
    proton = ProtonCLIProvider(
        ctx.cfg,
        ctx.state,
        ctx.logger,
        after_call=lambda: session.writeback(ctx.runtime, ctx.paths, store),
        session_dir=ctx.paths.session,
    )
    # A Dropbox access token lives four hours and a run up to six, so the provider
    # gets a fresh one before every batch's fetch, the only step that calls Dropbox.
    dropbox = DropboxAPIProvider(ctx.cfg, ctx.state, ctx.logger, token="")

    def fetch(batch_id: int) -> dict[str, int]:
        dropbox.token = access_token(ctx.cfg, ctx.runtime)
        ctx.logger.add_secret(dropbox.token)
        return batch.fetch(ctx, dropbox, batch_id)

    # Unconditional: the one Proton call a quiet night is guaranteed to make. It forces any
    # pending token rotation (after_call writes the session back) and keeps the 60-day
    # idle expiry away, besides gating on the destination UID.
    proton.root_uid(PHASE)
    budget = int(run["budget_minutes"]) * 60
    start_epoch = int(run["start_epoch"])
    durations: list[float] = []
    totals: Counter[str] = Counter()
    completed = 0
    failed_batch = None
    for row in planned:
        elapsed = now() - start_epoch
        if not should_start(
            elapsed=elapsed,
            longest=max(durations, default=0.0),
            budget=budget,
            completed=completed,
        ):
            break
        batch_id = int(row["id"])
        began = now()
        details: dict[str, object] = {}
        with ctx.state.connection:
            ctx.state.connection.execute(
                "UPDATE batches SET started_at=? WHERE id=?", (utc_now(), batch_id)
            )
        steps = (
            ("fetch", lambda bid=batch_id: fetch(bid)),
            ("verify", lambda bid=batch_id: batch.verify(ctx, bid)),
            ("upload", lambda bid=batch_id: batch.upload(ctx, proton, bid)),
            ("confirm", lambda bid=batch_id: batch.confirm(ctx, bid)),
            ("checkpoint", lambda bid=batch_id: batch.checkpoint(ctx, store, bid)),
        )
        try:
            for name, step in steps:
                step_began = now()
                ctx.logger.info(
                    PHASE,
                    "step",
                    f"batch {int(row['number'])} of {len(planned)}: {name} started",
                    batch=int(row["number"]),
                    step=name,
                )
                details.update(step())
                details[f"{name}_seconds"] = round(now() - step_began, 1)
        except Exception:  # provider errors included: the batch row must say FAILED
            with ctx.state.connection:
                ctx.state.connection.execute(
                    "UPDATE batches SET status='FAILED', completed_at=?, details_json=? WHERE id=?",
                    (utc_now(), json.dumps(details, sort_keys=True), batch_id),
                )
            ctx.state.update_run(
                ctx.run_id, remaining_batches=len(planned) - completed, chain=0
            )
            raise
        details["seconds"] = round(now() - began, 1)
        with ctx.state.connection:
            ctx.state.connection.execute(
                "UPDATE batches SET details_json=? WHERE id=?",
                (json.dumps(details, sort_keys=True), batch_id),
            )
        durations.append(now() - began)
        totals.update(
            {
                k: v
                for k, v in details.items()
                if isinstance(v, int) and not k.endswith("_seconds")
            }
        )
        status = ctx.state.connection.execute(
            "SELECT status FROM batches WHERE id=?", (batch_id,)
        ).fetchone()[0]
        if status == "FAILED":
            failed_batch = int(row["number"])
            break
        completed += 1
    remaining = len(planned) - completed
    chain = remaining > 0 and completed > 0 and failed_batch is None
    ctx.state.update_run(ctx.run_id, remaining_batches=remaining, chain=int(chain))
    outputs = {
        "planned": len(planned),
        "completed": completed,
        "remaining": remaining,
        "chain": chain,
        "budget_seconds": budget,
        "elapsed_seconds": round(now() - start_epoch, 1),
        "longest_batch_seconds": round(max(durations, default=0.0), 1),
        **dict(totals),
    }
    if failed_batch is not None:
        ctx.logger.error(
            PHASE,
            "gate",
            "a batch failed confirmation",
            provider_category="VERIFICATION_FAILURE",
            batch=failed_batch,
            **outputs,
        )
        raise PhaseError(f"batch {failed_batch} failed confirmation")
    if remaining and completed == 0:
        raise PhaseError("no batch completed inside the budget; not chaining")
    ctx.logger.info(PHASE, "gate", "batches finished for this run", **outputs)
    return PhaseResult(outputs=outputs)

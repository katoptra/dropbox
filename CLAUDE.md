# dropbox

A nightly, self-chaining mirror of a Dropbox account into one Proton Drive folder, with
its state in an age-encrypted SQLite database in a bucket. `README.md` says what it
mirrors, how it works step by step, how to fork it and the runbook;
[katoptra/lib](https://github.com/katoptra/lib)'s README is the manual for the toolbox
this mirror includes. This file is what a change must not break.

Nothing in this repo starts a run: an external scheduler dispatches `sync.yml` nightly.
This mirror includes the toolbox alone: `Taskfile.yml` owns the order, one
`python -m migrator <command>` per step, and `src/migrator/` owns every decision but when
to walk Proton, which is lib's `due`. The infrastructure modules there are from
donphi/dropbox_proton at `cfd0e57`, MIT; the phases under `src/migrator/phases/` and the
Taskfile are this repository's own.

## Must knows

- **Plan by default.** `batches`, `trash`, `reconcile`, `report` and `empty-trash` mutate
  only with `--apply`, which `task pipeline` passes and `task plan-pipeline` never does.
  No mutation is trusted on its exit status: the state push is trusted once the object
  lands, and nothing is recorded as uploaded on a command's exit code alone.
- **A missing state beside history is refused.** A lost state must never look like an
  empty mirror. The rollback is `task state-rollback`; never delete the history to make
  a run start fresh.
- **A truncated listing can never become a trash list.** `delta` refuses a listing under
  `listing_floor_ratio` of the mirrored count, and `trash` runs only when every planned
  batch landed. `trash` takes a folder whole only when `mirror_objects` holds nothing
  live under it, otherwise its files by name, 50 paths per call; it works inside the run
  budget, checkpoints every 50 units and chains the rest; a run cut off mid-phase
  repeats at most 50 units.
- **`checkpoint` is the last step of a batch**, a dated copy first and then a server-side
  copy to the canonical key, so a killed run repeats at most one batch. A batch that
  recorded nothing fails the run rather than chaining, so an identical failure cannot loop.
- **The session is this mirror's own.** Two processes holding it race its rotating
  refresh token. Never run `sync` or `empty-trash` from a laptop while an Actions run may
  be going. The reconcile walk's workers each run from their own copy of the session, and
  the copy a refresh rewrote is adopted.
- **`run_budget_minutes` stays 20 minutes under `sync.yml`'s `timeout-minutes`**, so the
  last batch's upload and the report finish. The chain is the workflow's, on `.run/chain`.
- **Files are keyed by lowercased path**, so a case-only rename changes nothing in the
  delta; display paths are rebuilt from each ancestor's own name because Dropbox cases
  parent segments inconsistently.
- **The logs are public.** The report is built from the state and carries counts only;
  errors print as their class unless `MIRROR_VERBOSE=1`; `op run` masks every value.
- **Run flags go after the double dash** (`task sync -- RUN_BUDGET_MIN=30 RECONCILE=true`,
  or the workflow's `vars` input). The Taskfile maps `RUN_BUDGET_MIN` to the environment
  the migrator reads; `RECONCILE` (`true`, `false` or `auto`) is lib's `due`'s, which
  leaves `.run/reconcile` for the migrator when the last complete walk is
  `RECONCILE_HOURS` (168) old.
- **`config/mirror.toml` is strict** and rejects unknown keys; the three account
  identifiers come from the environment and override its keys.

## Verifying a change

- `task test` runs the pytest suite inside the image, offline; `task lint` is ruff.
- `task check` renders every command of the pipeline inside the image and diffs it
  against `render.txt`; `task render-update` accepts a change. The `check` workflow runs
  check, test and lint on every pull request, with no secret.
- `task plan` runs the read path against the real account and bucket, changes nothing in
  Proton, and prints the report it would make.
- `task status` fetches the state and prints counts; both leave `.run/state.sqlite` for
  inspection.

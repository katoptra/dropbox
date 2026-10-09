# dropbox

This repository has the pipeline of a nightly mirror of a Dropbox account in one Proton
Drive folder. If a run has more work than its budget, it starts the next run through the
chain. The state of the mirror is an SQLite database in a bucket, encrypted with age.
`README.md` tells:

- The files that the mirror contains
- How each step operates
- How to fork the mirror
- How to operate it, with the runbook.

The README of [katoptra/lib](https://github.com/katoptra/lib) is the manual for the toolbox
and the proton engine that this mirror includes. This file gives the rules that each change
must obey.

No part of this repository starts a run. An external scheduler dispatches `sync.yml`
nightly, at 11:42 UTC.

The proton engine supplies the session verbs and `empty-trash`. It also supplies the `pull`
and `push` of the bucket. `Taskfile.yml` sets the sequence of the steps, with one
`python -m migrator <command>` for each step of the mirror. The code in `src/migrator/`
makes each decision, but the `due` verb of lib finds if a run does the Proton walk.

The infrastructure modules in `src/migrator/` are from donphi/dropbox_proton at `cfd0e57`,
with the MIT license. The phases in `src/migrator/phases/` and the Taskfile are from this
repository.

## Constraints

- **Where a change goes.** `Taskfile.yml` has these verbs:
  - `pipeline` and `plan-pipeline`
  - The steps of the pipeline, each one `python -m migrator <command>`: `clock-phase`,
    `state`, `inventory`, `delta`, `plan-phase`, `batches`, `trash`, `reconcile` with
    `record-walk`, and `report-phase`
  - `status`, `offline`, `fmt` and `state-rollback`
  - `report-mirror`, `report-engine` and `empty-trash-pipeline`.

  Make all other changes to verbs in lib: in the toolbox or in the proton engine. Then each
  mirror that includes that file gets the change.
- **Excludes.** The `excludes:` of the two includes contain these verbs:
  - On the toolbox include: `report-mirror`
  - On the proton include: `pipeline`, `plan-pipeline`, `trash`, `empty-trash-pipeline` and
    `report-engine`.

  `report-engine` has no commands, because the pipeline does not use the `stage` and the
  `confirm` of the engine.
- **Root vars.** Root vars hold only the values of this mirror. Do not put an engine default
  in a root var, because then the command line cannot set it
  ([lib README, Rules a mirror keeps](https://github.com/katoptra/lib#rules-a-mirror-keeps)).
  `RECONCILE_HOURS` (168) is the only root var.
- **The run budget.** `run_budget_minutes` (335) in `config/mirror.toml` must stay 20
  minutes less than `timeout-minutes` (355) in `sync.yml`. Thus, the migrator can upload
  the last batch and make the report before the timeout. The chain is a step of the sync
  workflow of lib: it starts the next run if the pipeline wrote `.run/chain`.
- **`config/mirror.toml` is strict.** It rejects a key that it does not know. The
  environment gives the three account identifiers, and they override the keys of the file.
- **Storage.** For each change that adds storage, calculate the new storage. Compare it
  with these values:
  - The baseline: 161,650 files and 610 GiB in Proton Drive (the state on 2026-10-09)
  - The ceiling: `ceiling_gb` (4,000 GiB), and the storage limit of the Proton plan.
- **Writing.** Use ASD-STE100 and the rules in the
  [Writing section of the org CONTRIBUTING](https://github.com/katoptra/.github/blob/main/CONTRIBUTING.md#writing).
  Read that section before you write.

## Must knows

- **Without `--apply`, a step only makes a plan.** These steps change data only with
  `--apply`:
  - `batches`
  - `trash`
  - `reconcile`
  - `report`
  - `empty-trash`.

  `task pipeline` gives `--apply`, and `task plan-pipeline` does not. For a command that
  changes data, an exit code of 0 is not a sufficient indication that the change occurred.
  The mirror accepts a state push only when the object is in the bucket. It records a file
  as uploaded only when the summary of `upload` includes that file.
- **If the bucket has history but no state, the run stops.** The run must not accept a
  missing state as an empty mirror. To repair the state, use `task state-rollback`. Do not
  delete the history to make a run start with no state.
- **A short listing cannot become a trash list.**
  - `delta` rejects a listing that has less than `listing_floor_ratio` of the number of
    mirrored files.
  - `trash` runs only if all the planned batches are in Proton.
  - `trash` moves a folder to the trash in one call only if `mirror_objects` has no
    remaining file in it. In the other folders, it gives the path of each deleted file to
    `filesystem trash`, 50 paths in each call.
  - `trash` operates in the run budget. It makes a checkpoint after each 50 units, and the
    chain does the remaining units. If a run stops in the middle of the phase, the next run
    does a maximum of 50 units again.
- **`checkpoint` is the last step of a batch.** It pushes a dated copy of the state first.
  Then it makes a server-side copy of it at the canonical key. Thus, if the runner stops a
  run, the next run does a maximum of one batch again. If a batch records no file, the run
  stops with a failure and does not start the next run. Thus, the same failure cannot occur
  in a loop.
- **This mirror has a session that no other mirror uses.**
  [lib, The session](https://github.com/katoptra/lib#the-session) gives the rules for a
  session. After each CLI call, the migrator compares `auth-session.json` with the digest in
  `.run/session.sha`. If the token changed, it sends the session back to the bucket. Each
  worker of the reconcile walk uses a copy of the session that no other worker uses. The
  session directory then gets the copy that a refresh changed, also if the walk stops with
  an error.
- **The key of each file is its path in lowercase letters.** Thus, if a new name changes
  only uppercase and lowercase letters, the delta does not change. The mirror makes each
  display path from the name of each folder above the file. The cause is that Dropbox does
  not always give a parent folder the same uppercase and lowercase letters.
- **The logs are public.** The report uses only the data in the state, and it contains only
  counts. An error shows only its class, unless `MIRROR_VERBOSE=1`. `op run` removes each
  secret value from the output.
- **Put the flags of a run after the double dash** (`task sync -- RUN_BUDGET_MIN=30
  RECONCILE=true`), or in the `vars` input of the workflow. The Taskfile gives
  `RUN_BUDGET_MIN` to the migrator in its environment. `RECONCILE` (`true`, `false` or
  `auto`) is for the `due` verb of lib. `due` writes `.run/reconcile` for the migrator when
  the last completed walk is `RECONCILE_HOURS` (168) or more before the run.
- **A run failure is the only alert.** The healthcheck in healthchecks.io has the cron
  `42 11 * * *` UTC and a grace time of 3 hours. On Thursdays, the weekly Proton walk adds
  approximately 95 minutes to the run, and the grace time includes it.

## Verifying a change

- `task run -- task offline` runs these checks in the image, with no network and no
  credentials:
  - pytest
  - `ruff check`
  - `ruff format --check`
  - A dry run of `task empty-trash-pipeline` from the command line, as `task empty-trash`
    starts it.
- `task check` makes a render of each command of the pipeline in the image. Then it compares
  the render with `render.txt`. `task render-update` accepts a change.
- The `check` workflow uses the check workflow of lib, with `offline: true`. Thus, on each
  pull request, it runs `task check` and then `task run -- task offline`, with no secrets.
- `task plan` does the read-only part of a run with the account and the bucket of the
  mirror. It makes no change in Proton, and it prints the report of the plan.
- `task status` gets the state and prints counts. After `task plan` or `task status`, the
  state is in `.run/state.sqlite`, where you can examine it.
- The checks of the engine are in lib:
  `cd ../lib/examples/proton && task run -- task offline`.

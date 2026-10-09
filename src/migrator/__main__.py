from __future__ import annotations

import argparse
import sys

from . import commands
from .env import Runtime
from .runner import PHASES, run_phase

COMMANDS = {
    "clock": commands.clock,
    "state": commands.state,
    "status": commands.status,
    "state-push": commands.state_push,
    "state-rollback": commands.state_rollback,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="migrator", description="Dropbox to Proton Drive mirror"
    )
    parser.add_argument(
        "--apply", action="store_true", help="permit mutation phases to act"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for command in PHASES:
        sub.add_parser(command, help=f"run the {command} phase")
    for command in COMMANDS:
        p = sub.add_parser(command)
        p.add_argument("args", nargs="*")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runtime = Runtime.from_environ()
    try:
        if args.command in PHASES:
            status = run_phase(args.command, apply=args.apply, runtime=runtime)
            print(f"{args.command}: {status}")
            return 0 if status in {"PASS", "PLANNED"} else 2
        return int(COMMANDS[args.command](runtime, args.args))  # type: ignore[operator]
    except Exception as exc:  # noqa: BLE001 - all classes: a traceback can show stderr
        # The text of an error can contain the stderr of a provider, with path names.
        # Thus, if MIRROR_VERBOSE is not 1, the CI log shows only the class of the
        # error.
        detail = runtime.redact(f": {exc}") if runtime.verbose else ""
        print(f"ERROR: {type(exc).__name__}{detail}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("INTERRUPTED", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

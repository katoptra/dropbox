from __future__ import annotations

import re
from pathlib import Path

TASKFILE = Path(__file__).resolve().parents[1] / "Taskfile.yml"


def _tasks_with_desc(text: str) -> set[str]:
    names = set()
    for match in re.finditer(
        r"^  ([a-z][a-z0-9-]*):\n((?:    .*\n)+)", text, re.MULTILINE
    ):
        block = match.group(2)
        if "desc:" in block and "internal: true" not in block:
            names.add(match.group(1))
    return names


def test_every_operator_task_is_in_the_menu():
    text = TASKFILE.read_text(encoding="utf-8")
    menu = text[text.index("MENU:") : text.index("\ntasks:")]
    listed = set(re.findall(r"task ([a-z][a-z0-9-]*)", menu))
    # The menu lines of the toolbox show pipeline and plan-pipeline, as sync and plan.
    expected = _tasks_with_desc(text[text.index("\ntasks:") :]) - {
        "pipeline",
        "plan-pipeline",
    }
    # A regression in the parser can return an empty set. Then the subset check that
    # follows is correct with no data, and it hides each missing task.
    assert expected
    assert expected <= listed, sorted(expected - listed)

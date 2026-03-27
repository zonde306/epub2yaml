from __future__ import annotations

from typing import Any

from models import TaskDefinition
from task_runner import TaskRunner


class ExtractorGraph:
    """
    Minimal orchestration wrapper.

    This keeps a stable boundary for future LangGraph migration while
    currently delegating execution to the sequential task runner.
    """

    def __init__(self, task_runner: TaskRunner) -> None:
        self.task_runner = task_runner

    def run(self, task: TaskDefinition) -> dict[str, Any]:
        return self.task_runner.run_task(task)

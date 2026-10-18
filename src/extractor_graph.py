from __future__ import annotations

import asyncio
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

    async def run_async(self, task: TaskDefinition) -> dict[str, Any]:
        """在已有事件循环中直接 await。"""
        return await self.task_runner.run_task(task)

    def run(self, task: TaskDefinition) -> dict[str, Any]:
        """同步入口：内部驱动事件循环。

        ``TaskRunner.run_task`` 是 async 方法，直接 return 会得到协程对象，
        因此必须显式执行。
        """
        return asyncio.run(self.task_runner.run_task(task))

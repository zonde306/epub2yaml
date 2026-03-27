from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import yaml

from extractor_graph import ExtractorGraph
from models import AppConfig
from task_runner import TaskRunner

DEFAULT_CONFIG_PATH = Path("config.yaml")


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> AppConfig:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    input_epubs = [Path(value) for value in raw.get("input_epubs", [])]
    schema_paths = [Path(value) for value in raw.get("schema_paths", [])]
    prompt_templates = [Path(value) for value in raw.get("prompt_templates", [])]
    workspace_root = Path(raw.get("workspace_root", "workspace"))

    concurrency = raw.get("concurrency", {})
    model = raw.get("model", {})
    runtime = raw.get("runtime", {})
    progress = raw.get("progress", {})

    return AppConfig(
        input_epubs=input_epubs,
        schema_paths=schema_paths,
        prompt_templates=prompt_templates,
        workspace_root=workspace_root,
        enable_parallel_tasks=bool(concurrency.get("enable_parallel_tasks", True)),
        max_workers=int(concurrency.get("max_workers", 4)),
        resume=bool(runtime.get("resume", True)),
        retry_count=int(runtime.get("retry_count", 3)),
        retry_backoff_seconds=int(runtime.get("retry_backoff_seconds", 3)),
        emit_console_progress=bool(progress.get("emit_console_progress", True)),
        model_provider=str(model.get("provider", "openai")),
        model_name=str(model.get("name", "gpt-4.1")),
        streaming=bool(model.get("streaming", True)),
        base_url=str(model.get("base_url", "https://api.openai.com/v1")),
        api_key=str(model.get("api_key", "")),
    )


def main() -> int:
    config = load_config()
    runner = TaskRunner(config)
    graph = ExtractorGraph(runner)
    tasks = runner.build_tasks()

    if not tasks:
        print("No tasks found. Please check config.yaml input_epubs and schema_paths.")
        return 0

    results: list[dict[str, Any]] = []

    if config.enable_parallel_tasks and config.max_workers > 1:
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            future_map = {executor.submit(graph.run, task): task for task in tasks}
            for future in as_completed(future_map):
                results.append(future.result())
    else:
        for task in tasks:
            results.append(graph.run(task))

    failed_count = sum(1 for result in results if result.get("status") == "failed")
    completed_count = sum(1 for result in results if result.get("status") == "completed")
    print(f"Finished tasks: completed={completed_count} failed={failed_count} total={len(results)}")
    return 0 if failed_count == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

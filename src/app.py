from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

import yaml

from checkpoint_store import CheckpointStore
from models import AppConfig, BatchingConfig, TaskDefinition
from progress_store import ProgressStore
from task_runner import TaskRunner
from workspace_manager import WorkspaceManager

DEFAULT_CONFIG_PATH = Path("config.yaml")
DEFAULT_EXAMPLE_CONFIG_PATH = Path("config.example.yaml")
DEFAULT_INPUT_DIR = Path("input")
DEFAULT_WORKSPACE_DIR = Path("workspace")


def load_raw_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    if not config_path.exists():
        return {}
    return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}


def save_raw_config(raw_config: dict[str, Any], config_path: Path = DEFAULT_CONFIG_PATH) -> None:
    serialized = yaml.safe_dump(raw_config, allow_unicode=True, sort_keys=False, indent=2)
    config_path.write_text(serialized, encoding="utf-8")


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> AppConfig:
    raw = load_raw_config(config_path)

    input_epubs = [Path(value) for value in raw.get("input_epubs", [])]
    schema_paths = [Path(value) for value in raw.get("schema_paths", [])]
    prompt_templates = [Path(value) for value in raw.get("prompt_templates", [])]
    workspace_root = Path(raw.get("workspace_root", str(DEFAULT_WORKSPACE_DIR)))

    concurrency = raw.get("concurrency", {})
    model = raw.get("model", {})
    runtime = raw.get("runtime", {})
    progress = raw.get("progress", {})
    batching = raw.get("batching", {})

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
        batching=BatchingConfig(
            enable_multi_chapter=bool(batching.get("enable_multi_chapter", True)),
            max_input_tokens=int(batching.get("max_input_tokens", 12000)),
            prompt_overhead_tokens=int(batching.get("prompt_overhead_tokens", 1500)),
            reserve_output_tokens=int(batching.get("reserve_output_tokens", 3000)),
            allow_oversize_single_chapter=bool(batching.get("allow_oversize_single_chapter", True)),
            split_on_failure=bool(batching.get("split_on_failure", True)),
            split_after_retry_exhausted=bool(batching.get("split_after_retry_exhausted", True)),
            enable_checkpoint=bool(batching.get("enable_checkpoint", False)),
            checkpoint_every_n_chapters=int(batching.get("checkpoint_every_n_chapters", 10)),
        ),
    )



def ensure_project_layout(config_path: Path = DEFAULT_CONFIG_PATH, *, force: bool = False) -> Path:
    DEFAULT_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)

    if force or not config_path.exists():
        template_path = DEFAULT_EXAMPLE_CONFIG_PATH if DEFAULT_EXAMPLE_CONFIG_PATH.exists() else DEFAULT_CONFIG_PATH
        if template_path.exists() and template_path != config_path:
            shutil.copy2(template_path, config_path)
        elif not config_path.exists():
            save_raw_config(default_raw_config(), config_path)

    return config_path


def default_raw_config() -> dict[str, Any]:
    return {
        "input_epubs": [],
        "schema_paths": ["schemas/characters.yaml", "schemas/world.yaml"],
        "prompt_templates": [
            "prompts/base.md",
            "prompts/retry_format.md",
            "prompts/retry_schema.md",
        ],
        "workspace_root": "workspace",
        "concurrency": {
            "enable_parallel_tasks": True,
            "task_unit": "epub_schema",
            "max_workers": 4,
        },
        "model": {
            "provider": "openai",
            "name": "gpt-4.1",
            "streaming": True,
            "base_url": "https://api.openai.com/v1",
            "api_key": "",
        },
        "runtime": {
            "resume": True,
            "retry_count": 3,
            "retry_backoff_seconds": 3,
        },
        "progress": {
            "emit_console_progress": True,
        },
        "batching": {
            "enable_multi_chapter": True,
            "max_input_tokens": 12000,
            "prompt_overhead_tokens": 1500,
            "reserve_output_tokens": 3000,
            "allow_oversize_single_chapter": True,
            "split_on_failure": True,
            "split_after_retry_exhausted": True,
            "enable_checkpoint": False,
            "checkpoint_every_n_chapters": 10,
        },
    }


def copy_epub_to_input(source_path: Path, input_dir: Path = DEFAULT_INPUT_DIR) -> Path:
    if not source_path.exists() or not source_path.is_file():
        raise FileNotFoundError(f"EPUB file not found: {source_path}")

    input_dir.mkdir(parents=True, exist_ok=True)
    target_path = input_dir / source_path.name
    shutil.copy2(source_path, target_path)
    return target_path


def upsert_input_epub(raw_config: dict[str, Any], epub_path: Path) -> dict[str, Any]:
    normalized = str(epub_path).replace("\\", "/")
    input_epubs = list(raw_config.get("input_epubs", []))
    if normalized not in input_epubs:
        input_epubs.append(normalized)
    raw_config["input_epubs"] = input_epubs
    return raw_config


def init_command(config_path: Path, *, force: bool = False) -> int:
    created_config = ensure_project_layout(config_path, force=force)
    print(f"项目初始化完成: config={created_config} input={DEFAULT_INPUT_DIR} workspace={DEFAULT_WORKSPACE_DIR}")
    return 0


def add_epub_command(config_path: Path, epub_source: Path) -> int:
    ensure_project_layout(config_path)
    raw_config = load_raw_config(config_path)
    copied_path = copy_epub_to_input(epub_source)
    upsert_input_epub(raw_config, copied_path)
    save_raw_config(raw_config, config_path)
    print(f"已加入 EPUB: {copied_path}")
    return 0


def run_command(config_path: Path = DEFAULT_CONFIG_PATH) -> int:
    config = load_config(config_path)
    runner = TaskRunner(config)
    tasks = runner.build_tasks()

    if not tasks:
        print("No tasks found. Please check config.yaml input_epubs and schema_paths.")
        return 0

    results: list[dict[str, Any]] = []

    if config.enable_parallel_tasks and config.max_workers > 1:
        epub_buckets: dict[str, deque[Any]] = defaultdict(deque)
        for task in tasks:
            epub_key = task.workspace.epub_name
            epub_buckets[epub_key].append(task)

        epub_order = list(epub_buckets.keys())
        epub_cursor = 0

        def pick_next_item() -> Any | None:
            nonlocal epub_cursor
            if not epub_order:
                return None
            checked = 0
            while checked < len(epub_order):
                epub_key = epub_order[epub_cursor]
                bucket = epub_buckets[epub_key]
                if bucket:
                    item = bucket.popleft()
                    epub_cursor = (epub_cursor + 1) % len(epub_order)
                    return item
                epub_cursor = (epub_cursor + 1) % len(epub_order)
                checked += 1
            return None

        def run_one_step(item: Any) -> tuple[Any, dict[str, Any]]:
            state = item if hasattr(item, "pending_batches") else runner.prepare_task_state(item)
            step_result = runner.step_task(state)
            return state, step_result

        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            future_map: dict[Any, bool] = {}

            def fill_workers() -> None:
                while len(future_map) < config.max_workers:
                    item = pick_next_item()
                    if item is None:
                        break
                    future_map[executor.submit(run_one_step, item)] = True

            fill_workers()
            while future_map:
                done, _ = wait(list(future_map.keys()), return_when=FIRST_COMPLETED)
                for future in done:
                    future_map.pop(future)
                    state, step_result = future.result()
                    if step_result.get("is_completed") or step_result.get("is_failed"):
                        results.append(step_result["progress"])
                    else:
                        epub_buckets[state.task.workspace.epub_name].append(state)
                fill_workers()
    else:
        for task in tasks:
            results.append(runner.run_task(task))

    failed_count = sum(1 for result in results if result.get("status") == "failed")
    completed_count = sum(1 for result in results if result.get("status") == "completed")
    print(f"Finished tasks: completed={completed_count} failed={failed_count} total={len(results)}")
    return 0 if failed_count == 0 else 2


def restore_command(
    config_path: Path,
    *,
    epub_path: Path,
    schema_path: Path,
    checkpoint_id: str | None = None,
    latest: bool = False,
) -> int:
    config = load_config(config_path)
    workspace_manager = WorkspaceManager(config.workspace_root)
    checkpoint_store = CheckpointStore()
    progress_store = ProgressStore()

    task = build_task_definition(workspace_manager, epub_path=epub_path, schema_path=schema_path)
    snapshot = checkpoint_store.restore_checkpoint(
        task,
        schema_name=schema_path.stem,
        checkpoint_id=checkpoint_id,
        latest=latest,
    )

    if task.stream_buffer_path.exists():
        workspace_manager.cleanup_stale_stream(task.stream_buffer_path)

    progress = progress_store.load(task.progress_path)
    progress = progress_store.mark_checkpoint_restored(progress, checkpoint_id=str(snapshot["checkpoint_id"]))
    progress_store.save(task.progress_path, progress)

    print(
        "checkpoint 恢复完成: "
        f"epub={epub_path} schema={schema_path} checkpoint={snapshot['checkpoint_id']} "
        f"output={task.output_path} progress={task.progress_path}"
    )
    return 0


def build_task_definition(workspace_manager: WorkspaceManager, *, epub_path: Path, schema_path: Path) -> TaskDefinition:
    workspace = workspace_manager.ensure_workspace(epub_path)
    schema_name = schema_path.stem
    return TaskDefinition(
        epub_path=epub_path,
        workspace=workspace,
        schema_path=schema_path,
        prompt_template_paths=[],
        output_path=workspace.output_path_for_schema(schema_name),
        progress_path=workspace.progress_path_for_schema(schema_name),
        stream_buffer_path=workspace.stream_buffer_path_for_schema(schema_name),
    )


def test_command() -> int:
    completed = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"], check=False)
    return int(completed.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="EPUB LLM extraction helper commands")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="config file path")

    subparsers = parser.add_subparsers(dest="command")

    init_parser = subparsers.add_parser("init", help="create default config and runtime directories")
    init_parser.add_argument("--force", action="store_true", help="overwrite config file with example/default content")

    add_epub_parser = subparsers.add_parser("add-epub", help="copy an epub into input directory and register it in config")
    add_epub_parser.add_argument("epub_path", help="path to the epub file to add")

    restore_parser = subparsers.add_parser("restore", help="restore output and progress from a saved checkpoint")
    restore_parser.add_argument("--epub", required=True, help="epub path used to locate the workspace")
    restore_parser.add_argument("--schema", required=True, help="schema path to restore")
    checkpoint_group = restore_parser.add_mutually_exclusive_group(required=True)
    checkpoint_group.add_argument("--checkpoint", help="checkpoint id such as ch0010")
    checkpoint_group.add_argument("--latest", action="store_true", help="restore the latest checkpoint")

    subparsers.add_parser("run", help="run extraction tasks from config")
    subparsers.add_parser("test", help="run unit tests")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config_path = Path(args.config)

    if args.command in (None, "run"):
        return run_command(config_path)
    if args.command == "init":
        return init_command(config_path, force=bool(args.force))
    if args.command == "add-epub":
        return add_epub_command(config_path, Path(args.epub_path))
    if args.command == "restore":
        return restore_command(
            config_path,
            epub_path=Path(args.epub),
            schema_path=Path(args.schema),
            checkpoint_id=getattr(args, "checkpoint", None),
            latest=bool(getattr(args, "latest", False)),
        )
    if args.command == "test":
        return test_command()

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

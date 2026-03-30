from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from checkpoint_store import CheckpointStore
from epub_reader import extract_epub
from llm_client import build_model_client
from models import AppConfig, Chapter, ChapterBatch, PromptTemplate, SchemaDefinition, StreamModelClient, TaskDefinition, build_checkpoint_id
from progress_store import ProgressStore
from prompt_builder import PromptBuilder
from prompt_loader import PromptLoader
from schema_loader import SchemaLoader
from schema_validator import SchemaValidator
from workspace_manager import WorkspaceManager
from yaml_store import YamlStore


@dataclass
class TaskExecutionState:
    """单个任务的执行状态，支持批次级步进执行。"""

    task: TaskDefinition
    chapters: list[Chapter]
    schema_definition: SchemaDefinition
    prompt_templates: list[PromptTemplate]
    progress: dict[str, Any]
    pending_batches: deque[ChapterBatch] = field(default_factory=deque)
    is_completed: bool = False
    is_failed: bool = False


class TaskRunner:
    def __init__(
        self,
        config: AppConfig,
        *,
        model_client: StreamModelClient | None = None,
        schema_loader: SchemaLoader | None = None,
        prompt_loader: PromptLoader | None = None,
        prompt_builder: PromptBuilder | None = None,
        schema_validator: SchemaValidator | None = None,
        yaml_store: YamlStore | None = None,
        progress_store: ProgressStore | None = None,
        workspace_manager: WorkspaceManager | None = None,
        checkpoint_store: CheckpointStore | None = None,
    ) -> None:
        self.config = config
        self.model_client = model_client or build_model_client(config)
        self.schema_loader = schema_loader or SchemaLoader()
        self.prompt_loader = prompt_loader or PromptLoader()
        self.prompt_builder = prompt_builder or PromptBuilder()
        self.schema_validator = schema_validator or SchemaValidator()
        self.yaml_store = yaml_store or YamlStore()
        self.progress_store = progress_store or ProgressStore()
        self.workspace_manager = workspace_manager or WorkspaceManager(config.workspace_root)
        self.checkpoint_store = checkpoint_store or CheckpointStore()

    def build_tasks(self) -> list[TaskDefinition]:
        tasks: list[TaskDefinition] = []
        for epub_path in self.config.input_epubs:
            workspace = self.workspace_manager.ensure_workspace(epub_path)
            for schema_path in self.config.schema_paths:
                schema_name = schema_path.stem
                tasks.append(
                    TaskDefinition(
                        epub_path=epub_path,
                        workspace=workspace,
                        schema_path=schema_path,
                        prompt_template_paths=list(self.config.prompt_templates),
                        output_path=workspace.output_path_for_schema(schema_name),
                        progress_path=workspace.progress_path_for_schema(schema_name),
                        stream_buffer_path=workspace.stream_buffer_path_for_schema(schema_name),
                    )
                )
        return tasks

    def prepare_task_state(self, task: TaskDefinition) -> TaskExecutionState:
        """初始化任务执行状态，用于后续批次级步进执行。"""
        self._log(task, f"task started epub={task.epub_path} schema={task.schema_path}")
        chapters = extract_epub(str(task.epub_path))
        schema_definition = self.schema_loader.load(task.schema_path)
        prompt_templates = self.prompt_loader.load_templates(task.prompt_template_paths)
        self.yaml_store.initialize_output(task.output_path, schema_definition)
        progress = self._load_or_initialize_progress(task, total_chapters=len(chapters))

        self._log(
            task,
            f"task prepared total_chapters={len(chapters)} output={task.output_path} progress={task.progress_path}",
        )

        if self.config.resume and task.stream_buffer_path.exists():
            self.workspace_manager.cleanup_stale_stream(task.stream_buffer_path)
            self._log(task, f"removed stale stream buffer path={task.stream_buffer_path}")

        start_index = int(progress.get("last_completed_chapter_index", 0))
        remaining_chapters = chapters[start_index:]
        batches = self._build_initial_batches(task, remaining_chapters)

        return TaskExecutionState(
            task=task,
            chapters=chapters,
            schema_definition=schema_definition,
            prompt_templates=prompt_templates,
            progress=progress,
            pending_batches=deque(batches),
        )

    def step_task(self, state: TaskExecutionState) -> dict[str, Any]:
        """执行一个批次后返回，支持公平调度让出 worker。

        返回值包含:
        - progress: 当前进度
        - is_completed: 任务是否已完成
        - is_failed: 任务是否失败
        """
        if state.is_completed or state.is_failed:
            return {
                "progress": state.progress,
                "is_completed": state.is_completed,
                "is_failed": state.is_failed,
            }

        if not state.pending_batches:
            # 没有待执行批次，标记完成
            if state.progress.get("status") != "failed":
                state.progress["status"] = "completed"
                state.progress["batch_status"] = "completed"
                self.progress_store.save(state.task.progress_path, state.progress)
                self._log(state.task, "task completed")
            state.is_completed = True
            return {
                "progress": state.progress,
                "is_completed": True,
                "is_failed": False,
            }

        batch = state.pending_batches.popleft()
        self._log(
            state.task,
            f"batch start range={batch.display_range} depth={batch.split_depth} count={batch.chapter_count} tokens={batch.token_estimate}",
        )

        batch_result = self._run_batch_single(
            state.task,
            batch=batch,
            total_chapters=len(state.chapters),
            schema_definition=state.schema_definition,
            prompt_templates=state.prompt_templates,
            progress=state.progress,
        )
        state.progress = batch_result["progress"]

        if batch_result.get("needs_split"):
            # 批次失败需要拆分，将子批次加入队列尾部，让其他任务先执行
            left_batch, right_batch = batch.split()
            self._log(
                state.task,
                f"batch split parent={batch.batch_id} range={batch.display_range} -> left={left_batch.display_range} right={right_batch.display_range} reason={batch_result.get('split_reason', 'unknown')}",
            )
            state.pending_batches.append(left_batch)
            state.pending_batches.append(right_batch)
            return {
                "progress": state.progress,
                "is_completed": False,
                "is_failed": False,
            }

        if state.progress.get("status") == "failed":
            state.is_failed = True
            self._log(state.task, f"task failed last_error={self._last_error(state.progress)}")
            return {
                "progress": state.progress,
                "is_completed": False,
                "is_failed": True,
            }

        return {
            "progress": state.progress,
            "is_completed": False,
            "is_failed": False,
        }

    def run_task(self, task: TaskDefinition) -> dict[str, Any]:
        """执行任务直到完成，保持向后兼容。

        注意：此方法会一次性执行完整个任务。如需公平调度，
        请使用 prepare_task_state 和 step_task 方法。
        """
        state = self.prepare_task_state(task)

        while not state.is_completed and not state.is_failed:
            result = self.step_task(state)
            if result.get("is_completed") or result.get("is_failed"):
                break

        return state.progress

    def _build_initial_batches(self, task: TaskDefinition, chapters: list[Chapter]) -> list[ChapterBatch]:
        if not chapters:
            return []

        if not self.config.batching.enable_multi_chapter:
            return [ChapterBatch.from_chapters([chapter]) for chapter in chapters]

        budget = self.config.batching.chapter_token_budget
        if budget <= 0:
            self._log(task, "batching disabled by zero chapter token budget, fallback to single chapter batches")
            return [ChapterBatch.from_chapters([chapter]) for chapter in chapters]

        checkpoint_window_size = self._checkpoint_window_size()
        batches: list[ChapterBatch] = []
        pending: list[Chapter] = []
        pending_tokens = 0

        for chapter in chapters:
            if pending and checkpoint_window_size > 0:
                pending_window_index = self._chapter_window_index(pending[0].chapter_index)
                chapter_window_index = self._chapter_window_index(chapter.chapter_index)
                if chapter_window_index != pending_window_index:
                    batches.append(ChapterBatch.from_chapters(pending))
                    pending = []
                    pending_tokens = 0

            if chapter.token_estimate > budget:
                if not self.config.batching.allow_oversize_single_chapter:
                    raise ValueError(
                        f"chapter {chapter.chapter_id} token estimate {chapter.token_estimate} exceeds budget {budget}"
                    )
                if pending:
                    batches.append(ChapterBatch.from_chapters(pending))
                    pending = []
                    pending_tokens = 0
                oversize_batch = ChapterBatch.from_chapters([chapter])
                batches.append(oversize_batch)
                self._log(
                    task,
                    f"oversize single chapter batch allowed range={oversize_batch.display_range} chapter_id={chapter.chapter_id} tokens={chapter.token_estimate} budget={budget}",
                )
                continue

            projected_tokens = pending_tokens + chapter.token_estimate
            if pending and projected_tokens > budget:
                batches.append(ChapterBatch.from_chapters(pending))
                pending = [chapter]
                pending_tokens = chapter.token_estimate
                continue

            pending.append(chapter)
            pending_tokens = projected_tokens

        if pending:
            batches.append(ChapterBatch.from_chapters(pending))

        return batches

    def _run_batch(
        self,
        task: TaskDefinition,
        *,
        batch: ChapterBatch,
        total_chapters: int,
        schema_definition: SchemaDefinition,
        prompt_templates: list[PromptTemplate],
        progress: dict[str, Any],
    ) -> dict[str, Any]:
        batch_retry = 0
        last_error = ""

        while batch_retry < self.config.retry_count:
            attempted_templates: list[str] = []
            for template in prompt_templates:
                attempted_templates.append(str(template.path).replace("\\", "/"))
                self._log(
                    task,
                    f"batch attempt range={batch.display_range} depth={batch.split_depth} retry={batch_retry + 1}/{self.config.retry_count} template={template.name}",
                )
                progress = self.progress_store.update_running(
                    progress,
                    batch=batch,
                    completed_chapters=int(progress.get("completed_chapters", 0)),
                    total_chapters=total_chapters,
                    template=template,
                    attempted_templates=attempted_templates,
                    retry_attempt=batch_retry,
                    status="running" if batch_retry == 0 else "retrying",
                    last_error=last_error,
                )
                self.progress_store.save(task.progress_path, progress)

                current_output = self.yaml_store.load_yaml(task.output_path, default={schema_definition.root_key: {}})
                prompt = self.prompt_builder.build(
                    template,
                    batch=batch,
                    schema_definition=schema_definition,
                    existing_yaml=self.yaml_store.dump_to_string(current_output),
                    existing_worldinfo=self._load_existing_worldinfo(task),
                    error_summary=last_error,
                )
                prompt_debug_path = task.workspace.prompt_debug_path_for_attempt(
                    schema_name=schema_definition.schema_name,
                    start_chapter_index=batch.start_chapter_index,
                    end_chapter_index=batch.end_chapter_index,
                    split_depth=batch.split_depth,
                    retry_attempt=batch_retry + 1,
                    template_name=template.name,
                )
                response_debug_path = task.workspace.response_debug_path_for_attempt(
                    schema_name=schema_definition.schema_name,
                    start_chapter_index=batch.start_chapter_index,
                    end_chapter_index=batch.end_chapter_index,
                    split_depth=batch.split_depth,
                    retry_attempt=batch_retry + 1,
                    template_name=template.name,
                )
                self._write_text(prompt_debug_path, prompt)
                self._log(
                    task,
                    f"debug capture prompt={prompt_debug_path} response={response_debug_path}",
                )

                try:
                    yaml_text, chunk_count = self._collect_stream(
                        task.stream_buffer_path,
                        response_debug_path,
                        prompt,
                    )
                except Exception as exc:  # noqa: BLE001
                    last_error = f"stream failed: {exc}"
                    self._log(task, last_error)
                    progress = self.progress_store.update_stream(progress, receive_status="interrupted", chunk_count=0)
                    progress = self.progress_store.update_validation(progress, result="failed", error_count=1, last_error=last_error)
                    self.progress_store.save(task.progress_path, progress)
                    self._emit_progress(task, schema_definition, progress, template.name)
                    continue

                self._log(task, f"stream completed range={batch.display_range} chunk_count={chunk_count}")
                progress = self.progress_store.update_stream(progress, receive_status="completed", chunk_count=chunk_count)
                parsed_yaml, parse_result = self.schema_validator.parse_yaml_text(yaml_text)
                if not parse_result.ok or parsed_yaml is None:
                    last_error = parse_result.summary()
                    self._log(task, f"yaml parse failed range={batch.display_range} error={last_error}")
                    progress = self.progress_store.update_validation(
                        progress,
                        result="failed",
                        error_count=len(parse_result.errors),
                        last_error=last_error,
                    )
                    self.progress_store.save(task.progress_path, progress)
                    self._emit_progress(task, schema_definition, progress, template.name)
                    continue

                sanitized_yaml = self.schema_validator.sanitize_increment_data(parsed_yaml)
                validation_result = self.schema_validator.validate_increment(sanitized_yaml, schema_definition)
                if not validation_result.ok:
                    last_error = validation_result.summary()
                    self._log(task, f"schema validation failed range={batch.display_range} error={last_error}")
                    progress = self.progress_store.update_validation(
                        progress,
                        result="failed",
                        error_count=len(validation_result.errors),
                        last_error=last_error,
                    )
                    self.progress_store.save(task.progress_path, progress)
                    self._emit_progress(task, schema_definition, progress, template.name)
                    continue

                merged_data, merge_stats = self.yaml_store.merge_increment(current_output, sanitized_yaml, schema_definition)
                self.yaml_store.write_yaml(task.output_path, merged_data)
                self._log(
                    task,
                    f"merge completed range={batch.display_range} replaced={merge_stats.replaced_nodes} appended={merge_stats.appended_nodes}",
                )
                progress = self.progress_store.update_validation(progress, result="passed", error_count=0, last_error="")
                progress = self.progress_store.update_merge(progress, merge_stats)
                progress = self.progress_store.mark_batch_completed(
                    progress,
                    batch=batch,
                    total_chapters=total_chapters,
                )
                self.progress_store.save(task.progress_path, progress)
                checkpoint_id = self._maybe_save_checkpoint(
                    task,
                    schema_definition=schema_definition,
                    progress=progress,
                    total_chapters=total_chapters,
                )
                self._emit_progress(task, schema_definition, progress, template.name)
                self.workspace_manager.cleanup_stale_stream(task.stream_buffer_path)
                self._log(
                    task,
                    f"batch completed range={batch.display_range} depth={batch.split_depth} end_chapter={batch.end_chapter_index}/{total_chapters}",
                )
                if checkpoint_id:
                    self._log(task, f"checkpoint saved id={checkpoint_id} range={batch.display_range}")
                return {"progress": progress}

            batch_retry += 1
            if batch_retry < self.config.retry_count and self.config.retry_backoff_seconds > 0:
                self._log(task, f"batch retry sleeping seconds={self.config.retry_backoff_seconds}")
                time.sleep(self.config.retry_backoff_seconds)

        progress = self.progress_store.mark_failed(
            progress,
            last_error=last_error or "batch retries exhausted",
            retry_attempt=batch_retry,
            batch=batch,
        )
        self.progress_store.save(task.progress_path, progress)
        self._log(task, f"batch failed range={batch.display_range} last_error={self._last_error(progress)}")
        self._emit_progress(task, schema_definition, progress, "failed")
        return {"progress": progress}

    def _run_batch_single(
        self,
        task: TaskDefinition,
        *,
        batch: ChapterBatch,
        total_chapters: int,
        schema_definition: SchemaDefinition,
        prompt_templates: list[PromptTemplate],
        progress: dict[str, Any],
    ) -> dict[str, Any]:
        """执行单个批次，失败时返回拆分标记而不是递归执行。

        与 _run_batch 不同，此方法在批次失败且可拆分时返回 needs_split=True，
        让调用方决定如何处理拆分后的子批次，从而实现公平调度。
        """
        batch_result = self._run_batch(
            task,
            batch=batch,
            total_chapters=total_chapters,
            schema_definition=schema_definition,
            prompt_templates=prompt_templates,
            progress=progress,
        )
        progress = batch_result["progress"]

        if progress.get("status") != "failed":
            return {"progress": progress, "needs_split": False}

        # 检查是否应该拆分
        if self._should_split_batch(batch):
            split_reason = self._last_error(progress) or "batch retries exhausted"
            progress = self.progress_store.mark_batch_split(progress, batch=batch, reason=split_reason)
            self.progress_store.save(task.progress_path, progress)
            return {
                "progress": progress,
                "needs_split": True,
                "split_reason": split_reason,
            }

        return {"progress": progress, "needs_split": False}

    def _should_split_batch(self, batch: ChapterBatch) -> bool:
        if batch.chapter_count <= 1:
            return False
        if not self.config.batching.enable_multi_chapter:
            return False
        if not self.config.batching.split_on_failure:
            return False
        if not self.config.batching.split_after_retry_exhausted:
            return False
        return True

    def _collect_stream(self, stream_buffer_path: Path, response_debug_path: Path, prompt: str) -> tuple[str, int]:
        chunks: list[str] = []
        chunk_count = 0
        stream_buffer_path.parent.mkdir(parents=True, exist_ok=True)
        response_debug_path.parent.mkdir(parents=True, exist_ok=True)
        with (
            stream_buffer_path.open("w", encoding="utf-8") as stream_handle,
            response_debug_path.open("w", encoding="utf-8") as debug_handle,
        ):
            for chunk in self.model_client.stream_yaml(prompt):
                stream_handle.write(chunk)
                stream_handle.flush()
                debug_handle.write(chunk)
                debug_handle.flush()
                chunks.append(chunk)
                chunk_count += 1
        return "".join(chunks), chunk_count

    def _load_or_initialize_progress(self, task: TaskDefinition, *, total_chapters: int) -> dict[str, Any]:
        if task.progress_path.exists() and self.config.resume:
            loaded = self.progress_store.load(task.progress_path)
            if loaded:
                return loaded
        return self.progress_store.initialize(
            task.progress_path,
            epub_path=str(task.epub_path).replace("\\", "/"),
            workspace_root=str(task.workspace.root).replace("\\", "/"),
            schema_path=str(task.schema_path).replace("\\", "/"),
            output_path=str(task.output_path).replace("\\", "/"),
            stream_buffer_path=str(task.stream_buffer_path).replace("\\", "/"),
            total_chapters=total_chapters,
            max_attempts=self.config.retry_count,
            checkpoint_enabled=self.config.batching.enable_checkpoint,
            checkpoint_every_n_chapters=self._checkpoint_window_size(),
        )

    def _checkpoint_window_size(self) -> int:
        if not self.config.batching.enable_checkpoint:
            return 0
        return max(int(self.config.batching.checkpoint_every_n_chapters), 0)

    def _chapter_window_index(self, chapter_index: int) -> int:
        checkpoint_window_size = self._checkpoint_window_size()
        if checkpoint_window_size <= 0:
            return 0
        return max((chapter_index - 1) // checkpoint_window_size, 0)

    def _should_save_checkpoint(self, *, progress: dict[str, Any], total_chapters: int) -> bool:
        checkpoint_window_size = self._checkpoint_window_size()
        if checkpoint_window_size <= 0:
            return False

        chapter_index = int(progress.get("last_completed_chapter_index", 0))
        if chapter_index <= 0:
            return False

        checkpoint = progress.get("checkpoint", {})
        last_saved_chapter_index = int(checkpoint.get("last_saved_chapter_index", 0)) if isinstance(checkpoint, dict) else 0
        if chapter_index <= last_saved_chapter_index:
            return False
        if chapter_index >= total_chapters:
            return True
        return chapter_index % checkpoint_window_size == 0

    def _maybe_save_checkpoint(
        self,
        task: TaskDefinition,
        *,
        schema_definition: SchemaDefinition,
        progress: dict[str, Any],
        total_chapters: int,
    ) -> str:
        if not self._should_save_checkpoint(progress=progress, total_chapters=total_chapters):
            return ""

        chapter_index = int(progress.get("last_completed_chapter_index", 0))
        checkpoint_id = build_checkpoint_id(chapter_index)
        progress = self.progress_store.mark_checkpoint_saved(
            progress,
            checkpoint_id=checkpoint_id,
            chapter_index=chapter_index,
        )
        self.progress_store.save(task.progress_path, progress)

        metadata = self.checkpoint_store.save_checkpoint(
            task,
            schema_name=schema_definition.schema_name,
            progress=progress,
            total_chapters=total_chapters,
        )
        return str(metadata.get("checkpoint_id", ""))

    def _load_existing_worldinfo(self, task: TaskDefinition) -> str:
        world_path = task.workspace.output_dir / "world.yaml"
        if not world_path.exists():
            return "{}"
        world_data = self.yaml_store.load_yaml(world_path, default={"worldinfo": {}})
        return self.yaml_store.dump_to_string(world_data)

    def _emit_progress(
        self,
        task: TaskDefinition,
        schema_definition: SchemaDefinition,
        progress: dict[str, Any],
        template_name: str,
    ) -> None:
        if not self.config.emit_console_progress:
            return
        retry = progress.get("retry", {})
        merge = progress.get("merge", {})
        status = progress.get("status", "unknown")
        current = progress.get("last_completed_chapter_index", 0)
        total = progress.get("total_chapters", 0)
        batch_range = progress.get("current_batch_range", "")
        batch_depth = progress.get("current_batch_depth", 0)
        batch_status = progress.get("batch_status", "unknown")
        last_error = self._last_error(progress)
        suffix = f" last_error={last_error}" if last_error else ""
        print(
            f"[{task.workspace.epub_name}][{schema_definition.schema_name}] "
            f"{current}/{total} batch={batch_range} depth={batch_depth} template={template_name} "
            f"status={status}/{batch_status} retries={retry.get('current_attempt', 0)} "
            f"replaced={merge.get('replaced_nodes', 0)} appended={merge.get('appended_nodes', 0)}{suffix}"
        )

    def _log(self, task: TaskDefinition, message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] [{task.workspace.epub_name}][{task.schema_path.stem}] {message}"
        print(log_line)
        log_path = task.workspace.log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(log_line + "\n")

    def _write_text(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            handle.write(content)

    def _last_error(self, progress: dict[str, Any]) -> str:
        retry = progress.get("retry", {})
        last_error = retry.get("last_error", "")
        if isinstance(last_error, str):
            return last_error.strip()
        return str(last_error)

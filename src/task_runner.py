from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from epub_reader import extract_epub
from llm_client import build_model_client
from models import AppConfig, Chapter, PromptTemplate, SchemaDefinition, StreamModelClient, TaskDefinition
from progress_store import ProgressStore
from prompt_builder import PromptBuilder
from prompt_loader import PromptLoader
from schema_loader import SchemaLoader
from schema_validator import SchemaValidator
from workspace_manager import WorkspaceManager
from yaml_store import YamlStore


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

    def run_task(self, task: TaskDefinition) -> dict[str, Any]:
        chapters = extract_epub(str(task.epub_path))
        schema_definition = self.schema_loader.load(task.schema_path)
        prompt_templates = self.prompt_loader.load_templates(task.prompt_template_paths)
        self.yaml_store.initialize_output(task.output_path, schema_definition)
        progress = self._load_or_initialize_progress(task, total_chapters=len(chapters))

        if self.config.resume and task.stream_buffer_path.exists():
            self.workspace_manager.cleanup_stale_stream(task.stream_buffer_path)

        start_index = int(progress.get("last_completed_chapter_index", 0))
        existing_worldinfo = self._load_existing_worldinfo(task)

        for chapter in chapters[start_index:]:
            chapter_result = self._run_chapter(
                task,
                chapter=chapter,
                total_chapters=len(chapters),
                schema_definition=schema_definition,
                prompt_templates=prompt_templates,
                progress=progress,
                existing_worldinfo=existing_worldinfo,
            )
            progress = chapter_result["progress"]
            if progress.get("status") == "failed":
                return progress

        if progress.get("status") != "failed":
            progress["status"] = "completed"
            self.progress_store.save(task.progress_path, progress)
        return progress

    def _run_chapter(
        self,
        task: TaskDefinition,
        *,
        chapter: Chapter,
        total_chapters: int,
        schema_definition: SchemaDefinition,
        prompt_templates: list[PromptTemplate],
        progress: dict[str, Any],
        existing_worldinfo: str,
    ) -> dict[str, Any]:
        chapter_retry = 0
        last_error = ""

        while chapter_retry < self.config.retry_count:
            attempted_templates: list[str] = []
            for template in prompt_templates:
                attempted_templates.append(str(template.path).replace("\\", "/"))
                progress = self.progress_store.update_running(
                    progress,
                    chapter_index=chapter.chapter_index,
                    completed_chapters=int(progress.get("completed_chapters", 0)),
                    total_chapters=total_chapters,
                    template=template,
                    attempted_templates=attempted_templates,
                    retry_attempt=chapter_retry,
                    status="running" if chapter_retry == 0 else "retrying",
                    last_error=last_error,
                )
                self.progress_store.save(task.progress_path, progress)

                current_output = self.yaml_store.load_yaml(task.output_path, default={schema_definition.root_key: []})
                prompt = self.prompt_builder.build(
                    template,
                    chapter=chapter,
                    schema_definition=schema_definition,
                    existing_yaml=self.yaml_store.dump_to_string(current_output),
                    existing_worldinfo=existing_worldinfo,
                    error_summary=last_error,
                )

                try:
                    yaml_text, chunk_count = self._collect_stream(task.stream_buffer_path, prompt)
                except Exception as exc:  # noqa: BLE001
                    last_error = f"stream failed: {exc}"
                    progress = self.progress_store.update_stream(progress, receive_status="interrupted", chunk_count=0)
                    progress = self.progress_store.update_validation(progress, result="failed", error_count=1, last_error=last_error)
                    self.progress_store.save(task.progress_path, progress)
                    continue

                progress = self.progress_store.update_stream(progress, receive_status="completed", chunk_count=chunk_count)
                parsed_yaml, parse_result = self.schema_validator.parse_yaml_text(yaml_text)
                if not parse_result.ok or parsed_yaml is None:
                    last_error = parse_result.summary()
                    progress = self.progress_store.update_validation(
                        progress,
                        result="failed",
                        error_count=len(parse_result.errors),
                        last_error=last_error,
                    )
                    self.progress_store.save(task.progress_path, progress)
                    continue

                validation_result = self.schema_validator.validate_increment(parsed_yaml, schema_definition)
                if not validation_result.ok:
                    last_error = validation_result.summary()
                    progress = self.progress_store.update_validation(
                        progress,
                        result="failed",
                        error_count=len(validation_result.errors),
                        last_error=last_error,
                    )
                    self.progress_store.save(task.progress_path, progress)
                    continue

                merged_data, merge_stats = self.yaml_store.merge_increment(current_output, parsed_yaml, schema_definition)
                self.yaml_store.write_yaml(task.output_path, merged_data)
                progress = self.progress_store.update_validation(progress, result="passed", error_count=0, last_error="")
                progress = self.progress_store.update_merge(progress, merge_stats)
                progress = self.progress_store.mark_chapter_completed(
                    progress,
                    chapter_index=chapter.chapter_index,
                    chapter_id=chapter.chapter_id,
                    total_chapters=total_chapters,
                )
                self.progress_store.save(task.progress_path, progress)
                self._emit_progress(task, schema_definition, progress, template.name)
                self.workspace_manager.cleanup_stale_stream(task.stream_buffer_path)
                return {"progress": progress}

            chapter_retry += 1
            if chapter_retry < self.config.retry_count and self.config.retry_backoff_seconds > 0:
                time.sleep(self.config.retry_backoff_seconds)

        progress = self.progress_store.mark_failed(progress, last_error=last_error or "chapter retries exhausted", retry_attempt=chapter_retry)
        self.progress_store.save(task.progress_path, progress)
        self._emit_progress(task, schema_definition, progress, "failed")
        return {"progress": progress}

    def _collect_stream(self, stream_buffer_path: Path, prompt: str) -> tuple[str, int]:
        chunks: list[str] = []
        chunk_count = 0
        stream_buffer_path.parent.mkdir(parents=True, exist_ok=True)
        with stream_buffer_path.open("w", encoding="utf-8") as handle:
            for chunk in self.model_client.stream_yaml(prompt):
                handle.write(chunk)
                handle.flush()
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
        )

    def _load_existing_worldinfo(self, task: TaskDefinition) -> str:
        world_path = task.workspace.output_dir / "world.yaml"
        if not world_path.exists():
            return "{}"
        world_data = self.yaml_store.load_yaml(world_path, default={"worldinfo": []})
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
        print(
            f"[{task.workspace.epub_name}][{schema_definition.schema_name}] "
            f"{current}/{total} template={template_name} status={status} "
            f"retries={retry.get('current_attempt', 0)} "
            f"replaced={merge.get('replaced_nodes', 0)} appended={merge.get('appended_nodes', 0)}"
        )

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any, Protocol


@dataclass(slots=True)
class Chapter:
    chapter_index: int
    chapter_id: str
    title: str
    text: str
    source_path: str
    token_estimate: int


@dataclass(slots=True)
class ChapterBatch:
    batch_id: str
    parent_batch_id: str
    split_depth: int
    start_chapter_index: int
    end_chapter_index: int
    chapters: list[Chapter]
    token_estimate: int
    chapter_count: int

    @classmethod
    def from_chapters(
        cls,
        chapters: list[Chapter],
        *,
        split_depth: int = 0,
        parent_batch_id: str = "",
        batch_id: str = "",
    ) -> ChapterBatch:
        if not chapters:
            raise ValueError("chapters must not be empty")
        start_chapter_index = chapters[0].chapter_index
        end_chapter_index = chapters[-1].chapter_index
        resolved_batch_id = batch_id or build_batch_id(start_chapter_index, end_chapter_index, split_depth)
        return cls(
            batch_id=resolved_batch_id,
            parent_batch_id=parent_batch_id,
            split_depth=split_depth,
            start_chapter_index=start_chapter_index,
            end_chapter_index=end_chapter_index,
            chapters=list(chapters),
            token_estimate=sum(chapter.token_estimate for chapter in chapters),
            chapter_count=len(chapters),
        )

    @property
    def display_range(self) -> str:
        return build_chapter_range_label(self.start_chapter_index, self.end_chapter_index)

    def split(self) -> tuple[ChapterBatch, ChapterBatch]:
        if self.chapter_count <= 1:
            raise ValueError("single chapter batch cannot be split")
        midpoint = self.chapter_count // 2
        left = ChapterBatch.from_chapters(
            self.chapters[:midpoint],
            split_depth=self.split_depth + 1,
            parent_batch_id=self.batch_id,
        )
        right = ChapterBatch.from_chapters(
            self.chapters[midpoint:],
            split_depth=self.split_depth + 1,
            parent_batch_id=self.batch_id,
        )
        return left, right


@dataclass(slots=True)
class WorkspacePaths:
    epub_name: str
    root: Path
    source_dir: Path
    output_dir: Path
    state_dir: Path
    checkpoint_dir: Path
    temp_dir: Path
    logs_dir: Path

    def output_path_for_schema(self, schema_name: str) -> Path:
        return self.output_dir / f"{schema_name}.yaml"

    def progress_path_for_schema(self, schema_name: str) -> Path:
        return self.state_dir / f"{schema_name}.progress.yaml"

    def checkpoint_dir_for_id(self, checkpoint_id: str) -> Path:
        return self.checkpoint_dir / checkpoint_id

    def checkpoint_state_dir_for_id(self, checkpoint_id: str) -> Path:
        return self.checkpoint_dir_for_id(checkpoint_id) / "state"

    def checkpoint_output_dir_for_id(self, checkpoint_id: str) -> Path:
        return self.checkpoint_dir_for_id(checkpoint_id) / "output"

    def checkpoint_output_path_for_schema(self, schema_name: str, checkpoint_id: str) -> Path:
        return self.checkpoint_output_dir_for_id(checkpoint_id) / f"{schema_name}.yaml"

    def checkpoint_progress_path_for_schema(self, schema_name: str, checkpoint_id: str) -> Path:
        return self.checkpoint_state_dir_for_id(checkpoint_id) / f"{schema_name}.progress.yaml"

    def checkpoint_meta_path_for_schema(self, schema_name: str, checkpoint_id: str) -> Path:
        return self.checkpoint_state_dir_for_id(checkpoint_id) / f"{schema_name}.meta.yaml"

    def checkpoint_latest_path_for_schema(self, schema_name: str) -> Path:
        return self.checkpoint_dir / f"{schema_name}.latest.yaml"

    def stream_buffer_path_for_schema(self, schema_name: str) -> Path:
        return self.temp_dir / f"{schema_name}.stream.txt"

    def log_path(self) -> Path:
        return self.logs_dir / "run.log"

    def debug_dir(self) -> Path:
        return self.logs_dir / "debug"

    def prompt_debug_path_for_attempt(
        self,
        schema_name: str,
        start_chapter_index: int,
        end_chapter_index: int,
        split_depth: int,
        retry_attempt: int,
        template_name: str,
    ) -> Path:
        return self.debug_dir() / build_debug_artifact_name(
            schema_name=schema_name,
            start_chapter_index=start_chapter_index,
            end_chapter_index=end_chapter_index,
            split_depth=split_depth,
            retry_attempt=retry_attempt,
            template_name=template_name,
            suffix="prompt.txt",
        )

    def response_debug_path_for_attempt(
        self,
        schema_name: str,
        start_chapter_index: int,
        end_chapter_index: int,
        split_depth: int,
        retry_attempt: int,
        template_name: str,
    ) -> Path:
        return self.debug_dir() / build_debug_artifact_name(
            schema_name=schema_name,
            start_chapter_index=start_chapter_index,
            end_chapter_index=end_chapter_index,
            split_depth=split_depth,
            retry_attempt=retry_attempt,
            template_name=template_name,
            suffix="response.yaml",
        )


def build_debug_artifact_name(
    *,
    schema_name: str,
    start_chapter_index: int,
    end_chapter_index: int,
    split_depth: int,
    retry_attempt: int,
    template_name: str,
    suffix: str,
) -> str:
    safe_schema = normalize_debug_token(schema_name)
    safe_template = normalize_debug_token(template_name)
    chapter_range = build_chapter_range_label(start_chapter_index, end_chapter_index)
    return f"{safe_schema}.{chapter_range}.d{split_depth}.r{retry_attempt:02d}.{safe_template}.{suffix}"


def build_batch_id(start_chapter_index: int, end_chapter_index: int, split_depth: int) -> str:
    return f"{build_chapter_range_label(start_chapter_index, end_chapter_index)}-d{split_depth}"


def build_checkpoint_id(chapter_index: int) -> str:
    return f"ch{chapter_index:04d}"


def build_chapter_range_label(start_chapter_index: int, end_chapter_index: int) -> str:
    return f"ch{start_chapter_index:04d}-ch{end_chapter_index:04d}"


def normalize_debug_token(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip().lower())
    return normalized.strip("-._") or "value"



@dataclass(slots=True)
class SchemaField:
    path: str
    field_type: str
    required: bool
    instruction: str = ""


@dataclass(slots=True)
class SchemaDefinition:
    schema_name: str
    schema_path: Path
    root_key: str
    match_key: str
    schema_text: str
    raw_schema: dict[str, Any]
    fields: list[SchemaField] = field(default_factory=list)
    allowed_top_level_keys: list[str] = field(default_factory=list)
    required_item_keys: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PromptTemplate:
    name: str
    path: Path
    index: int
    content: str


@dataclass(slots=True)
class ValidationIssue:
    path: str
    reason: str


@dataclass(slots=True)
class ValidationResult:
    ok: bool
    errors: list[ValidationIssue] = field(default_factory=list)

    def summary(self) -> str:
        if self.ok:
            return "validation passed"
        return "; ".join(f"{issue.path}: {issue.reason}" for issue in self.errors)


@dataclass(slots=True)
class MergeStats:
    replaced_nodes: int = 0
    appended_nodes: int = 0


@dataclass(slots=True)
class RetryConfig:
    max_attempts: int = 3
    backoff_seconds: int = 3


@dataclass(slots=True)
class BatchingConfig:
    enable_multi_chapter: bool = True
    max_input_tokens: int = 12000
    prompt_overhead_tokens: int = 1500
    reserve_output_tokens: int = 3000
    allow_oversize_single_chapter: bool = True
    split_on_failure: bool = True
    split_after_retry_exhausted: bool = True
    enable_checkpoint: bool = False
    checkpoint_every_n_chapters: int = 10

    @property
    def chapter_token_budget(self) -> int:
        return max(self.max_input_tokens - self.prompt_overhead_tokens - self.reserve_output_tokens, 0)


@dataclass(slots=True)
class AppConfig:
    input_epubs: list[Path]
    schema_paths: list[Path]
    prompt_templates: list[Path]
    workspace_root: Path
    enable_parallel_tasks: bool = True
    max_workers: int = 4
    resume: bool = True
    retry_count: int = 3
    retry_backoff_seconds: int = 3
    emit_console_progress: bool = True
    model_provider: str = "openai"
    model_name: str = "gpt-4.1"
    streaming: bool = True
    base_url: str = "https://api.openai.com/v1"
    api_key: str = ""
    batching: BatchingConfig = field(default_factory=BatchingConfig)


@dataclass(slots=True)
class TaskDefinition:
    epub_path: Path
    workspace: WorkspacePaths
    schema_path: Path
    prompt_template_paths: list[Path]
    output_path: Path
    progress_path: Path
    stream_buffer_path: Path


@dataclass(slots=True)
class TaskRuntimeState:
    epub_path: str
    workspace_root: str
    chapter_index: int
    chapter_id: str
    total_chapters: int
    schema_name: str
    schema_path: str
    root_key: str
    match_key: str
    current_prompt_template: str
    prompt_template_index: int
    output_path: str
    progress_path: str
    stream_buffer_path: str


class StreamModelClient(Protocol):
    def stream_yaml(self, prompt: str) -> Iterable[str]:
        ...

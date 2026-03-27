from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
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
class WorkspacePaths:
    epub_name: str
    root: Path
    source_dir: Path
    output_dir: Path
    state_dir: Path
    temp_dir: Path
    logs_dir: Path

    def output_path_for_schema(self, schema_name: str) -> Path:
        return self.output_dir / f"{schema_name}.yaml"

    def progress_path_for_schema(self, schema_name: str) -> Path:
        return self.state_dir / f"{schema_name}.progress.yaml"

    def stream_buffer_path_for_schema(self, schema_name: str) -> Path:
        return self.temp_dir / f"{schema_name}.stream.txt"

    def log_path(self) -> Path:
        return self.logs_dir / "run.log"


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

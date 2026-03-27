from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from models import ChapterBatch, MergeStats, PromptTemplate


class ProgressStore:
    def load(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        content = path.read_text(encoding="utf-8").strip()
        if not content:
            return {}
        data = yaml.safe_load(content) or {}
        if not isinstance(data, dict):
            raise ValueError(f"progress root must be mapping: {path}")
        return data

    def initialize(
        self,
        path: Path,
        *,
        epub_path: str,
        workspace_root: str,
        schema_path: str,
        output_path: str,
        stream_buffer_path: str,
        total_chapters: int,
        max_attempts: int,
    ) -> dict[str, Any]:
        data = {
            "epub_path": epub_path,
            "workspace_root": workspace_root,
            "schema_path": schema_path,
            "output_path": output_path,
            "stream_buffer_path": stream_buffer_path,
            "total_chapters": total_chapters,
            "last_completed_chapter_index": 0,
            "last_completed_chapter_id": "",
            "current_chapter_index": 1 if total_chapters else 0,
            "completed_chapters": 0,
            "progress_percent": 0.0,
            "status": "pending",
            "current_batch_id": "",
            "current_batch_range": "",
            "current_batch_depth": 0,
            "current_batch_parent_id": "",
            "batch_retry_count": 0,
            "batch_status": "pending",
            "last_split_reason": "",
            "prompt": {
                "current_template": "",
                "template_index": 0,
                "attempted_templates": [],
            },
            "retry": {
                "current_attempt": 0,
                "max_attempts": max_attempts,
                "last_error": "",
            },
            "stream": {
                "last_receive_status": "pending",
                "last_chunk_count": 0,
            },
            "validation": {
                "last_result": "pending",
                "last_error_count": 0,
            },
            "merge": {
                "replaced_nodes": 0,
                "appended_nodes": 0,
            },
            "updated_at": utc_now_iso(),
        }
        self.save(path, data)
        return data

    def save(self, path: Path, data: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        data["updated_at"] = utc_now_iso()
        serialized = yaml.safe_dump(data, allow_unicode=True, sort_keys=False, indent=2)
        path.write_text(serialized, encoding="utf-8")

    def update_running(
        self,
        data: dict[str, Any],
        *,
        batch: ChapterBatch,
        completed_chapters: int,
        total_chapters: int,
        template: PromptTemplate,
        attempted_templates: list[str],
        retry_attempt: int,
        status: str,
        last_error: str = "",
    ) -> dict[str, Any]:
        data["current_chapter_index"] = batch.start_chapter_index
        data["completed_chapters"] = completed_chapters
        data["progress_percent"] = calculate_progress(completed_chapters, total_chapters)
        data["status"] = status
        data["prompt"] = {
            "current_template": str(template.path).replace("\\", "/"),
            "template_index": template.index,
            "attempted_templates": attempted_templates,
        }
        data["retry"]["current_attempt"] = retry_attempt
        data["retry"]["last_error"] = last_error
        self._set_batch_context(data, batch)
        data["batch_retry_count"] = retry_attempt
        data["batch_status"] = status
        return data

    def update_stream(self, data: dict[str, Any], *, receive_status: str, chunk_count: int) -> dict[str, Any]:
        data["stream"] = {
            "last_receive_status": receive_status,
            "last_chunk_count": chunk_count,
        }
        return data

    def update_validation(self, data: dict[str, Any], *, result: str, error_count: int, last_error: str) -> dict[str, Any]:
        data["validation"] = {
            "last_result": result,
            "last_error_count": error_count,
        }
        data["retry"]["last_error"] = last_error
        return data

    def update_merge(self, data: dict[str, Any], stats: MergeStats) -> dict[str, Any]:
        data["merge"] = {
            "replaced_nodes": stats.replaced_nodes,
            "appended_nodes": stats.appended_nodes,
        }
        return data

    def mark_batch_completed(
        self,
        data: dict[str, Any],
        *,
        batch: ChapterBatch,
        total_chapters: int,
    ) -> dict[str, Any]:
        self._set_batch_context(data, batch)
        data["last_completed_chapter_index"] = batch.end_chapter_index
        data["last_completed_chapter_id"] = batch.chapters[-1].chapter_id
        data["completed_chapters"] = batch.end_chapter_index
        data["current_chapter_index"] = min(batch.end_chapter_index + 1, total_chapters)
        data["progress_percent"] = calculate_progress(batch.end_chapter_index, total_chapters)
        data["status"] = "completed" if batch.end_chapter_index >= total_chapters else "running"
        data["batch_status"] = "completed"
        return data

    def mark_batch_split(self, data: dict[str, Any], *, batch: ChapterBatch, reason: str) -> dict[str, Any]:
        self._set_batch_context(data, batch)
        data["status"] = "splitting"
        data["batch_status"] = "split"
        data["last_split_reason"] = reason
        return data

    def mark_failed(
        self,
        data: dict[str, Any],
        *,
        last_error: str,
        retry_attempt: int,
        batch: ChapterBatch | None = None,
    ) -> dict[str, Any]:
        if batch is not None:
            self._set_batch_context(data, batch)
            data["batch_retry_count"] = retry_attempt
            data["batch_status"] = "failed"
        data["status"] = "failed"
        data["retry"]["current_attempt"] = retry_attempt
        data["retry"]["last_error"] = last_error
        return data

    def _set_batch_context(self, data: dict[str, Any], batch: ChapterBatch) -> None:
        data["current_batch_id"] = batch.batch_id
        data["current_batch_range"] = batch.display_range
        data["current_batch_depth"] = batch.split_depth
        data["current_batch_parent_id"] = batch.parent_batch_id


def calculate_progress(completed: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((completed / total) * 100, 2)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


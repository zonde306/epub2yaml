from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import yaml

from models import TaskDefinition, WorkspacePaths, build_checkpoint_id


class CheckpointStore:
    def save_checkpoint(
        self,
        task: TaskDefinition,
        *,
        schema_name: str,
        progress: dict[str, Any],
        total_chapters: int,
    ) -> dict[str, Any]:
        chapter_index = int(progress.get("last_completed_chapter_index", 0))
        if chapter_index <= 0:
            raise ValueError("cannot save checkpoint before any chapter is completed")

        checkpoint_id = build_checkpoint_id(chapter_index)
        self._validate_runtime_files(task.output_path, task.progress_path)

        output_snapshot_path = task.workspace.checkpoint_output_path_for_schema(schema_name, checkpoint_id)
        progress_snapshot_path = task.workspace.checkpoint_progress_path_for_schema(schema_name, checkpoint_id)
        meta_path = task.workspace.checkpoint_meta_path_for_schema(schema_name, checkpoint_id)
        latest_path = task.workspace.checkpoint_latest_path_for_schema(schema_name)

        output_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        progress_snapshot_path.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(task.output_path, output_snapshot_path)
        shutil.copy2(task.progress_path, progress_snapshot_path)

        metadata = {
            "checkpoint_id": checkpoint_id,
            "schema_name": schema_name,
            "epub_path": str(task.epub_path).replace("\\", "/"),
            "schema_path": str(task.schema_path).replace("\\", "/"),
            "workspace_root": str(task.workspace.root).replace("\\", "/"),
            "chapter_index": chapter_index,
            "total_chapters": total_chapters,
            "is_final": chapter_index >= total_chapters,
            "output_path": str(output_snapshot_path).replace("\\", "/"),
            "progress_path": str(progress_snapshot_path).replace("\\", "/"),
            "saved_at": utc_now_iso(),
        }
        self._write_yaml(meta_path, metadata)
        self._write_yaml(
            latest_path,
            {
                "checkpoint_id": checkpoint_id,
                "schema_name": schema_name,
                "chapter_index": chapter_index,
                "saved_at": metadata["saved_at"],
            },
        )
        return metadata

    def list_checkpoints(self, workspace: WorkspacePaths, *, schema_name: str) -> list[dict[str, Any]]:
        if not workspace.checkpoint_dir.exists():
            return []

        checkpoints: list[dict[str, Any]] = []
        for checkpoint_dir in workspace.checkpoint_dir.iterdir():
            if not checkpoint_dir.is_dir():
                continue
            meta_path = workspace.checkpoint_meta_path_for_schema(schema_name, checkpoint_dir.name)
            if not meta_path.exists():
                continue
            metadata = self._load_yaml(meta_path)
            if metadata:
                checkpoints.append(metadata)

        checkpoints.sort(key=lambda item: (int(item.get("chapter_index", 0)), str(item.get("checkpoint_id", ""))))
        return checkpoints

    def resolve_checkpoint_id(
        self,
        workspace: WorkspacePaths,
        *,
        schema_name: str,
        checkpoint_id: str | None = None,
        latest: bool = False,
    ) -> str:
        if checkpoint_id and latest:
            raise ValueError("checkpoint_id and latest cannot be used together")
        if checkpoint_id:
            return checkpoint_id
        if latest:
            latest_path = workspace.checkpoint_latest_path_for_schema(schema_name)
            latest_data = self._load_yaml(latest_path)
            resolved = str(latest_data.get("checkpoint_id", "")).strip()
            if not resolved:
                raise FileNotFoundError(f"latest checkpoint not found for schema: {schema_name}")
            return resolved
        raise ValueError("either checkpoint_id or latest must be provided")

    def load_checkpoint(
        self,
        workspace: WorkspacePaths,
        *,
        schema_name: str,
        checkpoint_id: str,
    ) -> dict[str, Any]:
        output_path = workspace.checkpoint_output_path_for_schema(schema_name, checkpoint_id)
        progress_path = workspace.checkpoint_progress_path_for_schema(schema_name, checkpoint_id)
        meta_path = workspace.checkpoint_meta_path_for_schema(schema_name, checkpoint_id)
        self._validate_checkpoint_files(output_path=output_path, progress_path=progress_path, meta_path=meta_path)
        return {
            "checkpoint_id": checkpoint_id,
            "output_path": output_path,
            "progress_path": progress_path,
            "meta_path": meta_path,
            "meta": self._load_yaml(meta_path),
        }

    def restore_checkpoint(
        self,
        task: TaskDefinition,
        *,
        schema_name: str,
        checkpoint_id: str | None = None,
        latest: bool = False,
    ) -> dict[str, Any]:
        resolved_id = self.resolve_checkpoint_id(
            task.workspace,
            schema_name=schema_name,
            checkpoint_id=checkpoint_id,
            latest=latest,
        )
        snapshot = self.load_checkpoint(task.workspace, schema_name=schema_name, checkpoint_id=resolved_id)
        self._copy_replace(snapshot["output_path"], task.output_path)
        self._copy_replace(snapshot["progress_path"], task.progress_path)
        return snapshot

    def _validate_runtime_files(self, output_path: Path, progress_path: Path) -> None:
        missing = [str(path) for path in (output_path, progress_path) if not path.exists()]
        if missing:
            raise FileNotFoundError(f"runtime files missing for checkpoint save: {', '.join(missing)}")

    def _validate_checkpoint_files(self, *, output_path: Path, progress_path: Path, meta_path: Path) -> None:
        missing = [str(path) for path in (output_path, progress_path, meta_path) if not path.exists()]
        if missing:
            raise FileNotFoundError(f"checkpoint files missing: {', '.join(missing)}")

    def _copy_replace(self, source: Path, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        temp_target = target.with_name(f"{target.name}.tmp")
        shutil.copy2(source, temp_target)
        temp_target.replace(target)

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        content = path.read_text(encoding="utf-8").strip()
        if not content:
            return {}
        data = yaml.safe_load(content) or {}
        if not isinstance(data, dict):
            raise ValueError(f"yaml root must be mapping: {path}")
        return data

    def _write_yaml(self, path: Path, data: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = yaml.safe_dump(data, allow_unicode=True, sort_keys=False, indent=2)
        path.write_text(serialized, encoding="utf-8")


def utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

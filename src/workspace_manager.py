from __future__ import annotations

import re
import shutil
from pathlib import Path

from models import WorkspacePaths

INVALID_WORKSPACE_CHARS = re.compile(r"[^a-zA-Z0-9._-]+")


class WorkspaceManager:
    def __init__(self, workspace_root: Path) -> None:
        self.workspace_root = workspace_root

    def ensure_workspace(self, epub_path: Path) -> WorkspacePaths:
        epub_name = slugify_epub_name(epub_path.stem)
        root = self.workspace_root / epub_name
        source_dir = root / "source"
        output_dir = root / "output"
        state_dir = root / "state"
        temp_dir = root / "temp"
        logs_dir = root / "logs"

        for directory in (self.workspace_root, root, source_dir, output_dir, state_dir, temp_dir, logs_dir):
            directory.mkdir(parents=True, exist_ok=True)

        source_copy_path = source_dir / epub_path.name
        if epub_path.exists() and not source_copy_path.exists():
            shutil.copy2(epub_path, source_copy_path)

        return WorkspacePaths(
            epub_name=epub_name,
            root=root,
            source_dir=source_dir,
            output_dir=output_dir,
            state_dir=state_dir,
            temp_dir=temp_dir,
            logs_dir=logs_dir,
        )

    def cleanup_stale_stream(self, stream_buffer_path: Path) -> None:
        if stream_buffer_path.exists():
            stream_buffer_path.unlink()


def slugify_epub_name(name: str) -> str:
    stripped = name.strip().lower()
    normalized = INVALID_WORKSPACE_CHARS.sub("-", stripped)
    normalized = normalized.strip("-._")
    return normalized or "epub"

from __future__ import annotations

from pathlib import Path

from models import PromptTemplate

DEFAULT_PROMPT_ORDER = ["base.md", "retry_format.md", "retry_schema.md"]


class PromptLoader:
    def load_templates(self, prompt_paths: list[Path]) -> list[PromptTemplate]:
        ordered_paths = sort_prompt_paths(prompt_paths)
        templates: list[PromptTemplate] = []
        for index, path in enumerate(ordered_paths):
            templates.append(
                PromptTemplate(
                    name=path.stem,
                    path=path,
                    index=index,
                    content=path.read_text(encoding="utf-8"),
                )
            )
        return templates


def sort_prompt_paths(prompt_paths: list[Path]) -> list[Path]:
    order_map = {name: index for index, name in enumerate(DEFAULT_PROMPT_ORDER)}
    return sorted(
        prompt_paths,
        key=lambda path: (order_map.get(path.name, len(DEFAULT_PROMPT_ORDER)), path.name),
    )

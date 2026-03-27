from __future__ import annotations

from typing import Any

from models import ChapterBatch, PromptTemplate, SchemaDefinition


class PromptBuilder:
    def build(
        self,
        template: PromptTemplate,
        *,
        batch: ChapterBatch,
        schema_definition: SchemaDefinition,
        existing_yaml: str,
        existing_worldinfo: str,
        error_summary: str,
    ) -> str:
        values: dict[str, Any] = {
            "source_text": build_batch_source_text(batch),
            "schema_text": schema_definition.schema_text,
            "existing_yaml": existing_yaml.strip() or "{}",
            "existing_worldinfo": existing_worldinfo.strip() or "{}",
            "error_summary": error_summary.strip() or "none",
            "root_key": schema_definition.root_key,
            "match_key": schema_definition.match_key,
            "output_rules": build_output_rules(schema_definition),
            "chapter_title": describe_batch_titles(batch),
            "chapter_id": batch.display_range,
            "batch_id": batch.batch_id,
            "batch_range": batch.display_range,
            "chapter_count": batch.chapter_count,
        }
        return render_template(template.content, values)


def build_batch_source_text(batch: ChapterBatch) -> str:
    sections: list[str] = []
    for chapter in batch.chapters:
        sections.append(
            "\n".join(
                [
                    f"### Chapter {chapter.chapter_index}",
                    f"chapter_id: {chapter.chapter_id}",
                    f"chapter_title: {chapter.title}",
                    "",
                    chapter.text.strip(),
                ]
            ).strip()
        )
    return "\n\n".join(section for section in sections if section).strip()


def describe_batch_titles(batch: ChapterBatch) -> str:
    if batch.chapter_count == 1:
        return batch.chapters[0].title
    first = batch.chapters[0]
    last = batch.chapters[-1]
    return f"{first.title} ~ {last.title}"


def build_output_rules(schema_definition: SchemaDefinition) -> str:
    lines = [
        f"- 顶层只能包含 `{schema_definition.root_key}`",
        f"- 若输出了 `{schema_definition.root_key}`，其值必须是对象映射，而不是列表",
        f"- `{schema_definition.root_key}` 下的每个 key 都必须是非空 `{schema_definition.match_key}`",
        "- 每个条目的 key 就是唯一标识，不要在条目内部重复输出同名 match_key 字段",
        "- 允许只返回新增或更新过的字段，不需要返回完整节点",
        "- 不要输出空字符串、仅空白字符串、空列表、空字典或 `~`",
        "- 若无法确认字段值，直接省略该字段，不要输出无意义占位值",
    ]
    return "\n".join(lines)


def render_template(template: str, values: dict[str, Any]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", str(value))
    return rendered

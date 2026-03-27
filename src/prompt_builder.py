from __future__ import annotations

from typing import Any

from models import Chapter, PromptTemplate, SchemaDefinition


class PromptBuilder:
    def build(
        self,
        template: PromptTemplate,
        *,
        chapter: Chapter,
        schema_definition: SchemaDefinition,
        existing_yaml: str,
        existing_worldinfo: str,
        error_summary: str,
    ) -> str:
        values: dict[str, Any] = {
            "source_text": chapter.text,
            "schema_text": schema_definition.schema_text,
            "existing_yaml": existing_yaml.strip() or "{}",
            "existing_worldinfo": existing_worldinfo.strip() or "{}",
            "error_summary": error_summary.strip() or "none",
            "root_key": schema_definition.root_key,
            "match_key": schema_definition.match_key,
            "output_rules": build_output_rules(schema_definition),
            "chapter_title": chapter.title,
            "chapter_id": chapter.chapter_id,
        }
        return render_template(template.content, values)


def build_output_rules(schema_definition: SchemaDefinition) -> str:
    lines = [
        f"- 顶层只能包含 `{schema_definition.root_key}`",
        f"- `{schema_definition.root_key}` 必须是列表",
        f"- 列表中的每个节点都必须包含 `{schema_definition.match_key}`",
        "- 每个节点必须是完整节点，不允许只返回局部字段补丁",
        "- 若无法确认字段值，省略该字段，不要输出无意义占位值",
    ]
    return "\n".join(lines)


def render_template(template: str, values: dict[str, Any]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", str(value))
    return rendered

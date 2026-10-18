"""实体索引摘要构建。

会话模式下模型看不到 output.yaml 全文，需要一份稳定的摘要来表达「当前已有什么」。
摘要只包含 key 与一行描述，且在同一 generation 内生成一次后不再变化——
跨段重复生成会让 prompt 前缀抖动，直接摧毁缓存命中。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from models import SchemaDefinition
from edit_tools import summarize_entry

INDEX_DETAIL_COMPACT = "compact"
INDEX_DETAIL_FULL = "full"

DEFAULT_MAX_ENTRIES = 400
DEFAULT_SUMMARY_LIMIT = 60
DEFAULT_INDEX_TOKEN_BUDGET = 6000

@dataclass(slots=True)
class IndexSnapshot:
    """一份索引快照，可直接渲染为稳定的 system/user 文本。"""

    generation_id: int
    start_chapter_index: int
    end_chapter_index: int
    entry_count: int
    total_entry_count: int
    truncated: bool
    text: str

    @property
    def empty(self) -> bool:
        return self.entry_count == 0

def build_index_text(
    document: dict[str, Any],
    schema_definition: SchemaDefinition,
    *,
    detail: str = INDEX_DETAIL_COMPACT,
    max_entries: int = DEFAULT_MAX_ENTRIES,
    summary_limit: int = DEFAULT_SUMMARY_LIMIT,
    token_budget: int = DEFAULT_INDEX_TOKEN_BUDGET,
) -> IndexSnapshot:
    """把当前文档压缩成索引文本。

    输出形如::

        已有条目（共 12 个，按 key 排序）:
        - Alice: 爱丽丝
        - Bob: 鲍勃
    """
    entries = document.get(schema_definition.root_key)
    if not isinstance(entries, dict):
        entries = {}

    keys = sorted(str(key) for key in entries.keys())
    total = len(keys)
    kept = keys[: max(max_entries, 0)]

    lines: list[str] = []
    included = 0
    approximate_chars = 0
    char_budget = max(token_budget, 1) * 4

    for key in kept:
        node = entries.get(key)
        summary = summarize_entry(node, limit=summary_limit)
        if detail == INDEX_DETAIL_FULL:
            line = f"- {key}: {summary}" if summary else f"- {key}"
        else:
            line = f"- {key}: {summary}" if summary else f"- {key}"
        approximate_chars += len(line) + 1
        if approximate_chars > char_budget and included > 0:
            break
        lines.append(line)
        included += 1

    truncated = included < total
    header = f"已有条目（共 {total} 个，按 key 排序）:"
    if truncated:
        header += f" 仅列出前 {included} 个"

    body = "\n".join(lines) if lines else "（暂无条目）"
    text = f"{header}\n{body}"

    return IndexSnapshot(
        generation_id=0,
        start_chapter_index=0,
        end_chapter_index=0,
        entry_count=included,
        total_entry_count=total,
        truncated=truncated,
        text=text,
    )

def build_watermark_text(*, generation_id: int, last_completed_chapter_index: int, total_chapters: int) -> str:
    """新 generation 的进度水位行：告诉模型「之前处理到哪里、上下文为何重置」。"""
    return (
        f"这是第 {generation_id} 轮上下文。已完成前 {last_completed_chapter_index} 章"
        f"（共 {total_chapters} 章）。之前轮次的原文与工具调用记录已从上下文中移除，"
        "但所有编辑结果都已写入输出文件，上面的条目索引即为当前状态。"
        "如需要某个条目的完整内容，请使用 read_entry 读取。"
    )

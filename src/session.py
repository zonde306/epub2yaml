"""连续会话式抽取：append-only 消息时间线与上下文旋转。

缓存命中要求消息前缀逐字节稳定（见 ``plans/agent-edit-session-refactor-plan.md``）：

- ``[0] system``  人设与工具规则，跨 generation 完全相同
- ``[1] system``  schema 全文，跨 generation 完全相同
- ``[2] user``    实体索引摘要 + 进度水位（每 generation 只出现一次）
- ``[3] assistant`` 收尾消息，标记段落循环的基线
- 之后每段只追加 ``user`` / ``assistant`` / ``tool`` 消息，绝不重写历史

因为只追加，``prefix_signature()`` 在连续请求之间必然构成严格前缀关系，
这是可以在 CI 中断言的确定性性质，不依赖对 API 缓存行为的假设。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from index_builder import IndexSnapshot, build_index_text, build_watermark_text

# ToolCall / AssistantTurn 定义在 models 中，由 llm_client 与 session 共享；此处重新导出。
from models import AssistantTurn, SessionConfig, SchemaDefinition, ToolCall
from yaml_store import YamlStore

__all__ = [
    "READY_MESSAGE",
    "AssistantTurn",
    "ExtractionSession",
    "PrefixMismatchError",
    "ToolCall",
    "UsageStats",
    "load_document_for_session",
    "sums_tokens",
]

READY_MESSAGE = "明白，请提供第一章内容。"

@dataclass(slots=True)
class UsageStats:
    """累计 token 用量与缓存命中观测。"""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    cache_hit_tokens: int = 0
    cache_miss_tokens: int = 0
    request_count: int = 0

    def add(
        self,
        *,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        cache_hit_tokens: int = 0,
        cache_miss_tokens: int = 0,
    ) -> None:
        self.prompt_tokens += max(int(prompt_tokens), 0)
        self.completion_tokens += max(int(completion_tokens), 0)
        self.cache_hit_tokens += max(int(cache_hit_tokens), 0)
        self.cache_miss_tokens += max(int(cache_miss_tokens), 0)
        self.request_count += 1

    @property
    def hit_ratio(self) -> float:
        total = self.cache_hit_tokens + self.cache_miss_tokens
        if total <= 0:
            return 0.0
        return self.cache_hit_tokens / total

    def to_progress_dict(self) -> dict[str, Any]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "cache_hit_tokens": self.cache_hit_tokens,
            "cache_miss_tokens": self.cache_miss_tokens,
            "hit_ratio": round(self.hit_ratio, 4),
            "request_count": self.request_count,
        }

class PrefixMismatchError(RuntimeError):
    """消息前缀不再是 append-only 的严格前缀关系。"""

class ExtractionSession:
    """单个任务（epub × schema）的连续会话。"""

    def __init__(
        self,
        *,
        schema_definition: SchemaDefinition,
        config: SessionConfig | None = None,
        persona_text: str = "",
        messages_path: Path | None = None,
        usage: UsageStats | None = None,
    ) -> None:
        self.schema_definition = schema_definition
        self.config = config or SessionConfig()
        self.persona_text = persona_text.strip()
        self.messages_path = Path(messages_path) if messages_path else None

        self.messages: list[dict[str, Any]] = []
        self.generation_id = 1
        self.rotate_count = 0
        self.segments_completed = 0
        self.last_segment_end_chapter = 0
        self.last_prompt_tokens = 0
        self.usage = usage or UsageStats()
        self.index_snapshot: IndexSnapshot | None = None

    # ------------------------------------------------------------------ 生命周期

    @property
    def is_started(self) -> bool:
        return bool(self.messages)

    def start(
        self,
        document: dict[str, Any],
        *,
        total_chapters: int = 0,
        last_completed_chapter_index: int = 0,
        generation_id: int | None = None,
    ) -> None:
        """建立 generation 头部：system 人设、system schema、索引摘要、收尾基线。"""
        if self.is_started:
            raise RuntimeError("session already started")
        if generation_id is not None:
            self.generation_id = generation_id

        self._append({"role": "system", "content": self.persona_text})
        self._append({"role": "system", "content": self._schema_block()})
        self._append(
            {
                "role": "user",
                "content": self._index_block(
                    document,
                    total_chapters=total_chapters,
                    last_completed_chapter_index=last_completed_chapter_index,
                    include_watermark=self.generation_id > 1,
                ),
            }
        )
        self._append({"role": "assistant", "content": READY_MESSAGE})

    def rotate(
        self,
        document: dict[str, Any],
        *,
        total_chapters: int = 0,
        last_completed_chapter_index: int | None = None,
    ) -> None:
        """开启新 generation：重置时间线，但 ``[0][1]`` 字节保持不变。"""
        if not self.is_started:
            raise RuntimeError("session must be started before rotation")

        if last_completed_chapter_index is not None:
            self.last_segment_end_chapter = last_completed_chapter_index

        system_blocks = [message for message in self.messages[:2]]
        self.messages = []
        self.generation_id += 1
        self.rotate_count += 1

        for message in system_blocks:
            self._append(message)
        self._append(
            {
                "role": "user",
                "content": self._index_block(
                    document,
                    total_chapters=total_chapters,
                    last_completed_chapter_index=self.last_segment_end_chapter,
                    include_watermark=True,
                ),
            }
        )
        self._append({"role": "assistant", "content": READY_MESSAGE})

    # ------------------------------------------------------------------ 追加消息

    def append_segment(self, source_text: str, *, batch_range: str, chapter_count: int) -> None:
        """追加一段原文。段落必须在一个 user 消息内完整给出。"""
        header = f"以下为第 {batch_range} 段原文，共 {chapter_count} 章。请开始分析并调用编辑工具。"
        self._append({"role": "user", "content": f"{header}\n\n{source_text.strip()}"})

    def append_repair_note(self, text: str) -> None:
        """追加一条修复指令。

        不重发原文：原文已在同一会话的历史消息中，重发只会翻倍 token 消耗。
        """
        self._append({"role": "user", "content": text.strip()})

    def append_assistant(self, turn: AssistantTurn) -> None:
        message: dict[str, Any] = {"role": "assistant", "content": turn.content or ""}
        if turn.has_tool_calls:
            message["tool_calls"] = [call.to_message_part() for call in turn.tool_calls]
        self._append(message)

    def append_tool_result(self, *, tool_call_id: str, payload: dict[str, Any]) -> None:
        self._append(
            {
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": json.dumps(payload, ensure_ascii=False),
            }
        )

    def mark_segment_complete(self, *, end_chapter_index: int) -> None:
        if end_chapter_index > self.last_segment_end_chapter:
            self.last_segment_end_chapter = end_chapter_index
        self.segments_completed += 1

    # ------------------------------------------------------------------ 旋转判断

    def should_rotate(self, *, prompt_tokens: int | None = None) -> bool:
        """是否应该开启新 generation。

        ``max_context_tokens<=0`` 表示不旋转（仅用于短书调试）。
        """
        budget = int(self.config.max_context_tokens)
        if budget <= 0:
            return False
        observed = self.last_prompt_tokens if prompt_tokens is None else int(prompt_tokens)
        return observed > budget

    def next_prompt_tokens(self, *, estimated_chars: int) -> int:
        """下一次请求的 prompt 规模估计（字符数 / 4）。"""
        current = sums_tokens(self.messages)
        return current + max(estimated_chars, 0) // 4

    # ------------------------------------------------------------------ 缓存断言

    def prefix_signature(self) -> str:
        """返回消息序列的确定性序列化，用于验证前缀严格性。

        每次重新计算，不缓存：这个值用于检测历史消息是否被改写，
        缓存会直接把要检测的改动掩盖掉。
        """
        return "".join(json.dumps(message, ensure_ascii=False, sort_keys=False) for message in self.messages)

    def assert_prefix_of(self, previous_signature: str) -> None:
        """确认本次请求的消息前缀仍然是之前请求的超集（严格前缀）。"""
        current = self.prefix_signature()
        if not current.startswith(previous_signature):
            raise PrefixMismatchError(
                "消息前缀不再是 append-only：缓存前缀被破坏。"
                "请检查是否有代码在修改历史消息。"
            )

    def system_prefix(self) -> str:
        """``[0][1]`` 两个 system 块的序列化，跨 generation 必须保持一致。"""
        if len(self.messages) < 2:
            return ""
        return "".join(
            json.dumps(message, ensure_ascii=False, sort_keys=False) for message in self.messages[:2]
        )

    # ------------------------------------------------------------------ 用量记账

    def record_usage(self, usage: dict[str, Any] | None) -> None:
        usage = usage or {}
        prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
        self.last_prompt_tokens = prompt_tokens
        self.usage.add(
            prompt_tokens=prompt_tokens,
            completion_tokens=int(usage.get("completion_tokens", 0) or 0),
            cache_hit_tokens=int(usage.get("prompt_cache_hit_tokens", 0) or 0),
            cache_miss_tokens=int(usage.get("prompt_cache_miss_tokens", 0) or 0),
        )

    # ------------------------------------------------------------------ 持久化

    def persist(self) -> None:
        """把消息与 generation 边界写入 ``messages.jsonl``。"""
        if not self.config.persist_messages or not self.messages_path:
            return
        self.messages_path.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = [
            json.dumps(
                {
                    "kind": "meta",
                    "generation_id": self.generation_id,
                    "rotate_count": self.rotate_count,
                    "segments_completed": self.segments_completed,
                    "last_segment_end_chapter": self.last_segment_end_chapter,
                },
                ensure_ascii=False,
            )
        ]
        for message in self.messages:
            lines.append(json.dumps({"kind": "message", **message}, ensure_ascii=False))
        self.messages_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def restore(self) -> bool:
        """从 ``messages.jsonl`` 恢复时间线。返回是否恢复成功。"""
        if not self.messages_path or not self.messages_path.exists():
            return False

        records: list[dict[str, Any]] = []
        for line in self.messages_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                # 崩溃可能留下半行，丢弃尾部不完整记录
                continue
        if not records:
            return False

        for record in records:
            if record.get("kind") == "meta":
                self.generation_id = int(record.get("generation_id", 1) or 1)
                self.rotate_count = int(record.get("rotate_count", 0) or 0)
                self.segments_completed = int(record.get("segments_completed", 0) or 0)
                self.last_segment_end_chapter = int(record.get("last_segment_end_chapter", 0) or 0)
                continue
            if record.get("kind") != "message":
                continue
            message = {key: value for key, value in record.items() if key != "kind"}
            if not message.get("role"):
                continue
            self.messages.append(message)

        return bool(self.messages)

    # ------------------------------------------------------------------ 内部

    def _append(self, message: dict[str, Any]) -> None:
        self.messages.append(message)

    def _schema_block(self) -> str:
        return (
            "以下是本次抽取任务使用的 schema。字段名称、层级与注释以它为准，"
            "不要新增 schema 之外的字段。\n\n```yaml\n"
            f"{self.schema_definition.schema_text.strip()}\n```"
        )

    def _index_block(
        self,
        document: dict[str, Any],
        *,
        total_chapters: int,
        last_completed_chapter_index: int,
        include_watermark: bool,
    ) -> str:
        snapshot = build_index_text(
            document,
            self.schema_definition,
            detail=self.config.index_detail,
        )
        snapshot.generation_id = self.generation_id
        snapshot.start_chapter_index = last_completed_chapter_index + 1
        snapshot.end_chapter_index = total_chapters
        self.index_snapshot = snapshot

        parts = [snapshot.text]
        if include_watermark:
            parts.append(
                build_watermark_text(
                    generation_id=self.generation_id,
                    last_completed_chapter_index=last_completed_chapter_index,
                    total_chapters=total_chapters,
                )
            )
        parts.append(
            f"输出根节点为 `{self.schema_definition.root_key}`，条目匹配键为 "
            f"`{self.schema_definition.match_key}`。请使用编辑工具修改内容，不要输出完整 YAML。"
        )
        return "\n\n".join(parts)

def sums_tokens(messages: Iterable[dict[str, Any]]) -> int:
    """按字符数估算消息列表的 token 数（与 epub_reader 的估算口径一致，除数为 4）。"""
    total_chars = 0
    for message in messages:
        total_chars += len(str(message.get("content", "") or ""))
        for call in message.get("tool_calls") or []:
            function = call.get("function") or {}
            total_chars += len(str(function.get("name", ""))) + len(str(function.get("arguments", "")))
    return total_chars // 4

def load_document_for_session(output_path: Path, schema_definition: SchemaDefinition) -> dict[str, Any]:
    return YamlStore().load_yaml(output_path, default={schema_definition.root_key: {}})

"""会话式（agent）抽取流程。

与 legacy 的 ``TaskRunner._run_batch`` 的区别：

- 不再要求模型输出完整 YAML，而是驱动其调用编辑工具
- 提示词前缀 append-only，schema 只出现一次，从而命中前缀缓存
- 段落失败以「追加新消息」的方式重试，而不是重跑整批 prompt
- 校验在每次工具调用后立即进行，错误以结构化 tool_result 回给模型

本模块只负责段落循环，批次组装/进度/checkpoint 仍由 ``TaskRunner`` 决定。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from edit_tools import EditTools
from index_builder import build_index_text
from models import (
    AppConfig,
    ChapterBatch,
    SchemaDefinition,
    StreamModelClient,
    TaskDefinition,
)
from session import ExtractionSession
from yaml_store import YamlStore

SEGMENT_RETRY_PREFIX = "上一段处理未完成"

@dataclass(slots=True)
class SegmentResult:
    """一个段落的执行结果。"""

    ok: bool
    reason: str = ""
    tool_rounds: int = 0
    edit_count: int = 0
    error_count: int = 0

@dataclass(slots=True)
class SegmentReport:
    """段落执行后需要回写到 progress 的信息。"""

    usage: dict[str, Any] = field(default_factory=dict)
    session_meta: dict[str, Any] = field(default_factory=dict)

def build_persona_text(schema_definition: SchemaDefinition, *, extra_rules: str = "") -> str:
    """构造稳定的 system 人设。必须跨 generation 逐字节一致，否则缓存前缀失效。"""
    lines = [
        "你是小说结构化分析专家。你的任务不是输出 YAML 文件，而是通过编辑工具",
        "逐步维护一份结构化的角色与设定数据库。",
        "",
        "## 工作方式",
        "",
        "- 每轮你会收到一段连续章节原文",
        "- 优先使用 batch_edit，把本段多个条目、多个字段的编辑合并到一次工具调用，减少往返轮数",
        "- batch_edit 的 operations 按顺序执行，每项包含 tool 和 arguments；参数与 upsert_entry / set_fields / append_items 相同",
        "- 批量操作全部校验通过才生效；失败时整批未写入，应修正 failed_index 处的错误后重发完整批次",
        "- 单处修改仍可直接使用 upsert_entry / set_fields / append_items",
        "- 写入前按需用 read_entry 确认现有内容，避免覆盖已有信息；已确认的内容不必重复读取",
        "- 一段处理完成后，停止调用工具并简短回复「本段完成」",
        "",
        "## 硬性要求",
        "",
        f"- 输出根节点为 `{schema_definition.root_key}`，条目匹配键为 `{schema_definition.match_key}`",
        "- 条目 key 必须使用英文字母、数字与下划线，空格替换为下划线，禁止中文 key",
        "- 字段层级与名称必须以 schema 为准，不要新增 schema 之外的字段",
        "- 不要写入空字符串、空数组、空对象，以及「略 / 同上 / 未知 / 未提及」这类占位值",
        "- 只记录本段原文中新增或发生变化的信息",
        "- 原文来自日语机器翻译，同一名词可能出现不同中文写法，需要统一命名",
        "- 总结使用全知视角，不要出现「当前 / 昨天 / 几年前」这类相对时间",
        "- 如果一个角色信息不足以构成有效条目，就不要为它创建条目",
    ]
    if extra_rules.strip():
        lines.extend(["", "## 输出规则", "", extra_rules.strip()])
    return "\n".join(lines)

def build_repair_message(result: SegmentResult) -> str:
    """段落失败后追加的新消息，要求模型修复而不是重跑整批。"""
    return (
        f"{SEGMENT_RETRY_PREFIX}：{result.reason}\n\n"
        "请检查上面的错误信息，只修复报告的问题，然后重新完成这一段。"
        "不要重复已经成功的编辑。"
    )

class AgentRunner:
    """驱动单个任务的会话式抽取。"""

    def __init__(
        self,
        *,
        config: AppConfig,
        task: TaskDefinition,
        schema_definition: SchemaDefinition,
        model_client: StreamModelClient,
        edit_tools: EditTools | None = None,
        session: ExtractionSession | None = None,
        yaml_store: YamlStore | None = None,
        logger: Any = None,
    ) -> None:
        self.config = config
        self.task = task
        self.schema_definition = schema_definition
        self.model_client = model_client
        self.yaml_store = yaml_store or YamlStore(config.merge)
        self.logger = logger
        schema_name = schema_definition.schema_name
        self.edit_tools = edit_tools or EditTools(
            output_path=task.output_path,
            schema_definition=schema_definition,
            journal_path=task.workspace.journal_path_for_schema(schema_name),
            merge_config=config.merge,
            allow_remove=config.tools.allow_remove,
            near_duplicate_check=config.tools.near_duplicate_check,
            forbidden_values=(*config.blacklist_keywords,),
        )
        self.session = session or ExtractionSession(
            schema_definition=schema_definition,
            config=config.session,
            persona_text=build_persona_text(schema_definition),
            messages_path=task.workspace.messages_path_for_schema(schema_name) if config.session.persist_messages else None,
        )

    # ------------------------------------------------------------------ 会话准备

    def ensure_session(
        self,
        *,
        total_chapters: int,
        last_completed_chapter_index: int,
    ) -> None:
        """按需建立或恢复会话时间线。"""
        if self.session.is_started:
            return

        document = self.yaml_store.load_yaml(
            self.task.output_path,
            default={self.schema_definition.root_key: {}},
        )

        if self.config.resume and self.session.restore():
            self._log(
                f"session restored generation={self.session.generation_id} "
                f"messages={len(self.session.messages)} last_chapter={self.session.last_segment_end_chapter}"
            )
            return

        self.session.start(
            document,
            total_chapters=total_chapters,
            last_completed_chapter_index=last_completed_chapter_index,
        )
        self._log(f"session started messages={len(self.session.messages)} generation={self.session.generation_id}")

    # ------------------------------------------------------------------ 段落执行

    async def run_segment(
        self,
        batch: ChapterBatch,
        *,
        source_text: str,
        total_chapters: int,
        resend_source: bool = True,
    ) -> SegmentResult:
        """执行一个段落，直到模型停止调用工具。

        ``resend_source=False`` 用于修复重试：原文已在历史消息中，不重复追加。
        """
        if resend_source:
            self.session.append_segment(
                source_text,
                batch_range=batch.display_range,
                chapter_count=batch.chapter_count,
            )
        self._log(f"segment start range={batch.display_range} chapters={batch.chapter_count}")

        max_rounds = max(coerce_positive_int(self.config.session.max_tool_rounds_per_segment, default=12), 1)
        tool_names = self.edit_tools.tool_names()
        tool_schemas = self.edit_tools.tool_schemas()
        edit_count = 0
        error_count = 0

        for round_index in range(max_rounds):
            completion = await self.model_client.complete_with_tools(self.session.messages, tool_schemas)
            self.session.record_usage(completion.usage)
            self.session.append_assistant(completion.turn)

            if not completion.turn.has_tool_calls:
                if edit_count == 0:
                    return SegmentResult(
                        ok=False,
                        reason="本段未产生任何编辑，也未报告已处理内容",
                        tool_rounds=round_index + 1,
                        edit_count=0,
                        error_count=error_count,
                    )
                self.session.mark_segment_complete(end_chapter_index=batch.end_chapter_index)
                self.session.persist()
                self._log(
                    f"segment done range={batch.display_range} rounds={round_index + 1} edits={edit_count} errors={error_count}"
                )
                return SegmentResult(
                    ok=True,
                    tool_rounds=round_index + 1,
                    edit_count=edit_count,
                    error_count=error_count,
                )

            for call in completion.turn.tool_calls:
                if call.name not in tool_names:
                    payload = {
                        "ok": False,
                        "error": {
                            "code": "unknown_tool",
                            "reason": f"未知工具: {call.name}",
                            "available_tools": ", ".join(sorted(tool_names)),
                        },
                    }
                    self.session.append_tool_result(tool_call_id=call.id, payload=payload)
                    error_count += 1
                    continue

                arguments = parse_tool_arguments(call.arguments)
                if arguments is None:
                    payload = {
                        "ok": False,
                        "error": {
                            "code": "bad_arguments",
                            "reason": "工具参数不是合法 JSON 对象",
                            "received": call.arguments[:200],
                        },
                    }
                    self.session.append_tool_result(tool_call_id=call.id, payload=payload)
                    error_count += 1
                    continue

                outcome = self.edit_tools.dispatch(call.name, arguments)
                self.session.append_tool_result(tool_call_id=call.id, payload=outcome.to_payload())
                if outcome.ok:
                    if call.name != "read_entry" and call.name != "list_entries":
                        edit_count += outcome.extra["edit_count"] if call.name == "batch_edit" else 1
                else:
                    error_count += 1

        self.session.persist()
        return SegmentResult(
            ok=False,
            reason=f"工具调用轮数超过上限 {max_rounds}，本段未完成",
            tool_rounds=max_rounds,
            edit_count=edit_count,
            error_count=error_count,
        )

    async def repair_segment(
        self,
        batch: ChapterBatch,
        *,
        source_text: str,
        total_chapters: int,
        previous: SegmentResult,
    ) -> SegmentResult:
        """段落失败后追加修复指令并重试。

        只追加一条 user 消息，不改写历史、也不重发原文——
        原文已在同一会话的历史消息里，重发会翻倍 token 消耗。
        """
        self.session.append_repair_note(build_repair_message(previous))
        self._log(f"segment repair range={batch.display_range} reason={previous.reason}")
        return await self.run_segment(batch, source_text=source_text, total_chapters=total_chapters, resend_source=False)

    # ------------------------------------------------------------------ 旋转

    def maybe_rotate(self, *, total_chapters: int, at_checkpoint_boundary: bool = False) -> bool:
        """按上下文预算与 checkpoint 窗口决定是否开启新 generation。

        对齐 checkpoint 窗口的目的是让旋转点可复现、可回滚。
        但预算超限不能无限延后，超过硬上限（1.5 倍）时立即旋转。
        """
        if not self.session.should_rotate():
            return False

        budget = coerce_positive_int(self.session.config.max_context_tokens, default=0)
        hard_limit = budget + max(budget // 2, 1)
        over_hard_limit = self.session.last_prompt_tokens > hard_limit
        if self.session.config.rotate_on_checkpoint and not at_checkpoint_boundary and not over_hard_limit:
            self._log(
                "session rotation deferred until checkpoint boundary "
                f"prompt_tokens={self.session.last_prompt_tokens} budget={budget}"
            )
            return False

        document = self.yaml_store.load_yaml(
            self.task.output_path,
            default={self.schema_definition.root_key: {}},
        )
        before = self.session.generation_id
        self.session.rotate(
            document,
            total_chapters=total_chapters,
            last_completed_chapter_index=self.session.last_segment_end_chapter,
        )
        self.session.persist()
        self._log(
            f"session rotated {before}->{self.session.generation_id} "
            f"last_chapter={self.session.last_segment_end_chapter} prompt_tokens={self.session.last_prompt_tokens} "
            f"checkpoint_boundary={at_checkpoint_boundary}"
        )
        return True

    def cache_report(self) -> dict[str, Any]:
        return self.session.usage.to_progress_dict()

    # ------------------------------------------------------------------ 内部

    def _log(self, message: str) -> None:
        if self.logger is not None:
            self.logger(message)

def coerce_positive_int(value: Any, *, default: int) -> int:
    """把配置值安全地转成正整数；配置写错时回退到默认值而不是抛异常。"""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default

def parse_tool_arguments(raw: str) -> dict[str, Any] | None:
    """解析工具参数。空串视为空对象；非对象一律视为非法。"""
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None

def extract_root_key_from_text(value: str) -> str | None:
    match = re.search(r"输出根节点为\s+`?([a-zA-Z_][a-zA-Z0-9_]*)", value)
    return match.group(1) if match else None

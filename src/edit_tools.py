"""结构化编辑工具集。

替代「LLM 输出全文 YAML」的工作方式：模型给出 key / 字段路径 / 值，
由本模块解析路径、执行修改、立即校验、原子回写，并在失败时返回可执行的错误信息。

设计约束见 ``plans/agent-edit-session-refactor-plan.md``。
"""

from __future__ import annotations

import copy
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from models import MergeConfig, SchemaDefinition
from schema_validator import SchemaValidator
from smart_array_merger import SmartArrayMerger
from yaml_store import YamlStore

ENTRY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")

DEFAULT_FORBIDDEN_VALUES = (
    "略",
    "同上",
    "同前",
    "待补",
    "未提及",
    "不明",
    "未知",
    "（略）",
    "(略",
    "tbd",
    "n/a",
)

TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "read_entry",
            "description": "读取当前输出中某个条目或其中某个字段的值。修改前应先读取，避免覆盖已有内容。",
            "parameters": {
                "type": "object",
                "properties": {
                    "key": {"type": "string", "description": "条目 key，必须为英文且使用下划线分隔"},
                    "path": {"type": "string", "description": "可选，点号分隔的字段路径，如 basic_settings.age"},
                },
                "required": ["key"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_entries",
            "description": "列出当前输出中的所有条目 key 及其一行摘要，用于确认是否已存在同名或近名条目。",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "最多返回多少个条目，默认 200"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "upsert_entry",
            "description": "新建或更新一个条目。条目不存在则创建；已存在则按字段深合并。不要用它整体替换已有条目。",
            "parameters": {
                "type": "object",
                "properties": {
                    "key": {"type": "string", "description": "条目 key，必须为英文且使用下划线分隔"},
                    "node": {"type": "object", "description": "要写入的字段映射，可以是部分字段"},
                },
                "required": ["key", "node"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "set_fields",
            "description": "设置条目的单个深层字段。路径用点号分隔，数组下标用方括号，如 appearance.outfits[0].description。",
            "parameters": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "path": {"type": "string", "description": "条目内部的字段路径，不含条目 key 本身"},
                    "value": {"description": "字段的新值，可以是字符串、数字、数组或对象"},
                },
                "required": ["key", "path", "value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "append_items",
            "description": "向某个数组字段追加元素。默认按值去重，重复元素不会被再次追加。",
            "parameters": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "path": {"type": "string", "description": "数组字段路径，不含条目 key 本身"},
                    "items": {"type": "array", "description": "要追加的元素列表"},
                    "mode": {
                        "type": "string",
                        "enum": ["append_unique", "append"],
                        "description": "append_unique 按值去重（默认），append 直接追加",
                    },
                },
                "required": ["key", "path", "items"],
            },
        },
    },
]

REMOTE_TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "remove_field",
            "description": "删除条目中的某个字段。默认禁用，仅在显式开启时可用。",
            "parameters": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "path": {"type": "string"},
                },
                "required": ["key", "path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "remove_entry",
            "description": "删除整个条目。默认禁用，仅在显式开启时可用。",
            "parameters": {
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "required": ["key"],
            },
        },
    },
]


class PathError(ValueError):
    """字段路径无法解析或不存在。"""


@dataclass(slots=True)
class EditOutcome:
    """一次工具调用的结果，直接作为 tool_result 回给模型。"""

    ok: bool
    code: str = ""
    reason: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def add(self, **kwargs: Any) -> "EditOutcome":
        self.extra.update(kwargs)
        return self

    def to_payload(self) -> dict[str, Any]:
        if self.ok:
            payload: dict[str, Any] = {"ok": True}
            payload.update(self.extra)
            return payload
        error: dict[str, Any] = {"code": self.code, "reason": self.reason}
        error.update(self.extra)
        return {"ok": False, "error": error}


def parse_field_path(path: str) -> list[str | int]:
    """把 ``a.b[0].c`` 解析为 ``["a", "b", 0, "c"]``。"""
    cleaned = (path or "").strip().lstrip(".")
    if not cleaned:
        raise PathError("path 不能为空")

    tokens: list[str | int] = []
    index = 0
    length = len(cleaned)
    while index < length:
        char = cleaned[index]
        if char == ".":
            index += 1
            if index >= length:
                raise PathError("路径不能以分隔符结尾")
            continue
        if char == "]":
            raise PathError(f"位置 {index} 出现多余的 ']'")
        if char == "[":
            closing = cleaned.find("]", index)
            if closing == -1:
                raise PathError(f"数组下标缺少 ']': {cleaned[index:]!r}")
            inner = cleaned[index + 1 : closing]
            if not inner.isdigit():
                raise PathError(f"数组下标必须是数字: {inner!r}")
            tokens.append(int(inner))
            index = closing + 1
            continue
        end = index
        while end < length and cleaned[end] not in ".[]":
            end += 1
        name = cleaned[index:end]
        if not name:
            raise PathError(f"位置 {index} 出现空字段名")
        tokens.append(name)
        index = end

    if not tokens:
        raise PathError(f"路径不含任何字段: {path!r}")
    return tokens


def get_at(root: Any, tokens: list[str | int]) -> tuple[bool, Any]:
    current = root
    for token in tokens:
        if isinstance(token, int):
            if not isinstance(current, list) or token >= len(current) or token < -len(current):
                return False, None
            current = current[token]
        else:
            if not isinstance(current, dict) or token not in current:
                return False, None
            current = current[token]
    return True, current


def set_at(root: Any, tokens: list[str | int], value: Any) -> None:
    """在 ``root`` 上按路径写入。中间层缺失的字典会自动补齐。"""
    current = root
    for position, token in enumerate(tokens[:-1]):
        following = tokens[position + 1]
        if isinstance(token, int):
            if not isinstance(current, list):
                raise PathError("路径中使用了数组下标，但该位置不是数组")
            if token >= len(current):
                raise PathError(f"数组下标越界: [{token}]")
            current = current[token]
            continue
        if not isinstance(current, dict):
            raise PathError("路径中间层不是对象")
        if token not in current or not isinstance(current[token], (dict, list)):
            # 缺失或类型不匹配时，按下一段的类型决定补对象还是数组
            current[token] = [] if isinstance(following, int) else {}
        current = current[token]

    last = tokens[-1]
    if isinstance(last, int):
        if not isinstance(current, list):
            raise PathError("路径中使用了数组下标，但该位置不是数组")
        if last >= len(current):
            raise PathError(f"数组下标越界: [{last}]，当前数组长度为 {len(current)}")
        current[last] = value
        return
    if not isinstance(current, dict):
        raise PathError("路径父级不是对象")
    current[last] = value


def delete_at(root: Any, tokens: list[str | int]) -> tuple[bool, Any]:
    """删除路径指向的字段，返回 ``(是否存在, 被删除的值)``。"""
    parent_tokens, last = tokens[:-1], tokens[-1]
    found, parent = get_at(root, parent_tokens)
    if not found:
        return False, None
    if isinstance(last, int):
        if not isinstance(parent, list) or last >= len(parent) or last < -len(parent):
            return False, None
        return True, parent.pop(last)
    if not isinstance(parent, dict) or last not in parent:
        return False, None
    return True, parent.pop(last)


def deep_merge(base: Any, incoming: Any) -> Any:
    """字段级深合并：字典递归合并，其他类型由 incoming 覆盖。"""
    if isinstance(base, dict) and isinstance(incoming, dict):
        merged = dict(base)
        for key, value in incoming.items():
            merged[key] = deep_merge(merged[key], value) if key in merged else value
        return merged
    return incoming


def is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return not value
    return False


DIGIT_RUN_PATTERN = re.compile(r"\d+")


def normalize_key_for_duplicate_check(key: str) -> str:
    """归一化 key 中的数字片段，用于识别「仅编号不同」的 key。

    ``Char0001`` 与 ``Char0002`` 归一化后都是 ``char#``，
    说明它们只是同一命名方案下的不同实体，不应互相判为重复。
    """
    return DIGIT_RUN_PATTERN.sub("#", key.strip().lower())


def is_numeric_sibling(left: str, right: str) -> bool:
    """两个 key 除数字编号外完全相同。

    这类 key 是不同实体（如 ``Char0001`` / ``Char0002``、
    ``Item_12`` / ``Item_13``），必须跳过近似重名检查。
    仅靠字符串相似度会把它们误判为同一个条目——
    实测 ``Char0001`` 与 ``Char0002`` 的相似度高达 0.875。
    """
    if left == right:
        return False
    return normalize_key_for_duplicate_check(left) == normalize_key_for_duplicate_check(right)


def summarize_entry(node: Any, *, limit: int = 80) -> str:
    """生成一行摘要，用于索引与 list_entries。"""
    if not isinstance(node, dict):
        return str(node)[:limit]
    for candidate in ("name", "content", "character_brief_description", "trigger_keywords"):
        value = node.get(candidate)
        text = _flatten_summary_text(value)
        if text:
            return text[:limit]
    for value in node.values():
        text = _flatten_summary_text(value)
        if text:
            return text[:limit]
    return ""


def _flatten_summary_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for item in value.values():
            text = _flatten_summary_text(item)
            if text:
                return text
        return ""
    if isinstance(value, list):
        for item in value:
            text = _flatten_summary_text(item)
            if text:
                return text
        return ""
    return ""


@dataclass(slots=True)
class _EditTransaction:
    """在内存中暂存编辑；校验全部通过后才提交文档与日志。"""

    document: dict[str, Any]
    journal: list[dict[str, Any]] = field(default_factory=list)

    def record(self, **record: Any) -> None:
        # 后续操作可能再次修改同一节点，日志必须保留操作发生时的快照。
        self.journal.append(copy.deepcopy(record))


class EditTools:
    """作用于单个任务 ``output.yaml`` 的结构化编辑工具集。"""

    def __init__(
        self,
        *,
        output_path: Path,
        schema_definition: SchemaDefinition,
        journal_path: Path | None = None,
        merge_config: MergeConfig | None = None,
        allow_remove: bool = False,
        near_duplicate_check: bool = True,
        forbidden_values: Iterable[str] = DEFAULT_FORBIDDEN_VALUES,
        max_near_duplicate_candidates: int = 5,
    ) -> None:
        self.output_path = Path(output_path)
        self.schema_definition = schema_definition
        self.journal_path = Path(journal_path) if journal_path else None
        self.merge_config = merge_config or MergeConfig()
        self.allow_remove = allow_remove
        self.near_duplicate_check = near_duplicate_check
        self.forbidden_values = tuple(value for value in forbidden_values if value)
        self.max_near_duplicate_candidates = max_near_duplicate_candidates

        self._yaml_store = YamlStore(self.merge_config)
        self._validator = SchemaValidator()
        self._merger = SmartArrayMerger(self.merge_config)

    # ------------------------------------------------------------------ 工具表

    def tool_schemas(self) -> list[dict[str, Any]]:
        schemas = [copy.deepcopy(item) for item in TOOL_SCHEMAS]
        if self.allow_remove:
            schemas.extend(copy.deepcopy(item) for item in REMOTE_TOOL_SCHEMAS)
        edit_names = [
            item["function"]["name"] for item in schemas
            if item["function"]["name"] not in ("read_entry", "list_entries")
        ]
        schemas.append({
            "type": "function",
            "function": {
                "name": "batch_edit",
                "description": (
                    "优先使用此工具一次提交多个条目、多个字段的编辑，减少模型往返轮数。"
                    "operations 按顺序执行，后项可依赖前项；参数与对应单项工具相同。"
                    "全部校验通过才统一写入，任一操作失败则整批不写入；"
                    "修正 failed_index（从 0 开始）处的错误后重发完整批次。"
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "operations": {
                            "type": "array",
                            "minItems": 1,
                            "description": "按执行顺序排列的编辑操作，支持跨条目混合新建、字段修改和数组追加",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "tool": {"type": "string", "enum": edit_names},
                                    "arguments": {
                                        "type": "object",
                                        "description": "对应单项工具的参数对象，例如 set_fields 使用 key、path、value",
                                    },
                                },
                                "required": ["tool", "arguments"],
                                "additionalProperties": False,
                            },
                        },
                    },
                    "required": ["operations"],
                    "additionalProperties": False,
                },
            },
        })
        return schemas

    def tool_names(self) -> set[str]:
        return {item["function"]["name"] for item in self.tool_schemas()}

    # -------------------------------------------------------------- 文档读写

    def load_document(self) -> dict[str, Any]:
        return self._yaml_store.load_yaml(
            self.output_path,
            default={self.schema_definition.root_key: {}},
        )

    def write_document(self, document: dict[str, Any]) -> None:
        """原子写入：先写同目录临时文件，再 ``os.replace`` 覆盖。"""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        serialized = self._yaml_store.dump_to_string(document) + "\n"
        temp_path = self.output_path.with_name(f".{self.output_path.name}.tmp")
        temp_path.write_text(serialized, encoding="utf-8")
        os.replace(temp_path, self.output_path)

    def read_entry(self, key: str, path: str = "") -> Any:
        document = self.load_document()
        entries = self._entries(document)
        if key not in entries:
            raise PathError(f"条目不存在: {key}")
        if not path:
            return entries[key]
        tokens = parse_field_path(path)
        found, value = get_at(entries[key], tokens)
        if not found:
            raise PathError(f"字段不存在: {key}.{path}")
        return value

    def list_entries(self, limit: int = 200) -> list[dict[str, str]]:
        document = self.load_document()
        entries = self._entries(document)
        result: list[dict[str, str]] = []
        for key, node in list(entries.items())[: max(limit, 0)]:
            result.append({"key": str(key), "summary": summarize_entry(node)})
        return result

    # -------------------------------------------------------------- 工具分发

    def dispatch(self, tool_name: str, arguments: dict[str, Any]) -> EditOutcome:
        """执行一次工具调用。单项与批量编辑均在校验通过后提交，失败时不修改文件。"""
        try:
            if tool_name == "read_entry":
                return self._op_read_entry(arguments)
            if tool_name == "list_entries":
                return self._op_list_entries(arguments)
            if tool_name == "batch_edit":
                operations = arguments.get("operations")
                if not isinstance(operations, list) or not operations:
                    return EditOutcome(
                        ok=False, code="bad_operations", reason="operations 必须是非空数组",
                        extra={"edit_count": 0},
                    )
                return self._apply_edits(operations, batch=True)
            return self._apply_edits([{"tool": tool_name, "arguments": arguments}], batch=False)
        except PathError as exc:
            return EditOutcome(ok=False, code="bad_path", reason=str(exc))

    def _apply_edits(self, operations: list[Any], *, batch: bool) -> EditOutcome:
        handlers = {
            "upsert_entry": self._op_upsert_entry,
            "set_fields": self._op_set_fields,
            "append_items": self._op_append_items,
            "remove_field": self._op_remove_field,
            "remove_entry": self._op_remove_entry,
        }
        transaction = _EditTransaction(self.load_document())
        results: list[dict[str, Any]] = []
        outcome = EditOutcome(ok=True)
        for index, operation in enumerate(operations):
            tool_name = operation.get("tool") if isinstance(operation, dict) else None
            arguments = operation.get("arguments") if isinstance(operation, dict) else None
            if not isinstance(tool_name, str) or not isinstance(arguments, dict):
                outcome = EditOutcome(
                    ok=False, code="bad_arguments",
                    reason="每项操作必须包含字符串 tool 和对象 arguments",
                )
            elif tool_name not in handlers:
                available = self.tool_names()
                if batch:
                    available.intersection_update(handlers)
                outcome = EditOutcome(
                    ok=False, code="unknown_tool", reason=f"未知编辑工具: {tool_name}",
                    extra={"available_tools": ", ".join(sorted(available))},
                )
            else:
                try:
                    outcome = handlers[tool_name](copy.deepcopy(arguments), transaction)
                except PathError as exc:
                    outcome = EditOutcome(ok=False, code="bad_path", reason=str(exc))

            if not outcome.ok:
                if not batch:
                    return outcome
                return EditOutcome(
                    ok=False, code="batch_failed",
                    reason=f"operations[{index}] 失败，整批未写入: {outcome.reason}",
                    extra={
                        "failed_index": index,
                        "tool": tool_name,
                        "operation_error": outcome.to_payload()["error"],
                        "edit_count": 0,
                        "hint": "本批次没有任何编辑生效；修正错误后重发完整 operations，不要只重发失败项",
                    },
                )
            results.append({"index": index, "tool": tool_name, **copy.deepcopy(outcome.to_payload())})

        if transaction.journal:
            self.write_document(transaction.document)
            for record in transaction.journal:
                self._append_journal(**record)
        if batch:
            return EditOutcome(ok=True).add(edit_count=len(operations), results=results)
        return outcome

    # -------------------------------------------------------------- 各工具实现

    def _op_read_entry(self, arguments: dict[str, Any]) -> EditOutcome:
        key = self._require_key(arguments, allow_missing=True)
        path = str(arguments.get("path", "") or "")
        value = self.read_entry(key, path)
        return EditOutcome(ok=True).add(key=key, path=path, value=value)

    def _op_list_entries(self, arguments: dict[str, Any]) -> EditOutcome:
        try:
            limit = int(arguments.get("limit", 200) or 200)
        except (TypeError, ValueError):
            return EditOutcome(ok=False, code="bad_limit", reason="limit 必须是整数")
        entries = self.list_entries(limit=limit)
        return EditOutcome(ok=True).add(count=len(entries), entries=entries)

    def _op_upsert_entry(self, arguments: dict[str, Any], transaction: _EditTransaction) -> EditOutcome:
        key = self._require_key(arguments)
        node = arguments.get("node")
        if not isinstance(node, dict):
            return EditOutcome(ok=False, code="bad_node", reason="node 必须是对象映射", extra={"expected": "object"})

        document = transaction.document
        entries = self._entries(document)
        existing = key in entries

        cleaned = self._clean_node(node)
        if cleaned is None:
            return EditOutcome(ok=False, code="empty_node", reason="node 清洗后为空，没有任何字段可写入")

        invalid = self._check_forbidden(cleaned)
        if invalid:
            return EditOutcome(ok=False, code="forbidden_value", reason=invalid["reason"], extra=invalid)

        if not existing:
            duplicates = self._near_duplicates(key, entries)
            if duplicates:
                return EditOutcome(
                    ok=False,
                    code="near_duplicate_key",
                    reason=f"已存在命名高度相似的条目: {duplicates[0]['key']}",
                    extra={"candidates": duplicates, "hint": "确认是同一个实体时请改用已有 key 更新"},
                )
            merged_node = cleaned
        else:
            merged_node = deep_merge(entries[key], cleaned)

        validation = self._validate_entry(key, merged_node)
        if not validation.ok:
            return validation

        before = entries.get(key)
        document[self.schema_definition.root_key][key] = merged_node
        transaction.record(
            op="upsert_entry",
            key=key,
            before=before,
            after=merged_node,
            created=not existing,
        )
        return EditOutcome(ok=True).add(
            key=key,
            created=not existing,
            updated=existing,
            entry_keys=sorted(merged_node.keys()),
        )

    def _op_set_fields(self, arguments: dict[str, Any], transaction: _EditTransaction) -> EditOutcome:
        key = self._require_key(arguments)
        raw_path = str(arguments.get("path", "") or "")
        if "value" not in arguments:
            return EditOutcome(ok=False, code="missing_value", reason="缺少 value 参数")
        value = arguments.get("value")

        document = transaction.document
        entries = self._entries(document)
        if key not in entries:
            return EditOutcome(
                ok=False,
                code="unknown_entry",
                reason=f"条目不存在，请先使用 upsert_entry 新建: {key}",
                extra={"candidates": self._near_duplicates(key, entries, threshold_scale=0.0)},
            )
        entry = entries[key]

        tokens = [key, *parse_field_path(raw_path)]
        found, previous = get_at(document, [self.schema_definition.root_key, *tokens])
        if not found:
            siblings = self._sibling_hints(entry, parse_field_path(raw_path))
            return EditOutcome(
                ok=False,
                code="unknown_field",
                reason=f"字段不存在: {key}.{raw_path}",
                extra={"allowed_siblings": siblings, "hint": "字段必须已存在于 schema 骨架中；新增字段请用 upsert_entry"},
            )

        if is_empty_value(value):
            return EditOutcome(ok=False, code="empty_value", reason="不允许写入空字符串、空数组或空对象")

        invalid = self._check_forbidden(value)
        if invalid:
            return EditOutcome(ok=False, code="forbidden_value", reason=invalid["reason"], extra=invalid)

        candidate = self._clone_entry(entry)
        try:
            set_at(candidate, parse_field_path(raw_path), value)
        except PathError as exc:
            return EditOutcome(ok=False, code="bad_path", reason=str(exc))

        validation = self._validate_entry(key, candidate)
        if not validation.ok:
            return validation

        self._assign_path(entry, parse_field_path(raw_path), value)
        transaction.record(op="set_fields", key=key, path=raw_path, before=previous, after=value)
        return EditOutcome(ok=True).add(key=key, path=raw_path, value=value)

    def _op_append_items(self, arguments: dict[str, Any], transaction: _EditTransaction) -> EditOutcome:
        key = self._require_key(arguments)
        raw_path = str(arguments.get("path", "") or "")
        items = arguments.get("items")
        mode = str(arguments.get("mode", "append_unique") or "append_unique")
        if mode not in ("append", "append_unique"):
            return EditOutcome(ok=False, code="bad_mode", reason=f"mode 只支持 append / append_unique，收到: {mode}")
        if not isinstance(items, list) or not items:
            return EditOutcome(ok=False, code="bad_items", reason="items 必须是非空数组")

        document = transaction.document
        entries = self._entries(document)
        if key not in entries:
            return EditOutcome(
                ok=False,
                code="unknown_entry",
                reason=f"条目不存在，请先使用 upsert_entry 新建: {key}",
            )
        entry = entries[key]
        tokens = parse_field_path(raw_path)
        found, target = get_at(entry, tokens)
        if not found:
            # 字段在 schema 骨架中声明为数组但尚未创建时，自动补一个空数组，
            # 避免模型为了追加一个元素而多跑一次 upsert 往返。
            if self._is_skeleton_list(tokens):
                set_at(entry, tokens, [])
                found, target = get_at(entry, tokens)
            else:
                return EditOutcome(
                    ok=False,
                    code="unknown_field",
                    reason=f"字段不存在: {key}.{raw_path}",
                    extra={"allowed_siblings": self._sibling_hints(entry, tokens)},
                )
        if not isinstance(target, list):
            return EditOutcome(
                ok=False,
                code="not_a_list",
                reason=f"目标字段不是数组，实际类型: {type(target).__name__}",
                extra={"hint": "数组字段请用 append_items；标量字段请用 set_fields"},
            )

        cleaned_items = []
        for item in items:
            cleaned = self._clean_node(item)
            if cleaned is None:
                continue
            invalid = self._check_forbidden(cleaned)
            if invalid:
                return EditOutcome(ok=False, code="forbidden_value", reason=invalid["reason"], extra=invalid)
            cleaned_items.append(cleaned)
        if not cleaned_items:
            return EditOutcome(ok=False, code="empty_items", reason="items 清洗后全部为空")

        appended, duplicates = self._merge_into_list(target, cleaned_items, dedupe=mode == "append_unique")
        if not appended:
            return EditOutcome(ok=True).add(key=key, path=raw_path, appended=0, duplicates=duplicates, length=len(target))

        validation = self._validate_entry(key, entry)
        if not validation.ok:
            return validation

        transaction.record(op="append_items", key=key, path=raw_path, appended=appended, duplicates=duplicates)
        return EditOutcome(ok=True).add(key=key, path=raw_path, appended=appended, duplicates=duplicates, length=len(target))

    def _op_remove_field(self, arguments: dict[str, Any], transaction: _EditTransaction) -> EditOutcome:
        if not self.allow_remove:
            return EditOutcome(ok=False, code="remove_disabled", reason="删除操作未启用（tools.allow_remove=false）")
        key = self._require_key(arguments)
        raw_path = str(arguments.get("path", "") or "")
        document = transaction.document
        entries = self._entries(document)
        if key not in entries:
            return EditOutcome(ok=False, code="unknown_entry", reason=f"条目不存在: {key}")
        found, previous = delete_at(entries[key], parse_field_path(raw_path))
        if not found:
            return EditOutcome(ok=False, code="unknown_field", reason=f"字段不存在: {key}.{raw_path}")
        validation = self._validate_entry(key, entries[key])
        if not validation.ok:
            return validation
        transaction.record(op="remove_field", key=key, path=raw_path, before=previous)
        return EditOutcome(ok=True).add(key=key, path=raw_path)

    def _op_remove_entry(self, arguments: dict[str, Any], transaction: _EditTransaction) -> EditOutcome:
        if not self.allow_remove:
            return EditOutcome(ok=False, code="remove_disabled", reason="删除操作未启用（tools.allow_remove=false）")
        key = self._require_key(arguments)
        document = transaction.document
        entries = self._entries(document)
        if key not in entries:
            return EditOutcome(ok=False, code="unknown_entry", reason=f"条目不存在: {key}")
        before = entries.pop(key)
        transaction.record(op="remove_entry", key=key, before=before)
        return EditOutcome(ok=True).add(key=key)

    # ------------------------------------------------------------------ 校验

    def _validate_entry(self, key: str, node: Any) -> EditOutcome:
        result = self._validator.validate_node(
            node,
            path=f"{self.schema_definition.root_key}.{key}",
            schema_definition=self.schema_definition,
        )
        if result.ok:
            return EditOutcome(ok=True)
        first = result.errors[0]
        siblings = self._skeleton_keys()
        return EditOutcome(
            ok=False,
            code="schema_violation",
            reason=first.reason,
            extra={
                "path": first.path,
                "error_count": len(result.errors),
                "all_errors": [{"path": issue.path, "reason": issue.reason} for issue in result.errors[:5]],
                "allowed_keys": siblings,
            },
        )

    def _skeleton_root(self) -> Any:
        root_value = self.schema_definition.raw_schema.get(self.schema_definition.root_key)
        if isinstance(root_value, dict) and root_value:
            return next(iter(root_value.values()))
        if isinstance(root_value, list) and root_value:
            return root_value[0]
        return {}

    def _is_skeleton_list(self, tokens: list[str | int]) -> bool:
        """判断骨架中该路径是否为数组（用于 append_items 自动初始化）。"""
        node: Any = self._skeleton_root()
        for position, token in enumerate(tokens):
            if isinstance(node, list):
                if not node:
                    return False
                node = node[0]
            if not isinstance(node, dict):
                return False
            if isinstance(token, int):
                return True
            matched: Any = None
            if str(token) in node:
                matched = node[str(token)]
            else:
                matched = self._find_placeholder_child(node)
                if matched is None:
                    return False
            node = matched
            if position == len(tokens) - 1:
                return isinstance(node, list)
        return isinstance(node, list)

    def _find_placeholder_child(self, node: Any) -> Any:
        if not isinstance(node, dict):
            return None
        placeholders = [key for key in node if isinstance(key, str) and "<" in key and ">" in key]
        if len(placeholders) == 1:
            return node[placeholders[0]]
        return None

    def _skeleton_keys(self, prefix: str = "") -> list[str]:
        """返回 schema 骨架中可用的字段名，供错误信息给出可执行提示。"""
        root_value = self.schema_definition.raw_schema.get(self.schema_definition.root_key)
        skeleton: Any = {}
        if isinstance(root_value, dict) and root_value:
            skeleton = next(iter(root_value.values()))
        elif isinstance(root_value, list) and root_value:
            skeleton = root_value[0]
        tokens = parse_field_path(prefix) if prefix else []
        found, node = get_at(skeleton, tokens) if tokens else (True, skeleton)
        if not found:
            node = skeleton
        if isinstance(node, dict):
            return [str(item) for item in node.keys()]
        if isinstance(node, list) and node and isinstance(node[0], dict):
            return [str(item) for item in node[0].keys()]
        return []

    def _sibling_hints(self, entry: Any, tokens: list[str | int]) -> list[str]:
        if not tokens:
            return []
        parent = entry
        for token in tokens[:-1]:
            if isinstance(token, int):
                return []
            if not isinstance(parent, dict) or token not in parent:
                break
            parent = parent[token]
        if isinstance(parent, dict):
            return [str(item) for item in parent.keys()]
        return []

    # -------------------------------------------------------------- 值处理

    def _require_key(self, arguments: dict[str, Any], *, allow_missing: bool = False) -> str:
        key = arguments.get("key")
        if key is None or not str(key).strip():
            if allow_missing:
                raise PathError("缺少 key 参数")
            raise PathError("缺少 key 参数")
        key = str(key)
        if not ENTRY_KEY_PATTERN.match(key):
            raise PathError(
                f"key 必须由英文字母、数字、下划线组成，收到: {key!r}（禁止中文 key，空格请替换为下划线）"
            )
        return key

    def _entries(self, document: dict[str, Any]) -> dict[str, Any]:
        root_key = self.schema_definition.root_key
        entries = document.get(root_key)
        if entries is None:
            document[root_key] = {}
            return document[root_key]
        if not isinstance(entries, dict):
            raise PathError(f"{root_key} 必须是对象映射，实际类型: {type(entries).__name__}")
        return entries

    def _clean_node(self, node: Any) -> Any:
        """复用 SchemaValidator 的清洗逻辑，去掉空值与空白字符串。"""
        sanitized = self._validator.sanitize_node(node)
        if sanitized is None:
            return None
        from schema_validator import _REMOVE  # noqa: PLC0415  (内部哨兵值)

        if sanitized is _REMOVE:
            return None
        return sanitized

    def _clone_entry(self, entry: Any) -> Any:
        if isinstance(entry, dict):
            return {key: self._clone_entry(value) for key, value in entry.items()}
        if isinstance(entry, list):
            return [self._clone_entry(item) for item in entry]
        return entry

    def _assign_path(self, root: Any, tokens: list[str | int], value: Any) -> None:
        """已有的字段用直接赋值，避免 set_at 重建中间层。"""
        parent = root
        for token in tokens[:-1]:
            parent = parent[token]
        parent[tokens[-1]] = value

    def _check_forbidden(self, node: Any, path: str = "") -> dict[str, Any] | None:
        if isinstance(node, str):
            stripped = node.strip()
            if not stripped:
                return None
            for forbidden in self.forbidden_values:
                if forbidden and forbidden in stripped:
                    return {
                        "reason": f"值中包含无意义占位内容: {forbidden}",
                        "forbidden_value": forbidden,
                        "path": path or "$",
                        "hint": "该值属于无意义占位内容，请省略该字段或补充真实信息",
                    }
            return None
        if isinstance(node, dict):
            for key, value in node.items():
                nested = self._check_forbidden(value, f"{path}.{key}" if path else str(key))
                if nested:
                    return nested
            return None
        if isinstance(node, list):
            for index, item in enumerate(node):
                nested = self._check_forbidden(item, f"{path}[{index}]")
                if nested:
                    return nested
            return None
        return None

    def _merge_into_list(self, target: list[Any], items: list[Any], *, dedupe: bool) -> tuple[int, int]:
        """按值去重后追加，返回 ``(追加数量, 重复数量)``。"""
        appended = 0
        duplicates = 0
        for item in items:
            if dedupe and any(self._items_equal(existing, item) for existing in target):
                duplicates += 1
                continue
            target.append(item)
            appended += 1
        return appended, duplicates

    def _items_equal(self, left: Any, right: Any) -> bool:
        if isinstance(left, dict) and isinstance(right, dict):
            match_key = self.schema_definition.match_key
            if match_key and isinstance(left.get(match_key), str) and isinstance(right.get(match_key), str):
                return bool(left[match_key] == right[match_key])
        return left == right

    def _near_duplicates(self, key: str, entries: dict[str, Any], *, threshold_scale: float = 1.0) -> list[dict[str, Any]]:
        """返回与 ``key`` 命名高度相似的已有条目。

        注意：仅靠字符串相似度会误伤编号型 key（``Char0001`` 与 ``Char0002``
        相似度高达 0.875，但显然是不同实体），因此先排除「仅编号不同」的情况。
        """
        if not self.near_duplicate_check and threshold_scale > 0.0:
            return []
        threshold = self.merge_config.similarity_threshold * threshold_scale
        candidates: list[tuple[float, str]] = []
        for existing_key in entries:
            existing = str(existing_key)
            if existing == key:
                continue
            # 仅编号不同的 key 是不同实体，不能互相判重。
            if is_numeric_sibling(existing, key):
                continue
            similarity = self._merger.similarity(existing, key)
            if similarity >= threshold:
                candidates.append((similarity, existing))
        candidates.sort(reverse=True)
        return [
            {"key": name, "similarity": round(score, 3)}
            for score, name in candidates[: self.max_near_duplicate_candidates]
        ]

    # ------------------------------------------------------------------ 日志

    def _append_journal(self, **record: Any) -> None:
        if not self.journal_path:
            return
        payload = {
            "at": datetime.now(timezone.utc).isoformat(),
            "output_path": str(self.output_path).replace("\\", "/"),
            **record,
        }
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        with self.journal_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

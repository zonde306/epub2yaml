from __future__ import annotations

from typing import Any
import re
import yaml

from models import SchemaDefinition, ValidationIssue, ValidationResult


_REMOVE = object()


class SchemaValidator:
    def parse_yaml_text(self, yaml_text: str) -> tuple[dict[str, Any] | None, ValidationResult]:
        yaml_text = self._strip_markdown_code_block(yaml_text)

        try:
            data = yaml.safe_load(yaml_text)
        except yaml.YAMLError as exc:
            return None, ValidationResult(ok=False, errors=[ValidationIssue(path="$", reason=f"invalid yaml: {exc}")])

        if data is None:
            return None, ValidationResult(ok=False, errors=[ValidationIssue(path="$", reason="yaml document is empty")])
        if not isinstance(data, dict):
            return None, ValidationResult(ok=False, errors=[ValidationIssue(path="$", reason="yaml root must be mapping")])
        return data, ValidationResult(ok=True)

    def sanitize_increment_data(self, data: dict[str, Any]) -> dict[str, Any]:
        sanitized = self.sanitize_node(data)
        if sanitized is _REMOVE or not isinstance(sanitized, dict):
            return {}
        return sanitized

    def sanitize_node(self, node: Any) -> Any:
        if node is None:
            return _REMOVE

        if isinstance(node, str):
            normalized = node.strip()
            return normalized if normalized else _REMOVE

        if isinstance(node, list):
            sanitized_items: list[Any] = []
            for item in node:
                sanitized_item = self.sanitize_node(item)
                if sanitized_item is _REMOVE:
                    continue
                sanitized_items.append(sanitized_item)
            return sanitized_items if sanitized_items else _REMOVE

        if isinstance(node, dict):
            sanitized_mapping: dict[str, Any] = {}
            for raw_key, value in node.items():
                sanitized_key = self._sanitize_key(raw_key)
                if sanitized_key is _REMOVE:
                    continue
                sanitized_value = self.sanitize_node(value)
                if sanitized_value is _REMOVE:
                    continue
                sanitized_mapping[sanitized_key] = sanitized_value
            return sanitized_mapping if sanitized_mapping else _REMOVE

        return node

    def validate_increment(self, data: dict[str, Any], schema_definition: SchemaDefinition) -> ValidationResult:
        """校验增量数据是否符合 schema 骨架。

        校验三类错误：
        - 顶层出现非预期 key（只允许 `root_key`）
        - 条目 key 不符合 match_key 要求（空、空白、或非字符串）
        - 条目出现 schema 之外的字段
        """
        errors: list[ValidationIssue] = []
        if not isinstance(data, dict):
            return ValidationResult(ok=False, errors=[ValidationIssue(path="$", reason="increment root must be mapping")])

        root_key = schema_definition.root_key
        for top_key in data:
            if top_key != root_key:
                errors.append(ValidationIssue(path=str(top_key), reason=f"unexpected top-level key, expected {root_key}"))

        items = data.get(root_key)
        if items is None:
            return ValidationResult(ok=not errors, errors=errors)
        if not isinstance(items, dict):
            errors.append(ValidationIssue(path=root_key, reason="must be a mapping"))
            return ValidationResult(ok=False, errors=errors)

        skeleton = self._extract_item_skeleton(schema_definition)
        for item_key, item_value in items.items():
            item_path = f"{root_key}.{item_key}"
            if self._is_missing_entry_key(item_key):
                errors.append(
                    ValidationIssue(path=item_path, reason=f"invalid entry key for match_key {schema_definition.match_key}")
                )
            if not isinstance(item_value, dict):
                errors.append(ValidationIssue(path=item_path, reason="entry must be a mapping"))
                continue
            self._validate_against_skeleton(
                node=item_value,
                skeleton=skeleton,
                path=item_path,
                errors=errors,
            )

        return ValidationResult(ok=not errors, errors=errors)

    def validate_node(
        self,
        node: Any,
        *,
        path: str,
        schema_definition: SchemaDefinition,
    ) -> ValidationResult:
        """校验单个节点，供编辑工具在校验单项修改时复用。"""
        errors: list[ValidationIssue] = []
        skeleton = self._extract_item_skeleton(schema_definition)
        self._validate_against_skeleton(node=node, skeleton=skeleton, path=path, errors=errors)
        return ValidationResult(ok=not errors, errors=errors)

    def _extract_item_skeleton(self, schema_definition: SchemaDefinition) -> Any:
        root_value = schema_definition.raw_schema[schema_definition.root_key]
        if isinstance(root_value, dict):
            first_value = next(iter(root_value.values()), {})
            return first_value if isinstance(first_value, dict) else {}
        if isinstance(root_value, list) and root_value:
            return root_value[0]
        return {}

    def _validate_against_skeleton(
        self,
        *,
        node: Any,
        skeleton: Any,
        path: str,
        errors: list[ValidationIssue],
    ) -> None:
        if isinstance(skeleton, dict):
            if not isinstance(node, dict):
                errors.append(ValidationIssue(path=path, reason="expected mapping"))
                return

            catch_all_key = self._find_placeholder_key(skeleton)
            for key, value in node.items():
                if key in skeleton:
                    child_skeleton = skeleton[key]
                    child_path = f"{path}.{key}"
                elif catch_all_key is not None:
                    # 动态键（如 relationships 的 dynamic_with_<CharacterName>）
                    child_skeleton = skeleton[catch_all_key]
                    child_path = f"{path}.{key}"
                else:
                    errors.append(ValidationIssue(path=f"{path}.{key}", reason="unexpected field"))
                    continue
                self._validate_against_skeleton(
                    node=value,
                    skeleton=child_skeleton,
                    path=child_path,
                    errors=errors,
                )
            return

        if isinstance(skeleton, list):
            if not isinstance(node, list):
                errors.append(ValidationIssue(path=path, reason="expected list"))
                return
            sample = skeleton[0] if skeleton else None
            if sample is None:
                return
            for index, item in enumerate(node):
                self._validate_against_skeleton(node=item, skeleton=sample, path=f"{path}[{index}]", errors=errors)
            return

        return

    def _find_placeholder_key(self, skeleton: dict[str, Any]) -> str | None:
        """找出骨架中的占位符键（如 ``dynamic_with_<CharacterName>``）。

        这类键表示「任意键名」，用于 relationships 这类动态映射。
        仅当骨架里只有唯一一个占位符键时才当作通配，避免歧义。
        """
        placeholders = [key for key in skeleton if isinstance(key, str) and "<" in key and ">" in key]
        if len(placeholders) == 1:
            return placeholders[0]
        return None

    def _sanitize_key(self, key: Any) -> Any:
        if isinstance(key, str):
            normalized = key.strip()
            return normalized if normalized else _REMOVE
        return key

    def _strip_markdown_code_block(self, text: str) -> str:
        """移除 markdown 代码块标记，支持多种格式。

        支持的格式：
        - ```\\n内容\\n```
        - ```yaml\\n内容\\n```
        - ```YAML\\n内容\\n```
        - 以及其他语言标记
        """
        text = text.strip()

        # 检查是否以 ``` 开头
        if not text.startswith("```"):
            return text

        # 找到第一个换行符，分离语言标记和内容
        first_newline = text.find("\n")
        if first_newline == -1:
            # 没有换行符，不是有效的代码块
            return text

        # 找到结束的 ```
        # 从文本末尾开始查找，支持内容中可能包含 ``` 的情况
        last_backticks = text.rfind("```")
        if last_backticks <= first_newline:
            # 没有找到结束标记，或者结束标记在开始标记之前
            # 尝试移除开头的 ``` 和语言标记
            return text[first_newline + 1 :].rstrip()

        # 提取内容部分（去掉开头的 ```xxx 和结尾的 ```）
        content = text[first_newline + 1 : last_backticks]
        return content.strip()

    def _is_missing_entry_key(self, value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        if isinstance(value, (list, dict)):
            return not value
        return False


def infer_scalar_type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"

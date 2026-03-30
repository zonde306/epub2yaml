from __future__ import annotations

from typing import Any
import re
import yaml

from models import SchemaDefinition, ValidationIssue, ValidationResult


_REMOVE = object()


class SchemaValidator:
    def parse_yaml_text(self, yaml_text: str) -> tuple[dict[str, Any] | None, ValidationResult]:
        if match := re.search(r"```(?:yaml)?\s*([\s\S]*?)\s*```$", yaml_text, re.MULTILINE|re.IGNORECASE):
            yaml_text = match.group(1)

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
        return ValidationResult(ok=True, errors=[])

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

            for key, value in node.items():
                if key not in skeleton:
                    errors.append(ValidationIssue(path=f"{path}.{key}", reason="unexpected field"))
                    continue
                self._validate_against_skeleton(node=value, skeleton=skeleton[key], path=f"{path}.{key}", errors=errors)
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

    def _sanitize_key(self, key: Any) -> Any:
        if isinstance(key, str):
            normalized = key.strip()
            return normalized if normalized else _REMOVE
        return key

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

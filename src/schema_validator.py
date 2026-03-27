from __future__ import annotations

from typing import Any

import yaml

from models import SchemaDefinition, ValidationIssue, ValidationResult


_REMOVE = object()


class SchemaValidator:
    def parse_yaml_text(self, yaml_text: str) -> tuple[dict[str, Any] | None, ValidationResult]:
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
            for key, value in node.items():
                sanitized_value = self.sanitize_node(value)
                if sanitized_value is _REMOVE:
                    continue
                sanitized_mapping[key] = sanitized_value
            return sanitized_mapping if sanitized_mapping else _REMOVE

        return node

    def validate_increment(self, data: dict[str, Any], schema_definition: SchemaDefinition) -> ValidationResult:
        errors: list[ValidationIssue] = []
        root_key = schema_definition.root_key
        match_key = schema_definition.match_key

        extra_top_level_keys = [key for key in data.keys() if key not in schema_definition.allowed_top_level_keys]
        for key in extra_top_level_keys:
            errors.append(ValidationIssue(path=key, reason="unexpected top-level key"))

        if root_key not in data:
            return ValidationResult(ok=not errors, errors=errors)

        root_value = data[root_key]
        if not isinstance(root_value, list):
            errors.append(ValidationIssue(path=root_key, reason="root value must be a list"))
            return ValidationResult(ok=False, errors=errors)

        if not root_value:
            return ValidationResult(ok=not errors, errors=errors)

        item_skeleton = self._extract_item_skeleton(schema_definition)
        for index, item in enumerate(root_value):
            item_path = f"{root_key}[{index}]"
            if not isinstance(item, dict):
                errors.append(ValidationIssue(path=item_path, reason="list item must be mapping"))
                continue

            if match_key not in item or self._is_missing_match_value(item[match_key]):
                errors.append(ValidationIssue(path=f"{item_path}.{match_key}", reason="required match key is missing"))

            self._validate_against_skeleton(
                node=item,
                skeleton=item_skeleton,
                path=item_path,
                errors=errors,
            )

        return ValidationResult(ok=not errors, errors=errors)

    def _extract_item_skeleton(self, schema_definition: SchemaDefinition) -> Any:
        root_value = schema_definition.raw_schema[schema_definition.root_key]
        skeleton: Any = {}
        if isinstance(root_value, dict):
            first_value = next(iter(root_value.values()), {})
            skeleton = first_value if isinstance(first_value, dict) else {}
        elif isinstance(root_value, list) and root_value:
            skeleton = root_value[0]

        if isinstance(skeleton, dict) and schema_definition.match_key and schema_definition.match_key not in skeleton:
            enriched = dict(skeleton)
            enriched[schema_definition.match_key] = ""
            return enriched
        return skeleton

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

        expected_type = infer_scalar_type_name(skeleton)
        if expected_type == "string" and not isinstance(node, str):
            errors.append(ValidationIssue(path=path, reason=f"expected string but got {type(node).__name__}"))
        elif expected_type == "integer" and (isinstance(node, bool) or not isinstance(node, int)):
            errors.append(ValidationIssue(path=path, reason=f"expected integer but got {type(node).__name__}"))
        elif expected_type == "number" and (isinstance(node, bool) or not isinstance(node, (int, float))):
            errors.append(ValidationIssue(path=path, reason=f"expected number but got {type(node).__name__}"))
        elif expected_type == "boolean" and not isinstance(node, bool):
            errors.append(ValidationIssue(path=path, reason=f"expected boolean but got {type(node).__name__}"))

    def _is_missing_match_value(self, value: Any) -> bool:
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

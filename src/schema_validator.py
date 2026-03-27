from __future__ import annotations

from typing import Any

import yaml

from models import SchemaDefinition, ValidationIssue, ValidationResult


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

    def validate_increment(self, data: dict[str, Any], schema_definition: SchemaDefinition) -> ValidationResult:
        errors: list[ValidationIssue] = []
        root_key = schema_definition.root_key
        match_key = schema_definition.match_key

        extra_top_level_keys = [key for key in data.keys() if key not in schema_definition.allowed_top_level_keys]
        for key in extra_top_level_keys:
            errors.append(ValidationIssue(path=key, reason="unexpected top-level key"))

        if root_key not in data:
            errors.append(ValidationIssue(path=root_key, reason="required root key is missing"))
            return ValidationResult(ok=False, errors=errors)

        root_value = data[root_key]
        if not isinstance(root_value, list):
            errors.append(ValidationIssue(path=root_key, reason="root value must be a list"))
            return ValidationResult(ok=False, errors=errors)

        if not root_value:
            return ValidationResult(ok=not errors, errors=errors)

        for index, item in enumerate(root_value):
            item_path = f"{root_key}[{index}]"
            if not isinstance(item, dict):
                errors.append(ValidationIssue(path=item_path, reason="list item must be mapping"))
                continue

            if match_key not in item or item[match_key] in (None, ""):
                errors.append(ValidationIssue(path=f"{item_path}.{match_key}", reason="required match key is missing"))

            self._validate_against_skeleton(
                node=item,
                skeleton=self._extract_item_skeleton(schema_definition),
                path=item_path,
                errors=errors,
            )

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
        elif expected_type == "integer" and not isinstance(node, int):
            errors.append(ValidationIssue(path=path, reason=f"expected integer but got {type(node).__name__}"))
        elif expected_type == "number" and not isinstance(node, (int, float)):
            errors.append(ValidationIssue(path=path, reason=f"expected number but got {type(node).__name__}"))
        elif expected_type == "boolean" and not isinstance(node, bool):
            errors.append(ValidationIssue(path=path, reason=f"expected boolean but got {type(node).__name__}"))


def infer_scalar_type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"

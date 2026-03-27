from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from models import SchemaDefinition, SchemaField

DEFAULT_MATCH_KEY = "name"
DEFAULT_REQUIRED_ITEM_KEYS = ["name"]
ROOT_KEY_MATCH_KEY_MAP = {
    "actors": "name",
    "worldinfo": "name",
}


class SchemaLoader:
    def load(self, schema_path: Path) -> SchemaDefinition:
        schema_text = schema_path.read_text(encoding="utf-8")
        raw_schema = load_schema_text(schema_text)
        if not isinstance(raw_schema, dict) or not raw_schema:
            raise ValueError(f"schema file is empty or invalid: {schema_path}")

        root_key = next(iter(raw_schema.keys()))
        root_value = raw_schema[root_key]
        match_key = ROOT_KEY_MATCH_KEY_MAP.get(root_key, DEFAULT_MATCH_KEY)
        fields = extract_fields(root_key, root_value)
        if not fields:
            fields = extract_fields_from_text(schema_text, root_key)
            raw_schema = build_raw_schema_from_fields(root_key, fields)

        allowed_top_level_keys = [root_key]
        required_item_keys = [match_key] if match_key else list(DEFAULT_REQUIRED_ITEM_KEYS)

        return SchemaDefinition(
            schema_name=schema_path.stem,
            schema_path=schema_path,
            root_key=root_key,
            match_key=match_key,
            schema_text=schema_text,
            raw_schema=raw_schema,
            fields=fields,
            allowed_top_level_keys=allowed_top_level_keys,
            required_item_keys=required_item_keys,
        )


def load_schema_text(schema_text: str) -> dict[str, Any]:
    try:
        loaded = yaml.safe_load(schema_text) or {}
        if isinstance(loaded, dict):
            return loaded
    except yaml.YAMLError:
        pass

    root_key = find_root_key(schema_text)
    fields = extract_fields_from_text(schema_text, root_key)
    return build_raw_schema_from_fields(root_key, fields)


def find_root_key(schema_text: str) -> str:
    for line in schema_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, _ = split_key_value(stripped)
        return key
    raise ValueError("schema text does not contain a root key")


def extract_fields_from_text(schema_text: str, root_key: str) -> list[SchemaField]:
    tokens = tokenize_schema_lines(schema_text)
    stack: list[dict[str, Any]] = []
    fields: list[SchemaField] = []

    for index, token in enumerate(tokens):
        indent = int(token["indent"])
        content = str(token["content"])
        next_token = tokens[index + 1] if index + 1 < len(tokens) else None

        while stack and indent <= int(stack[-1]["indent"]):
            stack.pop()

        if content.startswith("- "):
            parent = stack[-1] if stack else None
            if parent is None or parent.get("container_type") != "list":
                continue

            item_path = f"{parent['path']}[]"
            item_content = content[2:].strip()
            if is_mapping_entry(item_content):
                key, raw_value = split_key_value(item_content)
                field_type = infer_line_type(raw_value, indent, next_token, key=key, root_key=root_key)
                field_path = f"{item_path}.{key}"
                fields.append(
                    SchemaField(
                        path=field_path,
                        field_type=field_type,
                        required=key == ROOT_KEY_MATCH_KEY_MAP.get(root_key, DEFAULT_MATCH_KEY),
                        instruction="",
                    )
                )
                stack.append({"indent": indent, "path": item_path, "container_type": "object"})
                if raw_value == "":
                    stack.append({"indent": indent + 1, "path": field_path, "container_type": field_type})
            continue

        key, raw_value = split_key_value(content)

        if not stack:
            root_type = infer_line_type(raw_value, indent, next_token, key=key, root_key=root_key)
            stack.append({"indent": indent, "path": key, "container_type": root_type})
            continue

        parent = stack[-1]
        parent_path = str(parent["path"])

        if is_placeholder_key(key):
            placeholder_path = f"{parent_path}[]"
            placeholder_type = infer_line_type(raw_value, indent, next_token, key=key, root_key=root_key)
            stack.append({"indent": indent, "path": placeholder_path, "container_type": placeholder_type})
            continue

        field_type = infer_line_type(raw_value, indent, next_token, key=key, root_key=root_key)
        field_path = f"{parent_path}.{key}"
        fields.append(
            SchemaField(
                path=field_path,
                field_type=field_type,
                required=key == ROOT_KEY_MATCH_KEY_MAP.get(root_key, DEFAULT_MATCH_KEY),
                instruction="",
            )
        )

        if raw_value == "":
            stack.append({"indent": indent, "path": field_path, "container_type": field_type})

    return deduplicate_fields(fields)


def build_raw_schema_from_fields(root_key: str, fields: list[SchemaField]) -> dict[str, Any]:
    match_key = ROOT_KEY_MATCH_KEY_MAP.get(root_key, DEFAULT_MATCH_KEY)
    root: dict[str, Any] = {root_key: {f"<{match_key}>": {}}}
    all_paths = [field.path for field in fields]

    for field in sorted(fields, key=lambda item: (item.path.count("."), item.path)):
        if not field.path.startswith(f"{root_key}[]"):
            continue

        suffix = field.path[len(f"{root_key}[]") :].lstrip(".")
        if not suffix:
            continue

        parts = [part for part in suffix.split(".") if part]
        current: dict[str, Any] = root[root_key][f"<{match_key}>"]
        full_path_parts: list[str] = []

        for part_index, part in enumerate(parts):
            full_path_parts.append(part)
            current_path = f"{root_key}[].{'.'.join(full_path_parts)}"
            is_last = part_index == len(parts) - 1
            is_list_part = part.endswith("[]")
            key = part[:-2] if is_list_part else part

            if is_last:
                current[key] = default_value_for_path(current_path, field.field_type, all_paths)
                break

            next_part = parts[part_index + 1]
            next_is_list = next_part.endswith("[]")

            if is_list_part:
                if key not in current or not isinstance(current[key], list):
                    current[key] = [{}]
                if not current[key] or not isinstance(current[key][0], dict):
                    current[key] = [{}]
                current = current[key][0]
                continue

            if next_is_list:
                if key not in current or not isinstance(current[key], list):
                    current[key] = [{}]
                if not current[key] or not isinstance(current[key][0], dict):
                    current[key] = [{}]
                current = current[key][0]
            else:
                if key not in current or not isinstance(current[key], dict):
                    current[key] = {}
                current = current[key]

    return root



def default_value_for_path(field_path: str, field_type: str, all_paths: list[str]) -> Any:
    if field_type == "object":
        return {}
    if field_type == "list":
        child_prefix = f"{field_path}[]."
        if any(path.startswith(child_prefix) for path in all_paths):
            return [{}]
        return []
    if field_type == "boolean":
        return False
    if field_type == "integer":
        return 0
    if field_type == "number":
        return 0.0
    return ""


def tokenize_schema_lines(schema_text: str) -> list[dict[str, Any]]:
    tokens: list[dict[str, Any]] = []
    for line in schema_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        tokens.append({"indent": indent, "content": stripped})
    return tokens


def infer_line_type(
    raw_value: str,
    indent: int,
    next_token: dict[str, Any] | None,
    *,
    key: str,
    root_key: str,
) -> str:
    if raw_value == "":
        if next_token is not None and int(next_token["indent"]) > indent:
            next_content = str(next_token["content"])
            if next_content.startswith("- "):
                return "list"
            next_key, _ = split_key_value(next_content)
            if key == root_key and is_placeholder_key(next_key):
                return "list"
        return "object"

    normalized = strip_inline_comment(raw_value).strip()
    if normalized == "[]":
        return "list"
    if normalized == "{}":
        return "object"
    if normalized.lower() in {"true", "false"}:
        return "boolean"
    if normalized.isdigit():
        return "integer"
    return "string"


def strip_inline_comment(value: str) -> str:
    if " #" in value:
        return value.split(" #", 1)[0]
    return value


def extract_fields(root_key: str, node: Any, prefix: str | None = None) -> list[SchemaField]:
    current_prefix = prefix or root_key
    results: list[SchemaField] = []

    if isinstance(node, dict):
        for key, value in node.items():
            if is_placeholder_key(key):
                item_prefix = f"{current_prefix}[]"
                results.extend(extract_fields(root_key, value, item_prefix))
                continue

            field_type = infer_field_type(value)
            field_path = f"{current_prefix}.{key}" if current_prefix else key
            required = key == ROOT_KEY_MATCH_KEY_MAP.get(root_key, DEFAULT_MATCH_KEY)
            results.append(
                SchemaField(
                    path=field_path,
                    field_type=field_type,
                    required=required,
                    instruction="",
                )
            )
            results.extend(extract_fields(root_key, value, field_path))
        return deduplicate_fields(results)

    if isinstance(node, list) and node:
        item_prefix = f"{current_prefix}[]"
        sample = node[0]
        results.extend(extract_fields(root_key, sample, item_prefix))

    return deduplicate_fields(results)


def deduplicate_fields(fields: list[SchemaField]) -> list[SchemaField]:
    seen: set[str] = set()
    deduped: list[SchemaField] = []
    for field in fields:
        if field.path in seen:
            continue
        seen.add(field.path)
        deduped.append(field)
    return deduped


def infer_field_type(value: Any) -> str:
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "list"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


def is_placeholder_key(value: str) -> bool:
    return value.startswith("<") and value.endswith(">")


def is_mapping_entry(value: str) -> bool:
    if value.startswith("<") and value.endswith(">"):
        return False
    return ":" in value


def split_key_value(value: str) -> tuple[str, str]:
    key, raw_value = value.split(":", 1)
    return key.strip(), raw_value.strip()

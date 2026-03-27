from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from models import MergeStats, SchemaDefinition


class IndentedSafeDumper(yaml.SafeDumper):
    def increase_indent(self, flow: bool = False, indentless: bool = False) -> Any:
        return super().increase_indent(flow, False)


class YamlStore:
    def load_yaml(self, path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
        if not path.exists():
            return default.copy() if default is not None else {}

        content = path.read_text(encoding="utf-8").strip()
        if not content:
            return default.copy() if default is not None else {}

        data = yaml.safe_load(content)
        if data is None:
            return default.copy() if default is not None else {}
        if not isinstance(data, dict):
            raise ValueError(f"yaml root must be mapping: {path}")
        return data

    def initialize_output(self, path: Path, schema_definition: SchemaDefinition) -> None:
        if path.exists():
            return
        initial = {schema_definition.root_key: {}}
        self.write_yaml(path, initial)

    def write_yaml(self, path: Path, data: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = self._dump_yaml(data)
        path.write_text(serialized, encoding="utf-8")

    def merge_increment(
        self,
        current_data: dict[str, Any],
        increment_data: dict[str, Any],
        schema_definition: SchemaDefinition,
    ) -> tuple[dict[str, Any], MergeStats]:
        root_key = schema_definition.root_key
        existing_items = dict(current_data.get(root_key, {}) or {})
        incoming_items = dict(increment_data.get(root_key, {}) or {})
        stats = MergeStats()

        for item_key, item_value in incoming_items.items():
            if not isinstance(item_value, dict):
                raise ValueError("increment item must be mapping")

            if item_key in existing_items:
                existing_items[item_key] = self._merge_node(existing_items[item_key], item_value)
                stats.replaced_nodes += 1
            else:
                existing_items[item_key] = self._clone_node(item_value)
                stats.appended_nodes += 1

        merged = dict(current_data)
        merged[root_key] = existing_items
        return merged, stats

    def dump_to_string(self, data: dict[str, Any]) -> str:
        return self._dump_yaml(data).strip()

    def _dump_yaml(self, data: dict[str, Any]) -> str:
        return yaml.dump(
            data,
            allow_unicode=True,
            sort_keys=False,
            indent=2,
            Dumper=IndentedSafeDumper,
        )

    def _merge_node(self, current: Any, incoming: Any) -> Any:
        if isinstance(current, dict) and isinstance(incoming, dict):
            merged = {key: self._clone_node(value) for key, value in current.items()}
            for key, value in incoming.items():
                if key in merged:
                    merged[key] = self._merge_node(merged[key], value)
                else:
                    merged[key] = self._clone_node(value)
            return merged

        if isinstance(incoming, list):
            return [self._clone_node(item) for item in incoming]

        return self._clone_node(incoming)

    def _clone_node(self, node: Any) -> Any:
        if isinstance(node, dict):
            return {key: self._clone_node(value) for key, value in node.items()}
        if isinstance(node, list):
            return [self._clone_node(item) for item in node]
        return node



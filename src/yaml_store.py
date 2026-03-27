from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from models import MergeStats, SchemaDefinition


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
        initial = {schema_definition.root_key: []}
        self.write_yaml(path, initial)

    def write_yaml(self, path: Path, data: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = yaml.safe_dump(
            data,
            allow_unicode=True,
            sort_keys=False,
            indent=2,
        )
        path.write_text(serialized, encoding="utf-8")

    def merge_increment(
        self,
        current_data: dict[str, Any],
        increment_data: dict[str, Any],
        schema_definition: SchemaDefinition,
    ) -> tuple[dict[str, Any], MergeStats]:
        root_key = schema_definition.root_key
        match_key = schema_definition.match_key
        existing_items = list(current_data.get(root_key, []) or [])
        incoming_items = list(increment_data.get(root_key, []) or [])
        stats = MergeStats()

        index_by_key: dict[str, int] = {}
        for index, item in enumerate(existing_items):
            if isinstance(item, dict) and match_key in item:
                index_by_key[str(item[match_key])] = index

        for item in incoming_items:
            if not isinstance(item, dict):
                raise ValueError("increment item must be mapping")
            if match_key not in item:
                raise ValueError(f"increment item missing match key: {match_key}")

            item_key = str(item[match_key])
            if item_key in index_by_key:
                existing_index = index_by_key[item_key]
                existing_items[existing_index] = self._merge_node(existing_items[existing_index], item)
                stats.replaced_nodes += 1
            else:
                index_by_key[item_key] = len(existing_items)
                existing_items.append(self._clone_node(item))
                stats.appended_nodes += 1

        merged = dict(current_data)
        merged[root_key] = existing_items
        return merged, stats

    def dump_to_string(self, data: dict[str, Any]) -> str:
        return yaml.safe_dump(
            data,
            allow_unicode=True,
            sort_keys=False,
            indent=2,
        ).strip()

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

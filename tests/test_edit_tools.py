from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from edit_tools import (  # noqa: E402
    EditTools,
    PathError,
    deep_merge,
    parse_field_path,
    summarize_entry,
)
from schema_loader import SchemaLoader  # noqa: E402
from yaml_store import YamlStore  # noqa: E402

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "characters.yaml"

def build_tools(temp_dir: str, *, allow_remove: bool = False) -> tuple[EditTools, Path, Path]:
    root = Path(temp_dir)
    output_path = root / "characters.yaml"
    journal_path = root / "journal.jsonl"
    output_path.write_text("actors: {}\n", encoding="utf-8")
    schema = SchemaLoader().load(SCHEMA_PATH)
    tools = EditTools(
        output_path=output_path,
        schema_definition=schema,
        journal_path=journal_path,
        allow_remove=allow_remove,
    )
    return tools, output_path, journal_path

def minimal_node() -> dict:
    return {
        "name": {"ja": "アリス", "zh-CN": "爱丽丝", "romaji": "Alice"},
        "trigger_keywords": ["爱丽丝"],
        "basic_settings": {"gender": "Female", "race": "Human", "age": 19, "identity": ["学生"]},
    }

class ParseFieldPathTests(unittest.TestCase):
    def test_parses_dotted_path(self) -> None:
        self.assertEqual(parse_field_path("basic_settings.age"), ["basic_settings", "age"])

    def test_parses_array_index(self) -> None:
        self.assertEqual(
            parse_field_path("appearance.outfits[0].description"),
            ["appearance", "outfits", 0, "description"],
        )

    def test_parses_multi_digit_index(self) -> None:
        self.assertEqual(parse_field_path("a[10].b"), ["a", 10, "b"])

    def test_tolerates_leading_dot(self) -> None:
        self.assertEqual(parse_field_path(".basic_settings.age"), ["basic_settings", "age"])

    def test_leading_dot_path_resolves_to_same_field(self) -> None:
        # 与 test_tolerates_leading_dot 的区别：这里验证解析结果能被真实读取使用
        self.assertEqual(parse_field_path(".name.romaji"), ["name", "romaji"])

    def test_rejects_empty_path(self) -> None:
        with self.assertRaises(PathError):
            parse_field_path("   ")

    def test_rejects_trailing_separator(self) -> None:
        with self.assertRaises(PathError):
            parse_field_path("a.b.")

    def test_rejects_unclosed_index(self) -> None:
        with self.assertRaises(PathError):
            parse_field_path("a[0")

    def test_rejects_non_numeric_index(self) -> None:
        with self.assertRaises(PathError):
            parse_field_path("a[b]")

    def test_rejects_stray_closing_bracket(self) -> None:
        with self.assertRaises(PathError):
            parse_field_path("a]b")

class DeepMergeTests(unittest.TestCase):
    def test_merges_nested_dicts(self) -> None:
        base = {"a": {"x": 1, "y": 2}, "b": 3}
        incoming = {"a": {"y": 9, "z": 4}, "c": 5}
        self.assertEqual(
            deep_merge(base, incoming),
            {"a": {"x": 1, "y": 9, "z": 4}, "b": 3, "c": 5},
        )

    def test_incoming_replaces_scalar(self) -> None:
        self.assertEqual(deep_merge({"a": 1}, {"a": 2}), {"a": 2})

    def test_incoming_replaces_list_wholesale(self) -> None:
        self.assertEqual(deep_merge({"a": [1, 2]}, {"a": [3]}), {"a": [3]})

    def test_does_not_mutate_base(self) -> None:
        base = {"a": {"x": 1}}
        deep_merge(base, {"a": {"y": 2}})
        self.assertEqual(base, {"a": {"x": 1}})

class EditToolsUpsertTests(unittest.TestCase):
    def test_creates_new_entry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)

            outcome = tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            self.assertTrue(outcome.ok)
            self.assertTrue(outcome.extra["created"])
            stored = YamlStore().load_yaml(output_path)
            self.assertIn("Alice", stored["actors"])
            self.assertEqual(stored["actors"]["Alice"]["basic_settings"]["age"], 19)

    def test_updates_existing_entry_without_losing_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "upsert_entry",
                {"key": "Alice", "node": {"basic_settings": {"age": 20}}},
            )

            self.assertTrue(outcome.ok)
            self.assertTrue(outcome.extra["updated"])
            stored = YamlStore().load_yaml(output_path)
            # 未提及的字段必须保留
            self.assertEqual(stored["actors"]["Alice"]["basic_settings"]["gender"], "Female")
            self.assertEqual(stored["actors"]["Alice"]["basic_settings"]["age"], 20)

    def test_rejects_chinese_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)

            outcome = tools.dispatch("upsert_entry", {"key": "爱丽丝", "node": {"a": "1"}})

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "bad_path")
            self.assertNotIn("爱丽丝", YamlStore().load_yaml(output_path)["actors"])

    def test_rejects_key_with_space(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)

            outcome = tools.dispatch("upsert_entry", {"key": "Alice Smith", "node": {"a": "1"}})

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "bad_path")

    def test_rejects_non_mapping_node(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)

            outcome = tools.dispatch("upsert_entry", {"key": "Alice", "node": ["x"]})

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "bad_node")

    def test_rejects_node_that_sanitizes_to_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)

            outcome = tools.dispatch("upsert_entry", {"key": "Alice", "node": {"name": "   "}})

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "empty_node")

    def test_rejects_schema_violation_and_keeps_file_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, journal_path = build_tools(temp_dir)
            before = output_path.read_text(encoding="utf-8")

            outcome = tools.dispatch(
                "upsert_entry",
                {"key": "Bob", "node": {**minimal_node(), "bogus_field": "x"}},
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "schema_violation")
            self.assertEqual(output_path.read_text(encoding="utf-8"), before)
            self.assertFalse(journal_path.exists())

    def test_rejects_near_duplicate_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch("upsert_entry", {"key": "Alicia", "node": minimal_node()})

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "near_duplicate_key")
            candidates = outcome.extra["candidates"]
            self.assertEqual(candidates[0]["key"], "Alice")

    def test_rejects_forbidden_placeholder_value(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)

            outcome = tools.dispatch(
                "upsert_entry",
                {"key": "Alice", "node": {**minimal_node(), "basic_settings": {"gender": "未知"}}},
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "forbidden_value")

class EditToolsSetFieldTests(unittest.TestCase):
    def test_sets_nested_scalar(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "set_fields",
                {"key": "Alice", "path": "basic_settings.age", "value": 20},
            )

            self.assertTrue(outcome.ok)
            stored = YamlStore().load_yaml(output_path)
            self.assertEqual(stored["actors"]["Alice"]["basic_settings"]["age"], 20)

    def test_sets_array_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch(
                "upsert_entry",
                {
                    "key": "Alice",
                    "node": {
                        **minimal_node(),
                        "appearance": {"outfits": [{"outfit_type": "school", "description": "a"}]},
                    },
                },
            )

            outcome = tools.dispatch(
                "set_fields",
                {"key": "Alice", "path": "appearance.outfits[0].description", "value": "b"},
            )

            self.assertTrue(outcome.ok)
            stored = YamlStore().load_yaml(output_path)
            self.assertEqual(
                stored["actors"]["Alice"]["appearance"]["outfits"][0]["description"],
                "b",
            )

    def test_unknown_field_returns_sibling_hints(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "set_fields",
                {"key": "Alice", "path": "basic_settings.bogus", "value": "x"},
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "unknown_field")
            self.assertIn("gender", outcome.extra["allowed_siblings"])

    def test_unknown_entry_asks_for_upsert(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)

            outcome = tools.dispatch(
                "set_fields",
                {"key": "Nobody", "path": "basic_settings.age", "value": 1},
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "unknown_entry")

    def test_rejects_empty_value(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "set_fields",
                {"key": "Alice", "path": "basic_settings.race", "value": "   "},
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "empty_value")

    def test_missing_value_argument_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch("set_fields", {"key": "Alice", "path": "basic_settings.age"})

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "missing_value")

class EditToolsAppendTests(unittest.TestCase):
    def test_appends_new_item(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "append_items",
                {"key": "Alice", "path": "trigger_keywords", "items": ["小爱"]},
            )

            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.extra["appended"], 1)
            stored = YamlStore().load_yaml(output_path)
            self.assertEqual(stored["actors"]["Alice"]["trigger_keywords"], ["爱丽丝", "小爱"])

    def test_skips_duplicate_scalar_by_value(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "append_items",
                {"key": "Alice", "path": "trigger_keywords", "items": ["爱丽丝"]},
            )

            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.extra["appended"], 0)
            self.assertEqual(outcome.extra["duplicates"], 1)
            stored = YamlStore().load_yaml(output_path)
            self.assertEqual(stored["actors"]["Alice"]["trigger_keywords"], ["爱丽丝"])

    def test_skips_duplicate_object_with_identical_match_key_field(self) -> None:
        """对象元素在 match_key 字段相同时被去重。

        `offspring` 是含 `name` 字段的数组，因此会按 name 判断重复。
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch(
                "upsert_entry",
                {
                    "key": "Alice",
                    "node": {
                        **minimal_node(),
                        "offspring": [{"name": "Child", "race": "Human"}],
                    },
                },
            )

            outcome = tools.dispatch(
                "append_items",
                {
                    "key": "Alice",
                    "path": "offspring",
                    "items": [{"name": "Child", "race": "Elf"}],
                },
            )

            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.extra["appended"], 0)
            self.assertEqual(outcome.extra["duplicates"], 1)
            stored = YamlStore().load_yaml(output_path)
            self.assertEqual(len(stored["actors"]["Alice"]["offspring"]), 1)

    def test_appends_object_with_different_match_key_value(self) -> None:
        """match_key 字段不同时应作为新元素追加。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch(
                "upsert_entry",
                {
                    "key": "Alice",
                    "node": {
                        **minimal_node(),
                        "offspring": [{"name": "Child", "race": "Human"}],
                    },
                },
            )

            outcome = tools.dispatch(
                "append_items",
                {
                    "key": "Alice",
                    "path": "offspring",
                    "items": [{"name": "Second", "race": "Human"}],
                },
            )

            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.extra["appended"], 1)
            stored = YamlStore().load_yaml(output_path)
            self.assertEqual(len(stored["actors"]["Alice"]["offspring"]), 2)

    def test_object_without_match_key_field_appends_when_unequal(self) -> None:
        """不含 match_key 字段的对象元素按整体相等性判断，不同则追加。

        `outfits` 用 outfit_type 而非 name 作为身份，因此不会被去重。
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch(
                "upsert_entry",
                {
                    "key": "Alice",
                    "node": {
                        **minimal_node(),
                        "appearance": {"outfits": [{"outfit_type": "school", "description": "a"}]},
                    },
                },
            )

            outcome = tools.dispatch(
                "append_items",
                {
                    "key": "Alice",
                    "path": "appearance.outfits",
                    "items": [{"outfit_type": "school", "description": "changed"}],
                },
            )

            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.extra["appended"], 1)

    def test_append_mode_allows_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "append_items",
                {"key": "Alice", "path": "trigger_keywords", "items": ["爱丽丝"], "mode": "append"},
            )

            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.extra["appended"], 1)
            stored = YamlStore().load_yaml(output_path)
            self.assertEqual(len(stored["actors"]["Alice"]["trigger_keywords"]), 2)

    def test_auto_initializes_missing_skeleton_array(self) -> None:
        """骨架中声明为数组但尚未创建的字段，append 时自动补空数组。

        避免模型为了追加一个元素而多跑一次 upsert 往返。
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "append_items",
                {
                    "key": "Alice",
                    "path": "appearance.outfits",
                    "items": [{"outfit_type": "school", "description": "shirt"}],
                },
            )

            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.extra["appended"], 1)
            stored = YamlStore().load_yaml(output_path)
            self.assertEqual(len(stored["actors"]["Alice"]["appearance"]["outfits"]), 1)

    def test_rejects_append_to_non_array_skeleton_field(self) -> None:
        """骨架中是标量的字段不能 append，即使字段尚不存在。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "append_items",
                {"key": "Alice", "path": "appearance.physical_description", "items": ["x"]},
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "unknown_field")

    def test_supports_dynamic_placeholder_keys(self) -> None:
        """relationships 使用动态键（dynamic_with_<CharacterName>），校验器必须通配。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            created = tools.dispatch(
                "upsert_entry",
                {
                    "key": "Alice",
                    "node": {"relationships": {"dynamic_with_Bob": {"brief_of_relationship": "friend"}}},
                },
            )
            updated = tools.dispatch(
                "set_fields",
                {
                    "key": "Alice",
                    "path": "relationships.dynamic_with_Bob.brief_of_relationship",
                    "value": "rival",
                },
            )

            self.assertTrue(created.ok, created.reason)
            self.assertTrue(updated.ok, updated.reason)
            stored = YamlStore().load_yaml(output_path)
            self.assertEqual(
                stored["actors"]["Alice"]["relationships"]["dynamic_with_Bob"]["brief_of_relationship"],
                "rival",
            )

    def test_dynamic_key_still_rejects_unknown_subfield(self) -> None:
        """通配只放开键名，动态键内部的字段仍受骨架约束。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "upsert_entry",
                {
                    "key": "Alice",
                    "node": {"relationships": {"dynamic_with_Bob": {"bogus": "x"}}},
                },
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "schema_violation")

    def test_rejects_non_list_target(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "append_items",
                {"key": "Alice", "path": "basic_settings.age", "items": ["x"]},
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "not_a_list")

    def test_rejects_empty_items(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "append_items",
                {"key": "Alice", "path": "trigger_keywords", "items": []},
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "bad_items")

    def test_rejects_bad_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch(
                "append_items",
                {"key": "Alice", "path": "trigger_keywords", "items": ["x"], "mode": "replace"},
            )

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "bad_mode")

class EditToolsReadTests(unittest.TestCase):
    def test_reads_entry_and_field(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            whole = tools.dispatch("read_entry", {"key": "Alice"})
            field = tools.dispatch("read_entry", {"key": "Alice", "path": "basic_settings.age"})

            self.assertTrue(whole.ok)
            self.assertEqual(whole.extra["value"]["basic_settings"]["age"], 19)
            self.assertEqual(field.extra["value"], 19)

    def test_reads_missing_entry_as_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)

            outcome = tools.dispatch("read_entry", {"key": "Nobody"})

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "bad_path")

    def test_lists_entries_with_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch("list_entries", {})

            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.extra["count"], 1)
            self.assertEqual(outcome.extra["entries"][0]["key"], "Alice")
            self.assertTrue(outcome.extra["entries"][0]["summary"])

class EditToolsRemoveTests(unittest.TestCase):
    def test_remove_hidden_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)

            names = tools.tool_names()

            self.assertNotIn("remove_entry", names)
            self.assertNotIn("remove_field", names)

    def test_remove_tools_listed_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir, allow_remove=True)

            names = tools.tool_names()

            self.assertIn("remove_entry", names)
            self.assertIn("remove_field", names)

    def test_remove_field_requires_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch("remove_field", {"key": "Alice", "path": "basic_settings.age"})

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "remove_disabled")

    def test_remove_field_succeeds_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir, allow_remove=True)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch("remove_field", {"key": "Alice", "path": "basic_settings.age"})

            self.assertTrue(outcome.ok)
            stored = YamlStore().load_yaml(output_path)
            self.assertNotIn("age", stored["actors"]["Alice"]["basic_settings"])

    def test_remove_entry_succeeds_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir, allow_remove=True)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            outcome = tools.dispatch("remove_entry", {"key": "Alice"})

            self.assertTrue(outcome.ok)
            stored = YamlStore().load_yaml(output_path)
            self.assertEqual(stored["actors"], {})

class EditToolsJournalTests(unittest.TestCase):
    def test_journal_records_pre_image_on_update(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, journal_path = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})
            tools.dispatch("set_fields", {"key": "Alice", "path": "basic_settings.age", "value": 20})

            records = [json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines()]

            self.assertEqual(len(records), 2)
            self.assertEqual(records[0]["op"], "upsert_entry")
            self.assertTrue(records[0]["created"])
            self.assertEqual(records[1]["op"], "set_fields")
            self.assertEqual(records[1]["before"], 19)
            self.assertEqual(records[1]["after"], 20)

    def test_journal_stores_previous_node_for_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, journal_path = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})
            tools.dispatch("upsert_entry", {"key": "Alice", "node": {"basic_settings": {"age": 30}}})

            records = [json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines()]

            self.assertEqual(records[1]["before"]["basic_settings"]["age"], 19)

    def test_failed_operation_writes_no_journal_entry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, journal_path = build_tools(temp_dir)

            tools.dispatch("upsert_entry", {"key": "Bob", "node": {"bogus": "x"}})

            self.assertFalse(journal_path.exists())

class EditToolsDispatchTests(unittest.TestCase):
    def test_unknown_tool_lists_available_tools(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)

            outcome = tools.dispatch("no_such_tool", {})

            self.assertFalse(outcome.ok)
            self.assertEqual(outcome.code, "unknown_tool")
            self.assertIn("upsert_entry", outcome.extra["available_tools"])

    def test_tool_schemas_are_json_serializable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir, allow_remove=True)

            payload = json.dumps(tools.tool_schemas(), ensure_ascii=False)

            self.assertIn("upsert_entry", payload)

    def test_tool_schemas_are_isolated_copies(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, _, _ = build_tools(temp_dir)

            first = tools.tool_schemas()
            first[0]["function"]["name"] = "mutated"

            self.assertNotIn("mutated", [item["function"]["name"] for item in tools.tool_schemas()])

    def test_write_is_atomic_no_temp_file_left_behind(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})

            leftovers = [item.name for item in output_path.parent.iterdir() if item.name.endswith(".tmp")]

            self.assertEqual(leftovers, [])

    def test_document_stays_valid_yaml_after_many_operations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tools, output_path, _ = build_tools(temp_dir)
            tools.dispatch("upsert_entry", {"key": "Alice", "node": minimal_node()})
            tools.dispatch("set_fields", {"key": "Alice", "path": "basic_settings.age", "value": 21})
            tools.dispatch("append_items", {"key": "Alice", "path": "trigger_keywords", "items": ["アリス"]})

            stored = YamlStore().load_yaml(output_path)

            self.assertEqual(stored["actors"]["Alice"]["basic_settings"]["age"], 21)
            self.assertEqual(stored["actors"]["Alice"]["trigger_keywords"], ["爱丽丝", "アリス"])

class SummarizeEntryTests(unittest.TestCase):
    def test_prefers_name_field(self) -> None:
        node = {"name": {"zh-CN": "爱丽丝"}, "content": "别的"}
        self.assertEqual(summarize_entry(node), "爱丽丝")

    def test_falls_back_to_any_text(self) -> None:
        self.assertEqual(summarize_entry({"foo": "bar"}), "bar")

    def test_returns_empty_for_empty_mapping(self) -> None:
        self.assertEqual(summarize_entry({}), "")

    def test_truncates_long_text(self) -> None:
        self.assertLessEqual(len(summarize_entry({"foo": "x" * 500}, limit=10)), 10)

if __name__ == "__main__":
    unittest.main()

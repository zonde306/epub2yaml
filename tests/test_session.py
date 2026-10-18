from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from index_builder import build_index_text, build_watermark_text  # noqa: E402
from models import SessionConfig  # noqa: E402
from schema_loader import SchemaLoader  # noqa: E402
from session import (  # noqa: E402
    READY_MESSAGE,
    AssistantTurn,
    ExtractionSession,
    PrefixMismatchError,
    ToolCall,
    UsageStats,
    sums_tokens,
)

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "characters.yaml"

def build_session(temp_dir: str | None = None, **config_kwargs) -> ExtractionSession:
    schema = SchemaLoader().load(SCHEMA_PATH)
    messages_path = Path(temp_dir) / "messages.jsonl" if temp_dir else None
    return ExtractionSession(
        schema_definition=schema,
        config=SessionConfig(**config_kwargs),
        persona_text="你是一个小说分析师，请使用编辑工具修改输出。",
        messages_path=messages_path,
    )

def sample_document() -> dict:
    return {
        "actors": {
            "Alice": {"name": {"zh-CN": "爱丽丝"}},
            "Bob": {"name": {"zh-CN": "鲍勃"}},
        }
    }

class SessionStartTests(unittest.TestCase):
    def test_start_creates_four_leading_messages(self) -> None:
        session = build_session()
        session.start(sample_document())

        self.assertEqual(len(session.messages), 4)
        self.assertEqual(session.messages[0]["role"], "system")
        self.assertEqual(session.messages[1]["role"], "system")
        self.assertEqual(session.messages[2]["role"], "user")
        self.assertEqual(session.messages[3]["role"], "assistant")
        self.assertEqual(session.messages[3]["content"], READY_MESSAGE)

    def test_schema_block_appears_exactly_once(self) -> None:
        session = build_session()
        session.start(sample_document())
        session.append_segment("正文", batch_range="ch0001-ch0001", chapter_count=1)

        schema_modules = [message for message in session.messages if message["role"] == "system"]
        full_text = json.dumps(schema_modules, ensure_ascii=False)

        self.assertEqual(full_text.count("actors:"), 1)

    def test_index_lists_existing_entries(self) -> None:
        session = build_session()
        session.start(sample_document())

        index_text = session.messages[2]["content"]

        self.assertIn("Alice", index_text)
        self.assertIn("Bob", index_text)

    def test_start_twice_is_rejected(self) -> None:
        session = build_session()
        session.start(sample_document())

        with self.assertRaises(RuntimeError):
            session.start(sample_document())

    def test_rotate_before_start_is_rejected(self) -> None:
        session = build_session()

        with self.assertRaises(RuntimeError):
            session.rotate(sample_document())

class PrefixStabilityTests(unittest.TestCase):
    def test_append_only_prefix_is_strict(self) -> None:
        """核心断言：第 N 次请求的消息字节必须是第 N+1 次的严格前缀。"""
        session = build_session()
        session.start(sample_document())

        signatures = [session.prefix_signature()]
        for index in range(1, 4):
            session.append_segment(f"第 {index} 段正文", batch_range=f"ch000{index}-ch000{index}", chapter_count=1)
            signatures.append(session.prefix_signature())
            session.append_assistant(AssistantTurn(content=f"已处理第 {index} 段"))
            signatures.append(session.prefix_signature())

        for previous, current in zip(signatures, signatures[1:]):
            self.assertTrue(
                current.startswith(previous),
                "消息前缀被破坏：缓存将无法命中",
            )

    def test_assert_prefix_of_detects_violation(self) -> None:
        session = build_session()
        session.start(sample_document())
        signature = session.prefix_signature()

        session.append_segment("正文", batch_range="ch0001-ch0001", chapter_count=1)
        session.assert_prefix_of(signature)

        # 篡改历史消息应被检测出来
        session.messages[0]["content"] = "被改写的人设"
        with self.assertRaises(PrefixMismatchError):
            session.assert_prefix_of(signature)

    def test_history_messages_are_not_rewritten_by_append(self) -> None:
        session = build_session()
        session.start(sample_document())
        before = json.dumps(session.messages, ensure_ascii=False)

        session.append_segment("正文", batch_range="ch0001-ch0001", chapter_count=1)

        after_prefix = json.dumps(session.messages[:4], ensure_ascii=False)
        self.assertEqual(before, after_prefix)

    def test_rotate_keeps_system_blocks_byte_identical(self) -> None:
        """旋转后 ``[0][1]`` 必须字节相同，否则公共前缀缓存失效。"""
        session = build_session(max_context_tokens=100)
        session.start(sample_document())
        system_before = session.system_prefix()

        session.append_segment("正文", batch_range="ch0001-ch0001", chapter_count=1)
        session.mark_segment_complete(end_chapter_index=5)
        session.rotate(sample_document(), total_chapters=100, last_completed_chapter_index=5)

        self.assertEqual(session.system_prefix(), system_before)
        self.assertEqual(session.generation_id, 2)
        self.assertEqual(session.rotate_count, 1)

    def test_rotate_drops_history_but_keeps_watermark(self) -> None:
        session = build_session(max_context_tokens=100)
        session.start(sample_document())
        session.append_segment("很长的正文" * 10, batch_range="ch0001-ch0005", chapter_count=5)
        session.mark_segment_complete(end_chapter_index=5)

        session.rotate(sample_document(), total_chapters=100, last_completed_chapter_index=5)

        self.assertEqual(len(session.messages), 4)
        self.assertIn("第 2 轮上下文", session.messages[2]["content"])
        self.assertNotIn("很长的正文", json.dumps(session.messages, ensure_ascii=False))

    def test_first_generation_has_no_watermark(self) -> None:
        session = build_session()
        session.start(sample_document())

        self.assertNotIn("轮上下文", session.messages[2]["content"])

class ToolMessageTests(unittest.TestCase):
    def test_tool_call_round_trips_through_message(self) -> None:
        call = ToolCall(id="c1", name="upsert_entry", arguments='{"key":"Alice"}')

        restored = ToolCall.from_message_part(call.to_message_part())

        self.assertEqual(restored.id, "c1")
        self.assertEqual(restored.name, "upsert_entry")
        self.assertEqual(restored.arguments, '{"key":"Alice"}')

    def test_assistant_turn_with_tool_calls_marks_message(self) -> None:
        session = build_session()
        session.start(sample_document())

        session.append_assistant(AssistantTurn(tool_calls=[ToolCall(id="c1", name="read_entry", arguments="{}")]))

        message = session.messages[-1]
        self.assertEqual(message["role"], "assistant")
        self.assertEqual(message["tool_calls"][0]["function"]["name"], "read_entry")

    def test_plain_assistant_turn_has_no_tool_calls_key(self) -> None:
        session = build_session()
        session.start(sample_document())

        session.append_assistant(AssistantTurn(content="完成"))

        self.assertNotIn("tool_calls", session.messages[-1])

    def test_tool_result_is_json_encoded(self) -> None:
        session = build_session()
        session.start(sample_document())

        session.append_tool_result(tool_call_id="c1", payload={"ok": False, "error": {"code": "bad_path"}})

        message = session.messages[-1]
        self.assertEqual(message["role"], "tool")
        self.assertEqual(message["tool_call_id"], "c1")
        self.assertEqual(json.loads(message["content"])["error"]["code"], "bad_path")

class RotationTests(unittest.TestCase):
    def test_no_rotation_when_budget_disabled(self) -> None:
        session = build_session(max_context_tokens=0)
        session.start(sample_document())

        self.assertFalse(session.should_rotate(prompt_tokens=10_000_000))

    def test_rotation_triggers_above_budget(self) -> None:
        session = build_session(max_context_tokens=1000)
        session.start(sample_document())

        self.assertFalse(session.should_rotate(prompt_tokens=999))
        self.assertTrue(session.should_rotate(prompt_tokens=1001))

    def test_estimation_tracks_growth(self) -> None:
        session = build_session()
        session.start(sample_document())
        first = session.next_prompt_tokens(estimated_chars=0)

        session.append_segment("x" * 4000, batch_range="ch0001-ch0001", chapter_count=1)
        second = session.next_prompt_tokens(estimated_chars=0)

        self.assertGreater(second, first)

class UsageTests(unittest.TestCase):
    def test_records_cache_hit_and_miss(self) -> None:
        session = build_session()
        session.start(sample_document())

        session.record_usage(
            {"prompt_tokens": 1000, "completion_tokens": 50, "prompt_cache_hit_tokens": 900, "prompt_cache_miss_tokens": 100}
        )

        self.assertEqual(session.usage.cache_hit_tokens, 900)
        self.assertEqual(session.usage.cache_miss_tokens, 100)
        self.assertAlmostEqual(session.usage.hit_ratio, 0.9)
        self.assertEqual(session.last_prompt_tokens, 1000)

    def test_tolerates_missing_usage_fields(self) -> None:
        session = build_session()
        session.start(sample_document())

        session.record_usage(None)
        session.record_usage({})

        self.assertEqual(session.usage.request_count, 2)
        self.assertEqual(session.usage.hit_ratio, 0.0)

    def test_progress_dict_shape(self) -> None:
        usage = UsageStats()
        usage.add(prompt_tokens=100, cache_hit_tokens=80, cache_miss_tokens=20)

        payload = usage.to_progress_dict()

        self.assertEqual(payload["prompt_tokens"], 100)
        self.assertEqual(payload["hit_ratio"], 0.8)
        self.assertEqual(payload["request_count"], 1)

class PersistenceTests(unittest.TestCase):
    def test_persist_then_restore_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = build_session(temp_dir)
            session.start(sample_document())
            session.append_segment("正文", batch_range="ch0001-ch0001", chapter_count=1)
            session.mark_segment_complete(end_chapter_index=1)
            session.persist()

            restored = build_session(temp_dir)
            ok = restored.restore()

            self.assertTrue(ok)
            self.assertEqual(len(restored.messages), len(session.messages))
            self.assertEqual(restored.segments_completed, 1)
            self.assertEqual(restored.last_segment_end_chapter, 1)
            self.assertEqual(restored.prefix_signature(), session.prefix_signature())

    def test_restore_returns_false_without_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertFalse(build_session(temp_dir).restore())

    def test_restore_tolerates_truncated_tail_line(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = build_session(temp_dir)
            session.start(sample_document())
            session.persist()
            path = Path(temp_dir) / "messages.jsonl"
            path.write_text(path.read_text(encoding="utf-8") + '{"kind":"message","role":"us', encoding="utf-8")

            restored = build_session(temp_dir)

            self.assertTrue(restored.restore())
            self.assertEqual(len(restored.messages), 4)

    def test_persist_is_skipped_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = build_session(temp_dir, persist_messages=False)
            session.start(sample_document())

            session.persist()

            self.assertFalse((Path(temp_dir) / "messages.jsonl").exists())

class IndexBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = SchemaLoader().load(SCHEMA_PATH)

    def test_lists_sorted_keys(self) -> None:
        snapshot = build_index_text({"actors": {"Zed": {"name": "z"}, "Alice": {"name": "a"}}}, self.schema)

        self.assertLess(snapshot.text.index("Alice"), snapshot.text.index("Zed"))

    def test_reports_total_and_truncation(self) -> None:
        actors = {f"Char{index:03d}": {"name": f"n{index}"} for index in range(50)}

        snapshot = build_index_text({"actors": actors}, self.schema, max_entries=10)

        self.assertTrue(snapshot.truncated)
        self.assertEqual(snapshot.entry_count, 10)
        self.assertEqual(snapshot.total_entry_count, 50)

    def test_handles_empty_document(self) -> None:
        snapshot = build_index_text({"actors": {}}, self.schema)

        self.assertTrue(snapshot.empty)
        self.assertIn("暂无条目", snapshot.text)

    def test_is_deterministic_for_same_input(self) -> None:
        document = sample_document()

        first = build_index_text(document, self.schema).text
        second = build_index_text(document, self.schema).text

        self.assertEqual(first, second)

    def test_respects_token_budget(self) -> None:
        actors = {f"Char{index:03d}": {"name": "x" * 100} for index in range(200)}

        snapshot = build_index_text({"actors": actors}, self.schema, token_budget=100, max_entries=200)

        self.assertLess(len(snapshot.text), 100 * 4 + 200)

    def test_watermark_mentions_generation_and_progress(self) -> None:
        text = build_watermark_text(generation_id=3, last_completed_chapter_index=200, total_chapters=937)

        self.assertIn("第 3 轮", text)
        self.assertIn("200", text)
        self.assertIn("937", text)

class TokenEstimateTests(unittest.TestCase):
    def test_counts_content_and_tool_arguments(self) -> None:
        messages = [
            {"role": "assistant", "content": "", "tool_calls": [{"function": {"name": "read_entry", "arguments": "x" * 400}}]},
        ]

        self.assertGreaterEqual(sums_tokens(messages), 100)

    def test_empty_list_is_zero(self) -> None:
        self.assertEqual(sums_tokens([]), 0)

if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_runner import AgentRunner, build_persona_text, build_repair_message, parse_tool_arguments, coerce_positive_int  # noqa: E402
from models import (  # noqa: E402
    AppConfig,
    AssistantTurn,
    Chapter,
    ChapterBatch,
    SessionConfig,
    ToolCall,
    ToolCompletion,
)
from schema_loader import SchemaLoader  # noqa: E402
from task_runner import TaskRunner  # noqa: E402
from workspace_manager import WorkspaceManager  # noqa: E402
from yaml_store import YamlStore  # noqa: E402

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "characters.yaml"

class TextDrivenClient:
    """每收到一段原文就生成一条 upsert_entry，随后收尾。

    不依赖预设脚本，可以驱动任意数量的段落——适合整体调度循环的测试。
    """

    def __init__(self) -> None:
        self.requests: list[list[dict[str, Any]]] = []
        self._counter = 0

    async def stream_yaml(self, prompt: str):
        if False:
            yield prompt

    async def complete_with_tools(self, messages, tools):
        self.requests.append(json.loads(json.dumps(messages, ensure_ascii=False)))
        usage = {
            "prompt_tokens": 1000,
            "completion_tokens": 10,
            "prompt_cache_hit_tokens": 900,
            "prompt_cache_miss_tokens": 100,
        }

        # 本段已回复过则收尾（真实模型完成后会停止调用工具）
        last_segment = -1
        for position, message in enumerate(messages):
            if message.get("role") == "user" and "段原文" in str(message.get("content", "")):
                last_segment = position
        if sum(1 for m in messages[last_segment + 1 :] if m.get("role") == "assistant") >= 1:
            return ToolCompletion(turn=AssistantTurn(content="本段完成"), usage=usage, finish_reason="stop")

        self._counter += 1
        key = f"Char{self._counter:04d}"
        return ToolCompletion(
            turn=AssistantTurn(tool_calls=[
                ToolCall(
                    id=f"call_{self._counter}",
                    name="upsert_entry",
                    arguments=json.dumps(
                        {
                            "key": key,
                            "node": {
                                "name": {"ja": f"キャラ{self._counter}", "zh-CN": f"角色{self._counter}", "romaji": key},
                                "trigger_keywords": [key],
                            },
                        },
                        ensure_ascii=False,
                    ),
                )
            ]),
            usage=usage,
            finish_reason="tool_calls",
        )

def make_minimal_epub(path: Path, *, chapters: list[str]) -> None:
    """写一个只包含容器/OPF/若干 XHTML 的最小可用 EPUB。"""
    import zipfile

    entries: list[tuple[str, str]] = [
        (
            "META-INF/container.xml",
            '<?xml version="1.0"?>'
            '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
            '<rootfiles><rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>'
            "</rootfiles></container>",
        ),
        (
            "OEBPS/content.opf",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">'
            '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>t</dc:title>'
            '<dc:identifier id="id">u</dc:identifier><dc:language>ja</dc:language></metadata>'
            "<manifest>"
            + "".join(
                f'<item id="c{index}" href="c{index}.xhtml" media-type="application/xhtml+xml"/>'
                for index in range(1, len(chapters) + 1)
            )
            + "</manifest><spine>"
            + "".join(f'<itemref idref="c{index}"/>' for index in range(1, len(chapters) + 1))
            + "</spine></package>",
        ),
    ]
    for index, text in enumerate(chapters, start=1):
        entries.append(
            (
                f"OEBPS/c{index}.xhtml",
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<html xmlns="http://www.w3.org/1999/xhtml"><head>'
                f"<title>第{index}章</title></head><body><p>{text}</p></body></html>",
            )
        )
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in entries:
            archive.writestr(name, content)

class ScriptedModelClient:
    """按预设脚本返回工具调用的假客户端，并记录每次请求的消息快照。"""

    def __init__(self, script: list[AssistantTurn]) -> None:
        self.script = list(script)
        self.requests: list[list[dict[str, Any]]] = []
        self.tools_seen: list[int] = []

    async def stream_yaml(self, prompt: str):
        if False:
            yield prompt

    async def complete_with_tools(self, messages, tools):
        # 深拷贝快照，模拟真实请求序列化边界
        self.requests.append(json.loads(json.dumps(messages, ensure_ascii=False)))
        self.tools_seen.append(len(tools))
        if not self.script:
            return ToolCompletion(turn=AssistantTurn(content="noop"), finish_reason="stop")
        turn = self.script.pop(0)
        usage = {"prompt_tokens": 1000, "completion_tokens": 10,
                 "prompt_cache_hit_tokens": 800, "prompt_cache_miss_tokens": 200}
        return ToolCompletion(turn=turn, usage=usage, finish_reason="tool_calls" if turn.has_tool_calls else "stop")

def tool_turn(call_id: str, name: str, arguments: dict[str, Any]) -> AssistantTurn:
    return AssistantTurn(
        tool_calls=[ToolCall(id=call_id, name=name, arguments=json.dumps(arguments, ensure_ascii=False))]
    )

def build_batch(start: int = 1, end: int = 2) -> ChapterBatch:
    chapters = [
        Chapter(index, f"ch{index:04d}", f"第{index}章", f"正文内容{index}", f"{index}.xhtml", 10)
        for index in range(start, end + 1)
    ]
    return ChapterBatch.from_chapters(chapters)

def build_runner(temp_dir: str, model_client: Any, **config_overrides: Any) -> tuple[AgentRunner, Any]:
    root = Path(temp_dir)
    workspace_root = root / "workspace"
    epub_path = root / "book.epub"
    epub_path.write_bytes(b"dummy")
    workspace = WorkspaceManager(workspace_root).ensure_workspace(epub_path)
    schema = SchemaLoader().load(SCHEMA_PATH)
    workspace.output_path_for_schema("characters").write_text("actors: {}\n", encoding="utf-8")

    from models import TaskDefinition

    task = TaskDefinition(
        epub_path=epub_path,
        workspace=workspace,
        schema_path=SCHEMA_PATH,
        prompt_template_paths=[],
        output_path=workspace.output_path_for_schema("characters"),
        progress_path=workspace.progress_path_for_schema("characters"),
        stream_buffer_path=workspace.stream_buffer_path_for_schema("characters"),
    )
    config = AppConfig(
        input_epubs=[], schema_paths=[], prompt_templates=[], workspace_root=workspace_root,
        **config_overrides,
    )
    runner = AgentRunner(
        config=config, task=task, schema_definition=schema, model_client=model_client,
    )
    return runner, task

class ParseArgumentsTests(unittest.TestCase):
    def test_empty_string_is_empty_object(self) -> None:
        self.assertEqual(parse_tool_arguments(""), {})

    def test_parses_object(self) -> None:
        self.assertEqual(parse_tool_arguments('{"key":"A"}'), {"key": "A"})

    def test_rejects_invalid_json(self) -> None:
        self.assertIsNone(parse_tool_arguments("{not json"))

    def test_rejects_non_object_json(self) -> None:
        self.assertIsNone(parse_tool_arguments('["a"]'))
        self.assertIsNone(parse_tool_arguments("42"))

class CoerceIntTests(unittest.TestCase):
    def test_returns_default_on_garbage(self) -> None:
        self.assertEqual(coerce_positive_int("abc", default=7), 7)
        self.assertEqual(coerce_positive_int(None, default=7), 7)

    def test_returns_default_on_non_positive(self) -> None:
        self.assertEqual(coerce_positive_int(0, default=7), 7)
        self.assertEqual(coerce_positive_int(-3, default=7), 7)

    def test_accepts_numeric_string(self) -> None:
        self.assertEqual(coerce_positive_int("5", default=7), 5)

class PersonaTests(unittest.TestCase):
    def test_persona_mentions_tools_and_root_key(self) -> None:
        schema = SchemaLoader().load(SCHEMA_PATH)

        text = build_persona_text(schema)

        self.assertIn("actors", text)
        self.assertIn("upsert_entry", text)
        self.assertIn("禁止中文 key", text)

    def test_persona_is_deterministic(self) -> None:
        schema = SchemaLoader().load(SCHEMA_PATH)

        first = build_persona_text(schema)
        second = build_persona_text(schema)

        self.assertEqual(first, second)

class RepairMessageTests(unittest.TestCase):
    def test_repair_message_includes_reason(self) -> None:
        from agent_runner import SegmentResult

        text = build_repair_message(SegmentResult(ok=False, reason="字段不存在"))

        self.assertIn("字段不存在", text)

class SegmentLoopTests(unittest.TestCase):
    def test_successful_segment_applies_edits(self) -> None:
        script = [
            tool_turn("c1", "upsert_entry", {
                "key": "Alice",
                "node": {
                    "name": {"ja": "アリス", "zh-CN": "爱丽丝", "romaji": "Alice"},
                    "trigger_keywords": ["爱丽丝"],
                },
            }),
            AssistantTurn(content="本段完成"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, task = build_runner(temp_dir, client)
            runner.ensure_session(total_chapters=2, last_completed_chapter_index=0)

            result = asyncio.run(
                runner.run_segment(build_batch(), source_text="第一章正文", total_chapters=2)
            )

            self.assertTrue(result.ok, result.reason)
            self.assertEqual(result.edit_count, 1)
            stored = YamlStore().load_yaml(task.output_path)
            self.assertIn("Alice", stored["actors"])

    def test_read_only_segment_counts_as_no_edit(self) -> None:
        """只读不写视为空转，避免模型用 read_entry 假装完成。"""
        script = [
            tool_turn("c1", "read_entry", {"key": "Alice"}),
            AssistantTurn(content="本段完成"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = build_runner(temp_dir, client)
            runner.ensure_session(total_chapters=2, last_completed_chapter_index=0)

            result = asyncio.run(
                runner.run_segment(build_batch(), source_text="正文", total_chapters=2)
            )

            self.assertFalse(result.ok)
            self.assertIn("未产生任何编辑", result.reason)

    def test_tool_round_limit_fails_segment(self) -> None:
        # 永远不停调用工具
        script = [tool_turn(f"c{i}", "read_entry", {"key": "Alice"}) for i in range(20)]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = build_runner(
                temp_dir, client, session=SessionConfig(max_tool_rounds_per_segment=3)
            )
            runner.ensure_session(total_chapters=2, last_completed_chapter_index=0)

            result = asyncio.run(
                runner.run_segment(build_batch(), source_text="正文", total_chapters=2)
            )

            self.assertFalse(result.ok)
            self.assertIn("上限", result.reason)
            self.assertEqual(result.tool_rounds, 3)

    def test_invalid_tool_arguments_reported_to_model(self) -> None:
        script = [
            AssistantTurn(tool_calls=[ToolCall(id="c1", name="upsert_entry", arguments="{broken")]),
            AssistantTurn(tool_calls=[
                ToolCall(id="c2", name="upsert_entry", arguments=json.dumps({
                    "key": "Bob",
                    "node": {"name": {"ja": "b", "zh-CN": "鲍勃", "romaji": "Bob"}, "trigger_keywords": ["鲍勃"]},
                }, ensure_ascii=False))
            ]),
            AssistantTurn(content="完成"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, task = build_runner(temp_dir, client)
            runner.ensure_session(total_chapters=2, last_completed_chapter_index=0)

            result = asyncio.run(
                runner.run_segment(build_batch(), source_text="正文", total_chapters=2)
            )

            self.assertTrue(result.ok, result.reason)
            self.assertGreaterEqual(result.error_count, 1)
            # 错误必须以 tool_result 形式回给模型
            tool_messages = [m for m in runner.session.messages if m["role"] == "tool"]
            self.assertTrue(any("bad_arguments" in m["content"] for m in tool_messages))

    def test_unknown_tool_reported(self) -> None:
        script = [
            tool_turn("c1", "no_such_tool", {}),
            AssistantTurn(content="完成"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = build_runner(temp_dir, client)
            runner.ensure_session(total_chapters=2, last_completed_chapter_index=0)

            result = asyncio.run(
                runner.run_segment(build_batch(), source_text="正文", total_chapters=2)
            )

            tool_messages = [m for m in runner.session.messages if m["role"] == "tool"]
            self.assertTrue(any("unknown_tool" in m["content"] for m in tool_messages))
            self.assertFalse(result.ok)

    def test_schema_violation_is_recoverable_by_model(self) -> None:
        script = [
            tool_turn("c1", "upsert_entry", {"key": "Alice", "node": {"bogus": "x"}}),
            tool_turn("c2", "upsert_entry", {
                "key": "Alice",
                "node": {"name": {"ja": "a", "zh-CN": "爱丽丝", "romaji": "Alice"}, "trigger_keywords": ["x"]},
            }),
            AssistantTurn(content="完成"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, task = build_runner(temp_dir, client)
            runner.ensure_session(total_chapters=2, last_completed_chapter_index=0)

            result = asyncio.run(
                runner.run_segment(build_batch(), source_text="正文", total_chapters=2)
            )

            self.assertTrue(result.ok)
            self.assertIn("Alice", YamlStore().load_yaml(task.output_path)["actors"])

class CachePrefixIntegrationTests(unittest.TestCase):
    def test_request_prefixes_are_strictly_increasing(self) -> None:
        """真实请求序列必须是严格前缀关系，否则缓存无法命中。"""
        script = [
            tool_turn("c1", "upsert_entry", {"key": "Alice", "node": {
                "name": {"ja": "a", "zh-CN": "爱丽丝", "romaji": "Alice"}, "trigger_keywords": ["x"]}}),
            AssistantTurn(content="第一段完成"),
            tool_turn("c2", "upsert_entry", {"key": "Bob", "node": {
                "name": {"ja": "b", "zh-CN": "鲍勃", "romaji": "Bob"}, "trigger_keywords": ["y"]}}),
            AssistantTurn(content="第二段完成"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = build_runner(temp_dir, client)
            runner.ensure_session(total_chapters=4, last_completed_chapter_index=0)

            asyncio.run(runner.run_segment(build_batch(1, 2), source_text="第一段", total_chapters=4))
            asyncio.run(runner.run_segment(build_batch(3, 4), source_text="第二段", total_chapters=4))

            self.assertGreaterEqual(len(client.requests), 3)

        # 逐次比较：每次请求的消息序列必须是下一次的严格前缀
        for previous, current in zip(client.requests, client.requests[1:]):
            self.assertEqual(current[: len(previous)], previous, "消息前缀被破坏，缓存将无法命中")

    def test_schema_and_persona_sent_once_per_generation(self) -> None:
        script = [
            tool_turn("c1", "upsert_entry", {"key": "Alice", "node": {
                "name": {"ja": "a", "zh-CN": "爱丽丝", "romaji": "Alice"}, "trigger_keywords": ["x"]}}),
            AssistantTurn(content="完成"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = build_runner(temp_dir, client)
            runner.ensure_session(total_chapters=2, last_completed_chapter_index=0)

            asyncio.run(runner.run_segment(build_batch(), source_text="正文", total_chapters=2))

            for request in client.requests:
                system_messages = [m for m in request if m["role"] == "system"]
                self.assertEqual(len(system_messages), 2)

    def test_rotation_preserves_system_prefix(self) -> None:
        script = [
            tool_turn("c1", "upsert_entry", {"key": "Alice", "node": {
                "name": {"ja": "a", "zh-CN": "爱丽丝", "romaji": "Alice"}, "trigger_keywords": ["x"]}}),
            AssistantTurn(content="完成"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = build_runner(
                temp_dir, client, session=SessionConfig(max_context_tokens=1)
            )
            runner.ensure_session(total_chapters=10, last_completed_chapter_index=0)
            system_before = runner.session.system_prefix()
            first_request = client.requests[0] if client.requests else None

            asyncio.run(runner.run_segment(build_batch(1, 1), source_text="正文", total_chapters=10))
            rotated = runner.maybe_rotate(total_chapters=10)

            self.assertTrue(rotated)
            self.assertEqual(runner.session.generation_id, 2)
            self.assertEqual(runner.session.system_prefix(), system_before)
            if first_request is not None:
                after = client.requests[-1] if client.requests else None
                if after is not None:
                    self.assertEqual(after[:2], first_request[:2])

    def test_usage_is_accumulated_across_requests(self) -> None:
        script = [
            tool_turn("c1", "upsert_entry", {"key": "Alice", "node": {
                "name": {"ja": "a", "zh-CN": "爱丽丝", "romaji": "Alice"}, "trigger_keywords": ["x"]}}),
            AssistantTurn(content="完成"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = build_runner(temp_dir, client)
            runner.ensure_session(total_chapters=2, last_completed_chapter_index=0)

            asyncio.run(runner.run_segment(build_batch(), source_text="正文", total_chapters=2))

            report = runner.cache_report()

            self.assertEqual(report["cache_hit_tokens"], 1600)
            self.assertEqual(report["cache_miss_tokens"], 400)
            self.assertAlmostEqual(report["hit_ratio"], 0.8)

class SegmentRetryTests(unittest.TestCase):
    def test_repair_appends_without_resending_source(self) -> None:
        script = [
            AssistantTurn(tool_calls=[ToolCall(id="c1", name="read_entry", arguments=json.dumps({"key": "A"}))]),
            AssistantTurn(content="done"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = build_runner(temp_dir, client)
            runner.ensure_session(total_chapters=2, last_completed_chapter_index=0)

            first = asyncio.run(runner.run_segment(build_batch(1, 1), source_text="原文内容", total_chapters=2))
            self.assertFalse(first.ok)

            before = len(runner.session.messages)
            asyncio.run(runner.repair_segment(
                build_batch(1, 1), source_text="原文内容", total_chapters=2, previous=first
            ))

            # 修复只追加消息，且不重复注入原文
            self.assertGreater(len(runner.session.messages), before)
            source_injections = [
                m for m in runner.session.messages
                if m["role"] == "user" and "原文内容" in str(m.get("content", ""))
            ]
            self.assertEqual(len(source_injections), 1)

class RotationBoundaryTests(unittest.TestCase):
    def _runner(self, temp_dir: str, max_context_tokens: int, **extra):
        script = [
            tool_turn("c1", "upsert_entry", {"key": "Alice", "node": {
                "name": {"ja": "a", "zh-CN": "爱丽丝", "romaji": "Alice"}, "trigger_keywords": ["x"]}}),
            AssistantTurn(content="完成"),
        ]
        client = ScriptedModelClient(script)
        runner, task = build_runner(
            temp_dir, client,
            session=SessionConfig(max_context_tokens=max_context_tokens, **extra),
        )
        runner.ensure_session(total_chapters=10, last_completed_chapter_index=0)
        return runner, client

    def test_defers_rotation_until_checkpoint_boundary(self) -> None:
        """预算超限但未到 checkpoint 边界、且未超硬上限时应推迟旋转。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = self._runner(temp_dir, max_context_tokens=1000)
            asyncio.run(runner.run_segment(build_batch(1, 1), source_text="正文", total_chapters=10))
            # 超过预算 1000，但未超硬上限 1500
            runner.session.last_prompt_tokens = 1200

            rotated = runner.maybe_rotate(total_chapters=10, at_checkpoint_boundary=False)

            self.assertFalse(rotated)
            self.assertEqual(runner.session.generation_id, 1)

    def test_rotates_at_checkpoint_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = self._runner(temp_dir, max_context_tokens=1000)
            asyncio.run(runner.run_segment(build_batch(1, 1), source_text="正文", total_chapters=10))
            runner.session.last_prompt_tokens = 1200

            rotated = runner.maybe_rotate(total_chapters=10, at_checkpoint_boundary=True)

            self.assertTrue(rotated)
            self.assertEqual(runner.session.generation_id, 2)

    def test_rotates_immediately_when_over_hard_limit(self) -> None:
        """远超硬上限时不再等待 checkpoint 边界。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = self._runner(temp_dir, max_context_tokens=100)
            asyncio.run(runner.run_segment(build_batch(1, 1), source_text="正文", total_chapters=10))
            # 硬上限为 150
            runner.session.last_prompt_tokens = 2000

            rotated = runner.maybe_rotate(total_chapters=10, at_checkpoint_boundary=False)

            self.assertTrue(rotated)

    def test_no_rotation_when_under_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner, _ = self._runner(temp_dir, max_context_tokens=1_000_000)
            asyncio.run(runner.run_segment(build_batch(1, 1), source_text="正文", total_chapters=10))

            self.assertFalse(runner.maybe_rotate(total_chapters=10, at_checkpoint_boundary=True))

class TaskRunnerAgentPipelineTests(unittest.TestCase):
    def test_agent_mode_uses_agent_runner_and_records_progress(self) -> None:
        script = [
            tool_turn("c1", "upsert_entry", {"key": "Alice", "node": {
                "name": {"ja": "a", "zh-CN": "爱丽丝", "romaji": "Alice"}, "trigger_keywords": ["x"]}}),
            AssistantTurn(content="完成"),
        ]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workspace_root = root / "workspace"
            epub_path = root / "book.epub"
            epub_path.write_bytes(b"dummy")
            workspace = WorkspaceManager(workspace_root).ensure_workspace(epub_path)
            workspace.output_path_for_schema("characters").write_text("actors: {}\n", encoding="utf-8")

            from models import TaskDefinition

            task = TaskDefinition(
                epub_path=epub_path,
                workspace=workspace,
                schema_path=SCHEMA_PATH,
                prompt_template_paths=[],
                output_path=workspace.output_path_for_schema("characters"),
                progress_path=workspace.progress_path_for_schema("characters"),
                stream_buffer_path=workspace.stream_buffer_path_for_schema("characters"),
            )
            config = AppConfig(
                input_epubs=[], schema_paths=[], prompt_templates=[], workspace_root=workspace_root,
                pipeline_mode="agent",
            )
            runner = TaskRunner(config, model_client=client)
            batch = build_batch(1, 2)
            schema = SchemaLoader().load(SCHEMA_PATH)
            progress = {
                "total_chapters": 2,
                "last_completed_chapter_index": 0,
                "completed_chapters": 0,
                "status": "pending",
                "retry": {"last_error": ""},
                "merge": {"replaced_nodes": 0, "appended_nodes": 0},
            }

            result = asyncio.run(runner._step_agent_batch(
                task, batch=batch, total_chapters=2, schema_definition=schema,
                progress=progress,
            ))

            updated = result["progress"]
            self.assertEqual(updated["last_completed_chapter_index"], 2)
            self.assertEqual(updated["status"], "completed")
            self.assertIn("session", updated)
            self.assertEqual(updated["session"]["generation_id"], 1)
            self.assertIn("cache", updated)
            self.assertEqual(updated["cache"]["cache_hit_tokens"], 1600)
            self.assertIn("Alice", YamlStore().load_yaml(task.output_path)["actors"])

    def test_split_batches_are_requeued_in_chapter_order(self) -> None:
        """会话模式必须按章节顺序推进：拆分后子批次插到队首，而不是队尾。"""
        script = [AssistantTurn(content="空转")]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workspace_root = root / "workspace"
            epub_path = root / "book.epub"
            epub_path.write_bytes(b"dummy")
            workspace = WorkspaceManager(workspace_root).ensure_workspace(epub_path)
            workspace.output_path_for_schema("characters").write_text("actors: {}\n", encoding="utf-8")

            from models import TaskDefinition
            from task_runner import TaskExecutionState

            task = TaskDefinition(
                epub_path=epub_path,
                workspace=workspace,
                schema_path=SCHEMA_PATH,
                prompt_template_paths=[],
                output_path=workspace.output_path_for_schema("characters"),
                progress_path=workspace.progress_path_for_schema("characters"),
                stream_buffer_path=workspace.stream_buffer_path_for_schema("characters"),
            )
            config = AppConfig(
                input_epubs=[], schema_paths=[], prompt_templates=[], workspace_root=workspace_root,
                pipeline_mode="agent", retry_count=1, retry_backoff_seconds=0,
            )
            runner = TaskRunner(config, model_client=client)
            schema = SchemaLoader().load(SCHEMA_PATH)
            batch = build_batch(1, 2)
            later_batch = build_batch(3, 4)
            state = TaskExecutionState(
                task=task,
                chapters=[],
                schema_definition=schema,
                prompt_templates=[],
                progress={
                    "total_chapters": 4,
                    "last_completed_chapter_index": 0,
                    "completed_chapters": 0,
                    "status": "pending",
                    "retry": {"last_error": ""},
                    "merge": {"replaced_nodes": 0, "appended_nodes": 0},
                },
                pending_batches=__import__("collections").deque([batch, later_batch]),
            )

            asyncio.run(runner.step_task(state))

            remaining = list(state.pending_batches)
            self.assertEqual(len(remaining), 3)
            # 左半段、右半段必须排在原本靠后的批次之前
            self.assertEqual(remaining[0].start_chapter_index, 1)
            self.assertEqual(remaining[1].start_chapter_index, 2)
            self.assertEqual(remaining[2].start_chapter_index, 3)

    def test_agent_mode_failure_requests_split(self) -> None:
        script = [AssistantTurn(content="我什么都没做")]
        client = ScriptedModelClient(script)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workspace_root = root / "workspace"
            epub_path = root / "book.epub"
            epub_path.write_bytes(b"dummy")
            workspace = WorkspaceManager(workspace_root).ensure_workspace(epub_path)
            workspace.output_path_for_schema("characters").write_text("actors: {}\n", encoding="utf-8")

            from models import TaskDefinition

            task = TaskDefinition(
                epub_path=epub_path,
                workspace=workspace,
                schema_path=SCHEMA_PATH,
                prompt_template_paths=[],
                output_path=workspace.output_path_for_schema("characters"),
                progress_path=workspace.progress_path_for_schema("characters"),
                stream_buffer_path=workspace.stream_buffer_path_for_schema("characters"),
            )
            config = AppConfig(
                input_epubs=[], schema_paths=[], prompt_templates=[], workspace_root=workspace_root,
                pipeline_mode="agent", retry_count=1, retry_backoff_seconds=0,
            )
            runner = TaskRunner(config, model_client=client)
            schema = SchemaLoader().load(SCHEMA_PATH)
            progress = {
                "total_chapters": 4,
                "last_completed_chapter_index": 0,
                "completed_chapters": 0,
                "status": "pending",
                "retry": {"last_error": ""},
                "merge": {"replaced_nodes": 0, "appended_nodes": 0},
            }

            result = asyncio.run(runner._step_agent_batch(
                task, batch=build_batch(1, 2), total_chapters=4, schema_definition=schema,
                progress=progress,
            ))

            self.assertTrue(result["needs_split"])
            self.assertEqual(result["progress"]["status"], "failed")

    def test_legacy_mode_does_not_touch_agent_runners(self) -> None:
        config = AppConfig(
            input_epubs=[], schema_paths=[], prompt_templates=[],
            workspace_root=Path("workspace"), pipeline_mode="legacy",
        )
        runner = TaskRunner(config)

        self.assertFalse(config.use_agent_pipeline)
        self.assertEqual(runner._agent_runners, {})

class RunCommandAgentModeTests(unittest.TestCase):
    """验证完整 run_command 调度循环在 agent 模式下能跑通（不只测单个批次）。"""

    def test_run_command_drives_all_batches_to_completion(self) -> None:
        import yaml as yaml_module

        from app import run_command_async

        client = TextDrivenClient()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            epub_path = root / "book.epub"
            make_minimal_epub(epub_path, chapters=["第一段正文", "第二段正文", "第三段正文"])
            config_path = root / "config.yaml"
            config_path.write_text(
                yaml_module.safe_dump(
                    {
                        "input_epubs": [str(epub_path)],
                        "schema_paths": [str(SCHEMA_PATH)],
                        "prompt_templates": [],
                        "workspace_root": str(root / "workspace"),
                        "pipeline": {"mode": "agent"},
                        "session": {"max_tool_rounds_per_segment": 6},
                        "batching": {
                            "enable_multi_chapter": False,
                            "enable_checkpoint": True,
                            "checkpoint_every_n_chapters": 2,
                        },
                        "runtime": {"retry_count": 1, "retry_backoff_seconds": 0},
                        "progress": {"emit_console_progress": False},
                    },
                    allow_unicode=True,
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            exit_code = asyncio.run(run_command_async(config_path, model_client=client))

            self.assertEqual(exit_code, 0)
            output_path = next((root / "workspace").rglob("characters.yaml"))
            stored = YamlStore().load_yaml(output_path)
            self.assertGreater(len(stored.get("actors", {})), 0)
            progress_path = next((root / "workspace").rglob("state/characters.progress.yaml"))
            progress = yaml_module.safe_load(progress_path.read_text(encoding="utf-8"))
            self.assertEqual(progress["status"], "completed")
            self.assertEqual(progress["last_completed_chapter_index"], 3)
            self.assertEqual(progress["agent"]["mode"], "agent")
            self.assertIn("cache", progress)

if __name__ == "__main__":
    unittest.main()

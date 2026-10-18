from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from email.message import Message
from pathlib import Path
from typing import Any, AsyncIterator, cast
from unittest.mock import patch
from urllib import error

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from app import restore_command, run_command  # noqa: E402
from checkpoint_store import CheckpointStore  # noqa: E402
from llm_client import (  # noqa: E402
    ApiStreamError,
    EchoStreamModelClient,
    OpenAICompatibleStreamModelClient,
    accumulate_tool_call_deltas,
    build_chat_completions_payload,
    build_model_client,
    build_request_headers,
    build_tool_completion_payload,
    detect_root_key,
    extract_stream_delta_text,
    extract_stream_usage,
    finalize_tool_calls,
    first_choice_of,
    normalize_base_url,
    read_http_error,
)
from models import AppConfig, BatchingConfig, Chapter, ChapterBatch, PromptTemplate, TaskDefinition  # noqa: E402
from progress_store import ProgressStore  # noqa: E402
from prompt_builder import PromptBuilder  # noqa: E402
from schema_loader import SchemaLoader  # noqa: E402
from schema_validator import SchemaValidator  # noqa: E402
from task_runner import TaskExecutionState, TaskRunner  # noqa: E402
from workspace_manager import WorkspaceManager, slugify_epub_name  # noqa: E402
from yaml_store import YamlStore  # noqa: E402


class WorkspaceManagerTests(unittest.TestCase):
    def test_slugify_epub_name_normalizes_symbols(self) -> None:
        self.assertEqual(slugify_epub_name(" My Novel 01! "), "my-novel-01")

    def test_slugify_epub_name_keeps_unicode_file_name_instead_of_falling_back_to_epub(self) -> None:
        self.assertEqual(slugify_epub_name("给了失去一切卖身的少女一切的结果"), "给了失去一切卖身的少女一切的结果")

    def test_ensure_workspace_creates_expected_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workspace_root = root / "workspace"
            input_epub = root / "示例 小说.epub"
            input_epub.write_bytes(b"dummy")

            manager = WorkspaceManager(workspace_root)
            workspace = manager.ensure_workspace(input_epub)

            self.assertEqual(workspace.epub_name, "示例-小说")
            self.assertEqual(workspace.root, workspace_root / "示例-小说")
            self.assertTrue(workspace.root.exists())
            self.assertTrue(workspace.source_dir.exists())
            self.assertTrue(workspace.output_dir.exists())
            self.assertTrue(workspace.state_dir.exists())
            self.assertTrue(workspace.temp_dir.exists())
            self.assertTrue(workspace.logs_dir.exists())
            self.assertTrue((workspace.source_dir / input_epub.name).exists())
            self.assertTrue(workspace.debug_dir().exists())

    def test_workspace_builds_debug_artifact_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workspace_root = root / "workspace"
            input_epub = root / "示例 小说.epub"
            input_epub.write_bytes(b"dummy")

            manager = WorkspaceManager(workspace_root)
            workspace = manager.ensure_workspace(input_epub)

            prompt_path = workspace.prompt_debug_path_for_attempt(
                schema_name="characters",
                start_chapter_index=5,
                end_chapter_index=9,
                split_depth=1,
                retry_attempt=2,
                template_name="retry_format",
            )
            response_path = workspace.response_debug_path_for_attempt(
                schema_name="characters",
                start_chapter_index=5,
                end_chapter_index=9,
                split_depth=1,
                retry_attempt=2,
                template_name="retry_format",
            )

            self.assertEqual(prompt_path.parent, workspace.debug_dir())
            self.assertEqual(response_path.parent, workspace.debug_dir())
            self.assertEqual(prompt_path.name, "characters.ch0005-ch0009.d1.r02.retry_format.prompt.txt")
            self.assertEqual(response_path.name, "characters.ch0005-ch0009.d1.r02.retry_format.response.yaml")


class SchemaLoaderTests(unittest.TestCase):
    def test_load_extracts_root_match_key_and_fields(self) -> None:
        loader = SchemaLoader()
        schema = loader.load(Path("schemas/characters.yaml"))

        self.assertEqual(schema.schema_name, "characters")
        self.assertEqual(schema.root_key, "actors")
        self.assertEqual(schema.match_key, "name")
        self.assertIn("actors", schema.allowed_top_level_keys)
        field_paths = {field.path for field in schema.fields}
        self.assertIn("actors[].name", field_paths)
        self.assertIn("actors[].trigger_keywords", field_paths)
        self.assertIsInstance(schema.raw_schema["actors"], dict)


class SchemaValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.loader = SchemaLoader()
        self.validator = SchemaValidator()

    def test_parse_yaml_text_rejects_invalid_yaml(self) -> None:
        data, result = self.validator.parse_yaml_text("actors: [")

        self.assertIsNone(data)
        self.assertFalse(result.ok)
        self.assertTrue(result.errors)

    def test_parse_yaml_text_strips_plain_markdown_code_block(self) -> None:
        yaml_text = "```\nactors:\n  - name: 张三\n```"
        data, result = self.validator.parse_yaml_text(yaml_text)

        self.assertTrue(result.ok)
        self.assertIsNotNone(data)
        self.assertIn("actors", data or {})

    def test_parse_yaml_text_strips_yaml_markdown_code_block(self) -> None:
        yaml_text = "```yaml\nactors:\n  - name: 张三\n```"
        data, result = self.validator.parse_yaml_text(yaml_text)

        self.assertTrue(result.ok)
        self.assertIsNotNone(data)
        self.assertIn("actors", data or {})

    def test_parse_yaml_text_strips_yaml_markdown_code_block_uppercase(self) -> None:
        yaml_text = "```YAML\nactors:\n  - name: 张三\n```"
        data, result = self.validator.parse_yaml_text(yaml_text)

        self.assertTrue(result.ok)
        self.assertIsNotNone(data)
        self.assertIn("actors", data or {})

    def test_parse_yaml_text_handles_missing_closing_backticks(self) -> None:
        yaml_text = "```yaml\nactors:\n  - name: 张三"
        data, result = self.validator.parse_yaml_text(yaml_text)

        self.assertTrue(result.ok)
        self.assertIsNotNone(data)
        self.assertIn("actors", data or {})

    def test_parse_yaml_text_handles_plain_yaml_without_code_block(self) -> None:
        yaml_text = "actors:\n  - name: 张三"
        data, result = self.validator.parse_yaml_text(yaml_text)

        self.assertTrue(result.ok)
        self.assertIsNotNone(data)
        self.assertIn("actors", data or {})

    def test_sanitize_increment_data_removes_empty_values_and_trims_strings(self) -> None:
        payload = {
            "worldinfo": {
                "  黑风寨  ": {
                    "trigger_keywords": [" 黑风寨 ", "   ", None],
                    "content": "   ",
                    "metadata": {},
                    "enabled": False,
                    "priority": 0,
                    "notes": {
                        "summary": "  山寨势力  ",
                        "empty_text": "\n\t",
                        "empty_list": [],
                    },
                },
                "   ": {
                    "content": None,
                },
            },
            "other": {},
        }

        sanitized = self.validator.sanitize_increment_data(payload)

        self.assertEqual(
            sanitized,
            {
                "worldinfo": {
                    "黑风寨": {
                        "trigger_keywords": ["黑风寨"],
                        "enabled": False,
                        "priority": 0,
                        "notes": {"summary": "山寨势力"},
                    }
                }
            },
        )

    def test_validate_increment_accepts_empty_mapping(self) -> None:
        schema = self.loader.load(Path("schemas/world.yaml"))

        result = self.validator.validate_increment({"worldinfo": {}}, schema)

        self.assertTrue(result.ok)

    def test_validate_increment_accepts_partial_update_after_sanitize(self) -> None:
        schema = self.loader.load(Path("schemas/world.yaml"))
        payload = {
            "worldinfo": {
                "黑风寨": {
                    "content": "位于山谷中的势力。",
                }
            }
        }

        sanitized = self.validator.sanitize_increment_data(payload)
        result = self.validator.validate_increment(sanitized, schema)

        self.assertTrue(result.ok)

    def test_validate_increment_rejects_missing_match_key(self) -> None:
        schema = self.loader.load(Path("schemas/world.yaml"))
        payload = {
            "worldinfo": {
                "   ": {
                    "content": "位于山谷中的势力。",
                }
            }
        }

        result = self.validator.validate_increment(payload, schema)

        self.assertFalse(result.ok)
        self.assertTrue(any(error.path == "worldinfo.   " for error in result.errors))

    def test_validate_increment_rejects_unexpected_top_level_key(self) -> None:
        schema = self.loader.load(Path("schemas/world.yaml"))
        payload = {"actors": {}}

        result = self.validator.validate_increment(payload, schema)

        self.assertFalse(result.ok)
        self.assertTrue(any(error.path == "actors" for error in result.errors))

    def test_validate_increment_rejects_unexpected_field(self) -> None:
        schema = self.loader.load(Path("schemas/world.yaml"))
        payload = {
            "worldinfo": {
                "黑风寨": {
                    "unknown": "x",
                }
            }
        }

        result = self.validator.validate_increment(payload, schema)

        self.assertFalse(result.ok)
        self.assertTrue(any(error.path.endswith(".unknown") for error in result.errors))


class PromptBuilderTests(unittest.TestCase):
    def test_build_renders_placeholders(self) -> None:
        template = PromptTemplate(
            name="base",
            path=Path("prompts/base.md"),
            index=0,
            content="章节={{chapter_title}} range={{batch_range}} count={{chapter_count}} root={{root_key}} error={{error_summary}}\n{{source_text}}\n{{output_rules}}",
        )
        batch = ChapterBatch.from_chapters(
            [
                Chapter(
                    chapter_index=1,
                    chapter_id="ch0001-demo",
                    title="第一章",
                    text="正文一",
                    source_path="chapter1.xhtml",
                    token_estimate=1,
                ),
                Chapter(
                    chapter_index=2,
                    chapter_id="ch0002-demo",
                    title="第二章",
                    text="正文二",
                    source_path="chapter2.xhtml",
                    token_estimate=1,
                ),
            ]
        )
        schema = SchemaLoader().load(Path("schemas/world.yaml"))

        prompt = PromptBuilder().build(
            template,
            batch=batch,
            schema_definition=schema,
            existing_yaml="{}",
            existing_worldinfo="{}",
            error_summary="schema validation failed",
        )

        self.assertIn("章节=第一章 ~ 第二章", prompt)
        self.assertIn("range=ch0001-ch0002", prompt)
        self.assertIn("count=2", prompt)
        self.assertIn("root=worldinfo", prompt)
        self.assertIn("error=schema validation failed", prompt)
        self.assertIn("### Chapter 1", prompt)
        self.assertIn("chapter_id: ch0001-demo", prompt)
        self.assertIn("chapter_title: 第二章", prompt)
        self.assertIn("顶层只能包含 `worldinfo`", prompt)
        self.assertIn("其值必须是对象映射，而不是列表", prompt)


class ProgressStoreTests(unittest.TestCase):
    def test_mark_batch_completed_updates_batch_progress_fields(self) -> None:
        store = ProgressStore()
        progress = store.initialize(
            Path(tempfile.gettempdir()) / "epub2dict-progress-store-test.yaml",
            epub_path="input/book.epub",
            workspace_root="workspace/book",
            schema_path="schemas/world.yaml",
            output_path="workspace/book/output/world.yaml",
            stream_buffer_path="workspace/book/temp/world.stream.txt",
            total_chapters=10,
            max_attempts=3,
        )
        batch = ChapterBatch.from_chapters(
            [
                Chapter(3, "ch0003", "第三章", "A", "a.xhtml", 10),
                Chapter(4, "ch0004", "第四章", "B", "b.xhtml", 10),
            ],
            split_depth=1,
            parent_batch_id="ch0001-ch0004-d0",
        )

        updated = store.mark_batch_completed(progress, batch=batch, total_chapters=10)

        self.assertEqual(updated["last_completed_chapter_index"], 4)
        self.assertEqual(updated["last_completed_chapter_id"], "ch0004")
        self.assertEqual(updated["current_batch_range"], "ch0003-ch0004")
        self.assertEqual(updated["current_batch_depth"], 1)
        self.assertEqual(updated["current_batch_parent_id"], "ch0001-ch0004-d0")
        self.assertEqual(updated["batch_status"], "completed")
        self.assertEqual(updated["current_chapter_index"], 5)

    def test_initialize_includes_checkpoint_metadata(self) -> None:
        store = ProgressStore()
        progress = store.initialize(
            Path(tempfile.gettempdir()) / "epub2dict-progress-store-checkpoint.yaml",
            epub_path="input/book.epub",
            workspace_root="workspace/book",
            schema_path="schemas/world.yaml",
            output_path="workspace/book/output/world.yaml",
            stream_buffer_path="workspace/book/temp/world.stream.txt",
            total_chapters=10,
            max_attempts=3,
            checkpoint_enabled=True,
            checkpoint_every_n_chapters=3,
        )

        self.assertEqual(
            progress["checkpoint"],
            {
                "enabled": True,
                "every_n_chapters": 3,
                "last_saved_id": "",
                "last_saved_chapter_index": 0,
                "last_restored_id": "",
                "last_restored_at": "",
            },
        )


class YamlStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = YamlStore()
        self.schema = SchemaLoader().load(Path("schemas/world.yaml"))

    def test_merge_increment_merges_and_appends_nodes(self) -> None:
        current = {
            "worldinfo": {
                "黑风寨": {
                    "trigger_keywords": ["旧关键字"],
                    "content": "旧内容",
                    "type": "势力",
                }
            }
        }
        increment = {
            "worldinfo": {
                "黑风寨": {
                    "trigger_keywords": ["黑风寨"],
                    "content": "新内容",
                },
                "青石镇": {
                    "trigger_keywords": ["青石镇"],
                    "content": "新地点",
                },
            }
        }

        merged, stats = self.store.merge_increment(current, increment, self.schema)

        self.assertEqual(stats.replaced_nodes, 1)
        self.assertEqual(stats.appended_nodes, 1)
        self.assertEqual(len(merged["worldinfo"]), 2)
        self.assertEqual(merged["worldinfo"]["黑风寨"]["content"], "新内容")
        self.assertEqual(merged["worldinfo"]["黑风寨"]["type"], "势力")
        self.assertEqual(merged["worldinfo"]["青石镇"]["content"], "新地点")

    def test_merge_increment_recursively_merges_nested_dicts(self) -> None:
        current = {
            "worldinfo": {
                "黑风寨": {
                    "details": {
                        "summary": "旧摘要",
                        "extra": {"region": "北境", "climate": "寒冷"},
                    },
                }
            }
        }
        increment = {
            "worldinfo": {
                "黑风寨": {
                    "details": {
                        "summary": "新摘要",
                        "extra": {"region": "西境"},
                    },
                }
            }
        }

        merged, stats = self.store.merge_increment(current, increment, self.schema)

        self.assertEqual(stats.replaced_nodes, 1)
        self.assertEqual(
            merged["worldinfo"]["黑风寨"]["details"],
            {
                "summary": "新摘要",
                "extra": {"region": "西境", "climate": "寒冷"},
            },
        )

    def test_merge_increment_appends_unmatched_list_items(self) -> None:
        """数组语义为「相似替换 + 追加」，见 plans/yaml-array-merge-design.md。

        旧断言（整替）与合并规则设计冲突：相似度超阈值时原位替换，否则追加。
        本用例用相似度 0.0 的值，验证追加分支。
        """
        current = {
            "worldinfo": {
                "黑风寨": {
                    "trigger_keywords": ["旧关键字", "别名"],
                }
            }
        }
        increment = {
            "worldinfo": {
                "黑风寨": {
                    "trigger_keywords": ["封印之地"],
                }
            }
        }

        merged, stats = self.store.merge_increment(current, increment, self.schema)

        self.assertEqual(stats.replaced_nodes, 1)
        merged_keywords = merged["worldinfo"]["黑风寨"]["trigger_keywords"]
        # 无相似元素可匹配，原数组全部保留，新元素追加到末尾
        self.assertEqual(merged_keywords, ["旧关键字", "别名", "封印之地"])

    def test_merge_increment_replaces_similar_list_items_in_place(self) -> None:
        """相似度超过阈值（默认 0.6）的数组元素在原文位置被替换，不产生新元素。

        "旧关键字" 与 "新关键字" 相似度为 0.75。
        """
        current = {
            "worldinfo": {
                "黑风寨": {
                    "trigger_keywords": ["旧关键字", "别名"],
                }
            }
        }
        increment = {
            "worldinfo": {
                "黑风寨": {
                    "trigger_keywords": ["新关键字"],
                }
            }
        }

        merged, _ = self.store.merge_increment(current, increment, self.schema)

        self.assertEqual(merged["worldinfo"]["黑风寨"]["trigger_keywords"], ["新关键字", "别名"])

    def test_merge_increment_requires_mapping_value(self) -> None:
        with self.assertRaises(ValueError):
            self.store.merge_increment(
                {"worldinfo": {}},
                {"worldinfo": {"黑风寨": "错误值"}},
                self.schema,
            )


class TaskRunnerTests(unittest.TestCase):
    def test_build_initial_batches_groups_chapters_without_splitting(self) -> None:
        config = AppConfig(
            input_epubs=[],
            schema_paths=[],
            prompt_templates=[],
            workspace_root=Path("workspace"),
            batching=BatchingConfig(
                enable_multi_chapter=True,
                max_input_tokens=100,
                prompt_overhead_tokens=10,
                reserve_output_tokens=20,
            ),
        )
        runner = TaskRunner(config)
        chapters = [
            Chapter(1, "ch0001", "第一章", "A", "a.xhtml", 30),
            Chapter(2, "ch0002", "第二章", "B", "b.xhtml", 35),
            Chapter(3, "ch0003", "第三章", "C", "c.xhtml", 20),
            Chapter(4, "ch0004", "第四章", "D", "d.xhtml", 50),
        ]
        task = TaskDefinition(
            epub_path=Path("input/book.epub"),
            workspace=WorkspaceManager(Path("workspace")).ensure_workspace(Path("input/book.epub")),
            schema_path=Path("schemas/world.yaml"),
            prompt_template_paths=[],
            output_path=Path("workspace/book/output/world.yaml"),
            progress_path=Path("workspace/book/state/world.progress.yaml"),
            stream_buffer_path=Path("workspace/book/temp/world.stream.txt"),
        )

        batches = runner._build_initial_batches(task, chapters)

        self.assertEqual([batch.display_range for batch in batches], ["ch0001-ch0002", "ch0003-ch0004"])
        self.assertEqual([batch.chapter_count for batch in batches], [2, 2])

    def test_build_initial_batches_respects_checkpoint_windows(self) -> None:
        config = AppConfig(
            input_epubs=[],
            schema_paths=[],
            prompt_templates=[],
            workspace_root=Path("workspace"),
            batching=BatchingConfig(
                enable_multi_chapter=True,
                max_input_tokens=1000,
                prompt_overhead_tokens=10,
                reserve_output_tokens=20,
                enable_checkpoint=True,
                checkpoint_every_n_chapters=3,
            ),
        )
        runner = TaskRunner(config)
        chapters = [
            Chapter(1, "ch0001", "第一章", "A", "a.xhtml", 10),
            Chapter(2, "ch0002", "第二章", "B", "b.xhtml", 10),
            Chapter(3, "ch0003", "第三章", "C", "c.xhtml", 10),
            Chapter(4, "ch0004", "第四章", "D", "d.xhtml", 10),
            Chapter(5, "ch0005", "第五章", "E", "e.xhtml", 10),
            Chapter(6, "ch0006", "第六章", "F", "f.xhtml", 10),
            Chapter(7, "ch0007", "第七章", "G", "g.xhtml", 10),
        ]
        task = TaskDefinition(
            epub_path=Path("input/book.epub"),
            workspace=WorkspaceManager(Path("workspace")).ensure_workspace(Path("input/book.epub")),
            schema_path=Path("schemas/world.yaml"),
            prompt_template_paths=[],
            output_path=Path("workspace/book/output/world.yaml"),
            progress_path=Path("workspace/book/state/world.progress.yaml"),
            stream_buffer_path=Path("workspace/book/temp/world.stream.txt"),
        )

        batches = runner._build_initial_batches(task, chapters)

        self.assertEqual([batch.display_range for batch in batches], ["ch0001-ch0003", "ch0004-ch0006", "ch0007-ch0007"])

    def test_build_initial_batches_allows_oversize_single_chapter(self) -> None:
        config = AppConfig(
            input_epubs=[],
            schema_paths=[],
            prompt_templates=[],
            workspace_root=Path("workspace"),
            batching=BatchingConfig(
                enable_multi_chapter=True,
                max_input_tokens=100,
                prompt_overhead_tokens=10,
                reserve_output_tokens=20,
                allow_oversize_single_chapter=True,
            ),
        )
        runner = TaskRunner(config)
        task = TaskDefinition(
            epub_path=Path("input/book.epub"),
            workspace=WorkspaceManager(Path("workspace")).ensure_workspace(Path("input/book.epub")),
            schema_path=Path("schemas/world.yaml"),
            prompt_template_paths=[],
            output_path=Path("workspace/book/output/world.yaml"),
            progress_path=Path("workspace/book/state/world.progress.yaml"),
            stream_buffer_path=Path("workspace/book/temp/world.stream.txt"),
        )
        chapters = [
            Chapter(1, "ch0001", "第一章", "A", "a.xhtml", 90),
            Chapter(2, "ch0002", "第二章", "B", "b.xhtml", 10),
        ]

        batches = runner._build_initial_batches(task, chapters)

        self.assertEqual([batch.display_range for batch in batches], ["ch0001-ch0001", "ch0002-ch0002"])
        self.assertEqual([batch.chapter_count for batch in batches], [1, 1])

    def test_should_save_checkpoint_on_window_boundary_and_final_batch(self) -> None:
        config = AppConfig(
            input_epubs=[],
            schema_paths=[],
            prompt_templates=[],
            workspace_root=Path("workspace"),
            batching=BatchingConfig(enable_checkpoint=True, checkpoint_every_n_chapters=3),
        )
        runner = TaskRunner(config)

        progress_boundary = {"last_completed_chapter_index": 3, "checkpoint": {"last_saved_chapter_index": 0}}
        progress_final = {"last_completed_chapter_index": 7, "checkpoint": {"last_saved_chapter_index": 6}}

        self.assertTrue(runner._should_save_checkpoint(progress=progress_boundary, total_chapters=7))
        self.assertTrue(runner._should_save_checkpoint(progress=progress_final, total_chapters=7))

    def test_collect_stream_writes_stream_and_debug_response_files(self) -> None:
        class StubStreamModelClient:
            last_prompt: str

            async def stream_yaml(self, prompt: str) -> AsyncIterator[str]:
                self.last_prompt = prompt
                yield "worldinfo:\n"
                yield "  黑风寨:\n"
                yield "    content: 新内容\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workspace_root = root / "workspace"
            config = AppConfig(
                input_epubs=[],
                schema_paths=[],
                prompt_templates=[],
                workspace_root=workspace_root,
            )
            client = cast(Any, StubStreamModelClient())
            runner = TaskRunner(config, model_client=client)

            stream_buffer_path = workspace_root / "epub" / "temp" / "world.stream.txt"
            response_debug_path = workspace_root / "epub" / "logs" / "debug" / "world.ch0001-ch0001.d0.r01.base.response.yaml"

            yaml_text, chunk_count = asyncio.run(runner._collect_stream(stream_buffer_path, response_debug_path, "hello prompt"))

            self.assertEqual(client.last_prompt, "hello prompt")
            self.assertEqual(chunk_count, 3)
            self.assertEqual(yaml_text, "worldinfo:\n  黑风寨:\n    content: 新内容\n")
            self.assertEqual(stream_buffer_path.read_text(encoding="utf-8"), yaml_text)
            self.assertEqual(response_debug_path.read_text(encoding="utf-8"), yaml_text)

    def test_step_task_runs_single_batch_and_yields(self) -> None:
        config = AppConfig(
            input_epubs=[],
            schema_paths=[],
            prompt_templates=[],
            workspace_root=Path("workspace"),
        )
        runner = TaskRunner(config)
        workspace = WorkspaceManager(Path("workspace")).ensure_workspace(Path("input/book.epub"))
        task = TaskDefinition(
            epub_path=Path("input/book.epub"),
            workspace=workspace,
            schema_path=Path("schemas/world.yaml"),
            prompt_template_paths=[],
            output_path=workspace.output_path_for_schema("world"),
            progress_path=workspace.progress_path_for_schema("world"),
            stream_buffer_path=workspace.stream_buffer_path_for_schema("world"),
        )
        batch1 = ChapterBatch.from_chapters([Chapter(1, "ch0001", "第一章", "A", "a.xhtml", 10)])
        batch2 = ChapterBatch.from_chapters([Chapter(2, "ch0002", "第二章", "B", "b.xhtml", 10)])
        state = TaskExecutionState(
            task=task,
            chapters=[],
            schema_definition=SchemaLoader().load(Path("schemas/world.yaml")),
            prompt_templates=[],
            progress={"status": "running"},
            pending_batches=__import__("collections").deque([batch1, batch2]),
        )

        async def mock_run_batch_single(*args, **kwargs):
            return {"progress": {"status": "running"}, "needs_split": False}

        with patch.object(runner, "_run_batch_single", side_effect=mock_run_batch_single) as run_batch_single:
            result = asyncio.run(runner.step_task(state))

        self.assertFalse(result["is_completed"])
        self.assertFalse(result["is_failed"])
        self.assertEqual(len(state.pending_batches), 1)
        run_batch_single.assert_called_once()

    def test_run_command_rotates_epubs_in_parallel_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = root / "config.yaml"
            raw_config = {
                "input_epubs": ["input/book-a.epub", "input/book-b.epub"],
                "schema_paths": ["schemas/world.yaml", "schemas/characters.yaml"],
                "prompt_templates": [],
                "workspace_root": str(root / "workspace").replace("\\", "/"),
                "concurrency": {"enable_parallel_tasks": True, "max_workers": 2},
            }
            config_path.write_text(yaml.safe_dump(raw_config, allow_unicode=True, sort_keys=False), encoding="utf-8")

            dispatch_log: list[str] = []

            def fake_prepare(task: TaskDefinition):
                dispatch_log.append(f"prepare:{task.workspace.epub_name}:{task.schema_path.stem}")
                return type(
                    "StubState",
                    (),
                    {
                        "task": task,
                        "pending_batches": [],
                        "is_completed": False,
                        "is_failed": False,
                    },
                )()

            async def fake_step(state):
                return {"progress": {"status": "completed"}, "is_completed": True, "is_failed": False}

            with patch.object(TaskRunner, "prepare_task_state", side_effect=fake_prepare), patch.object(TaskRunner, "step_task", side_effect=fake_step):
                exit_code = run_command(config_path)

            self.assertEqual(exit_code, 0)
            self.assertGreaterEqual(len(dispatch_log), 4)
            self.assertEqual(dispatch_log[0], "prepare:book-a:world")
            self.assertEqual(dispatch_log[1], "prepare:book-b:world")


class CheckpointStoreTests(unittest.TestCase):
    def test_save_checkpoint_creates_snapshot_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workspace_root = root / "workspace"
            epub_path = root / "book.epub"
            epub_path.write_bytes(b"dummy")

            workspace = WorkspaceManager(workspace_root).ensure_workspace(epub_path)
            task = TaskDefinition(
                epub_path=epub_path,
                workspace=workspace,
                schema_path=Path("schemas/world.yaml"),
                prompt_template_paths=[],
                output_path=workspace.output_path_for_schema("world"),
                progress_path=workspace.progress_path_for_schema("world"),
                stream_buffer_path=workspace.stream_buffer_path_for_schema("world"),
            )
            task.output_path.write_text("worldinfo: {}\n", encoding="utf-8")
            progress = {
                "last_completed_chapter_index": 3,
                "checkpoint": {
                    "enabled": True,
                    "every_n_chapters": 3,
                    "last_saved_id": "ch0003",
                    "last_saved_chapter_index": 3,
                    "last_restored_id": "",
                    "last_restored_at": "",
                },
            }
            task.progress_path.write_text(yaml.safe_dump(progress, allow_unicode=True, sort_keys=False), encoding="utf-8")

            metadata = CheckpointStore().save_checkpoint(task, schema_name="world", progress=progress, total_chapters=7)

            checkpoint_id = metadata["checkpoint_id"]
            self.assertEqual(checkpoint_id, "ch0003")
            self.assertTrue(task.workspace.checkpoint_output_path_for_schema("world", checkpoint_id).exists())
            self.assertTrue(task.workspace.checkpoint_progress_path_for_schema("world", checkpoint_id).exists())
            self.assertTrue(task.workspace.checkpoint_meta_path_for_schema("world", checkpoint_id).exists())

    def test_restore_command_overwrites_runtime_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workspace_root = root / "workspace"
            config_path = root / "config.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "input_epubs": [],
                        "schema_paths": ["schemas/world.yaml"],
                        "prompt_templates": [],
                        "workspace_root": str(workspace_root).replace("\\", "/"),
                    },
                    allow_unicode=True,
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            epub_path = root / "book.epub"
            epub_path.write_bytes(b"dummy")

            workspace_manager = WorkspaceManager(workspace_root)
            workspace = workspace_manager.ensure_workspace(epub_path)
            output_path = workspace.output_path_for_schema("world")
            progress_path = workspace.progress_path_for_schema("world")
            stream_buffer_path = workspace.stream_buffer_path_for_schema("world")
            output_path.write_text("worldinfo:\n  wrong: {}\n", encoding="utf-8")
            progress_path.write_text("last_completed_chapter_index: 7\n", encoding="utf-8")
            stream_buffer_path.write_text("stale", encoding="utf-8")

            checkpoint_id = "ch0003"
            checkpoint_output_path = workspace.checkpoint_output_path_for_schema("world", checkpoint_id)
            checkpoint_progress_path = workspace.checkpoint_progress_path_for_schema("world", checkpoint_id)
            checkpoint_meta_path = workspace.checkpoint_meta_path_for_schema("world", checkpoint_id)
            checkpoint_output_path.parent.mkdir(parents=True, exist_ok=True)
            checkpoint_progress_path.parent.mkdir(parents=True, exist_ok=True)
            checkpoint_output_path.write_text("worldinfo:\n  restored: {}\n", encoding="utf-8")
            checkpoint_progress_path.write_text("last_completed_chapter_index: 3\n", encoding="utf-8")
            checkpoint_meta_path.write_text(
                yaml.safe_dump({"checkpoint_id": checkpoint_id, "chapter_index": 3}, allow_unicode=True, sort_keys=False),
                encoding="utf-8",
            )

            exit_code = restore_command(
                config_path,
                epub_path=epub_path,
                schema_path=Path("schemas/world.yaml"),
                checkpoint_id=checkpoint_id,
                latest=False,
            )

            self.assertEqual(exit_code, 0)
            self.assertEqual(output_path.read_text(encoding="utf-8"), "worldinfo:\n  restored: {}\n")
            restored_progress = yaml.safe_load(progress_path.read_text(encoding="utf-8"))
            self.assertEqual(restored_progress["last_completed_chapter_index"], 3)
            self.assertEqual(restored_progress["checkpoint"]["last_restored_id"], checkpoint_id)
            self.assertFalse(stream_buffer_path.exists())

class LlmClientTests(unittest.TestCase):
    def test_detect_root_key(self) -> None:
        prompt = "输出根节点必须是 actors"
        self.assertEqual(detect_root_key(prompt), "actors")

    def test_normalize_base_url(self) -> None:
        self.assertEqual(normalize_base_url("https://api.openai.com/v1/"), "https://api.openai.com/v1")

    def test_build_chat_completions_payload(self) -> None:
        payload = build_chat_completions_payload("gpt-4.1", "hello")
        self.assertEqual(payload["model"], "gpt-4.1")
        self.assertTrue(payload["stream"])
        self.assertEqual(payload["messages"][0]["content"], "hello")

    def test_extract_stream_delta_text_from_string(self) -> None:
        chunk = {"choices": [{"delta": {"content": "abc"}}]}
        self.assertEqual(extract_stream_delta_text(chunk), "abc")

    def test_extract_stream_delta_text_from_parts(self) -> None:
        chunk = {
            "choices": [
                {
                    "delta": {
                        "content": [
                            {"text": "a"},
                            {"text": "b"},
                        ]
                    }
                }
            ]
        }
        self.assertEqual(extract_stream_delta_text(chunk), "ab")

    def test_build_tool_completion_payload_includes_usage_and_tools(self) -> None:
        messages = [{"role": "system", "content": "persona"}, {"role": "user", "content": "chapter"}]
        tools = [{"type": "function", "function": {"name": "read_entry"}}]

        payload = build_tool_completion_payload("deepseek-flash", messages, tools)

        self.assertTrue(payload["stream"])
        # 没有 include_usage 就无法观测缓存命中
        self.assertEqual(payload["stream_options"], {"include_usage": True})
        self.assertEqual(payload["tools"], tools)
        self.assertEqual(payload["tool_choice"], "auto")
        self.assertIs(payload["messages"], messages)

    def test_build_tool_completion_payload_omits_tools_when_empty(self) -> None:
        payload = build_tool_completion_payload("m", [{"role": "user", "content": "x"}], [])

        self.assertNotIn("tools", payload)
        self.assertNotIn("tool_choice", payload)

    def test_accumulates_tool_call_arguments_across_chunks(self) -> None:
        accumulator: dict = {}
        accumulate_tool_call_deltas(accumulator, [{"index": 0, "id": "call_a", "function": {"name": "upsert_entry", "arguments": "{\"key\":"}}])
        accumulate_tool_call_deltas(accumulator, [{"index": 0, "function": {"arguments": "\"Alice\"}"}}])

        calls = finalize_tool_calls(accumulator)

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].id, "call_a")
        self.assertEqual(calls[0].name, "upsert_entry")
        self.assertEqual(json.loads(calls[0].arguments), {"key": "Alice"})

    def test_accumulates_split_function_name(self) -> None:
        """name 也可能被拆片下发，不能被后片覆盖。"""
        accumulator: dict = {}
        accumulate_tool_call_deltas(accumulator, [{"index": 0, "id": "c", "function": {"name": "upsert_"}}])
        accumulate_tool_call_deltas(accumulator, [{"index": 0, "function": {"name": "entry"}}])

        self.assertEqual(finalize_tool_calls(accumulator)[0].name, "upsert_entry")

    def test_finalize_keeps_parallel_tool_calls_in_index_order(self) -> None:
        accumulator: dict = {}
        accumulate_tool_call_deltas(accumulator, [{"index": 1, "id": "b", "function": {"name": "read_entry"}}])
        accumulate_tool_call_deltas(accumulator, [{"index": 0, "id": "a", "function": {"name": "list_entries"}}])

        calls = finalize_tool_calls(accumulator)

        self.assertEqual([call.name for call in calls], ["list_entries", "read_entry"])

    def test_finalize_skips_calls_without_name(self) -> None:
        accumulator: dict = {}
        accumulate_tool_call_deltas(accumulator, [{"index": 0, "id": "a", "function": {"arguments": "{}"}}])

        self.assertEqual(finalize_tool_calls(accumulator), [])

    def test_finalize_generates_missing_id(self) -> None:
        accumulator: dict = {}
        accumulate_tool_call_deltas(accumulator, [{"index": 3, "function": {"name": "read_entry"}}])

        self.assertEqual(finalize_tool_calls(accumulator)[0].id, "call_3")

    def test_extract_stream_usage_includes_cache_fields(self) -> None:
        usage = extract_stream_usage(
            {"usage": {"prompt_tokens": 1000, "prompt_cache_hit_tokens": 900, "prompt_cache_miss_tokens": 100}}
        )

        self.assertEqual(usage["prompt_cache_hit_tokens"], 900)
        self.assertEqual(usage["prompt_cache_miss_tokens"], 100)

    def test_extract_stream_usage_ignores_absent_or_malformed(self) -> None:
        self.assertEqual(extract_stream_usage({"choices": []}), {})
        self.assertEqual(extract_stream_usage({"usage": None}), {})
        self.assertEqual(extract_stream_usage({"usage": {"prompt_tokens": "x"}}), {})

    def test_first_choice_of_handles_shapes(self) -> None:
        self.assertIsNone(first_choice_of({}))
        self.assertIsNone(first_choice_of({"choices": []}))
        self.assertIsNone(first_choice_of({"choices": ["x"]}))
        self.assertEqual(first_choice_of({"choices": [{"finish_reason": "stop"}]}), {"finish_reason": "stop"})

    def test_echo_client_tool_completion_returns_empty_turn(self) -> None:
        client = EchoStreamModelClient()

        completion = asyncio.run(client.complete_with_tools([{"role": "user", "content": "x"}], []))

        self.assertFalse(completion.turn.has_tool_calls)
        self.assertEqual(completion.finish_reason, "stop")

    def test_build_model_client_without_api_key_still_uses_real_client(self) -> None:
        """无 api_key 不应被拦下：很多端点不需要鉴权。"""
        config = AppConfig(
            input_epubs=[],
            schema_paths=[],
            prompt_templates=[],
            workspace_root=Path("workspace"),
            api_key="",
        )
        client = build_model_client(config)
        self.assertEqual(client.__class__.__name__, "OpenAICompatibleStreamModelClient")

    def test_build_model_client_dry_run_returns_echo_client(self) -> None:
        config = AppConfig(
            input_epubs=[],
            schema_paths=[],
            prompt_templates=[],
            workspace_root=Path("workspace"),
            api_key="",
            dry_run=True,
        )
        client = build_model_client(config)
        self.assertEqual(client.__class__.__name__, "EchoStreamModelClient")

    def test_build_model_client_passes_extra_body_and_headers(self) -> None:
        config = AppConfig(
            input_epubs=[],
            schema_paths=[],
            prompt_templates=[],
            workspace_root=Path("workspace"),
            api_key="",
            base_url="http://127.0.0.1:11434/v1",
            model_name="qwen3",
            extra_body={"top_k": 20},
            extra_headers={"x-api-key": "abc", "Authorization": None},
        )
        client = build_model_client(config)
        self.assertIsInstance(client, OpenAICompatibleStreamModelClient)
        assert isinstance(client, OpenAICompatibleStreamModelClient)
        self.assertEqual(client.extra_body, {"top_k": 20})
        self.assertEqual(client.extra_headers, {"x-api-key": "abc", "Authorization": None})

    def test_build_model_client_with_api_key_returns_real_client(self) -> None:
        config = AppConfig(
            input_epubs=[],
            schema_paths=[],
            prompt_templates=[],
            workspace_root=Path("workspace"),
            api_key="secret",
            base_url="https://api.openai.com/v1",
            model_name="gpt-4.1",
        )
        client = build_model_client(config)
        self.assertEqual(client.__class__.__name__, "OpenAICompatibleStreamModelClient")

    def test_real_client_without_api_key_does_not_raise_on_credentials(self) -> None:
        """无 key 的客户端应能构建，只在 model name 也缺失时才报错。"""
        client = OpenAICompatibleStreamModelClient(
            base_url="http://127.0.0.1:11434/v1",
            api_key="",
            model_name="qwen3",
        )
        client._require_request_params()

    def test_real_client_requires_model_name(self) -> None:
        client = OpenAICompatibleStreamModelClient(
            base_url="https://api.openai.com/v1",
            api_key="secret",
            model_name="",
        )
        with self.assertRaises(ApiStreamError):
            asyncio.run(self._collect_stream(client, "hello"))

    def test_model_name_can_come_from_extra_body(self) -> None:
        client = OpenAICompatibleStreamModelClient(
            base_url="http://127.0.0.1:11434/v1",
            model_name="",
            extra_body={"model": "deepseek-chat"},
        )
        client._require_request_params()
        payload = build_chat_completions_payload(
            client.model_name, "x", extra_body=client.extra_body
        )
        self.assertEqual(payload["model"], "deepseek-chat")

    def test_extra_body_deep_merges_and_overrides(self) -> None:
        payload = build_tool_completion_payload(
            "m",
            [{"role": "user", "content": "x"}],
            [{"type": "function", "function": {"name": "t"}}],
            extra_body={
                "stream_options": {"include_usage": True, "extra": "kept"},
                "top_k": 20,
                "tools": [],
            },
        )

        self.assertEqual(payload["stream_options"], {"include_usage": True, "extra": "kept"})
        self.assertEqual(payload["top_k"], 20)
        self.assertEqual(payload["tools"], [])
        self.assertEqual(payload["tool_choice"], "auto")

    def test_extra_body_none_value_removes_default_key(self) -> None:
        payload = build_chat_completions_payload(
            "m", "x", extra_body={"reasoning_effort": None, "thinking": {"type": "enabled"}}
        )

        self.assertNotIn("reasoning_effort", payload)
        self.assertEqual(payload["thinking"], {"type": "enabled"})

    def test_extra_body_empty_is_noop(self) -> None:
        baseline = build_chat_completions_payload("m", "x")
        self.assertEqual(
            build_chat_completions_payload("m", "x", extra_body={}), baseline
        )
        self.assertEqual(
            build_chat_completions_payload("m", "x", extra_body=None), baseline
        )

    def test_build_request_headers_with_api_key(self) -> None:
        headers = build_request_headers("secret")

        self.assertEqual(headers["Authorization"], "Bearer secret")
        self.assertEqual(headers["Content-Type"], "application/json")

    def test_build_request_headers_without_api_key_omits_authorization(self) -> None:
        headers = build_request_headers("")

        self.assertNotIn("Authorization", headers)
        self.assertEqual(headers["Accept"], "text/event-stream")

    def test_extra_headers_override_is_case_insensitive(self) -> None:
        headers = build_request_headers(
            "secret", {"authorization": "Token xyz", "CONTENT-TYPE": "application/json; charset=utf-8"}
        )

        auth_keys = [key for key in headers if key.lower() == "authorization"]
        self.assertEqual(len(auth_keys), 1)
        self.assertEqual(headers[auth_keys[0]], "Token xyz")
        content_keys = [key for key in headers if key.lower() == "content-type"]
        self.assertEqual(len(content_keys), 1)
        self.assertEqual(headers[content_keys[0]], "application/json; charset=utf-8")

    def test_extra_headers_none_removes_default_header(self) -> None:
        headers = build_request_headers("secret", {"User-Agent": None, "Authorization": None})

        self.assertNotIn("User-Agent", headers)
        self.assertNotIn("Authorization", headers)

    def test_extra_headers_values_are_stringified(self) -> None:
        headers = build_request_headers(
            "", {"X-Num": 5, "X-Bool": True, "X-Obj": {"a": 1}}
        )

        self.assertEqual(headers["X-Num"], "5")
        self.assertEqual(headers["X-Bool"], "true")
        self.assertEqual(headers["X-Obj"], '{"a":1}')

    def test_extra_headers_blank_name_ignored(self) -> None:
        headers = build_request_headers("", {"  ": "boom"})

        self.assertNotIn("boom", headers.values())

    async def _collect_stream(self, client: Any, prompt: str) -> list[str]:
        result = []
        async for chunk in client.stream_yaml(prompt):
            result.append(chunk)
        return result

    def test_read_http_error(self) -> None:
        exc = error.HTTPError(
            url="https://example.com",
            code=401,
            msg="Unauthorized",
            hdrs=Message(),
            fp=None,
        )
        message = read_http_error(exc)
        self.assertIn("Unauthorized", message)


if __name__ == "__main__":
    unittest.main()

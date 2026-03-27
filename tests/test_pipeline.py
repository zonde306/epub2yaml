from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from urllib import error

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from llm_client import (  # noqa: E402
    ApiStreamError,
    OpenAICompatibleStreamModelClient,
    build_model_client,
    build_chat_completions_payload,
    detect_root_key,
    extract_stream_delta_text,
    normalize_base_url,
    read_http_error,
)
from models import AppConfig, BatchingConfig, Chapter, ChapterBatch, PromptTemplate, TaskDefinition  # noqa: E402
from progress_store import ProgressStore  # noqa: E402
from prompt_builder import PromptBuilder  # noqa: E402
from schema_loader import SchemaLoader  # noqa: E402
from schema_validator import SchemaValidator  # noqa: E402
from task_runner import TaskRunner  # noqa: E402
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

    def test_merge_increment_replaces_lists_as_a_whole(self) -> None:
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

        merged, stats = self.store.merge_increment(current, increment, self.schema)

        self.assertEqual(stats.replaced_nodes, 1)
        self.assertEqual(merged["worldinfo"]["黑风寨"]["trigger_keywords"], ["新关键字"])

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

    def test_collect_stream_writes_stream_and_debug_response_files(self) -> None:
        class StubStreamModelClient:
            def stream_yaml(self, prompt: str):
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
            client = StubStreamModelClient()
            runner = TaskRunner(config, model_client=client)

            stream_buffer_path = workspace_root / "epub" / "temp" / "world.stream.txt"
            response_debug_path = workspace_root / "epub" / "logs" / "debug" / "world.ch0001-ch0001.d0.r01.base.response.yaml"

            yaml_text, chunk_count = runner._collect_stream(stream_buffer_path, response_debug_path, "hello prompt")

            self.assertEqual(client.last_prompt, "hello prompt")
            self.assertEqual(chunk_count, 3)
            self.assertEqual(yaml_text, "worldinfo:\n  黑风寨:\n    content: 新内容\n")
            self.assertEqual(stream_buffer_path.read_text(encoding="utf-8"), yaml_text)
            self.assertEqual(response_debug_path.read_text(encoding="utf-8"), yaml_text)


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

    def test_build_model_client_without_api_key_returns_echo_client(self) -> None:
        config = AppConfig(
            input_epubs=[],
            schema_paths=[],
            prompt_templates=[],
            workspace_root=Path("workspace"),
            api_key="",
        )
        client = build_model_client(config)
        self.assertEqual(client.__class__.__name__, "EchoStreamModelClient")

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

    def test_real_client_requires_api_key(self) -> None:
        client = OpenAICompatibleStreamModelClient(
            base_url="https://api.openai.com/v1",
            api_key="",
            model_name="gpt-4.1",
        )
        with self.assertRaises(ApiStreamError):
            list(client.stream_yaml("hello"))

    def test_read_http_error(self) -> None:
        exc = error.HTTPError(
            url="https://example.com",
            code=401,
            msg="Unauthorized",
            hdrs=None,
            fp=None,
        )
        message = read_http_error(exc)
        self.assertIn("Unauthorized", message)


if __name__ == "__main__":
    unittest.main()

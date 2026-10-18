"""离线端到端验证：用真实 epub 章节文本驱动完整 agent 管线。

由于当前环境没有可用的 API 凭据，这里用一个确定性脚本客户端代替模型：
它对收到的每段原文生成一次 upsert_entry 调用。
这样可以真实验证除「模型输出质量」之外的全部机制：

- 段落循环与工具执行
- 消息前缀的 append-only 严格性
- 上下文旋转（含跨 generation 的 system 块字节一致性）
- 会话落盘与崩溃恢复
- 进度推进与缓存用量记账
- 多批次连续推进（含 checkpoint 边界）
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from app import load_config  # noqa: E402
from epub_reader import extract_epub  # noqa: E402
from models import (  # noqa: E402
    AppConfig,
    AssistantTurn,
    ChapterBatch,
    SessionConfig,
    TaskDefinition,
    ToolCall,
    ToolCompletion,
)
from prompt_builder import build_batch_source_text  # noqa: E402
from schema_loader import SchemaLoader  # noqa: E402
from task_runner import TaskRunner  # noqa: E402
from workspace_manager import WorkspaceManager  # noqa: E402
from yaml_store import YamlStore  # noqa: E402

class DeterministicAnalysisClient:
    """按章节序号生成确定性的 upsert_entry 调用。"""

    def __init__(self) -> None:
        self.requests: list[list[dict]] = []
        self.completions = 0

    async def stream_yaml(self, prompt: str):
        if False:
            yield prompt

    async def complete_with_tools(self, messages, tools):
        self.requests.append(json.loads(json.dumps(messages, ensure_ascii=False)))
        self.completions += 1

        # 找到本轮段落原文，从中抽取章节号
        source = ""
        last_segment_position = -1
        for position, message in enumerate(messages):
            if message.get("role") == "user" and "段原文" in str(message.get("content", "")):
                source = str(message["content"])
                last_segment_position = position

        # 真实模型在一段完成后会停下来（不再发起工具调用）。
        # 若本段已经有 assistant 回复过，则本次收尾。
        assistant_after = sum(
            1 for message in messages[last_segment_position + 1 :] if message.get("role") == "assistant"
        )
        if assistant_after >= 1:
            return ToolCompletion(
                turn=AssistantTurn(content="本段完成"),
                usage=make_usage(len(source)),
                finish_reason="stop",
            )

        indices = extract_chapter_indices(source)

        if not indices:
            return ToolCompletion(
                turn=AssistantTurn(content="本段无可记录内容"),
                usage=make_usage(len(source)),
                finish_reason="stop",
            )

        calls = []
        for position, index in enumerate(indices):
            calls.append(
                ToolCall(
                    id=f"call_{index}_{position}",
                    name="upsert_entry",
                    arguments=json.dumps(
                        {
                            "key": f"Char{index:04d}",
                            "node": {
                                "name": {
                                    "ja": f"キャラ{index}",
                                    "zh-CN": f"角色{index}",
                                    "romaji": f"Char{index}",
                                },
                                "trigger_keywords": [f"角色{index}", f"Char{index}"],
                                "basic_settings": {
                                    "gender": "Female",
                                    "race": "Human",
                                    "age": 18,
                                    "identity": ["学生"],
                                },
                            },
                        },
                        ensure_ascii=False,
                    ),
                )
            )
        return ToolCompletion(
            turn=AssistantTurn(tool_calls=calls),
            usage=make_usage(len(source)),
            finish_reason="tool_calls",
        )

def make_usage(source_chars: int) -> dict:
    """模拟真实 usage：前缀越长命中越多。"""
    prompt_tokens = max(source_chars // 4, 100)
    miss = min(200, prompt_tokens // 5)
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": 50,
        "prompt_cache_hit_tokens": prompt_tokens - miss,
        "prompt_cache_miss_tokens": miss,
    }

def extract_chapter_indices(text: str) -> list[int]:
    """从段落原文里抓 `### Chapter N` 标记。"""
    import re

    return [int(match) for match in re.findall(r"### Chapter (\d+)", text)]

def check_prefix_monotonic(requests: list[list[dict]]) -> tuple[bool, str]:
    """验证请求序列的消息前缀严格递增。

    旋转（generation 切换）会有意重置时间线，此时前缀合法地变短。
    判定方法是：每次请求的 system 块必须与首条请求的 system 块一致，
    且同一 generation 内部消息序列必须严格前缀递增。
    """
    if not requests:
        return False, "没有请求记录"

    baseline_system = requests[0][:2]
    for position, request in enumerate(requests):
        if request[:2] != baseline_system:
            return False, f"第 {position} 次请求的 system 块与首条不一致：旋转后缓存前缀失效"

    violations = 0
    rotation_points = 0
    for previous, current in zip(requests, requests[1:]):
        if current[: len(previous)] == previous:
            continue
        # 旋转：system 块保持一致，且消息序列回到 generation 头部长度。
        # 头部在追加段原文前是 4 条（2 system + index user + ready assistant）。
        is_rotation = (
            current[:2] == baseline_system
            and len(current) <= 5
            and len(previous) > len(current)
        )
        if is_rotation:
            rotation_points += 1
            continue
        violations += 1
    if violations:
        return False, f"{violations} 处前缀被破坏（非旋转原因）"
    return True, f"{len(requests)} 次请求前缀合法（旋转 {rotation_points} 次，system 块始终一致）"

def guard_real_workspace(workspace: object) -> None:
    """阻止本脚本写入仓库内的真实工作区。

    本脚本用硬编码假数据验证机制，一旦写进真实任务就会污染 output.yaml
    与 session/journal，让后续真实运行继承假条目。
    """
    resolved = Path(str(getattr(workspace, "root"))).resolve()
    real_root = (Path.cwd() / "workspace").resolve()
    if resolved == real_root or real_root in resolved.parents:
        raise RuntimeError(f"拒绝写入真实工作区 {resolved}；本脚本必须运行在临时沙箱中")


def main() -> int:
    config = load_config()
    epubs = sorted(Path("input").glob("*.epub"))
    if not epubs:
        print("SKIP: no epub in input/")
        return 1

    epub_path = epubs[0]
    chapters = extract_epub(str(epub_path))
    print(f"epub            : {epub_path.name[:50]}")
    print(f"chapters        : {len(chapters)}")

    # 关键：本脚本使用硬编码假数据，绝不能写入真实工作区。
    # 早期版本直接沿用 config.workspace_root，把假条目 Char0001..N 写进了真实任务，
    # 污染了 output/characters.yaml 与 session/。现在强制使用临时目录。
    sandbox_root = Path(tempfile.mkdtemp(prefix="epub2dict_e2e_"))
    print(f"sandbox root    : {sandbox_root}")

    runner_config = AppConfig(
        input_epubs=[epub_path],
        schema_paths=[Path("schemas/characters.yaml")],
        prompt_templates=[],
        workspace_root=sandbox_root,
        pipeline_mode="agent",
        retry_count=1,
        retry_backoff_seconds=0,
        emit_console_progress=False,
        session=SessionConfig(
            max_context_tokens=12000,
            max_tool_rounds_per_segment=6,
            rotate_on_checkpoint=True,
            persist_messages=True,
        ),
        merge=config.merge,
        blacklist_keywords=config.blacklist_keywords,
    )
    runner_config.batching.enable_multi_chapter = True
    runner_config.batching.max_input_tokens = 8000
    runner_config.batching.enable_checkpoint = True
    runner_config.batching.checkpoint_every_n_chapters = 40

    workspace = WorkspaceManager(runner_config.workspace_root).ensure_workspace(epub_path)
    guard_real_workspace(workspace)

    # 沙箱每次新建，本不会有残留；这两步只是防御未来改动。
    session_dir = workspace.session_dir_for_schema("characters")
    if session_dir.exists():
        shutil.rmtree(session_dir)
    stale_progress = workspace.progress_path_for_schema("characters")
    if stale_progress.exists():
        stale_progress.unlink()
    schema = SchemaLoader().load(Path("schemas/characters.yaml"))
    task = TaskDefinition(
        epub_path=epub_path,
        workspace=workspace,
        schema_path=Path("schemas/characters.yaml"),
        prompt_template_paths=[],
        output_path=workspace.output_path_for_schema("characters"),
        progress_path=workspace.progress_path_for_schema("characters"),
        stream_buffer_path=workspace.stream_buffer_path_for_schema("characters"),
    )
    if task.output_path.exists():
        task.output_path.unlink()
    task.output_path.write_text("actors: {}\n", encoding="utf-8")

    client = DeterministicAnalysisClient()
    runner = TaskRunner(runner_config, model_client=client)

    # 只跑前 6 段，足以覆盖多个 generation 与 checkpoint 边界
    segment_plan = build_segments(chapters, budget_tokens=2000, max_segments=6)
    print(f"segments planned: {len(segment_plan)}")

    progress: dict = {
        "total_chapters": len(chapters),
        "last_completed_chapter_index": 0,
        "completed_chapters": 0,
        "status": "pending",
        "retry": {"last_error": ""},
        "merge": {"replaced_nodes": 0, "appended_nodes": 0},
    }

    agent = runner._agent_runner_for(task, schema)
    agent.ensure_session(total_chapters=len(chapters), last_completed_chapter_index=0)

    generations_seen: list[int] = []
    for batch in segment_plan:
        result = asyncio.run(
            agent.run_segment(batch, source_text=build_batch_source_text(batch), total_chapters=len(chapters))
        )
        if not result.ok:
            print(f"FAIL segment {batch.display_range}: {result.reason}")
            return 1
        agent.session.mark_segment_complete(end_chapter_index=batch.end_chapter_index)
        agent.session.persist()
        progress["last_completed_chapter_index"] = batch.end_chapter_index
        agent.session.last_prompt_tokens = estimate_prompt_tokens(client)
        agent.maybe_rotate(total_chapters=len(chapters), at_checkpoint_boundary=True)
        generations_seen.append(agent.session.generation_id)

    print()
    print("=== 结果 ===")
    document = YamlStore().load_yaml(task.output_path)
    entries = document.get("actors", {})
    print(f"生成条目数      : {len(entries)}")
    print(f"generation 轨迹 : {generations_seen}")
    print(f"旋转次数        : {agent.session.rotate_count}")

    usage = agent.session.usage
    print(f"请求次数        : {usage.request_count}")
    print(f"cache_hit       : {usage.cache_hit_tokens}")
    print(f"cache_miss      : {usage.cache_miss_tokens}")
    print(f"hit_ratio       : {usage.hit_ratio:.3f}")

    ok, detail = check_prefix_monotonic(client.requests)
    print(f"前缀严格性      : {'PASS' if ok else 'FAIL'} — {detail}")

    # 旋转后 system 块必须字节一致
    system_prefix = agent.session.system_prefix()
    print(f"system 块长度   : {len(system_prefix)}")

    # 会话恢复验证
    restored = agent.session.__class__(
        schema_definition=schema,
        config=runner_config.session,
        persona_text=agent.session.persona_text,
        messages_path=task.workspace.messages_path_for_schema("characters"),
    )
    restored_ok = restored.restore()
    print(f"会话恢复        : {'PASS' if restored_ok else 'FAIL'} (messages={len(restored.messages)})")

    output_ok = len(entries) > 0
    print()
    verdict = "PASS" if (ok and restored_ok and output_ok and agent.session.rotate_count > 0) else "FAIL"
    print(f"总体校验        : {verdict}")
    return 0 if verdict == "PASS" else 1

def estimate_prompt_tokens(client: DeterministicAnalysisClient) -> int:
    """用最后一条请求的字符量估算 prompt token，模拟真实 usage。"""
    if not client.requests:
        return 0
    return len(json.dumps(client.requests[-1], ensure_ascii=False)) // 4

def build_segments(chapters, *, budget_tokens: int, max_segments: int) -> list[ChapterBatch]:
    segments: list[ChapterBatch] = []
    pending = []
    pending_tokens = 0
    for chapter in chapters:
        if pending and pending_tokens + chapter.token_estimate > budget_tokens:
            segments.append(ChapterBatch.from_chapters(pending))
            pending = []
            pending_tokens = 0
            if len(segments) >= max_segments:
                break
        pending.append(chapter)
        pending_tokens += chapter.token_estimate
    if pending and len(segments) < max_segments:
        segments.append(ChapterBatch.from_chapters(pending))
    return segments

if __name__ == "__main__":
    raise SystemExit(main())

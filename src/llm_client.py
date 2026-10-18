from __future__ import annotations

import json
import os
import re
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from models import AppConfig, AssistantTurn, StreamModelClient, ToolCall, ToolCompletion

ROOT_PATTERNS = [
    re.compile(r"输出根节点必须是\s+([a-zA-Z_][a-zA-Z0-9_]*)"),
    re.compile(r"输出目标从\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+开始"),
]
HTML_TITLE_PATTERN = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)
HTML_PARAGRAPH_PATTERN = re.compile(r"<p>(.*?)</p>", re.IGNORECASE | re.DOTALL)
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
CLOUDFLARE_ERROR_PATTERN = re.compile(r"Error\s*(\d{3,4})", re.IGNORECASE)
WHITESPACE_PATTERN = re.compile(r"\s+")
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; "
    ".NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; rv:11.0) like Gecko"
)
MAX_ERROR_TEXT_LENGTH = 240


class ApiStreamError(RuntimeError):
    pass


class EchoStreamModelClient(StreamModelClient):
    async def stream_yaml(self, prompt: str) -> AsyncIterable[str]:
        fallback_root = os.getenv("EPUB2DICT_FAKE_ROOT", "actors")
        root_key = detect_root_key(prompt) or fallback_root
        content = f"{root_key}: []\n"
        for line in content.splitlines(keepends=True):
            yield line

    async def complete_with_tools(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> ToolCompletion:
        """占位实现：不调用 API，也不发起工具调用。

        用于未配置 api_key 时打通流程。它会结束当前段落而不产生任何编辑。
        """
        return ToolCompletion(turn=AssistantTurn(content="noop"), finish_reason="stop")


class OpenAICompatibleStreamModelClient(StreamModelClient):
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str = "",
        model_name: str = "",
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        extra_body: Mapping[str, Any] | None = None,
        extra_headers: Mapping[str, Any] | None = None,
    ) -> None:
        self.base_url = normalize_base_url(base_url)
        self.api_key = api_key
        self.model_name = model_name
        self.timeout_seconds = timeout_seconds
        # 自定义请求体/请求头：用于适配各家 API 的额外参数要求
        # （如 ``thinking``、``top_k``、``search``、``X-Api-Version`` 等）。
        self.extra_body: dict[str, Any] = dict(extra_body or {})
        self.extra_headers: dict[str, Any] = dict(extra_headers or {})

    async def stream_yaml(self, prompt: str) -> AsyncIterable[str]:
        self._require_request_params()
        payload = build_chat_completions_payload(self.model_name, prompt, extra_body=self.extra_body)
        async for chunk in self._iter_stream_chunks(payload):
            text = extract_stream_delta_text(chunk)
            if text:
                yield text

    async def complete_with_tools(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> ToolCompletion:
        """流式完成一次带工具调用的回复。

        工具调用参数在流中按片段到达，需按 index 累积后才能解析。
        """
        self._require_request_params()
        payload = build_tool_completion_payload(
            self.model_name, messages, tools, extra_body=self.extra_body
        )
        accumulator: dict[int, ToolCallAccumulator] = {}
        content_parts: list[str] = []
        usage: dict[str, Any] = {}
        finish_reason = ""

        async for chunk in self._iter_stream_chunks(payload):
            choice = first_choice_of(chunk)
            if choice:
                delta = choice.get("delta")
                if isinstance(delta, dict):
                    content = delta.get("content")
                    if isinstance(content, str) and content:
                        content_parts.append(content)
                    accumulate_tool_call_deltas(accumulator, delta.get("tool_calls"))
                reason = choice.get("finish_reason")
                if isinstance(reason, str) and reason:
                    finish_reason = reason
            chunk_usage = extract_stream_usage(chunk)
            if chunk_usage:
                usage.update(chunk_usage)

        return ToolCompletion(
            turn=AssistantTurn(content="".join(content_parts), tool_calls=finalize_tool_calls(accumulator)),
            usage=usage,
            finish_reason=finish_reason,
        )

    def _require_request_params(self) -> None:
        # 注意：api_key 不是必填项。不少自建/Ollama/vLLM/LM Studio 端点
        # 完全不需要鉴权，头部则由 extra_headers 自行决定。
        if not self.model_name and "model" not in self.extra_body:
            raise ApiStreamError("model_name is required (or provide model via extra_body)")

    async def _iter_stream_chunks(self, payload: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        """发起流式请求并逐条产出已解析的 chunk。"""
        url = f"{self.base_url}/chat/completions"
        headers = build_request_headers(self.api_key, self.extra_headers)
        timeout = httpx.Timeout(self.timeout_seconds)

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("POST", url, json=payload, headers=headers) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if not line:
                            continue
                        if not line.startswith("data:"):
                            continue

                        data = line[5:].strip()
                        if data == "[DONE]":
                            break

                        try:
                            yield json.loads(data)
                        except json.JSONDecodeError as exc:
                            raise ApiStreamError(f"invalid stream chunk json: {exc}") from exc
        except httpx.HTTPStatusError as exc:
            message = read_http_error(exc.response, fallback_message=str(exc))
            raise ApiStreamError(f"http error {exc.response.status_code}: {message}") from exc
        except httpx.HTTPError as exc:
            raise ApiStreamError(f"network error: {compact_error_text(str(exc))}") from exc


def build_model_client(config: AppConfig) -> StreamModelClient:
    """按配置构建客户端。

    与旧行为差异：不再以 ``api_key`` 是否存在作为分支条件。
    ``api_key`` 为空时照样发起真实请求（无鉴权端点、或鉴权完全交由
    ``extra_headers`` 处理）；需要离线跑流程请显式用 ``dry_run: true``。
    """
    if config.dry_run:
        return EchoStreamModelClient()
    return OpenAICompatibleStreamModelClient(
        base_url=config.base_url,
        api_key=config.api_key,
        model_name=config.model_name,
        extra_body=config.extra_body,
        extra_headers=config.extra_headers,
    )


def build_chat_completions_payload(
    model_name: str,
    prompt: str,
    extra_body: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": model_name,
        "stream": True,
        # "thinking": {"type": "disabled"},
        "reasoning_effort": "low",
        "messages": [
            {
                "role": "user",
                "content": prompt,
            }
        ],
    }
    return apply_extra_body(payload, extra_body)

def build_tool_completion_payload(
    model_name: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
    extra_body: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """带工具调用的请求体。

    包含 ``stream_options.include_usage``，否则流式响应不会返回 usage，
    也就无法观测缓存命中（``prompt_cache_hit_tokens``）。
    """
    payload: dict[str, Any] = {
        "model": model_name,
        "stream": True,
        "stream_options": {"include_usage": True},
        "messages": messages,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"
    return apply_extra_body(payload, extra_body)


def apply_extra_body(
    payload: dict[str, Any],
    extra_body: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """把自定义字段深合并进请求体。

    规约：
    - dict 递归合并；
    - list / 标量整体覆盖（便于直接指定 ``messages`` 以外的数组型参数）；
    - 值为 ``None`` 时删除同名键（用于屏蔽默认参数，例如去掉 ``reasoning_effort``）。
    """
    if not extra_body:
        return payload
    return merge_mapping(payload, extra_body)


def merge_mapping(base: dict[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    for key, value in overlay.items():
        if value is None:
            base.pop(key, None)
            continue
        current = base.get(key)
        if isinstance(current, dict) and isinstance(value, Mapping):
            base[key] = merge_mapping(dict(current), value)
        else:
            base[key] = value
    return base


def build_request_headers(
    api_key: str,
    extra_headers: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """组合请求头。

    - ``api_key`` 非空时默认使用 ``Authorization: Bearer <key>``；
    - ``extra_headers`` 中的键**大小写不敏感**地覆盖同名默认头，
      因此可以整条改写为 ``x-api-key``、``api-key`` 等各家写法；
    - ``extra_headers`` 中值为 ``None`` 的键会删除该默认头（例如不带 User-Agent）；
    - 非字符串值（int/bool/嵌套）会被转成字符串或 JSON。
    """
    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "User-Agent": DEFAULT_USER_AGENT,
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    lowercase_index = {key.lower(): key for key in headers}
    for raw_name, raw_value in (extra_headers or {}).items():
        name = str(raw_name).strip()
        if not name:
            continue
        existing = lowercase_index.pop(name.lower(), None)
        if raw_value is None:
            if existing is not None:
                headers.pop(existing, None)
            continue
        if existing is not None and existing != name:
            headers.pop(existing, None)
        headers[name] = stringify_header_value(raw_value)
        lowercase_index[name.lower()] = name
    return headers


def stringify_header_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

@dataclass
class ToolCallAccumulator:
    """流式工具调用参数的累积器。

    OpenAI 兼容流会按 index 分片下发 ``function.arguments``，必须拼接后才能解析。
    """

    index: int
    id: str = ""
    name: str = ""
    arguments: str = ""

    def update(self, *, call_id: str = "", name: str = "", arguments: str = "") -> None:
        if call_id:
            self.id = call_id
        # name 与 arguments 都可能被提供方拆成多片下发，必须拼接而不是覆盖。
        if name:
            self.name += name
        if arguments:
            self.arguments += arguments

def first_choice_of(chunk: dict[str, Any]) -> dict[str, Any] | None:
    choices = chunk.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    first = choices[0]
    return first if isinstance(first, dict) else None

def accumulate_tool_call_deltas(
    accumulator: dict[int, ToolCallAccumulator],
    tool_call_deltas: Any,
) -> None:
    if not isinstance(tool_call_deltas, list):
        return
    for position, item in enumerate(tool_call_deltas):
        if not isinstance(item, dict):
            continue
        raw_index = item.get("index")
        index = int(raw_index) if isinstance(raw_index, int) else position
        entry = accumulator.get(index)
        if entry is None:
            entry = ToolCallAccumulator(index=index)
            accumulator[index] = entry
        function = item.get("function")
        function = function if isinstance(function, dict) else {}
        entry.update(
            call_id=str(item.get("id", "") or ""),
            name=str(function.get("name", "") or ""),
            arguments=str(function.get("arguments", "") or ""),
        )

def finalize_tool_calls(accumulator: dict[int, ToolCallAccumulator]) -> list[ToolCall]:
    calls: list[ToolCall] = []
    for index in sorted(accumulator):
        entry = accumulator[index]
        if not entry.name:
            continue
        calls.append(
            ToolCall(
                id=entry.id or f"call_{index}",
                name=entry.name,
                arguments=entry.arguments or "{}",
            )
        )
    return calls

def extract_stream_usage(chunk: dict[str, Any]) -> dict[str, Any]:
    """提取 usage，包括 DeepSeek 的缓存命中/未命中字段。"""
    usage = chunk.get("usage")
    if not isinstance(usage, dict) or not usage:
        return {}
    extracted: dict[str, Any] = {}
    for key in (
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "prompt_cache_hit_tokens",
        "prompt_cache_miss_tokens",
    ):
        value = usage.get(key)
        if isinstance(value, int):
            extracted[key] = value
    return extracted


def extract_stream_delta_text(chunk: dict[str, Any]) -> str:
    choices = chunk.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""

    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        return ""

    delta = first_choice.get("delta")
    if not isinstance(delta, dict):
        return ""

    content = delta.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(extract_content_part_text(item) for item in content)
    return ""


def extract_content_part_text(item: Any) -> str:
    if isinstance(item, str):
        return item
    if not isinstance(item, dict):
        return ""
    text = item.get("text")
    if isinstance(text, str):
        return text
    return ""


def read_http_error(response: httpx.Response | None, fallback_message: str = "") -> str:
    if response is None:
        return compact_error_text(fallback_message)

    try:
        body = response.text.strip()
    except Exception:  # noqa: BLE001
        try:
            raw_body = response.read()
            if isinstance(raw_body, bytes):
                body = raw_body.decode("utf-8", errors="replace").strip()
            else:
                body = str(raw_body).strip()
        except Exception:  # noqa: BLE001
            body = ""

    if looks_like_html(body):
        summary = summarize_html_error(body)
        if summary:
            return summary

    if body:
        return compact_error_text(body)

    reason_phrase = getattr(response, "reason_phrase", "")
    if isinstance(reason_phrase, str) and reason_phrase:
        return compact_error_text(fallback_message or reason_phrase)

    message = getattr(response, "msg", "")
    if isinstance(message, str) and message:
        return compact_error_text(fallback_message or message)

    return compact_error_text(fallback_message or str(response))


def normalize_base_url(value: str) -> str:
    return value.rstrip("/")


def detect_root_key(prompt: str) -> str | None:
    for pattern in ROOT_PATTERNS:
        match = pattern.search(prompt)
        if match:
            return match.group(1)
    return None


def looks_like_html(value: str) -> bool:
    lowered = value.lower()
    return "<html" in lowered or "<!doctype html" in lowered


def summarize_html_error(body: str) -> str:
    title = extract_html_text(HTML_TITLE_PATTERN, body)
    paragraph = extract_html_text(HTML_PARAGRAPH_PATTERN, body)
    cloudflare_code_match = CLOUDFLARE_ERROR_PATTERN.search(body)

    parts: list[str] = []
    if title:
        parts.append(title)
    if cloudflare_code_match:
        parts.append(f"Cloudflare {cloudflare_code_match.group(1)}")
    if paragraph:
        parts.append(paragraph)

    if not parts:
        return compact_error_text(body)
    return compact_error_text(" | ".join(parts))


def extract_html_text(pattern: re.Pattern[str], body: str) -> str:
    match = pattern.search(body)
    if not match:
        return ""
    text = HTML_TAG_PATTERN.sub(" ", match.group(1))
    return compact_error_text(text)


def compact_error_text(value: str) -> str:
    normalized = WHITESPACE_PATTERN.sub(" ", value).strip()
    if len(normalized) <= MAX_ERROR_TEXT_LENGTH:
        return normalized
    return normalized[: MAX_ERROR_TEXT_LENGTH - 3] + "..."

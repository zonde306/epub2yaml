from __future__ import annotations

import json
import os
import re
from collections.abc import AsyncIterable, AsyncIterator
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
        api_key: str,
        model_name: str,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.base_url = normalize_base_url(base_url)
        self.api_key = api_key
        self.model_name = model_name
        self.timeout_seconds = timeout_seconds

    async def stream_yaml(self, prompt: str) -> AsyncIterable[str]:
        self._require_credentials()
        payload = build_chat_completions_payload(self.model_name, prompt)
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
        self._require_credentials()
        payload = build_tool_completion_payload(self.model_name, messages, tools)
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

    def _require_credentials(self) -> None:
        if not self.api_key:
            raise ApiStreamError("api_key is required for real API streaming")
        if not self.model_name:
            raise ApiStreamError("model_name is required for real API streaming")

    async def _iter_stream_chunks(self, payload: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        """发起流式请求并逐条产出已解析的 chunk。"""
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; rv:11.0) like Gecko"
        }
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
    if config.api_key:
        return OpenAICompatibleStreamModelClient(
            base_url=config.base_url,
            api_key=config.api_key,
            model_name=config.model_name,
        )
    return EchoStreamModelClient()


def build_chat_completions_payload(model_name: str, prompt: str) -> dict[str, Any]:
    return {
        "model": model_name,
        "stream": True,
        "messages": [
            {
                "role": "user",
                "content": prompt,
            }
        ],
    }

def build_tool_completion_payload(
    model_name: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
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
    return payload

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

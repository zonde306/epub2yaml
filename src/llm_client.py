from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable
from typing import Any
from urllib import error, request

from models import AppConfig, StreamModelClient

ROOT_PATTERNS = [
    re.compile(r"输出根节点必须是\s+([a-zA-Z_][a-zA-Z0-9_]*)"),
    re.compile(r"输出目标从\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+开始"),
]
DEFAULT_TIMEOUT_SECONDS = 300


class ApiStreamError(RuntimeError):
    pass


class EchoStreamModelClient(StreamModelClient):
    def stream_yaml(self, prompt: str) -> Iterable[str]:
        fallback_root = os.getenv("EPUB2DICT_FAKE_ROOT", "actors")
        root_key = detect_root_key(prompt) or fallback_root
        content = f"{root_key}: []\n"
        for line in content.splitlines(keepends=True):
            yield line


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

    def stream_yaml(self, prompt: str) -> Iterable[str]:
        if not self.api_key:
            raise ApiStreamError("api_key is required for real API streaming")
        if not self.model_name:
            raise ApiStreamError("model_name is required for real API streaming")

        url = f"{self.base_url}/chat/completions"
        payload = build_chat_completions_payload(self.model_name, prompt)
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "Authorization": f"Bearer {self.api_key}",
        }
        http_request = request.Request(url=url, data=body, headers=headers, method="POST")

        try:
            with request.urlopen(http_request, timeout=self.timeout_seconds) as response:
                for raw_line in response:
                    line = raw_line.decode("utf-8", errors="replace").strip()
                    if not line or not line.startswith("data:"):
                        continue

                    data = line[5:].strip()
                    if data == "[DONE]":
                        break

                    try:
                        chunk = json.loads(data)
                    except json.JSONDecodeError as exc:
                        raise ApiStreamError(f"invalid stream chunk json: {exc}") from exc

                    text = extract_stream_delta_text(chunk)
                    if text:
                        yield text
        except error.HTTPError as exc:
            message = read_http_error(exc)
            raise ApiStreamError(f"http error {exc.code}: {message}") from exc
        except error.URLError as exc:
            raise ApiStreamError(f"network error: {exc.reason}") from exc


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


def read_http_error(exc: error.HTTPError) -> str:
    try:
        body = exc.read().decode("utf-8", errors="replace").strip()
    except Exception:  # noqa: BLE001
        body = exc.reason if isinstance(exc.reason, str) else ""
    return body or str(exc.reason)


def normalize_base_url(value: str) -> str:
    return value.rstrip("/")


def detect_root_key(prompt: str) -> str | None:
    for pattern in ROOT_PATTERNS:
        match = pattern.search(prompt)
        if match:
            return match.group(1)
    return None

"""端到端验证：model.extra_body / model.extra_headers 是否真的落到网络请求上。

做法：起一个本地 HTTP mock（模拟 OpenAI 兼容的 SSE 端点），用它抓取客户端
实际发出的请求行、请求头、请求体，然后逐项断言。

运行：
    D:/ProgramFiles/Python310/python tests/e2e_extra_params.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from app import normalize_extra_body, normalize_extra_headers  # noqa: E402
from llm_client import (  # noqa: E402
    OpenAICompatibleStreamModelClient,
    build_chat_completions_payload,
    build_request_headers,
)

SSE_BODY = (
    b'data: {"choices":[{"delta":{"content":"hello"},"finish_reason":null}]}\n\n'
    b"data: [DONE]\n\n"
)


class CapturingHandler(BaseHTTPRequestHandler):
    captured: dict[str, Any] = {}

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        CapturingHandler.captured = {
            "path": self.path,
            "headers": {key: value for key, value in self.headers.items()},
            "body": json.loads(raw.decode("utf-8")) if raw else {},
        }
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(SSE_BODY)))
        self.end_headers()
        self.wfile.write(SSE_BODY)

    def log_message(self, fmt: str, *args: Any) -> None:  # 静音
        return


def serve() -> tuple[HTTPServer, str]:
    server = HTTPServer(("127.0.0.1", 0), CapturingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    return server, f"http://{host}:{port}/v1"


async def collect(client: OpenAICompatibleStreamModelClient, prompt: str) -> str:
    parts: list[str] = []
    async for chunk in client.stream_yaml(prompt):
        parts.append(chunk)
    return "".join(parts)


def main() -> int:
    server, base_url = serve()
    failures: list[str] = []

    def check(name: str, actual: Any, expected: Any) -> None:
        status = "PASS" if actual == expected else "FAIL"
        if status == "FAIL":
            failures.append(f"{name}: expected={expected!r} actual={actual!r}")
        print(f"[{status}] {name} = {actual!r}")

    try:
        # ---------- 场景 1：无 api_key + 自定义 header（不发达 Bearer 头） ----------
        client = OpenAICompatibleStreamModelClient(
            base_url=base_url,
            api_key="",
            model_name="local-model",
            extra_headers={
                "X-Access-Token": "tok-123",  # 别家鉴权头
                "User-Agent": "epub2dict/1.0",  # 覆盖默认 UA
            },
            extra_body={
                "temperature": 0.2,
                "top_k": 20,
                "thinking": {"type": "enabled"},
                "parallel_tool_calls": False,
                "reasoning_effort": None,  # 删除默认键
            },
        )
        text = asyncio.run(collect(client, "ping"))
        captured = CapturingHandler.captured
        headers = {key.lower(): value for key, value in captured["headers"].items()}
        body = captured["body"]

        print("\n--- 场景 1：无 api_key + extra_headers/extra_body ---")
        check("stream text", text, "hello")
        check("request path", captured["path"], "/v1/chat/completions")
        check("Authorization absent", "authorization" in headers, False)
        check("custom auth header", headers.get("x-access-token"), "tok-123")
        check("overridden User-Agent", headers.get("user-agent"), "epub2dict/1.0")
        check("Content-Type", headers.get("content-type"), "application/json")
        check("Accept", headers.get("accept"), "text/event-stream")
        check("body.temperature", body.get("temperature"), 0.2)
        check("body.top_k", body.get("top_k"), 20)
        check("body.thinking", body.get("thinking"), {"type": "enabled"})
        check("body.parallel_tool_calls", body.get("parallel_tool_calls"), False)
        check("body.reasoning_effort removed", "reasoning_effort" in body, False)
        check("body.model", body.get("model"), "local-model")
        check("body.stream", body.get("stream"), True)

        # ---------- 场景 2：agent 管线 complete_with_tools 用同一套 body/header ----------
        client3 = OpenAICompatibleStreamModelClient(
            base_url=base_url,
            api_key="",
            model_name="agent-model",
            extra_headers={"X-Access-Token": "tok-agent"},
            extra_body={"top_k": 7, "stream_options": {"include_usage": True, "x": 1}},
        )
        completion = asyncio.run(
            client3.complete_with_tools(
                [{"role": "user", "content": "x"}],
                [{"type": "function", "function": {"name": "t"}}],
            )
        )
        captured3 = CapturingHandler.captured
        headers3 = {key.lower(): value for key, value in captured3["headers"].items()}
        body3 = captured3["body"]

        print("\n--- 场景 2：complete_with_tools 路径 ---")
        check("agent content", completion.turn.content, "hello")
        check("agent header", headers3.get("x-access-token"), "tok-agent")
        check("agent Authorization absent", "authorization" in headers3, False)
        check("agent body.top_k", body3.get("top_k"), 7)
        check("agent stream_options merged", body3.get("stream_options"), {"include_usage": True, "x": 1})
        check("agent tool_choice", body3.get("tool_choice"), "auto")

        # ---------- 场景 2b：有 api_key + 用 extra_headers 改写鉴权写法 ----------
        client2 = OpenAICompatibleStreamModelClient(
            base_url=base_url,
            api_key="ignored-by-header",
            model_name="remote-model",
            extra_headers={"authorization": "Token abc"},  # 大小写不敏感覆盖
        )
        asyncio.run(collect(client2, "ping"))
        headers2 = {key.lower(): value for key, value in CapturingHandler.captured["headers"].items()}
        auth_keys = [key for key in headers2 if key == "authorization"]

        print("\n--- 场景 2b：api_key + 大小写不敏感覆盖 Authorization ---")
        check("single authorization header", len(auth_keys), 1)
        check("authorization value", headers2.get("authorization"), "Token abc")

        # ---------- 场景 3：纯函数层（payload / header 组装） ----------
        print("\n--- 场景 3：纯函数层 ---")
        check("plain payload", build_chat_completions_payload("m", "x"),
              {"model": "m", "stream": True, "reasoning_effort": "low",
               "messages": [{"role": "user", "content": "x"}]})
        check("headers no key", "Authorization" in build_request_headers(""), False)
        check("headers with key", build_request_headers("k")["Authorization"], "Bearer k")
        check("header deletion", "User-Agent" in build_request_headers("", {"user-agent": None}), False)
        stringified = build_request_headers("", {"X-Num": 5, "X-Bool": True, "X-Obj": {"a": 1}})
        check("header stringify int", stringified["X-Num"], "5")
        check("header stringify bool", stringified["X-Bool"], "true")
        check("header stringify object", stringified["X-Obj"], '{"a":1}')

        # ---------- 场景 4：配置归一化 ----------
        print("\n--- 场景 4：配置归一化 ---")
        check("extra_body non-dict", normalize_extra_body("nope"), {})
        check("extra_body keeps null", normalize_extra_body({"a": None}), {"a": None})
        check("extra_headers trims blank key", normalize_extra_headers({"  ": "x", "A": "b"}), {"A": "b"})
        check("extra_headers keeps bool", normalize_extra_headers({"X": True}), {"X": True})
    finally:
        server.shutdown()

    print()
    if failures:
        for item in failures:
            print(f"FAILED -> {item}")
        return 1
    print("ALL PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

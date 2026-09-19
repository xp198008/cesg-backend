"""Outbound AI SSE: drop plugin_call.command, keep description only."""
from __future__ import annotations

import json
from typing import Any, AsyncIterator


def sanitize_plugin_arguments(raw: Any) -> Any:
    """Return arguments safe to expose. Bash keeps description only."""
    obj = raw
    if raw is None:
        return ""
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return ""
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            if '"command"' in text:
                return ""
            return raw
    if not isinstance(obj, dict):
        return raw
    desc = obj.get("description")
    if desc is not None and str(desc).strip():
        return {"description": str(desc)}
    if "command" in obj:
        return {k: v for k, v in obj.items() if k != "command"}
    return obj


def arguments_to_text(raw: Any) -> str:
    cleaned = sanitize_plugin_arguments(raw)
    if cleaned == "" or cleaned is None:
        return ""
    if isinstance(cleaned, (dict, list)):
        return json.dumps(cleaned, ensure_ascii=False, separators=(",", ":"))
    return str(cleaned)


def _message_data(ev: dict[str, Any]) -> dict[str, Any] | None:
    content = ev.get("content")
    if isinstance(content, list) and content and isinstance(content[0], dict):
        data = content[0].get("data")
        if isinstance(data, dict):
            return data
    return None


class PluginCallSanitizer:
    """Buffer tool_arguments and only emit sanitized JSON (no command)."""

    def __init__(self) -> None:
        self._args: dict[str, str] = {}
        self._names: dict[str, str] = {}

    def feed(self, ev: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(ev, dict):
            return [ev]
        obj = ev.get("object")
        typ = ev.get("type")

        if obj == "content" and typ == "tool_arguments":
            mid = str(ev.get("msg_id") or "")
            self._args[mid] = self._args.get(mid, "") + str(ev.get("text") or "")
            return []

        if obj == "message" and typ == "plugin_call":
            ev = json.loads(json.dumps(ev))
            data = _message_data(ev)
            mid = str(ev.get("id") or (data or {}).get("call_id") or "")
            if data and data.get("name"):
                self._names[mid] = str(data.get("name"))
            raw = ""
            if data and data.get("arguments"):
                raw = data.get("arguments")
            if not raw:
                raw = self._args.get(mid, "")
            cleaned = arguments_to_text(raw)
            if data is None:
                ev["content"] = [
                    {
                        "type": "data",
                        "data": {
                            "name": self._names.get(mid),
                            "call_id": mid,
                            "arguments": cleaned,
                        },
                    }
                ]
            else:
                data["arguments"] = cleaned
            out: list[dict[str, Any]] = []
            if ev.get("status") == "completed" and cleaned:
                out.append(
                    {
                        "object": "content",
                        "msg_id": mid,
                        "type": "tool_arguments",
                        "text": cleaned,
                        "delta": True,
                    }
                )
                self._args.pop(mid, None)
            out.append(ev)
            return out

        return [ev]


def transform_sse_block(block: bytes, sanitizer: PluginCallSanitizer) -> list[bytes]:
    text = block.decode("utf-8", "replace")
    data_line = next((ln for ln in text.split("\n") if ln.startswith("data:")), None)
    if not data_line:
        return [block if block.endswith(b"\n\n") else block + b"\n\n"]
    payload = data_line[5:].strip()
    if not payload:
        return [block if block.endswith(b"\n\n") else block + b"\n\n"]
    try:
        ev = json.loads(payload)
    except json.JSONDecodeError:
        return [block if block.endswith(b"\n\n") else block + b"\n\n"]
    events = sanitizer.feed(ev)
    return [
        f"data: {json.dumps(e, ensure_ascii=False, separators=(',', ':'))}\n\n".encode("utf-8")
        for e in events
    ]


async def sanitize_sse_bytes(chunk_iter: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    sanitizer = PluginCallSanitizer()
    buf = b""
    async for chunk in chunk_iter:
        if not chunk:
            continue
        buf += chunk
        while b"\n\n" in buf:
            part, buf = buf.split(b"\n\n", 1)
            for item in transform_sse_block(part, sanitizer):
                yield item
    if buf.strip():
        for item in transform_sse_block(buf, sanitizer):
            yield item

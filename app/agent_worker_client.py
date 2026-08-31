"""Agent Worker（AI 智能体）HTTP 客户端。

接口文档：docs/aiNew.pdf（最新）/ docs/AI.PDF
基础地址默认 http://113.207.68.94:5002
"""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class AgentWorkerError(RuntimeError):
    """Agent Worker 调用失败。"""


def _base_url() -> str:
    return (settings.agent_worker_base_url or "").rstrip("/")


def _auth_headers(user_id: str, company: str) -> list[tuple[str, str | bytes]]:
    # aiNew.pdf：x-user / x-company；同时带 x-user-id 兼容旧 Worker
    uid = str(user_id)
    headers: list[tuple[str, str | bytes]] = [
        ("x-user", uid),
        ("x-user-id", uid),
        ("x-company", company.encode("utf-8")),
    ]
    api_key = (settings.agent_worker_api_key or "").strip()
    if api_key:
        headers.append(("Authorization", f"Bearer {api_key}"))
    return headers


class AgentWorkerClient:
    def configured(self) -> bool:
        return bool(_base_url())

    def _timeout(self) -> httpx.Timeout:
        return httpx.Timeout(settings.agent_worker_timeout, connect=min(10.0, settings.agent_worker_timeout))

    def _video_timeout(self) -> httpx.Timeout:
        return httpx.Timeout(settings.agent_worker_video_timeout, connect=min(15.0, settings.agent_worker_video_timeout))

    async def health(self) -> dict[str, Any]:
        url = f"{_base_url()}/health"
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()

    async def chat_stream(
        self,
        *,
        user_id: str,
        company: str,
        session_id: str,
        input_messages: list[dict[str, Any]],
        stream: bool = True,
    ) -> AsyncIterator[bytes]:
        url = f"{_base_url()}/api/chat"
        payload = {
            "session_id": session_id,
            "input": input_messages,
            "stream": stream,
        }
        headers = _auth_headers(user_id, company)
        headers.append(("Content-Type", "application/json"))
        async with httpx.AsyncClient(timeout=self._video_timeout()) as client:
            async with client.stream("POST", url, json=payload, headers=headers) as resp:
                if resp.status_code >= 400:
                    body = await resp.aread()
                    raise AgentWorkerError(body.decode("utf-8", "replace") or f"HTTP {resp.status_code}")
                async for chunk in resp.aiter_bytes():
                    if chunk:
                        yield chunk

    async def chat_collect_text(
        self,
        *,
        user_id: str,
        company: str,
        session_id: str,
        input_messages: list[dict[str, Any]],
    ) -> str:
        """流式对话并拼接全部 text 增量。"""
        import json

        buffer = ""
        async for chunk in self.chat_stream(
            user_id=user_id,
            company=company,
            session_id=session_id,
            input_messages=input_messages,
            stream=True,
        ):
            buffer += chunk.decode("utf-8", "replace")
        parts: list[str] = []
        for block in buffer.split("\n\n"):
            line = next((ln.strip() for ln in block.split("\n") if ln.strip().startswith("data:")), "")
            if not line:
                continue
            payload = line[5:].strip()
            if not payload:
                continue
            try:
                ev = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if ev.get("object") == "content" and ev.get("type") == "text" and ev.get("text"):
                parts.append(str(ev["text"]))
        return "".join(parts)

    async def cancel_chat(self, *, session_id: str) -> dict[str, Any]:
        url = f"{_base_url()}/api/cancel/{session_id}"
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.post(url)
            resp.raise_for_status()
            return resp.json()

    async def get_session(self, *, session_id: str) -> dict[str, Any]:
        url = f"{_base_url()}/api/sessions/{session_id}"
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()

    async def get_vehicle_summary(
        self,
        *,
        plate: str | None = None,
        car_id: int | None = None,
        gps_hours: int = 3,
    ) -> dict[str, Any]:
        """GET /api/vehicle/summary（aiNew.pdf 第五节）。"""
        if not (plate or "").strip() and car_id is None:
            raise AgentWorkerError("plate 与 car_id 必须至少提供一个")
        url = f"{_base_url()}/api/vehicle/summary"
        params: dict[str, Any] = {"gps_hours": max(1, min(72, int(gps_hours or 3)))}
        if car_id is not None:
            params["car_id"] = int(car_id)
        if (plate or "").strip():
            params["plate"] = plate.strip()
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.get(url, params=params)
            if resp.status_code >= 400:
                detail = resp.text
                try:
                    body = resp.json()
                    if isinstance(body, dict) and body.get("detail") is not None:
                        detail = str(body.get("detail"))
                except Exception:
                    pass
                raise AgentWorkerError(detail or f"HTTP {resp.status_code}")
            data = resp.json()
            return data if isinstance(data, dict) else {"data": data}

    async def list_documents(
        self,
        *,
        dataset_id: str,
        page: int = 1,
        page_size: int = 20,
        keyword: str | None = None,
        category: str | None = None,
    ) -> dict[str, Any]:
        url = f"{_base_url()}/api/knowledge/datasets/{dataset_id}/documents"
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if keyword:
            params["keyword"] = keyword
        if category:
            params["metadata[category]"] = category
        headers = []
        api_key = (settings.agent_worker_api_key or "").strip()
        if api_key:
            headers.append(("Authorization", f"Bearer {api_key}"))
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.get(url, params=params, headers=headers or None)
            resp.raise_for_status()
            return resp.json()

    async def upload_document(
        self,
        *,
        dataset_id: str,
        filename: str,
        content: bytes,
        content_type: str | None = None,
        category: str | None = None,
    ) -> dict[str, Any]:
        url = f"{_base_url()}/api/knowledge/datasets/{dataset_id}/documents/upload"
        files = {"file": (filename, content, content_type or "application/octet-stream")}
        data: dict[str, str] = {}
        if category:
            data["category"] = category
        headers = []
        api_key = (settings.agent_worker_api_key or "").strip()
        if api_key:
            headers.append(("Authorization", f"Bearer {api_key}"))
        async with httpx.AsyncClient(timeout=self._video_timeout()) as client:
            resp = await client.post(url, files=files, data=data or None, headers=headers or None)
            if resp.status_code >= 400:
                raise AgentWorkerError(resp.text or f"HTTP {resp.status_code}")
            return resp.json()

    async def delete_document(self, *, dataset_id: str, document_id: str) -> None:
        url = f"{_base_url()}/api/knowledge/datasets/{dataset_id}/documents/{document_id}"
        headers = []
        api_key = (settings.agent_worker_api_key or "").strip()
        if api_key:
            headers.append(("Authorization", f"Bearer {api_key}"))
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.delete(url, headers=headers or None)
            if resp.status_code >= 400:
                raise AgentWorkerError(resp.text or f"HTTP {resp.status_code}")

    async def analyze_video_violation_stream(
        self,
        *,
        user_id: str,
        company: str,
        filename: str,
        content: bytes,
        content_type: str | None = None,
        session_id: str | None = None,
        extra_fields: dict[str, str] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """POST /api/video/violation，按文档消费 SSE，逐条 yield 解析后的事件。

        直至 object=complete / object=error 结束；错误事件也会 yield 后返回。
        extra_fields：可选业务上下文（车牌/终端/坐标/地址等），Worker 未识别字段会忽略。
        """
        import json

        url = f"{_base_url()}/api/video/violation"
        files = {"file": (filename, content, content_type or "video/mp4")}
        data: dict[str, str] = {"company": company, "user_id": str(user_id)}
        if session_id:
            data["session_id"] = session_id
        if extra_fields:
            for key, value in extra_fields.items():
                if key and value is not None and str(value).strip():
                    data[str(key)] = str(value).strip()
        headers = _auth_headers(user_id, company)

        async with httpx.AsyncClient(timeout=self._video_timeout()) as client:
            async with client.stream("POST", url, files=files, data=data, headers=headers) as resp:
                if resp.status_code >= 400:
                    body = await resp.aread()
                    detail = body.decode("utf-8", "replace")
                    try:
                        j = json.loads(detail)
                        if isinstance(j, dict) and j.get("detail") is not None:
                            detail = str(j.get("detail"))
                    except Exception:
                        pass
                    raise AgentWorkerError(detail or f"HTTP {resp.status_code}")

                buffer = ""
                async for chunk in resp.aiter_text():
                    if not chunk:
                        continue
                    buffer += chunk
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith("data:"):
                            payload = line[5:].strip()
                        else:
                            # 文档示例偶发无 data: 前缀，兼容裸 JSON 行
                            payload = line
                        if not payload or payload == "[DONE]":
                            continue
                        try:
                            ev = json.loads(payload)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(ev, dict):
                            continue
                        yield ev
                        obj = str(ev.get("object") or "")
                        if obj in ("complete", "error"):
                            return

    async def analyze_video_violation(
        self,
        *,
        user_id: str,
        company: str,
        filename: str,
        content: bytes,
        content_type: str | None = None,
        session_id: str | None = None,
        extra_fields: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """调用 /api/video/violation，消费 SSE 后返回 complete 事件字段（不含 object）。"""
        last_error = ""
        async for ev in self.analyze_video_violation_stream(
            user_id=user_id,
            company=company,
            filename=filename,
            content=content,
            content_type=content_type,
            session_id=session_id,
            extra_fields=extra_fields,
        ):
            obj = str(ev.get("object") or "")
            if obj == "complete":
                out = dict(ev)
                out.pop("object", None)
                return out
            if obj == "error":
                last_error = str(ev.get("detail") or ev.get("message") or "视频违章判定失败")
                raise AgentWorkerError(last_error)
        raise AgentWorkerError(last_error or "视频违章判定未返回 complete 事件")


agent_worker_client = AgentWorkerClient()

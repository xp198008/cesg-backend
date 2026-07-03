"""Agent Worker（AI 智能体）HTTP 客户端。

接口文档：docs/AI.PDF
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
    headers: list[tuple[str, str | bytes]] = [
        ("x-user-id", str(user_id)),
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

    async def analyze_video_violation(
        self,
        *,
        user_id: str,
        company: str,
        filename: str,
        content: bytes,
        content_type: str | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        url = f"{_base_url()}/api/video/violation"
        files = {"file": (filename, content, content_type or "video/mp4")}
        data: dict[str, str] = {"company": company, "user_id": str(user_id)}
        if session_id:
            data["session_id"] = session_id
        async with httpx.AsyncClient(timeout=self._video_timeout()) as client:
            resp = await client.post(url, files=files, data=data)
            if resp.status_code >= 400:
                detail = resp.text
                try:
                    j = resp.json()
                    detail = j.get("detail") if isinstance(j, dict) else detail
                except Exception:
                    pass
                raise AgentWorkerError(str(detail) or f"HTTP {resp.status_code}")
            return resp.json()


agent_worker_client = AgentWorkerClient()

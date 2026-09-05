"""Agent Worker（AI 智能体）HTTP 客户端。

对接文档：docs/AI.MD（v1.1/v1.2，2026-09-03）
基础地址默认 http://113.207.68.94:5002
"""
from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote, unquote

import httpx

from app.agent_worker_config import cached_runtime, refresh_ai_worker_cache

logger = logging.getLogger(__name__)


class AgentWorkerError(RuntimeError):
    """Agent Worker 调用失败。"""


async def _runtime() -> dict[str, Any]:
    data = cached_runtime()
    if data.get("base_url") or data.get("api_key"):
        return data
    return await refresh_ai_worker_cache()


def _base_url(runtime: dict[str, Any] | None = None) -> str:
    data = runtime if runtime is not None else cached_runtime()
    return str(data.get("base_url") or "").strip().rstrip("/")


def _api_key(runtime: dict[str, Any] | None = None) -> str:
    data = runtime if runtime is not None else cached_runtime()
    return str(data.get("api_key") or "").strip()


def _encode_company(company: str) -> str:
    """x-company 必须 percent-encode，服务端再解码（AI.MD §1.4）。"""
    return quote((company or "").strip(), safe="")


def _bearer_headers(runtime: dict[str, Any] | None = None) -> dict[str, str]:
    headers: dict[str, str] = {}
    key = _api_key(runtime)
    if key:
        headers["Authorization"] = f"Bearer {key}"
    return headers


def _auth_headers(user_id: str, company: str, runtime: dict[str, Any] | None = None) -> dict[str, str]:
    uid = str(user_id or "").strip() or "cesg_anonymous"
    headers = _bearer_headers(runtime)
    headers["x-user"] = uid
    headers["x-user-id"] = uid
    headers["x-company"] = _encode_company(company)
    return headers


def video_complete_failed(ev: dict[str, Any] | None) -> bool:
    """AI.MD §8：失败也可能以 complete 结束，不能当成无违章。"""
    if not isinstance(ev, dict):
        return False
    conclusion = str(ev.get("conclusion") or "").strip()
    analysis = str(ev.get("analysis") or "")
    return conclusion == "分析失败" or analysis.startswith("处理过程发生错误")


class AgentWorkerClient:
    def configured(self) -> bool:
        data = cached_runtime()
        return bool(data.get("enabled") and _base_url(data) and _api_key(data))

    def _timeout(self, runtime: dict[str, Any] | None = None) -> httpx.Timeout:
        sec = float((runtime or cached_runtime()).get("timeout_seconds") or 60)
        return httpx.Timeout(sec, connect=min(10.0, sec))

    def _video_timeout(self, runtime: dict[str, Any] | None = None) -> httpx.Timeout:
        sec = float((runtime or cached_runtime()).get("video_timeout_seconds") or 600)
        return httpx.Timeout(sec, connect=min(15.0, sec))

    async def _ensure_ready(self) -> dict[str, Any]:
        rt = await _runtime()
        if not rt.get("ready"):
            raise AgentWorkerError(str(rt.get("ready_reason") or "AI 接口未配置或未启用"))
        return rt

    def _raise_http(self, resp: httpx.Response, *, prefix: str = "") -> None:
        detail = (resp.text or "").strip()
        try:
            body = resp.json()
            if isinstance(body, dict) and body.get("detail") is not None:
                detail = str(body.get("detail"))
        except Exception:
            pass
        raise AgentWorkerError((prefix + (detail or f"HTTP {resp.status_code}")).strip())

    async def health(self) -> dict[str, Any]:
        await self._ensure_ready()
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
        await self._ensure_ready()
        url = f"{_base_url()}/api/chat"
        payload = {
            "session_id": session_id,
            "input": input_messages,
            "stream": stream,
        }
        headers = _auth_headers(user_id, company)
        headers["Content-Type"] = "application/json"
        async with httpx.AsyncClient(timeout=self._video_timeout()) as client:
            async with client.stream("POST", url, json=payload, headers=headers) as resp:
                if resp.status_code >= 400:
                    body = await resp.aread()
                    raise AgentWorkerError(body.decode("utf-8", "replace") or f"HTTP {resp.status_code}")
                async for chunk in resp.aiter_bytes():
                    if chunk:
                        yield chunk

    async def upload_chat_file(
        self,
        *,
        user_id: str,
        session_id: str,
        filename: str,
        content: bytes,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        """POST /file/upload，供 /api/chat 用路径引用图片（避免巨型 base64）。"""
        await self._ensure_ready()
        url = f"{_base_url()}/file/upload"
        files = {"file": (filename or "ocr.jpg", content, content_type or "image/jpeg")}
        data = {"user_id": str(user_id or "").strip() or "cesg_anonymous", "session_id": str(session_id)}
        async with httpx.AsyncClient(timeout=self._video_timeout()) as client:
            resp = await client.post(url, files=files, data=data, headers=_bearer_headers() or None)
            if resp.status_code >= 400:
                self._raise_http(resp, prefix="识别附件上传失败：")
            body = resp.json() if resp.content else {}
            return body if isinstance(body, dict) else {"data": body}

    async def chat_collect_text(
        self,
        *,
        user_id: str,
        company: str,
        session_id: str,
        input_messages: list[dict[str, Any]],
    ) -> str:
        """流式对话并拼接正文增量（仅 type=text 且 delta=true）。"""
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
        finished = False
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
            obj, typ = ev.get("object"), ev.get("type")
            if obj == "content" and typ == "text" and ev.get("delta"):
                text = ev.get("text")
                if text:
                    parts.append(str(text))
            elif obj == "response" and ev.get("status") == "completed":
                finished = True
                break
            elif obj == "response" and ev.get("status") == "error":
                raise AgentWorkerError(str(ev.get("detail") or "对话生成失败"))
            elif obj == "error":
                raise AgentWorkerError(str(ev.get("message") or ev.get("detail") or "对话生成失败"))
        if not finished and not parts:
            raise AgentWorkerError("对话未返回正文")
        return "".join(parts)

    async def cancel_chat(self, *, session_id: str) -> dict[str, Any]:
        await self._ensure_ready()
        url = f"{_base_url()}/api/cancel/{session_id}"
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.post(url, headers=_bearer_headers() or None)
            if resp.status_code >= 400:
                self._raise_http(resp)
            return resp.json()

    async def get_session(self, *, session_id: str) -> dict[str, Any]:
        await self._ensure_ready()
        url = f"{_base_url()}/api/sessions/{session_id}"
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.get(url, headers=_bearer_headers() or None)
            if resp.status_code >= 400:
                self._raise_http(resp)
            return resp.json()

    async def get_vehicle_summary(
        self,
        *,
        plate: str | None = None,
        car_id: int | None = None,
        gps_hours: int = 3,
    ) -> dict[str, Any]:
        """GET /api/vehicle/summary。"""
        if not (plate or "").strip() and car_id is None:
            raise AgentWorkerError("plate 与 car_id 必须至少提供一个")
        await self._ensure_ready()
        url = f"{_base_url()}/api/vehicle/summary"
        params: dict[str, Any] = {"gps_hours": max(1, min(72, int(gps_hours or 3)))}
        if car_id is not None:
            params["car_id"] = int(car_id)
        if (plate or "").strip():
            params["plate"] = plate.strip()
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.get(url, params=params, headers=_bearer_headers() or None)
            if resp.status_code >= 400:
                self._raise_http(resp)
            data = resp.json()
            return data if isinstance(data, dict) else {"data": data}

    def _resolve_kb_name(self, *, dataset_id: str | None = None, dataset_name: str | None = None) -> str:
        from app.ai_datasets import resolve_dataset_name

        return resolve_dataset_name(dataset_id=dataset_id, dataset_name=dataset_name)

    async def list_datasets(self) -> dict[str, Any]:
        await self._ensure_ready()
        url = f"{_base_url()}/api/knowledge/kb/datasets"
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.get(url, headers=_bearer_headers() or None)
            if resp.status_code >= 400:
                self._raise_http(resp, prefix="知识库清单失败：")
            return resp.json()

    def _filename_from_headers(self, headers: httpx.Headers, fallback: str = "") -> str:
        cd = headers.get("content-disposition") or ""
        starred = re.search(r"filename\*\s*=\s*(?:UTF-8''|utf-8'')([^;]+)", cd, re.I)
        if starred:
            return unquote(starred.group(1).strip().strip('"'))
        quoted = re.search(r'filename\s*=\s*"([^"]+)"', cd, re.I)
        if quoted:
            return quoted.group(1)
        plain = re.search(r"filename\s*=\s*([^;]+)", cd, re.I)
        if plain:
            return unquote(plain.group(1).strip().strip('"'))
        return fallback

    async def list_all_documents(
        self,
        *,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
    ) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        page = 1
        total = 0
        while page <= 50:
            data = await self.list_documents(
                dataset_id=dataset_id,
                dataset_name=dataset_name,
                page=page,
                page_size=100,
            )
            batch = data.get("items") if isinstance(data, dict) else None
            if not isinstance(batch, list):
                batch = []
            items.extend([it for it in batch if isinstance(it, dict)])
            total = int((data or {}).get("total") or len(items))
            if not batch or len(items) >= total:
                break
            page += 1
        return {"items": items, "total": total or len(items)}

    async def download_document_file(
        self,
        document_id: str,
        *,
        fallback_name: str = "",
    ) -> dict[str, Any]:
        await self._ensure_ready()
        doc_id = str(document_id or "").strip()
        if not doc_id:
            raise AgentWorkerError("文档 ID 无效")
        url = f"{_base_url()}/api/knowledge/kb/documents/{doc_id}/file"
        async with httpx.AsyncClient(timeout=self._video_timeout()) as client:
            resp = await client.get(url, headers=_bearer_headers() or None)
            if resp.status_code >= 400:
                self._raise_http(resp, prefix="知识库原文下载失败：")
            name = self._filename_from_headers(resp.headers, fallback_name)
            return {
                "content": resp.content,
                "filename": name or fallback_name or f"{doc_id}.bin",
                "content_type": resp.headers.get("content-type") or "application/octet-stream",
            }

    async def list_documents(
        self,
        *,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        page: int = 1,
        page_size: int = 20,
        keyword: str | None = None,
        category: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_ready()
        name = self._resolve_kb_name(dataset_id=dataset_id, dataset_name=dataset_name)
        url = f"{_base_url()}/api/knowledge/kb/documents"
        params: dict[str, Any] = {"dataset_name": name, "page": page, "page_size": page_size}
        if keyword:
            params["keyword"] = keyword
        if category:
            params["category"] = category
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.get(url, params=params, headers=_bearer_headers() or None)
            if resp.status_code >= 400:
                self._raise_http(resp, prefix="知识库文档列表失败：")
            return resp.json()

    async def _wait_kb_task(self, task_id: str, *, timeout_sec: float = 120.0) -> dict[str, Any]:
        import asyncio

        await self._ensure_ready()
        url = f"{_base_url()}/api/knowledge/kb/tasks/{task_id}"
        deadline = asyncio.get_event_loop().time() + timeout_sec
        last: dict[str, Any] = {}
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            while True:
                resp = await client.get(url, headers=_bearer_headers() or None)
                if resp.status_code >= 400:
                    self._raise_http(resp, prefix="知识库任务查询失败：")
                parsed = resp.json()
                last = parsed if isinstance(parsed, dict) else {}
                status = str(last.get("status") or "").strip()
                if status in ("done", "succeeded"):
                    return last
                if status == "failed":
                    raise AgentWorkerError(str(last.get("error") or "知识库索引失败"))
                if asyncio.get_event_loop().time() >= deadline:
                    raise AgentWorkerError("知识库索引超时，请稍后在任务列表查看")
                await asyncio.sleep(2.5)

    async def upload_document(
        self,
        *,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
        filename: str,
        content: bytes,
        content_type: str | None = None,
        category: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_ready()
        name = self._resolve_kb_name(dataset_id=dataset_id, dataset_name=dataset_name)
        url = f"{_base_url()}/api/knowledge/kb/documents"
        files = {"file": (filename, content, content_type or "application/octet-stream")}
        data: dict[str, str] = {"dataset_name": name}
        if category:
            data["category"] = category
        async with httpx.AsyncClient(timeout=self._video_timeout()) as client:
            resp = await client.post(url, files=files, data=data, headers=_bearer_headers() or None)
            if resp.status_code >= 400:
                self._raise_http(resp, prefix="知识库上传失败：")
            body = resp.json() if resp.content else {}
        if not isinstance(body, dict):
            return {"data": body}
        task_id = str(body.get("task_id") or "").strip()
        if task_id and str(body.get("status") or "") in ("", "processing", "pending", "running"):
            try:
                task = await self._wait_kb_task(task_id)
                body["task"] = task
                body["status"] = task.get("status") or body.get("status")
            except AgentWorkerError:
                raise
        return body

    async def delete_document(
        self,
        *,
        document_id: str,
        dataset_id: str | None = None,
        dataset_name: str | None = None,
    ) -> None:
        del dataset_id, dataset_name
        await self._ensure_ready()
        url = f"{_base_url()}/api/knowledge/kb/documents/{document_id}"
        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            resp = await client.delete(url, headers=_bearer_headers() or None)
            if resp.status_code >= 400 and resp.status_code != 204:
                self._raise_http(resp, prefix="知识库删除失败：")

    async def analyze_video_violation_stream(
        self,
        *,
        user_id: str,
        company: str,
        filename: str | None = None,
        content: bytes | None = None,
        content_type: str | None = None,
        images: list[dict[str, Any]] | None = None,
        session_id: str | None = None,
        extra_fields: dict[str, str] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """POST /api/video/violation：视频用 file，图片用 images[]，二选一。"""
        import json

        await self._ensure_ready()
        url = f"{_base_url()}/api/video/violation"
        data: dict[str, str] = {"company": company, "user_id": str(user_id)}
        if session_id:
            data["session_id"] = session_id
        if extra_fields:
            for key, value in extra_fields.items():
                if key and value is not None and str(value).strip():
                    data[str(key)] = str(value).strip()

        image_items = [it for it in (images or []) if it.get("content")]
        files: Any
        if image_items:
            files = [
                (
                    "images",
                    (
                        str(it.get("filename") or f"image_{i + 1}.jpg"),
                        it["content"],
                        str(it.get("content_type") or "image/jpeg"),
                    ),
                )
                for i, it in enumerate(image_items[:9])
            ]
        elif content:
            files = {"file": (filename or "video.mp4", content, content_type or "video/mp4")}
        else:
            raise AgentWorkerError("违章判定必须提供视频 file 或图片 images")

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
                        if not line or line.startswith(":"):
                            continue
                        if line.startswith("data:"):
                            payload = line[5:].strip()
                        else:
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
        filename: str | None = None,
        content: bytes | None = None,
        content_type: str | None = None,
        images: list[dict[str, Any]] | None = None,
        session_id: str | None = None,
        extra_fields: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """调用 /api/video/violation，返回 complete 字段；分析失败抛错。"""
        last_error = ""
        async for ev in self.analyze_video_violation_stream(
            user_id=user_id,
            company=company,
            filename=filename,
            content=content,
            content_type=content_type,
            images=images,
            session_id=session_id,
            extra_fields=extra_fields,
        ):
            obj = str(ev.get("object") or "")
            if obj == "complete":
                if video_complete_failed(ev):
                    raise AgentWorkerError(
                        str(ev.get("analysis") or ev.get("conclusion") or "违章判定分析失败")
                    )
                out = dict(ev)
                out.pop("object", None)
                return out
            if obj == "error":
                last_error = str(ev.get("detail") or ev.get("message") or "视频违章判定失败")
                raise AgentWorkerError(last_error)
        raise AgentWorkerError(last_error or "视频违章判定未返回 complete 事件")


agent_worker_client = AgentWorkerClient()

"""Agent Worker AI 接口代理（聊天 / 知识库 / 视频违章判定）。"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent_worker_client import AgentWorkerError, agent_worker_client
from app.ai_datasets import AI_DATASETS, resolve_ai_company, resolve_dataset_id
from app.config import settings
from app.database import get_db
from app.models import OrgCompany, SysUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ai", tags=["ai"])


class ChatContentBlock(BaseModel):
    type: str
    text: str | None = None
    file_url: str | None = None
    file_type: str | None = None
    image_url: str | None = None


class ChatMessage(BaseModel):
    role: str
    content: list[ChatContentBlock]


class ChatRequest(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=128)
    input: list[ChatMessage] = Field(..., min_length=1)
    stream: bool = True
    company: str | None = Field(None, max_length=128, description="覆盖 x-company，如 垫江公司")


class VideoUrlRequest(BaseModel):
    video_url: str = Field(..., min_length=8, max_length=2048)
    company: str | None = Field(None, max_length=128)
    session_id: str | None = Field(None, max_length=128)


async def _resolve_user_org_name(db: AsyncSession, user_id: str | None) -> str:
    if not user_id or not str(user_id).isdigit():
        return ""
    uid = int(user_id)
    row = await db.scalar(
        select(SysUser).where(SysUser.id == uid).limit(1)
    )
    if row is None or row.org_id is None:
        return ""
    org = await db.scalar(select(OrgCompany).where(OrgCompany.id == row.org_id).limit(1))
    return (org.name if org else "") or ""


async def _ai_context(
    db: AsyncSession,
    *,
    x_user_id: str | None,
    company_override: str | None = None,
) -> tuple[str, str]:
    user_id = (x_user_id or "cesg_anonymous").strip() or "cesg_anonymous"
    org_name = await _resolve_user_org_name(db, user_id if user_id.isdigit() else None)
    company = resolve_ai_company(org_name, override=company_override)
    return user_id, company


def _ensure_configured() -> None:
    if not agent_worker_client.configured():
        raise HTTPException(status_code=503, detail="Agent Worker 未配置（AGENT_WORKER_BASE_URL）")


@router.get("/health")
async def ai_health():
    _ensure_configured()
    try:
        data = await agent_worker_client.health()
        return {"ok": True, "data": data}
    except Exception as exc:
        logger.warning("Agent Worker health failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"Agent Worker 不可达：{exc}") from exc


@router.get("/datasets")
async def ai_datasets():
    items = [{"name": name, "dataset_id": dataset_id} for name, dataset_id in AI_DATASETS.items()]
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/company")
async def ai_company(
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    company: str | None = Query(None),
):
    user_id, resolved = await _ai_context(db, x_user_id=x_user_id, company_override=company)
    org_name = await _resolve_user_org_name(db, user_id if user_id.isdigit() else None)
    return {
        "ok": True,
        "data": {
            "user_id": user_id,
            "org_name": org_name,
            "company": resolved,
            "dataset_id": resolve_dataset_id(resolved),
        },
    }


@router.post("/chat")
async def ai_chat(
    payload: ChatRequest,
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    _ensure_configured()
    user_id, company = await _ai_context(db, x_user_id=x_user_id, company_override=payload.company)
    input_messages = [m.model_dump(exclude_none=True) for m in payload.input]

    async def event_stream():
        try:
            async for chunk in agent_worker_client.chat_stream(
                user_id=user_id,
                company=company,
                session_id=payload.session_id,
                input_messages=input_messages,
                stream=payload.stream,
            ):
                yield chunk
        except AgentWorkerError as exc:
            msg = f'data: {{"object":"error","message":{repr(str(exc))}}}\n\n'
            yield msg.encode("utf-8")
        except Exception as exc:
            logger.exception("AI chat stream failed")
            msg = f'data: {{"object":"error","message":{repr(str(exc))}}}\n\n'
            yield msg.encode("utf-8")

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/cancel/{session_id}")
async def ai_cancel(session_id: str):
    _ensure_configured()
    try:
        data = await agent_worker_client.cancel_chat(session_id=session_id)
        return {"ok": True, "data": data}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/sessions/{session_id}")
async def ai_session(session_id: str):
    _ensure_configured()
    try:
        data = await agent_worker_client.get_session(session_id=session_id)
        return {"ok": True, "data": data}
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/knowledge/datasets/{dataset_id}/documents")
async def ai_list_documents(
    dataset_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    keyword: str | None = Query(None),
    category: str | None = Query(None),
):
    _ensure_configured()
    try:
        data = await agent_worker_client.list_documents(
            dataset_id=dataset_id,
            page=page,
            page_size=page_size,
            keyword=keyword,
            category=category,
        )
        return {"ok": True, "data": data}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/knowledge/datasets/{dataset_id}/documents/upload")
async def ai_upload_document(
    dataset_id: str,
    file: UploadFile = File(...),
    category: str | None = Form(None),
):
    _ensure_configured()
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="文件不能为空")
    try:
        data = await agent_worker_client.upload_document(
            dataset_id=dataset_id,
            filename=file.filename or "upload.bin",
            content=content,
            content_type=file.content_type,
            category=category,
        )
        return {"ok": True, "data": data}
    except AgentWorkerError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.delete("/knowledge/datasets/{dataset_id}/documents/{document_id}")
async def ai_delete_document(dataset_id: str, document_id: str):
    _ensure_configured()
    try:
        await agent_worker_client.delete_document(dataset_id=dataset_id, document_id=document_id)
        return {"ok": True}
    except AgentWorkerError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/video/violation")
async def ai_video_violation(
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    file: UploadFile = File(...),
    company: str | None = Form(None),
    session_id: str | None = Form(None),
):
    _ensure_configured()
    user_id, resolved_company = await _ai_context(db, x_user_id=x_user_id, company_override=company)
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="视频文件不能为空")
    try:
        data = await agent_worker_client.analyze_video_violation(
            user_id=user_id,
            company=resolved_company,
            filename=file.filename or "video.mp4",
            content=content,
            content_type=file.content_type,
            session_id=session_id,
        )
        return {"ok": True, "data": data, "company": resolved_company}
    except AgentWorkerError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/video/violation-by-url")
async def ai_video_violation_by_url(
    payload: VideoUrlRequest,
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    """下载远程视频后转发至 Agent Worker 违章判定接口。"""
    _ensure_configured()
    user_id, resolved_company = await _ai_context(
        db, x_user_id=x_user_id, company_override=payload.company
    )
    url = (payload.video_url or "").strip()
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail="video_url 须为 http(s) 地址")

    suffix = Path(parsed.path).suffix.lower() or ".mp4"
    if suffix not in {".mp4", ".avi", ".mov", ".mkv", ".flv"}:
        suffix = ".mp4"

    try:
        async with httpx.AsyncClient(timeout=agent_worker_client._video_timeout(), follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            content = resp.content
            content_type = resp.headers.get("content-type") or "video/mp4"
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"下载视频失败：{exc}") from exc

    if not content:
        raise HTTPException(status_code=400, detail="视频内容为空")

    filename = Path(parsed.path).name or f"violation{suffix}"
    try:
        data = await agent_worker_client.analyze_video_violation(
            user_id=user_id,
            company=resolved_company,
            filename=filename,
            content=content,
            content_type=content_type,
            session_id=payload.session_id,
        )
        return {"ok": True, "data": data, "company": resolved_company}
    except AgentWorkerError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

"""Agent Worker AI 接口代理（聊天 / 知识库 / 视频违章判定）。"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import httpx
from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent_worker_client import AgentWorkerError, agent_worker_client
from app.agent_worker_config import (
    config_out,
    ensure_ai_worker_config,
    save_ai_worker_config,
)
from app.ai_datasets import AI_DATASETS, resolve_ai_company, resolve_dataset_id
from app.ai_stream_sanitize import sanitize_sse_bytes
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


class AgentWorkerConfigBody(BaseModel):
    provider: str = "agent"
    enabled: bool = True
    base_url: str | None = None
    api_key: str | None = None
    default_company: str | None = "三峰城服"
    timeout_seconds: int | None = Field(default=60, ge=5, le=600)
    video_timeout_seconds: int | None = Field(default=600, ge=30, le=3600)
    remark: str | None = None


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


async def _ensure_configured() -> None:
    if not await agent_worker_client.configured_async():
        from app.agent_worker_config import cached_runtime

        reason = cached_runtime().get("ready_reason") or "AI 接口未配置或未启用"
        raise HTTPException(status_code=503, detail=reason)


@router.get("/worker-config")
async def ai_worker_config_get(db: AsyncSession = Depends(get_db)):
    row = await ensure_ai_worker_config(db)
    data = config_out(row)
    return {
        "ok": True,
        "data": data,
        "ready": bool(data.get("ready")),
        "ready_reason": data.get("ready_reason"),
    }


@router.put("/worker-config")
async def ai_worker_config_put(body: AgentWorkerConfigBody, db: AsyncSession = Depends(get_db)):
    row = await save_ai_worker_config(
        db,
        provider=body.provider,
        enabled=body.enabled,
        base_url=body.base_url,
        api_key=body.api_key,
        default_company=body.default_company,
        timeout_seconds=body.timeout_seconds,
        video_timeout_seconds=body.video_timeout_seconds,
        remark=body.remark,
    )
    data = config_out(row)
    return {
        "ok": True,
        "data": data,
        "ready": bool(data.get("ready")),
        "ready_reason": data.get("ready_reason"),
    }


@router.post("/worker-config/test")
async def ai_worker_config_test():
    """用当前已保存配置探测 Agent Worker /health。"""
    try:
        data = await agent_worker_client.health()
        return {"ok": True, "data": data, "message": "连通正常"}
    except AgentWorkerError as exc:
        return {"ok": False, "message": str(exc)}
    except Exception as exc:
        return {"ok": False, "message": f"不可达：{exc}"}


@router.get("/health")
async def ai_health():
    await _ensure_configured()
    try:
        data = await agent_worker_client.health()
        return {"ok": True, "data": data}
    except Exception as exc:
        logger.warning("Agent Worker health failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"Agent Worker 不可达：{exc}") from exc


@router.get("/datasets")
async def ai_datasets():
    items = [
        {"name": name, "dataset_id": dataset_id, "dataset_name": name}
        for name, dataset_id in AI_DATASETS.items()
    ]
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
            "dataset_name": resolved,
            "dataset_id": resolve_dataset_id(resolved),
        },
    }


@router.post("/chat")
async def ai_chat(
    payload: ChatRequest,
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    await _ensure_configured()
    user_id, company = await _ai_context(db, x_user_id=x_user_id, company_override=payload.company)
    input_messages = [m.model_dump(exclude_none=True) for m in payload.input]
    try:
        input_messages = await _inject_cesg_mileage_context(db, input_messages)
    except Exception:
        logger.exception("inject CESG mileage into chat failed")

    async def event_stream():
        try:
            async for chunk in sanitize_sse_bytes(
                agent_worker_client.chat_stream(
                    user_id=user_id,
                    company=company,
                    session_id=payload.session_id,
                    input_messages=input_messages,
                    stream=payload.stream,
                )
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
    await _ensure_configured()
    try:
        data = await agent_worker_client.cancel_chat(session_id=session_id)
        return {"ok": True, "data": data}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


_PLATE_RE = re.compile(
    r"[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼]"
    r"[A-Z][A-Z0-9]{5,6}"
)


def _chat_text_blob(input_messages: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for msg in input_messages or []:
        for block in msg.get("content") or []:
            if not isinstance(block, dict):
                continue
            text = str(block.get("text") or "").strip()
            if text:
                parts.append(text)
    return "\n".join(parts)


def _plates_from_chat(text: str) -> list[str]:
    seen: list[str] = []
    for hit in _PLATE_RE.findall(text or ""):
        plate = str(hit).strip().upper()
        if plate and plate not in seen:
            seen.append(plate)
    return seen[:8]


async def _inject_cesg_mileage_context(
    db: AsyncSession,
    input_messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """对话里注入统一日行驶：有 OBD 用 OBD 起止差，无 OBD 用 GPS 日结。"""
    from datetime import timedelta

    from app.obd_mileage_daily import query_obd_mileage_daily
    from app.timeutil import china_now_naive

    blob = _chat_text_blob(input_messages)
    plates = _plates_from_chat(blob)
    today = china_now_naive()
    start = (today - timedelta(days=6)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    lines = [
        "【CESG统一日行驶，回答当天/近七日行驶公里必须用下面数字。",
        "有OBD：当天=当前总里程−昨日结束；无OBD：用GPS日结dtlc。",
        "不要把累计表显 mileage 当成当天行驶。】",
    ]
    if plates:
        items = await query_obd_mileage_daily(
            db, start_day=start, end_day=end, plates=plates, backfill=True
        )
        by_plate: dict[str, list[dict[str, Any]]] = {}
        for item in items:
            plate = str(item.get("plate_no") or item.get("vehicle") or "").strip()
            if plate:
                by_plate.setdefault(plate, []).append(item)
        for plate in plates:
            days = by_plate.get(plate) or []
            today_row = next((x for x in days if str(x.get("day") or "") == end), {})
            week_km = round(sum(float(x.get("mileage") or 0) for x in days), 2)
            today_km = today_row.get("drive_km")
            if today_km is None:
                today_km = today_row.get("mileage")
            source = str(today_row.get("source") or "")
            kind = "GPS日结" if source.startswith("gps") else ("OBD异常" if "abnormal" in source else "OBD")
            lines.append(
                f"{plate} 今日{today_km if today_km is not None else '—'}km "
                f"近7日{week_km}km 来源{kind}({source or '无'})"
            )
    else:
        lines.append("用户未点名车牌时：若技能返回了累计表显，先说明那是总里程，当天公里以CESG统一日行驶为准。")
    context = {
        "role": "user",
        "content": [{"type": "text", "text": "\n".join(lines)}],
    }
    return [context, *(input_messages or [])]


async def _attach_cesg_mileage(db: AsyncSession, data: dict[str, Any], plate: str | None) -> dict[str, Any]:
    """用 CESG 统一日行驶覆盖摘要里的累计表显 / 空 drive_km。"""
    from datetime import timedelta

    from app.obd_mileage_daily import query_obd_mileage_daily
    from app.timeutil import china_now_naive

    plate_no = (plate or data.get("plate") or "").strip()
    if not plate_no:
        return data
    today = china_now_naive()
    start = (today - timedelta(days=6)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    try:
        items = await query_obd_mileage_daily(
            db, start_day=start, end_day=end, plates=[plate_no], backfill=True
        )
    except Exception:  # noqa: BLE001
        return data
    by_day = {str(x.get("day") or ""): x for x in items}
    today_row = by_day.get(end) or {}
    data["cesg_mileage"] = {
        "today_km": today_row.get("drive_km") if today_row.get("drive_km") is not None else today_row.get("mileage"),
        "week_km": round(sum(float(x.get("mileage") or 0) for x in items), 2),
        "source": today_row.get("source") or "",
        "abnormal": bool(today_row.get("abnormal")),
        "days": [
            {
                "day": x.get("day"),
                "drive_km": x.get("drive_km") if x.get("drive_km") is not None else x.get("mileage"),
                "start_mileage": x.get("startMileage"),
                "end_mileage": x.get("endMileage"),
                "source": x.get("source"),
            }
            for x in items
        ],
    }
    status = data.get("current_status")
    if isinstance(status, dict) and today_row:
        status["drive_km"] = data["cesg_mileage"]["today_km"]
        status["dtlc"] = data["cesg_mileage"]["today_km"]
    fuels = data.get("fuel_prev_week")
    if isinstance(fuels, list):
        for row in fuels:
            if not isinstance(row, dict):
                continue
            day = str(row.get("day") or "").replace("-", "")[:8]
            hit = by_day.get(day)
            if not hit:
                continue
            km = hit.get("drive_km") if hit.get("drive_km") is not None else hit.get("mileage")
            row["drive_km"] = km
            row["start_mileage"] = hit.get("startMileage")
            row["end_mileage"] = hit.get("endMileage")
            fuel = row.get("fuel_l")
            try:
                if fuel is not None and km and float(km) > 0.1:
                    row["fuel_per_100km"] = round(float(fuel) / float(km) * 100.0, 2)
            except (TypeError, ValueError):
                pass
    return data


@router.get("/vehicle/summary")
async def ai_vehicle_summary(
    plate: str | None = Query(None),
    car_id: int | None = Query(None, ge=1),
    gps_hours: int = Query(3, ge=1, le=72),
    db: AsyncSession = Depends(get_db),
):
    """代理 Agent Worker 车辆综合摘要，并叠 CESG 统一日行驶里程。"""
    await _ensure_configured()
    if not (plate or "").strip() and car_id is None:
        raise HTTPException(status_code=400, detail="plate 与 car_id 必须至少提供一个")
    try:
        data = await agent_worker_client.get_vehicle_summary(
            plate=plate,
            car_id=car_id,
            gps_hours=gps_hours,
        )
        if isinstance(data, dict):
            data = await _attach_cesg_mileage(db, data, plate)
        return {"ok": True, "data": data}
    except AgentWorkerError as exc:
        text = str(exc)
        status = 404 if "未找到" in text else 502
        raise HTTPException(status_code=status, detail=text) from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text) from exc


@router.get("/vehicle/risk-assessment")
async def ai_vehicle_risk_assessment(
    plate: str | None = Query(None),
    car_id: int | None = Query(None, ge=1),
):
    """代理 Agent Worker 车辆风险定级（风险管控-风险智能分析综合风险分）。"""
    await _ensure_configured()
    if not (plate or "").strip() and car_id is None:
        raise HTTPException(status_code=400, detail="plate 与 car_id 必须至少提供一个")
    try:
        data = await agent_worker_client.get_vehicle_risk_assessment(
            plate=plate,
            car_id=car_id,
        )
        return {"ok": True, "data": data}
    except AgentWorkerError as exc:
        text = str(exc)
        status = 404 if "未找到" in text else 502
        raise HTTPException(status_code=status, detail=text) from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text) from exc


@router.get("/attachment")
async def ai_attachment(
    url: str = Query(..., min_length=4, max_length=2048),
    filename: str | None = Query(None, max_length=255),
):
    """经 HTTPS 代理拉取 Agent Worker 附件，避免浏览器拦截 HTTP 不安全下载。"""
    await _ensure_configured()
    safe_name = "".join(ch for ch in str(filename or "") if ch not in "/\\").strip() or None
    try:
        file = await agent_worker_client.fetch_attachment(url, fallback_name=safe_name or "")
    except AgentWorkerError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    name = safe_name or file.get("filename") or "download"
    ascii_name = name.encode("ascii", "ignore").decode("ascii") or "download"
    return Response(
        content=file["content"],
        media_type=file.get("content_type") or "application/octet-stream",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{quote(name)}'
            ),
        },
    )


@router.get("/sessions/{session_id}")
async def ai_session(session_id: str):
    await _ensure_configured()
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
    await _ensure_configured()
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
    await _ensure_configured()
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
    await _ensure_configured()
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
    await _ensure_configured()
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
    await _ensure_configured()
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

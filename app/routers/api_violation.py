"""主动安全/违章报警兼容接口。

为旧版 carManagerV11 安全管理页面提供最小可用的列表、处理、审核和状态流转能力。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Header, Query, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import OrgCompany, Vehicle, VehicleViolation, ViolationTicket, ViolationTypeDict
from app.org_scope import collect_org_company_subtree_ids, require_x_org_id_header

router = APIRouter(prefix="/api/violation", tags=["violation"])


class ViolationHandleIn(BaseModel):
    action: str = Field("confirm", max_length=32)
    remark: str | None = Field(None, max_length=2000)
    handler_name: str | None = Field(None, max_length=64)


class ViolationAuditIn(BaseModel):
    result: str = Field(..., max_length=32)
    remark: str | None = Field(None, max_length=2000)
    auditor_name: str | None = Field(None, max_length=64)


class TicketProcessIn(BaseModel):
    remark: str | None = Field(None, max_length=2000)


class TicketAppealIn(BaseModel):
    remark: str | None = Field(None, max_length=2000)


_TICKET_APPEAL_ALLOWED_EXTS = {".xls", ".xlsx", ".doc", ".docx", ".pdf", ".jpg", ".jpeg", ".bmp", ".png", ".txt"}
_TICKET_APPEAL_MAX_FILE_BYTES = 20 * 1024 * 1024


def _ticket_appeal_attachment_base_dir() -> Path:
    return Path(__file__).resolve().parent.parent.parent / "data" / "ticket_appeal_attachments"


def _now() -> datetime:
    return datetime.now()


def _json_loads(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except Exception:
        return fallback


async def _save_ticket_appeal_upload(violation_id: int, file: UploadFile) -> dict[str, Any]:
    original = (file.filename or "").strip()
    if not original:
        raise HTTPException(status_code=400, detail="附件缺少文件名")
    ext = Path(original).suffix.lower()
    if ext not in _TICKET_APPEAL_ALLOWED_EXTS:
        raise HTTPException(status_code=400, detail="申诉附件仅支持 EXCEL、WORD、PDF、JPG、BMP、PNG、TXT")

    chunks: list[bytes] = []
    total = 0
    while True:
        piece = await file.read(1024 * 1024)
        if not piece:
            break
        total += len(piece)
        if total > _TICKET_APPEAL_MAX_FILE_BYTES:
            raise HTTPException(status_code=400, detail=f"附件 {original} 超过 20MB")
        chunks.append(piece)
    if total <= 0:
        raise HTTPException(status_code=400, detail=f"附件 {original} 为空")

    root = _ticket_appeal_attachment_base_dir()
    sub = root / str(int(violation_id))
    sub.mkdir(parents=True, exist_ok=True)
    stored = f"{uuid.uuid4().hex}{ext}"
    dest = sub / stored
    dest.write_bytes(b"".join(chunks))
    rel = f"{int(violation_id)}/{stored}"
    return {
        "name": original[:255],
        "size": total,
        "rel": rel,
        "url": f"/cmmedia/ticket-appeal-attachments/{rel}",
    }


def _dt_text(v: datetime | None) -> str | None:
    return v.strftime("%Y-%m-%d %H:%M:%S") if v else None


def _row_out(row: VehicleViolation, ticket_by_biz: dict[str, ViolationTicket] | None = None) -> dict:
    ticket_by_biz = ticket_by_biz or {}
    ticket = ticket_by_biz.get(row.biz_no or "")
    evidence = _json_loads(row.ttx_evidence_refs, {})
    return {
        "id": row.id,
        "biz_no": row.biz_no,
        "external_alarm_id": row.external_alarm_id,
        "terminal_id": row.terminal_id,
        "vehicle_id": row.vehicle_id,
        "plate_no": row.plate_no,
        "company_id": row.company_id,
        "violation_type_code": row.violation_type_code,
        "violation_type_name": row.violation_type_name,
        "violation_time": _dt_text(row.violation_time),
        "lat": row.lat,
        "lng": row.lng,
        "address": row.address,
        "source": row.source,
        "transparent_type": row.transparent_type,
        "raw_preview": row.raw_preview,
        "stream_snapshot_refs": _json_loads(row.stream_snapshot_refs, []),
        "stream_snapshot_paths": _json_loads(row.stream_snapshot_refs, []),
        "ttx_evidence_refs": _json_loads(row.ttx_evidence_refs, []),
        "evidence_images": evidence.get("images", []) if isinstance(evidence, dict) else [],
        "evidence_videos": evidence.get("videos", []) if isinstance(evidence, dict) else [],
        "status": row.status,
        "pre_audit_kind": row.pre_audit_kind,
        "ticket_appeal_remark": row.ticket_appeal_remark,
        "ticket_appeal_attachments": _json_loads(row.ticket_appeal_attachment_refs, []),
        "handler_remark": row.handler_remark,
        "handler_name": row.handler_name,
        "handled_at": _dt_text(row.handled_at),
        "auditor_name": row.auditor_name,
        "audited_at": _dt_text(row.audited_at),
        "audit_reject_remark": row.audit_reject_remark,
        "appeal_reason": row.appeal_reason,
        "appeal_submitted_at": _dt_text(row.appeal_submitted_at),
        "appeal_status": row.appeal_status,
        "created_at": _dt_text(row.created_at),
        "has_ticket": ticket is not None,
        "has_violation_ticket": ticket is not None,
        "ticket_process_type": ticket.process_type if ticket else None,
        "ticket_amount": ticket.amount if ticket else None,
        "ticket_remark": ticket.remark if ticket else None,
        "ticket_status": ticket.status if ticket else None,
        "ticket_created_by_name": ticket.created_by_name if ticket else None,
        "ticket_created_at": _dt_text(ticket.created_at) if ticket else None,
        "source_label": {
            "jt808_adas": "JT808 ADAS",
            "jt808_dsm": "JT808 DSM",
            "manual": "人工录入",
        }.get((row.source or "").strip(), row.source or ""),
    }


async def _scoped_query(db: AsyncSession, x_org_id: str | None):
    q = select(VehicleViolation)
    if x_org_id:
        root = require_x_org_id_header(x_org_id)
        exists = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
        if exists:
            subtree = await collect_org_company_subtree_ids(db, root)
            q = q.where(or_(VehicleViolation.company_id.in_(subtree), VehicleViolation.company_id.is_(None)))
    return q


@router.get("/list")
async def violation_list(
    status: str | None = Query(None),
    plate_no: str | None = Query(None),
    terminal_id: str | None = Query(None),
    source: str | None = Query(None),
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    limit: int | None = Query(None, ge=1, le=2000),
    offset: int | None = Query(None, ge=0),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    q = await _scoped_query(db, x_org_id)
    if status:
        if status == "pending":
            q = q.where(
                or_(
                    VehicleViolation.status == "待处理",
                    and_(VehicleViolation.status == "待审核", VehicleViolation.pre_audit_kind == "preprocess"),
                )
            )
        elif status == "handled":
            q = q.where(VehicleViolation.status == "已处理")
        else:
            q = q.where(VehicleViolation.status == status.strip())
    if plate_no:
        q = q.where(VehicleViolation.plate_no.ilike(f"%{plate_no.strip()}%"))
    if terminal_id:
        q = q.where(VehicleViolation.terminal_id.ilike(f"%{terminal_id.strip()}%"))
    if source:
        q = q.where(VehicleViolation.source == source.strip())
    if start_time:
        try:
            q = q.where(VehicleViolation.violation_time >= datetime.fromisoformat(start_time.replace("/", "-")))
        except ValueError:
            pass
    if end_time:
        try:
            q = q.where(VehicleViolation.violation_time <= datetime.fromisoformat(end_time.replace("/", "-")))
        except ValueError:
            pass

    total = await db.scalar(select(func.count()).select_from(q.subquery())) or 0
    lim = limit or page_size
    off = offset if offset is not None else (page - 1) * page_size
    rows = (await db.execute(q.order_by(VehicleViolation.violation_time.desc(), VehicleViolation.id.desc()).offset(off).limit(lim))).scalars().all()
    biz = [x.biz_no for x in rows if x.biz_no]
    ticket_by_biz: dict[str, ViolationTicket] = {}
    if biz:
        for ticket in (await db.execute(select(ViolationTicket).where(ViolationTicket.biz_no.in_(biz)))).scalars().all():
            if ticket.biz_no:
                ticket_by_biz[ticket.biz_no] = ticket
    return {"ok": True, "total": total, "items": [_row_out(x, ticket_by_biz) for x in rows], "page": page, "page_size": page_size}


@router.get("/{violation_id}/detail")
async def violation_detail(violation_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": _row_out(row)}


@router.post("/{violation_id}/fetch-device-media")
async def violation_fetch_device_media(violation_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    evidence = _json_loads(row.ttx_evidence_refs, {})
    if isinstance(evidence, dict):
        images = evidence.get("images") if isinstance(evidence.get("images"), list) else []
        videos = evidence.get("videos") if isinstance(evidence.get("videos"), list) else []
    else:
        images = []
        videos = []
    return {
        "ok": True,
        "message": "已读取本地同步的 JT808 证据",
        "images": images,
        "videos": videos,
        "downlink": [],
    }


@router.patch("/{violation_id}/handle")
async def violation_handle(violation_id: int, body: ViolationHandleIn, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    action = (body.action or "confirm").strip()
    row.handler_remark = body.remark
    row.handler_name = body.handler_name or "系统用户"
    row.handled_at = _now()
    if action in ("false_alarm", "false", "误报"):
        row.status = "误报"
        row.pre_audit_kind = "false_alarm"
    else:
        row.status = "待审核"
        row.pre_audit_kind = "preprocess"
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.patch("/{violation_id}/audit")
async def violation_audit(violation_id: int, body: ViolationAuditIn, db: AsyncSession = Depends(get_db)):
    """待审核 → 已处理 / 罚单待处理（生成罚单路径）/ 待处理（打回）。

    依据 ``pre_audit_kind`` 分流：
    - approve + ``ticket``        → 罚单待处理（进入罚单处理页）
    - approve + ``ticket_appeal`` → 已处理，并将关联罚单结案为「完成」
    - approve + 其它（确认/误报）  → 已处理
    - reject  + ``ticket_appeal`` → 退回「罚单待处理」（pre_audit_kind 恢复为 ticket）
    - reject  + 其它              → 退回「待处理」
    """
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if (row.status or "").strip() != "待审核":
        raise HTTPException(status_code=400, detail="仅「待审核」记录可进行审核确认或打回")

    result = (body.result or "").strip().lower()
    auditor = (body.auditor_name or "系统用户").strip()[:64]

    if result in ("approve", "approved", "同意", "通过", "agree"):
        pak = (row.pre_audit_kind or "").strip()
        if pak == "ticket":
            row.status = "罚单待处理"
        elif pak == "ticket_appeal":
            row.status = "已处理"
            row.pre_audit_kind = None
            ticket = await db.scalar(select(ViolationTicket).where(ViolationTicket.biz_no == row.biz_no).limit(1))
            if ticket:
                ticket.status = "完成"
        else:
            row.status = "已处理"
        row.auditor_name = auditor
        row.audited_at = _now()
        row.audit_reject_remark = None
    elif result in ("reject", "rejected", "驳回"):
        rr = (body.remark or "").strip()
        if not rr:
            raise HTTPException(status_code=400, detail="打回须填写打回意见")
        prev_pak = (row.pre_audit_kind or "").strip()
        row.handler_name = None
        row.handled_at = None
        row.handler_remark = None
        row.auditor_name = auditor
        row.audited_at = _now()
        row.audit_reject_remark = rr[:500]
        if prev_pak == "ticket_appeal":
            row.status = "罚单待处理"
            row.pre_audit_kind = "ticket"
        else:
            row.status = "待处理"
            row.pre_audit_kind = None
    else:
        raise HTTPException(status_code=400, detail="result 须为 approve（同意）或 reject（驳回）")

    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.patch("/{violation_id}/ticket-process-complete")
async def violation_ticket_complete(violation_id: int, body: TicketProcessIn, db: AsyncSession = Depends(get_db)):
    """罚单待处理 → 已处理，并将关联罚单结案为「完成」。"""
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if (row.status or "").strip() != "罚单待处理":
        raise HTTPException(status_code=400, detail="仅「罚单待处理」记录可操作处理完成")
    row.status = "已处理"
    row.handler_remark = body.remark or row.handler_remark
    row.handled_at = _now()
    ticket = await db.scalar(select(ViolationTicket).where(ViolationTicket.biz_no == row.biz_no).limit(1))
    if ticket:
        ticket.status = "完成"
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.patch("/{violation_id}/ticket-appeal-submit")
async def violation_ticket_appeal(violation_id: int, body: TicketAppealIn, db: AsyncSession = Depends(get_db)):
    """罚单待处理 → 待审核（罚单岗发起申诉），写入申诉说明并回到审核队列。"""
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if (row.status or "").strip() != "罚单待处理":
        raise HTTPException(status_code=400, detail="仅「罚单待处理」记录可提交申诉")
    rm = (body.remark or "").strip()
    if not rm:
        raise HTTPException(status_code=400, detail="申诉说明不能为空")
    row.status = "待审核"
    row.pre_audit_kind = "ticket_appeal"
    row.ticket_appeal_remark = rm[:2000]
    row.audit_reject_remark = None
    row.auditor_name = None
    row.audited_at = None
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.post("/{violation_id}/ticket-appeal-submit-with-attachments")
async def violation_ticket_appeal_with_attachments(
    violation_id: int,
    remark: str = Form(..., min_length=1, max_length=2000),
    files: list[UploadFile] | None = File(None),
    db: AsyncSession = Depends(get_db),
):
    """罚单待处理 → 待审核（罚单岗申诉），支持上传申诉附件。"""
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if (row.status or "").strip() != "罚单待处理":
        raise HTTPException(status_code=400, detail="仅「罚单待处理」记录可提交申诉")

    rm = (remark or "").strip()
    if not rm:
        raise HTTPException(status_code=400, detail="申诉说明不能为空")

    refs: list[dict[str, Any]] = []
    for f in files or []:
        refs.append(await _save_ticket_appeal_upload(violation_id, f))

    row.status = "待审核"
    row.pre_audit_kind = "ticket_appeal"
    row.ticket_appeal_remark = rm[:2000]
    row.ticket_appeal_attachment_refs = json.dumps(refs, ensure_ascii=False) if refs else None
    row.audit_reject_remark = None
    row.auditor_name = None
    row.audited_at = None
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.patch("/{violation_id}/false-alarm-reopen")
async def violation_false_alarm_reopen(violation_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    row.status = "待处理"
    row.pre_audit_kind = None
    row.appeal_status = None
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


"""主动安全报警关联罚单兼容接口。"""
from __future__ import annotations

from datetime import datetime

from app.timeutil import china_now_naive

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import VehicleViolation, ViolationTicket

router = APIRouter(prefix="/api/violation-ticket", tags=["violation-ticket"])


class ViolationTicketCreateIn(BaseModel):
    biz_no: str = Field(..., min_length=1, max_length=32)
    process_type: str = Field(..., min_length=1, max_length=64)
    remark: str | None = Field(None, max_length=2000)
    amount: float = Field(0.0, ge=0)
    created_by_name: str | None = Field(None, max_length=64)


def _row_out(row: ViolationTicket) -> dict:
    return {
        "id": row.id,
        "biz_no": row.biz_no,
        "violation_id": row.violation_id,
        "process_type": row.process_type,
        "remark": row.remark,
        "amount": row.amount,
        "status": row.status,
        "created_by_name": row.created_by_name,
        "created_at": row.created_at.isoformat(sep=" ", timespec="seconds") if row.created_at else None,
    }


@router.post("")
async def violation_ticket_create(body: ViolationTicketCreateIn, db: AsyncSession = Depends(get_db)):
    bn = body.biz_no.strip()
    creator = (body.created_by_name or "").strip()[:64] or None
    vio = await db.scalar(select(VehicleViolation).where(VehicleViolation.biz_no == bn).limit(1))
    if vio is None:
        raise HTTPException(status_code=404, detail="未找到对应报警记录")
    existing = await db.scalar(select(ViolationTicket).where(ViolationTicket.biz_no == bn).limit(1))
    if existing is not None:
        if vio.status == "待处理":
            vio.status = "待审核"
            vio.pre_audit_kind = "ticket"
            if creator:
                vio.handler_name = creator
                vio.handled_at = china_now_naive()
        if creator and not (existing.created_by_name or "").strip():
            existing.created_by_name = creator
        await db.flush()
        await db.refresh(existing)
        return {"ok": True, "item": _row_out(existing), "already_existed": True}
    row = ViolationTicket(
        biz_no=bn,
        violation_id=vio.id,
        process_type=body.process_type.strip(),
        remark=body.remark,
        amount=float(body.amount or 0),
        status="待处理",
        created_by_name=creator,
    )
    db.add(row)
    vio.status = "待审核"
    vio.pre_audit_kind = "ticket"
    if creator:
        vio.handler_name = creator
        vio.handled_at = china_now_naive()
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "item": _row_out(row)}


@router.get("/list")
async def violation_ticket_list(
    biz_no: str | None = Query(None),
    status: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    q = select(ViolationTicket)
    if biz_no:
        q = q.where(ViolationTicket.biz_no.ilike(f"%{biz_no.strip()}%"))
    if status:
        q = q.where(ViolationTicket.status == status.strip())
    total = await db.scalar(select(func.count()).select_from(q.subquery())) or 0
    rows = (
        await db.execute(q.order_by(ViolationTicket.id.desc()).offset((page - 1) * page_size).limit(page_size))
    ).scalars().all()
    return {"ok": True, "total": total, "items": [_row_out(x) for x in rows], "page": page, "page_size": page_size}


@router.get("/by-biz-no/{biz_no}")
async def violation_ticket_by_biz_no(biz_no: str, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(ViolationTicket).where(ViolationTicket.biz_no == biz_no).limit(1))
    if row is None:
        return {"ok": True, "item": None}
    return {"ok": True, "item": _row_out(row)}


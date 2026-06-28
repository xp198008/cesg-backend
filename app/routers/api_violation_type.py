"""违章类型字典：基础数据本地 CRUD（对齐 violation_type_maintenance.html）。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ViolationTypeDict

router = APIRouter(prefix="/api/violation-type", tags=["violation-type"])

_ALLOWED_SEVERITY = frozenset({"轻微", "一般", "严重"})


def _severity_or_400(raw: str | None) -> str:
    severity = (raw or "").strip() or "一般"
    if severity not in _ALLOWED_SEVERITY:
        raise HTTPException(status_code=400, detail="严重程度须为：轻微、一般、严重")
    return severity


class ViolationTypeCreateIn(BaseModel):
    type_code: str = Field(..., min_length=1, max_length=32)
    type_name: str = Field(..., min_length=1, max_length=64)
    description: str | None = Field(None, max_length=2000)
    severity: str = Field("一般")


class ViolationTypeUpdateIn(BaseModel):
    type_name: str | None = Field(None, min_length=1, max_length=64)
    description: str | None = Field(None, max_length=2000)
    severity: str | None = None


def _row_out(row: ViolationTypeDict) -> dict:
    return {
        "id": row.id,
        "type_code": row.type_code,
        "type_name": row.type_name,
        "description": row.description,
        "severity": row.severity,
        "deduction_score": row.deduction_score,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@router.get("/list")
async def violation_type_list(
    type_code: str | None = Query(None),
    type_name: str | None = Query(None),
    severity: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(ViolationTypeDict)
    if type_code and type_code.strip():
        stmt = stmt.where(ViolationTypeDict.type_code.ilike(f"%{type_code.strip()}%"))
    if type_name and type_name.strip():
        stmt = stmt.where(ViolationTypeDict.type_name.ilike(f"%{type_name.strip()}%"))
    if severity and severity.strip():
        stmt = stmt.where(ViolationTypeDict.severity == severity.strip())
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        await db.execute(
            stmt.order_by(ViolationTypeDict.id.desc()).offset((page - 1) * page_size).limit(page_size)
        )
    ).scalars().all()
    return {"total": total, "items": [_row_out(x) for x in rows], "page": page, "page_size": page_size}


@router.get("/{tid}")
async def violation_type_get(tid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(ViolationTypeDict).where(ViolationTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": _row_out(row)}


@router.post("")
async def violation_type_create(body: ViolationTypeCreateIn, db: AsyncSession = Depends(get_db)):
    code = body.type_code.strip()
    dup = await db.scalar(select(ViolationTypeDict.id).where(ViolationTypeDict.type_code == code).limit(1))
    if dup is not None:
        raise HTTPException(status_code=400, detail="违章类型编码已存在")
    row = ViolationTypeDict(
        type_code=code,
        type_name=body.type_name.strip(),
        description=(body.description or "").strip() or None,
        severity=_severity_or_400(body.severity),
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.patch("/{tid}")
async def violation_type_update(tid: int, body: ViolationTypeUpdateIn, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(ViolationTypeDict).where(ViolationTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if body.type_name is not None:
        row.type_name = body.type_name.strip()
    if body.description is not None:
        row.description = body.description.strip() or None
    if body.severity is not None:
        row.severity = _severity_or_400(body.severity)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.delete("/{tid}")
async def violation_type_delete(tid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(ViolationTypeDict).where(ViolationTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    await db.delete(row)
    await db.flush()
    return {"ok": True}

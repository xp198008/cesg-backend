"""故障类型字典：基础数据本地 CRUD。"""
from __future__ import annotations

import secrets
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import FaultTypeDict

router = APIRouter(prefix="/api/fault-type", tags=["fault-type"])

_ALLOWED_LEVEL = frozenset({"高", "中", "低"})


def _gen_type_code() -> str:
    return f"FT{datetime.now().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(2).upper()}"


async def _allocate_unique_type_code(db: AsyncSession) -> str:
    for _ in range(12):
        code = _gen_type_code()
        exists = await db.scalar(select(FaultTypeDict.id).where(FaultTypeDict.type_code == code).limit(1))
        if exists is None:
            return code
    raise HTTPException(status_code=500, detail="生成类型编码失败，请重试")


def _level_or_400(raw: str | None) -> str:
    level = (raw or "").strip() or "中"
    if level not in _ALLOWED_LEVEL:
        raise HTTPException(status_code=400, detail="故障级别须为：高、中、低")
    return level


class FaultTypeCreateIn(BaseModel):
    type_name: str = Field(..., min_length=1, max_length=64)
    description: str | None = Field(None, max_length=2000)
    fault_level: str = Field("中")


class FaultTypeUpdateIn(BaseModel):
    type_name: str | None = Field(None, min_length=1, max_length=64)
    description: str | None = Field(None, max_length=2000)
    fault_level: str | None = None


def _row_out(row: FaultTypeDict) -> dict:
    return {
        "id": row.id,
        "type_code": row.type_code,
        "type_name": row.type_name,
        "description": row.description,
        "fault_level": row.fault_level,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@router.get("/list")
async def fault_type_list(
    type_code: str | None = Query(None),
    type_name: str | None = Query(None),
    fault_level: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(FaultTypeDict)
    if type_code and type_code.strip():
        stmt = stmt.where(FaultTypeDict.type_code.ilike(f"%{type_code.strip()}%"))
    if type_name and type_name.strip():
        stmt = stmt.where(FaultTypeDict.type_name.ilike(f"%{type_name.strip()}%"))
    if fault_level and fault_level.strip():
        stmt = stmt.where(FaultTypeDict.fault_level == fault_level.strip())
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        await db.execute(
            stmt.order_by(FaultTypeDict.id.desc()).offset((page - 1) * page_size).limit(page_size)
        )
    ).scalars().all()
    return {"total": total, "items": [_row_out(x) for x in rows], "page": page, "page_size": page_size}


@router.get("/{tid}")
async def fault_type_get(tid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(FaultTypeDict).where(FaultTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": _row_out(row)}


@router.post("")
async def fault_type_create(body: FaultTypeCreateIn, db: AsyncSession = Depends(get_db)):
    row = FaultTypeDict(
        type_code=await _allocate_unique_type_code(db),
        type_name=body.type_name.strip(),
        description=(body.description or "").strip() or None,
        fault_level=_level_or_400(body.fault_level),
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.patch("/{tid}")
async def fault_type_update(tid: int, body: FaultTypeUpdateIn, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(FaultTypeDict).where(FaultTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if body.type_name is not None:
        row.type_name = body.type_name.strip()
    if body.description is not None:
        row.description = body.description.strip() or None
    if body.fault_level is not None:
        row.fault_level = _level_or_400(body.fault_level)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.delete("/{tid}")
async def fault_type_delete(tid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(FaultTypeDict).where(FaultTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    await db.delete(row)
    await db.flush()
    return {"ok": True}

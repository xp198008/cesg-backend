"""道路类型字典：基础数据本地 CRUD。"""
from __future__ import annotations

import secrets

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal, get_db
from app.models import RoadTypeDict
from app.timeutil import china_now_naive

router = APIRouter(prefix="/api/road-type", tags=["road-type"])

DEFAULT_ROAD_TYPES = (
    ("城市道路", "市区普通道路"),
    ("快速路", "城市快速路"),
    ("高速公路", "高速公路"),
    ("国道", "国家级公路"),
    ("省道", "省级公路"),
    ("县道", "县级公路"),
    ("乡道", "乡级公路"),
)


def _gen_type_code() -> str:
    return f"RT{china_now_naive().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(2).upper()}"


async def _allocate_unique_type_code(db: AsyncSession) -> str:
    for _ in range(12):
        code = _gen_type_code()
        exists = await db.scalar(select(RoadTypeDict.id).where(RoadTypeDict.type_code == code).limit(1))
        if exists is None:
            return code
    raise HTTPException(status_code=500, detail="生成类型编码失败，请重试")


async def _ensure_unique_name(db: AsyncSession, type_name: str, exclude_id: int | None = None) -> None:
    stmt = select(RoadTypeDict.id).where(RoadTypeDict.type_name == type_name)
    if exclude_id is not None:
        stmt = stmt.where(RoadTypeDict.id != exclude_id)
    exists = await db.scalar(stmt.limit(1))
    if exists is not None:
        raise HTTPException(status_code=400, detail="该道路类型已存在，不允许重复")


class RoadTypeCreateIn(BaseModel):
    type_name: str = Field(..., min_length=1, max_length=64)
    description: str | None = Field(None, max_length=2000)
    sort_order: int | None = None


class RoadTypeUpdateIn(BaseModel):
    type_name: str | None = Field(None, min_length=1, max_length=64)
    description: str | None = Field(None, max_length=2000)
    sort_order: int | None = None


def _row_out(row: RoadTypeDict) -> dict:
    return {
        "id": row.id,
        "type_code": row.type_code,
        "type_name": row.type_name,
        "description": row.description,
        "sort_order": row.sort_order or 0,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


async def ensure_default_road_types() -> None:
    """空库首次启动时写入常用道路类型。"""
    async with AsyncSessionLocal() as db:
        n = await db.scalar(select(func.count()).select_from(RoadTypeDict))
        if n and n > 0:
            return
        for index, (name, desc) in enumerate(DEFAULT_ROAD_TYPES, start=1):
            db.add(
                RoadTypeDict(
                    type_code=await _allocate_unique_type_code(db),
                    type_name=name,
                    description=desc,
                    sort_order=index,
                )
            )
            await db.flush()
        await db.commit()


@router.get("/list")
async def road_type_list(
    type_code: str | None = Query(None),
    type_name: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(RoadTypeDict)
    if type_code and type_code.strip():
        stmt = stmt.where(RoadTypeDict.type_code.ilike(f"%{type_code.strip()}%"))
    if type_name and type_name.strip():
        stmt = stmt.where(RoadTypeDict.type_name.ilike(f"%{type_name.strip()}%"))
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        await db.execute(
            stmt.order_by(RoadTypeDict.sort_order.asc(), RoadTypeDict.id.asc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).scalars().all()
    return {"total": total, "items": [_row_out(x) for x in rows], "page": page, "page_size": page_size}


@router.get("/type-options")
async def road_type_options(db: AsyncSession = Depends(get_db)):
    rows = (
        await db.execute(select(RoadTypeDict).order_by(RoadTypeDict.sort_order.asc(), RoadTypeDict.id.asc()))
    ).scalars().all()
    return {
        "ok": True,
        "items": [{"id": x.id, "type_code": x.type_code, "type_name": x.type_name} for x in rows],
    }


@router.get("/{tid}")
async def road_type_get(tid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(RoadTypeDict).where(RoadTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": _row_out(row)}


@router.post("")
async def road_type_create(body: RoadTypeCreateIn, db: AsyncSession = Depends(get_db)):
    type_name = body.type_name.strip()
    if not type_name:
        raise HTTPException(status_code=400, detail="请填写道路类型名称")
    await _ensure_unique_name(db, type_name)
    max_order = await db.scalar(select(func.max(RoadTypeDict.sort_order))) or 0
    row = RoadTypeDict(
        type_code=await _allocate_unique_type_code(db),
        type_name=type_name,
        description=(body.description or "").strip() or None,
        sort_order=body.sort_order if body.sort_order is not None else int(max_order) + 1,
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.patch("/{tid}")
async def road_type_update(tid: int, body: RoadTypeUpdateIn, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(RoadTypeDict).where(RoadTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if body.type_name is not None:
        type_name = body.type_name.strip()
        if not type_name:
            raise HTTPException(status_code=400, detail="请填写道路类型名称")
        await _ensure_unique_name(db, type_name, exclude_id=tid)
        row.type_name = type_name
    if body.description is not None:
        row.description = body.description.strip() or None
    if body.sort_order is not None:
        row.sort_order = body.sort_order
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.delete("/{tid}")
async def road_type_delete(tid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(RoadTypeDict).where(RoadTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    await db.delete(row)
    await db.flush()
    return {"ok": True}

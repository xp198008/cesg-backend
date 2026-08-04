"""报警类型字典：基础数据本地 CRUD + 从 808 目录重置灌数。"""
from __future__ import annotations

import secrets

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.jt808_alarm_sync import jt808_alarm_type_catalog
from app.models import AlarmTypeDict, VehicleViolation
from app.timeutil import china_now_naive
from app.violation_risk import risk_from_safety_level

router = APIRouter(prefix="/api/alarm-type", tags=["alarm-type"])

_ALLOWED_SAFETY = frozenset({"高", "中", "低"})
_ALLOWED_STATUS = frozenset({"启用", "停用"})
_LEGACY_LEVEL_MAP = {
    "高级": "高",
    "中级": "中",
    "低级": "低",
    "高": "高",
    "中": "中",
    "低": "低",
}


def _gen_type_code() -> str:
    return f"AT{china_now_naive().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(2).upper()}"


async def _allocate_unique_type_code(db: AsyncSession) -> str:
    for _ in range(12):
        code = _gen_type_code()
        exists = await db.scalar(select(AlarmTypeDict.id).where(AlarmTypeDict.type_code == code).limit(1))
        if exists is None:
            return code
    raise HTTPException(status_code=500, detail="生成类型编码失败，请重试")


def _safety_or_400(raw: str | None) -> str:
    level = _LEGACY_LEVEL_MAP.get((raw or "").strip(), (raw or "").strip()) or "中"
    if level not in _ALLOWED_SAFETY:
        raise HTTPException(status_code=400, detail="安全级别须为：高、中、低")
    return level


def _status_or_400(raw: str | None) -> str:
    status = (raw or "").strip() or "启用"
    if status not in _ALLOWED_STATUS:
        raise HTTPException(status_code=400, detail="状态须为：启用、停用")
    return status


def _interval_or_400(raw: int | None) -> int:
    value = 0 if raw is None else int(raw)
    if value < 0:
        raise HTTPException(status_code=400, detail="最小间隔时长不能为负数")
    return value


def _legacy_alarm_level(safety: str) -> str:
    return {"高": "高级", "中": "中级", "低": "低级"}.get(safety, "中级")


async def _ensure_unique_type_name(db: AsyncSession, type_name: str, exclude_id: int | None = None) -> None:
    stmt = select(AlarmTypeDict.id).where(AlarmTypeDict.type_name == type_name)
    if exclude_id is not None:
        stmt = stmt.where(AlarmTypeDict.id != exclude_id)
    exists = await db.scalar(stmt.limit(1))
    if exists is not None:
        raise HTTPException(status_code=400, detail="类型名称已存在，不能重复")


class AlarmTypeCreateIn(BaseModel):
    type_name: str = Field(..., min_length=1, max_length=64)
    min_interval_minutes: int = Field(15, ge=0)
    status: str = Field("启用")
    safety_level: str = Field("中")
    description: str | None = Field(None, max_length=2000)
    # 兼容旧前端字段
    alarm_level: str | None = None


class AlarmTypeUpdateIn(BaseModel):
    type_name: str | None = Field(None, min_length=1, max_length=64)
    min_interval_minutes: int | None = Field(None, ge=0)
    status: str | None = None
    safety_level: str | None = None
    description: str | None = Field(None, max_length=2000)
    alarm_level: str | None = None


class AlarmTypeSyncIn(BaseModel):
    mode: str = "insert_only"


def _row_out(row: AlarmTypeDict) -> dict:
    safety = (getattr(row, "safety_level", None) or "").strip()
    if not safety:
        safety = _LEGACY_LEVEL_MAP.get((row.alarm_level or "").strip(), "中")
    return {
        "id": row.id,
        "type_code": row.type_code,
        "type_name": row.type_name,
        "description": row.description,
        "min_interval_minutes": int(getattr(row, "min_interval_minutes", 0) or 0),
        "status": (getattr(row, "status", None) or "启用").strip() or "启用",
        "safety_level": safety if safety in _ALLOWED_SAFETY else "中",
        "alarm_level": row.alarm_level,
        "data_source": row.data_source or "manual",
        "ttx_atp_code": row.ttx_atp_code,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@router.get("/list")
async def alarm_type_list(
    type_code: str | None = Query(None),
    type_name: str | None = Query(None),
    safety_level: str | None = Query(None),
    status: str | None = Query(None),
    alarm_level: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(AlarmTypeDict)
    if type_code and type_code.strip():
        stmt = stmt.where(AlarmTypeDict.type_code.ilike(f"%{type_code.strip()}%"))
    if type_name and type_name.strip():
        stmt = stmt.where(AlarmTypeDict.type_name.ilike(f"%{type_name.strip()}%"))
    if status and status.strip():
        stmt = stmt.where(AlarmTypeDict.status == status.strip())
    level_filter = (safety_level or alarm_level or "").strip()
    if level_filter:
        mapped = _LEGACY_LEVEL_MAP.get(level_filter, level_filter)
        if mapped in _ALLOWED_SAFETY:
            stmt = stmt.where(AlarmTypeDict.safety_level == mapped)
        else:
            stmt = stmt.where(AlarmTypeDict.alarm_level == level_filter)
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        await db.execute(
            stmt.order_by(AlarmTypeDict.id.desc()).offset((page - 1) * page_size).limit(page_size)
        )
    ).scalars().all()
    return {"total": total, "items": [_row_out(x) for x in rows], "page": page, "page_size": page_size}


@router.post("/reset-from-jt808")
async def alarm_type_reset_from_jt808(db: AsyncSession = Depends(get_db)):
    """清空报警类型表，并按 808 真实规则灌入（ADAS/DSM 报警×三级；BSD/事件不分级；含 OBD超速；默认启用/中/15分钟）。"""
    cleared = await db.scalar(select(func.count()).select_from(AlarmTypeDict)) or 0
    await db.execute(delete(AlarmTypeDict))
    await db.flush()

    catalog = jt808_alarm_type_catalog()
    inserted = 0
    stamp = china_now_naive().strftime("%Y%m%d%H%M%S")
    for index, name in enumerate(catalog, start=1):
        row = AlarmTypeDict(
            type_code=f"AT{stamp}{index:04d}",
            type_name=name,
            description=None,
            alarm_level="中级",
            safety_level="中",
            min_interval_minutes=15,
            status="启用",
            data_source="jt808",
            ttx_atp_code=None,
        )
        db.add(row)
        inserted += 1
    await db.flush()
    return {"ok": True, "cleared": int(cleared), "inserted": inserted}


@router.post("/sync-tongtianxing-catalog")
async def alarm_type_sync_tongtianxing_catalog(body: AlarmTypeSyncIn):
    """兼容老页面按钮；当前项目不再同步通天星，保留无副作用返回。"""

    return {"ok": True, "mode": body.mode, "inserted": 0, "updated": 0, "skipped": 0}


@router.get("/{tid}")
async def alarm_type_get(tid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(AlarmTypeDict).where(AlarmTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": _row_out(row)}


@router.post("")
async def alarm_type_create(body: AlarmTypeCreateIn, db: AsyncSession = Depends(get_db)):
    type_name = body.type_name.strip()
    await _ensure_unique_type_name(db, type_name)
    safety = _safety_or_400(body.safety_level or body.alarm_level)
    status = _status_or_400(body.status)
    interval = _interval_or_400(body.min_interval_minutes)
    row = AlarmTypeDict(
        type_code=await _allocate_unique_type_code(db),
        type_name=type_name,
        description=(body.description or "").strip() or None,
        alarm_level=_legacy_alarm_level(safety),
        safety_level=safety,
        min_interval_minutes=interval,
        status=status,
        data_source="manual",
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.patch("/{tid}")
async def alarm_type_update(tid: int, body: AlarmTypeUpdateIn, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(AlarmTypeDict).where(AlarmTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if body.type_name is not None:
        type_name = body.type_name.strip()
        await _ensure_unique_type_name(db, type_name, exclude_id=tid)
        row.type_name = type_name
    if body.description is not None:
        row.description = body.description.strip() or None
    if body.min_interval_minutes is not None:
        row.min_interval_minutes = _interval_or_400(body.min_interval_minutes)
    if body.status is not None:
        row.status = _status_or_400(body.status)
    if body.safety_level is not None or body.alarm_level is not None:
        safety = _safety_or_400(body.safety_level or body.alarm_level)
        row.safety_level = safety
        row.alarm_level = _legacy_alarm_level(safety)
        # 同步历史报警上的 risk_level，安全监控立即按新级别展示
        risk = risk_from_safety_level(safety)
        await db.execute(
            update(VehicleViolation)
            .where(VehicleViolation.violation_type_name == row.type_name)
            .values(risk_level=risk)
        )
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.delete("/{tid}")
async def alarm_type_delete(tid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(AlarmTypeDict).where(AlarmTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    await db.delete(row)
    await db.flush()
    return {"ok": True}

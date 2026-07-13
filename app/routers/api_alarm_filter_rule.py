"""报警过滤规则：基础数据 CRUD。"""
from __future__ import annotations

import secrets

from app.timeutil import china_now_naive
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.alarm_filter import ALLOWED_ALARM_LEVELS, KNOWN_ALARM_TYPE_NAMES, format_alarm_level, load_enabled_rules
from app.database import get_db
from app.models import AlarmFilterRule, SysUser, VehicleViolation
from app.violation_filters import violation_list_visibility

router = APIRouter(prefix="/api/alarm-filter-rule", tags=["alarm-filter-rule"])


class AlarmFilterRuleCreateIn(BaseModel):
    alarm_type_name: str = Field(..., min_length=1, max_length=64)
    alarm_level: str | None = Field(None, max_length=8)
    remark: str | None = Field(None, max_length=2000)
    enabled: bool = True


class AlarmFilterRuleUpdateIn(BaseModel):
    alarm_type_name: str | None = Field(None, min_length=1, max_length=64)
    alarm_level: str | None = Field(None, max_length=8)
    remark: str | None = Field(None, max_length=2000)
    enabled: bool | None = None


class AlarmFilterRuleEnabledIn(BaseModel):
    enabled: bool


def _level_or_none(raw: str | None) -> str | None:
    if raw is None:
        return None
    level = str(raw).strip()
    if not level:
        return None
    if level not in ALLOWED_ALARM_LEVELS:
        raise HTTPException(status_code=400, detail="报警级别须为：一级(1)、二级(2)，或留空表示不限")
    return level


def _type_name_or_400(raw: str) -> str:
    name = (raw or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="请选择报警类型")
    return name


async def _resolve_creator_name(db: AsyncSession, user_id: int | None) -> str | None:
    if not user_id:
        return None
    row = await db.scalar(select(SysUser.username).where(SysUser.id == user_id).limit(1))
    return (row or "").strip() or None


def _gen_rule_code() -> str:
    return f"AFR{china_now_naive().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(2).upper()}"


async def _allocate_unique_rule_code(db: AsyncSession) -> str:
    for _ in range(12):
        code = _gen_rule_code()
        exists = await db.scalar(select(AlarmFilterRule.id).where(AlarmFilterRule.rule_name == code).limit(1))
        if exists is None:
            return code
    raise HTTPException(status_code=500, detail="生成规则编码失败，请重试")


def _row_out(row: AlarmFilterRule) -> dict:
    return {
        "id": row.id,
        "rule_code": row.rule_name,
        "rule_name": row.rule_name,
        "alarm_type_name": row.alarm_type_name,
        "alarm_level": row.alarm_level,
        "alarm_level_label": format_alarm_level(row.alarm_level),
        "remark": row.remark,
        "enabled": bool(row.enabled),
        "created_by": row.created_by,
        "created_by_name": row.created_by_name,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


async def _ensure_unique(db: AsyncSession, type_name: str, level: str | None, exclude_id: int | None = None) -> None:
    stmt = select(AlarmFilterRule.id).where(
        AlarmFilterRule.alarm_type_name == type_name,
        AlarmFilterRule.alarm_level.is_(None) if level is None else AlarmFilterRule.alarm_level == level,
    )
    if exclude_id is not None:
        stmt = stmt.where(AlarmFilterRule.id != exclude_id)
    exists = await db.scalar(stmt.limit(1))
    if exists is not None:
        raise HTTPException(status_code=400, detail="相同报警类型与级别的过滤规则已存在")


@router.get("/effect-preview")
async def alarm_filter_rule_effect_preview(db: AsyncSession = Depends(get_db)):
    """诊断：过滤规则对当前库内报警记录的影响（便于排查列表未减少问题）。"""
    rules = await load_enabled_rules(db)
    pending_status = or_(
        VehicleViolation.status == "待处理",
        and_(VehicleViolation.status == "待审核", VehicleViolation.pre_audit_kind == "preprocess"),
    )

    async def _count(extra_visibility):
        stmt = select(func.count()).select_from(VehicleViolation).where(extra_visibility, pending_status)
        return int((await db.scalar(stmt)) or 0)

    without_filter = violation_list_visibility([])
    with_filter = violation_list_visibility(rules)
    pending_all = await _count(without_filter)
    pending_visible = await _count(with_filter)

    type_rows = (
        await db.execute(
            select(VehicleViolation.violation_type_name, func.count())
            .where(without_filter, pending_status)
            .group_by(VehicleViolation.violation_type_name)
            .order_by(func.count().desc())
            .limit(30)
        )
    ).all()

    return {
        "ok": True,
        "engine": "alarm-filter-v2",
        "active_rules": len(rules),
        "rules": [_row_out(r) for r in rules],
        "pending_total_in_db": pending_all,
        "pending_visible_after_filter": pending_visible,
        "pending_hidden_by_filter": max(0, pending_all - pending_visible),
        "top_pending_type_names": [
            {"violation_type_name": name or "—", "count": cnt}
            for name, cnt in type_rows
        ],
    }


@router.get("/options")
async def alarm_filter_rule_options():
    return {
        "alarm_types": [{"label": name, "value": name} for name in KNOWN_ALARM_TYPE_NAMES],
        "alarm_levels": [
            {"label": "不限", "value": ""},
            {"label": "一级", "value": "1"},
            {"label": "二级", "value": "2"},
        ],
    }


@router.get("/list")
async def alarm_filter_rule_list(
    rule_code: str | None = Query(None),
    rule_name: str | None = Query(None),
    alarm_type_name: str | None = Query(None),
    alarm_level: str | None = Query(None),
    enabled: bool | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(AlarmFilterRule)
    code_kw = (rule_code or rule_name or "").strip()
    if code_kw:
        stmt = stmt.where(AlarmFilterRule.rule_name.ilike(f"%{code_kw}%"))
    if alarm_type_name and alarm_type_name.strip():
        stmt = stmt.where(AlarmFilterRule.alarm_type_name.ilike(f"%{alarm_type_name.strip()}%"))
    if alarm_level is not None and str(alarm_level).strip():
        raw_level = str(alarm_level).strip()
        if raw_level == "none":
            stmt = stmt.where(AlarmFilterRule.alarm_level.is_(None))
        else:
            level = _level_or_none(raw_level)
            if level is None:
                stmt = stmt.where(AlarmFilterRule.alarm_level.is_(None))
            else:
                stmt = stmt.where(AlarmFilterRule.alarm_level == level)
    if enabled is not None:
        stmt = stmt.where(AlarmFilterRule.enabled.is_(enabled))
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        await db.execute(
            stmt.order_by(AlarmFilterRule.id.desc()).offset((page - 1) * page_size).limit(page_size)
        )
    ).scalars().all()
    return {"total": total, "items": [_row_out(x) for x in rows], "page": page, "page_size": page_size}


@router.get("/{rid}")
async def alarm_filter_rule_get(rid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(AlarmFilterRule).where(AlarmFilterRule.id == rid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": _row_out(row)}


@router.post("")
async def alarm_filter_rule_create(
    body: AlarmFilterRuleCreateIn,
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    type_name = _type_name_or_400(body.alarm_type_name)
    level = _level_or_none(body.alarm_level)
    await _ensure_unique(db, type_name, level)
    creator_id: int | None = None
    if x_user_id and str(x_user_id).strip().isdigit():
        creator_id = int(str(x_user_id).strip())
    row = AlarmFilterRule(
        rule_name=await _allocate_unique_rule_code(db),
        alarm_type_name=type_name,
        alarm_level=level,
        remark=(body.remark or "").strip() or None,
        enabled=bool(body.enabled),
        created_by=creator_id,
        created_by_name=await _resolve_creator_name(db, creator_id),
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.patch("/{rid}")
async def alarm_filter_rule_update(rid: int, body: AlarmFilterRuleUpdateIn, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(AlarmFilterRule).where(AlarmFilterRule.id == rid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    type_name = row.alarm_type_name
    level = row.alarm_level
    if body.alarm_type_name is not None:
        type_name = _type_name_or_400(body.alarm_type_name)
        row.alarm_type_name = type_name
    if body.alarm_level is not None:
        level = _level_or_none(body.alarm_level)
        row.alarm_level = level
    if body.remark is not None:
        row.remark = body.remark.strip() or None
    if body.enabled is not None:
        row.enabled = bool(body.enabled)
    await _ensure_unique(db, type_name, level, exclude_id=rid)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.patch("/{rid}/enabled")
async def alarm_filter_rule_set_enabled(rid: int, body: AlarmFilterRuleEnabledIn, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(AlarmFilterRule).where(AlarmFilterRule.id == rid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    row.enabled = bool(body.enabled)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.delete("/{rid}")
async def alarm_filter_rule_delete(rid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(AlarmFilterRule).where(AlarmFilterRule.id == rid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    await db.delete(row)
    await db.flush()
    return {"ok": True}

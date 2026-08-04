"""按报警类型字典（启用状态 / 最小间隔 / 安全级别）做入库闸门。"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, not_, or_, select, true
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AlarmTypeDict, VehicleViolation
from app.violation_risk import risk_from_safety_level

logger = logging.getLogger(__name__)


async def load_alarm_type_by_name(db: AsyncSession, type_name: str) -> AlarmTypeDict | None:
    name = (type_name or "").strip()
    if not name:
        return None
    return await db.scalar(select(AlarmTypeDict).where(AlarmTypeDict.type_name == name).limit(1))


async def load_disabled_alarm_type_names(db: AsyncSession) -> list[str]:
    rows = (
        await db.execute(
            select(AlarmTypeDict.type_name).where(AlarmTypeDict.status == "停用").order_by(AlarmTypeDict.id.asc())
        )
    ).scalars().all()
    return [str(x).strip() for x in rows if str(x or "").strip()]


def disabled_alarm_type_name_aliases(name: str) -> list[str]:
    """兼容历史「二级/2级」等写法，使停用软隐藏能盖住旧记录。"""
    text = (name or "").strip()
    if not text:
        return []
    aliases = {text}
    replacements = (
        ("一级", "1级"),
        ("二级", "2级"),
        ("三级", "3级"),
        ("1级", "一级"),
        ("2级", "二级"),
        ("3级", "三级"),
    )
    for src, dst in replacements:
        if text.endswith(src):
            aliases.add(text[: -len(src)] + dst)
    return [a for a in aliases if a]


def expand_disabled_alarm_type_names(disabled_names: list[str] | None) -> list[str]:
    names: list[str] = []
    for n in disabled_names or []:
        names.extend(disabled_alarm_type_name_aliases(n))
    seen: set[str] = set()
    uniq: list[str] = []
    for n in names:
        if n in seen:
            continue
        seen.add(n)
        uniq.append(n)
    return uniq


def build_disabled_alarm_type_exclusion_clause(disabled_names: list[str] | None):
    """列表软隐藏：隐藏类型名等于「停用」报警类型的历史记录。"""
    uniq = expand_disabled_alarm_type_names(disabled_names)
    if not uniq:
        return true()
    col = func.trim(VehicleViolation.violation_type_name)
    return or_(
        VehicleViolation.violation_type_name.is_(None),
        VehicleViolation.violation_type_name == "",
        not_(col.in_(uniq)),
    )


def risk_level_from_alarm_type(row: AlarmTypeDict | None, fallback_name: str | None = None) -> str:
    if row is not None:
        return risk_from_safety_level(row.safety_level)
    from app.violation_risk import RISK_MID

    return RISK_MID


async def load_alarm_type_risk_map(db: AsyncSession) -> dict[str, str]:
    """type_name → high/mid/low，供列表/补全按类型安全级别展示。"""
    rows = (await db.execute(select(AlarmTypeDict.type_name, AlarmTypeDict.safety_level))).all()
    out: dict[str, str] = {}
    for type_name, safety_level in rows:
        name = str(type_name or "").strip()
        if not name:
            continue
        out[name] = risk_from_safety_level(safety_level)
    return out


async def _within_min_interval(
    db: AsyncSession,
    *,
    vehicle_id: int | None,
    type_names: list[str],
    alarm_time: datetime,
    minutes: int,
) -> bool:
    if minutes <= 0 or vehicle_id is None or alarm_time is None:
        return False
    names = [n.strip() for n in type_names if (n or "").strip()]
    if not names:
        return False
    since = alarm_time - timedelta(minutes=int(minutes))
    # 下界用「> since」：距上次满整分钟（如 15 分钟）时允许再入，避免卡在边界永远被挡
    exists = await db.scalar(
        select(VehicleViolation.id)
        .where(
            VehicleViolation.vehicle_id == vehicle_id,
            VehicleViolation.violation_type_name.in_(names),
            VehicleViolation.violation_time > since,
            VehicleViolation.violation_time <= alarm_time,
        )
        .limit(1)
    )
    return exists is not None


async def evaluate_alarm_type_ingest(
    db: AsyncSession,
    *,
    type_name: str,
    vehicle_id: int | None,
    alarm_time: datetime,
    interval_type_names: list[str] | None = None,
) -> dict[str, Any]:
    """
    入库闸门：
    - 字典中不存在 / 停用 → 不入库
    - 最小间隔内同车同类型已有记录 → 不入库
    interval_type_names：间隔判定时额外纳入的历史类型名（兼容改名前旧记录）
    """
    name = (type_name or "").strip()
    row = await load_alarm_type_by_name(db, name)
    if row is None:
        return {
            "allow": False,
            "reason": "missing",
            "alarm_type": None,
            "risk_level": risk_level_from_alarm_type(None, name),
        }
    if (row.status or "").strip() != "启用":
        return {
            "allow": False,
            "reason": "disabled",
            "alarm_type": row,
            "risk_level": risk_level_from_alarm_type(row, name),
        }
    interval = int(row.min_interval_minutes or 0)
    interval_names = [name]
    for alias in interval_type_names or []:
        alias_n = (alias or "").strip()
        if alias_n and alias_n not in interval_names:
            interval_names.append(alias_n)
    if await _within_min_interval(
        db,
        vehicle_id=vehicle_id,
        type_names=interval_names,
        alarm_time=alarm_time,
        minutes=interval,
    ):
        return {
            "allow": False,
            "reason": "interval",
            "alarm_type": row,
            "risk_level": risk_level_from_alarm_type(row, name),
        }
    return {
        "allow": True,
        "reason": None,
        "alarm_type": row,
        "risk_level": risk_level_from_alarm_type(row, name),
    }


def log_alarm_type_gate(
    *,
    source: str,
    external_id: str,
    alarm_type_name: str,
    reason: str,
    plate: str = "",
    interval_minutes: int | None = None,
) -> None:
    logger.info(
        "主动安全报警按类型字典跳过入库: source=%s ext_id=%s plate=%s type=%s reason=%s interval=%s",
        source,
        external_id,
        plate or "-",
        alarm_type_name,
        reason,
        interval_minutes if interval_minutes is not None else "-",
    )

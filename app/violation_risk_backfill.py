"""启动时按报警类型安全级别补全 vehicle_violation.risk_level。"""
from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.alarm_type_gate import load_alarm_type_risk_map
from app.models import VehicleViolation
from app.violation_risk import RISK_MID

logger = logging.getLogger(__name__)


async def backfill_violation_risk_levels(db: AsyncSession) -> int:
    """按报警类型字典 safety_level 为全部报警记录写入/修正风险等级。"""
    risk_map = await load_alarm_type_risk_map(db)
    rows = (await db.execute(select(VehicleViolation))).scalars().all()
    updated = 0
    for row in rows:
        name = (row.violation_type_name or "").strip()
        expected = risk_map.get(name) or RISK_MID
        if (row.risk_level or "") != expected:
            row.risk_level = expected
            updated += 1
    if updated:
        await db.flush()
        logger.info("已按报警类型安全级别更新 %s 条报警记录的风险等级", updated)
    return updated

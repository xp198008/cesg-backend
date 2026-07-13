"""启动时补全 vehicle_violation.risk_level。"""
from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import VehicleViolation
from app.violation_risk import derive_risk_level

logger = logging.getLogger(__name__)


async def backfill_violation_risk_levels(db: AsyncSession) -> int:
    """按当前规则为全部报警记录写入/修正风险等级。"""
    rows = (await db.execute(select(VehicleViolation))).scalars().all()
    updated = 0
    for row in rows:
        expected = derive_risk_level(row.violation_type_name)
        if (row.risk_level or "") != expected:
            row.risk_level = expected
            updated += 1
    if updated:
        await db.flush()
        logger.info("已更新 %s 条报警记录的风险等级", updated)
    return updated

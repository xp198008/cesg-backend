"""启动时补全 vehicle_violation / vehicle_location 的空地址（逆地理）。"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.amap_regeo import resolve_address_wgs84
from app.amap_web_service_key import ensure_web_service_key
from app.models import VehicleLocation, VehicleViolation

logger = logging.getLogger(__name__)

_DEFAULT_LIMIT = 40
_REGEO_GAP_SEC = 0.05


async def _has_amap_key(db: AsyncSession) -> bool:
    key, _ = await ensure_web_service_key(db)
    return bool(key)


async def backfill_violation_addresses(db: AsyncSession, *, limit: int = _DEFAULT_LIMIT) -> int:
    """为有坐标但 address 为空的违章记录补地址。每次最多处理 limit 条。"""
    if not await _has_amap_key(db):
        logger.info("未配置逆地理 Key（map_api_config.web_service_key / 808 appkey1），跳过报警地址回填")
        return 0
    rows = (
        await db.execute(
            select(VehicleViolation)
            .where(
                and_(
                    VehicleViolation.lat.is_not(None),
                    VehicleViolation.lng.is_not(None),
                    or_(VehicleViolation.address.is_(None), VehicleViolation.address == ""),
                )
            )
            .order_by(VehicleViolation.id.desc())
            .limit(max(1, int(limit)))
        )
    ).scalars().all()
    updated = 0
    for row in rows:
        if row.lat is None or row.lng is None:
            continue
        addr = await resolve_address_wgs84(db, row.lat, row.lng, existing=row.address)
        if addr and addr != (row.address or ""):
            row.address = addr
            updated += 1
        if _REGEO_GAP_SEC > 0:
            await asyncio.sleep(_REGEO_GAP_SEC)
    if updated:
        await db.flush()
        logger.info("已补全 %s 条报警记录的位置地址", updated)
    return updated


async def backfill_vehicle_location_addresses(db: AsyncSession, *, limit: int = 30) -> int:
    """为有坐标但 current_position 为空的车辆位置快照补地址。"""
    if not await _has_amap_key(db):
        return 0
    rows = (
        await db.execute(
            select(VehicleLocation)
            .where(
                and_(
                    VehicleLocation.lat.is_not(None),
                    VehicleLocation.lng.is_not(None),
                    or_(VehicleLocation.current_position.is_(None), VehicleLocation.current_position == ""),
                )
            )
            .order_by(VehicleLocation.id.desc())
            .limit(max(1, int(limit)))
        )
    ).scalars().all()
    updated = 0
    for row in rows:
        if row.lat is None or row.lng is None:
            continue
        addr = await resolve_address_wgs84(db, row.lat, row.lng, existing=row.current_position)
        if addr and addr != (row.current_position or ""):
            row.current_position = addr
            updated += 1
        if _REGEO_GAP_SEC > 0:
            await asyncio.sleep(_REGEO_GAP_SEC)
    if updated:
        await db.flush()
        logger.info("已补全 %s 条车辆位置快照地址", updated)
    return updated

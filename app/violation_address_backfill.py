"""补全 vehicle_violation / vehicle_location 的空地址。

先短读待补记录并关掉会话，再在库外做逆地理，最后短写回库。
禁止开着 SQLite 事务去打高德 HTTP。
"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import and_, or_, select

from app.amap_regeo import regeo_wgs84
from app.amap_web_service_key import ensure_web_service_key
from app.database import AsyncSessionLocal
from app.jt808_address import lookup_jt808_address_cache
from app.models import VehicleLocation, VehicleViolation

logger = logging.getLogger(__name__)

_DEFAULT_LIMIT = 40
_REGEO_GAP_SEC = 0.05


async def _peek_amap_key() -> str:
    async with AsyncSessionLocal() as db:
        key, _ = await ensure_web_service_key(db)
        await db.commit()
    return (key or "").strip()


async def _resolve_addr(api_key: str, lat: float, lng: float) -> str:
    cached = await asyncio.to_thread(lookup_jt808_address_cache, float(lat), float(lng))
    if cached:
        return str(cached).strip()
    if not api_key:
        return ""
    addr = await regeo_wgs84(api_key, float(lat), float(lng))
    return str(addr).strip() if addr else ""


async def backfill_violation_addresses(limit: int = _DEFAULT_LIMIT) -> int:
    """为有坐标但 address 为空的违章记录补地址。"""
    key = await _peek_amap_key()
    if not key:
        logger.info("未配置逆地理 Key（map_api_config.web_service_key / 808 appkey1），跳过报警地址回填")
        return 0

    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(VehicleViolation.id, VehicleViolation.lat, VehicleViolation.lng)
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
        ).all()
        await db.commit()

    pending = [
        (int(vid), float(lat), float(lng))
        for vid, lat, lng in rows
        if lat is not None and lng is not None
    ]
    prepared: list[tuple[int, str]] = []
    for vid, lat, lng in pending:
        addr = await _resolve_addr(key, lat, lng)
        if addr:
            prepared.append((vid, addr))
        if _REGEO_GAP_SEC > 0:
            await asyncio.sleep(_REGEO_GAP_SEC)

    if not prepared:
        return 0

    updated = 0
    async with AsyncSessionLocal() as db:
        for vid, addr in prepared:
            row = await db.get(VehicleViolation, vid)
            if row is None or (row.address or "").strip():
                continue
            row.address = addr
            updated += 1
        await db.commit()
    if updated:
        logger.info("已补全 %s 条报警记录的位置地址", updated)
    return updated


async def backfill_vehicle_location_addresses(limit: int = 30) -> int:
    """为有坐标但 current_position 为空的车辆位置快照补地址。"""
    key = await _peek_amap_key()
    if not key:
        return 0

    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(VehicleLocation.id, VehicleLocation.lat, VehicleLocation.lng)
                .where(
                    and_(
                        VehicleLocation.lat.is_not(None),
                        VehicleLocation.lng.is_not(None),
                        or_(
                            VehicleLocation.current_position.is_(None),
                            VehicleLocation.current_position == "",
                        ),
                    )
                )
                .order_by(VehicleLocation.id.desc())
                .limit(max(1, int(limit)))
            )
        ).all()
        await db.commit()

    pending = [
        (int(vid), float(lat), float(lng))
        for vid, lat, lng in rows
        if lat is not None and lng is not None
    ]
    prepared: list[tuple[int, str]] = []
    for vid, lat, lng in pending:
        addr = await _resolve_addr(key, lat, lng)
        if addr:
            prepared.append((vid, addr))
        if _REGEO_GAP_SEC > 0:
            await asyncio.sleep(_REGEO_GAP_SEC)

    if not prepared:
        return 0

    updated = 0
    async with AsyncSessionLocal() as db:
        for vid, addr in prepared:
            row = await db.get(VehicleLocation, vid)
            if row is None or (row.current_position or "").strip():
                continue
            row.current_position = addr
            updated += 1
        await db.commit()
    if updated:
        logger.info("已补全 %s 条车辆位置快照地址", updated)
    return updated

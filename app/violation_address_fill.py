"""违章记录地址补全：读取或列表展示时按需逆地理并落库。"""
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.amap_regeo import resolve_address_wgs84
from app.models import VehicleViolation


async def ensure_violation_address(
    db: AsyncSession,
    row: VehicleViolation,
    *,
    force: bool = False,
) -> str:
    """有坐标且 address 为空时逆地理，写入 row.address 并返回。"""
    current = (row.address or "").strip()
    if current and not force:
        return current[:512]
    if row.lat is None or row.lng is None:
        return current
    addr = await resolve_address_wgs84(db, row.lat, row.lng, existing=row.address)
    if addr:
        row.address = addr
        await db.flush()
    return (row.address or "").strip()[:512]

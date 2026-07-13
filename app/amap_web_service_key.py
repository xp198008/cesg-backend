"""高德 Web 服务 Key：CESG 库优先，失败再从 808 appkey1 刷新并回写。

地图接口管理中的 api_key 是 JS API Key（前端画地图）；
web_service_key 是 Web 服务 Key（逆地理、轨迹纠偏等）。
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.jt808_address import get_jt808_regeo_amap_key
from app.models import MapApiConfig

_logger = logging.getLogger(__name__)

T = TypeVar("T")


async def _amap_row(db: AsyncSession) -> MapApiConfig | None:
    return await db.scalar(select(MapApiConfig).where(MapApiConfig.provider == "amap").limit(1))


async def get_stored_web_service_key(db: AsyncSession) -> str:
    row = await _amap_row(db)
    return ((row.web_service_key if row else "") or "").strip()


async def save_web_service_key(db: AsyncSession, key: str) -> str:
    """写入 map_api_config.web_service_key；无行时创建默认 amap 配置。"""
    key = (key or "").strip()
    row = await _amap_row(db)
    if row is None:
        row = MapApiConfig(
            provider="amap",
            default_zoom=12,
            default_center_lng=106.55156,
            default_center_lat=29.56301,
            remark="系统默认",
        )
        db.add(row)
    row.web_service_key = key or None
    await db.flush()
    return key


async def fetch_jt808_web_service_key(*, force_refresh: bool = False) -> str:
    return (await asyncio.to_thread(get_jt808_regeo_amap_key, force_refresh=force_refresh) or "").strip()


async def sync_web_service_key_from_jt808(
    db: AsyncSession,
    *,
    force_refresh: bool = True,
) -> str:
    """从 808 读取 appkey1；有值则写入 CESG 库并返回。"""
    key = await fetch_jt808_web_service_key(force_refresh=force_refresh)
    if not key:
        _logger.info("808 appkey1 未配置或 type1 非 gaode，无法同步 Web 服务 Key")
        return ""
    stored = await get_stored_web_service_key(db)
    if key != stored:
        await save_web_service_key(db, key)
        _logger.info("已从 808 同步 Web 服务 Key 到 map_api_config")
    return key


async def ensure_web_service_key(db: AsyncSession) -> tuple[str, str]:
    """日常优先用库内值；为空时从 808 拉一次并落库。

    返回 (key, source)：db | jt808 | empty
    """
    stored = await get_stored_web_service_key(db)
    if stored:
        return stored, "db"
    key = await sync_web_service_key_from_jt808(db, force_refresh=False)
    if key:
        return key, "jt808"
    return "", "empty"


async def refresh_web_service_key_after_failure(db: AsyncSession) -> str:
    """调用失败后：强制从 808 再取一次并同步到库。"""
    return await sync_web_service_key_from_jt808(db, force_refresh=True)


async def with_web_service_key(
    db: AsyncSession,
    call: Callable[[str], Awaitable[T]],
    *,
    is_success: Callable[[T], bool],
) -> tuple[T | None, str, str]:
    """用库内 Key 调用；失败则强制从 808 刷新后再调一次，成功则已写入库。

    返回 (result_or_None, key_used, source)
    """
    key, source = await ensure_web_service_key(db)
    last: T | None = None
    if key:
        last = await call(key)
        if is_success(last):
            return last, key, source

    refreshed = await refresh_web_service_key_after_failure(db)
    if not refreshed:
        return last, key, source
    if refreshed == key and last is not None:
        # Key 未变且已失败过，不再重复请求
        return last, key, source

    again = await call(refreshed)
    if is_success(again):
        # sync 已写入；若与库一致则无需再写
        return again, refreshed, "jt808"
    return again, refreshed, "jt808"

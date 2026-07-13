"""高德逆地理编码（与 JT808 AddressService.getAddressByGaode 对齐）。

优先使用 CESG 库 map_api_config.web_service_key（日常从 808 appkey1 同步）；
调用失败时强制从 808 再取一次并回写；仍无 Key 时不回退 JS Key。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.amap_web_service_key import with_web_service_key
from app.geo_utils import wgs84_to_gcj02
from app.jt808_address import (
    jt808_address_enabled,
    lookup_jt808_address_cache,
    save_jt808_address_cache,
)

_logger = logging.getLogger(__name__)

_REGEO_URL = "https://restapi.amap.com/v3/geocode/regeo"
_HTTP_TIMEOUT = 3.0
_MAX_ADDRESS_LEN = 512

_mem_cache: dict[str, str] = {}


def _cache_key(lat: float, lng: float) -> str:
    return f"{round(lat, 3)}:{round(lng, 3)}"


def _valid_coord(lat: float | None, lng: float | None) -> bool:
    if lat is None or lng is None:
        return False
    try:
        la, ln = float(lat), float(lng)
    except (TypeError, ValueError):
        return False
    if la == 0 and ln == 0:
        return False
    return -90 <= la <= 90 and -180 <= ln <= 180


def _parse_regeo_body(data: dict[str, Any]) -> str | None:
    if str(data.get("status")) != "1":
        return None
    regeocode = data.get("regeocode")
    if not isinstance(regeocode, dict):
        return None
    addr = regeocode.get("formatted_address")
    if not addr:
        return None
    text = str(addr).strip()
    pois = regeocode.get("pois")
    if isinstance(pois, list) and len(pois) > 1:
        poi = pois[1]
        if isinstance(poi, dict):
            try:
                dist = int(float(poi.get("distance") or 0))
                name = str(poi.get("name") or "").strip()
                if dist and name:
                    text = f"{text},距{name}{dist}米"
            except (TypeError, ValueError):
                pass
    return text[:_MAX_ADDRESS_LEN] if text else None


async def regeo_wgs84(api_key: str, lat: float, lng: float) -> str | None:
    key = (api_key or "").strip()
    if not key or not _valid_coord(lat, lng):
        return None
    ck = _cache_key(lat, lng)
    cached = _mem_cache.get(ck)
    if cached is not None:
        return cached or None

    jt808_cached = await asyncio.to_thread(lookup_jt808_address_cache, lat, lng)
    if jt808_cached:
        _mem_cache[ck] = jt808_cached
        return jt808_cached

    lng_gcj, lat_gcj = wgs84_to_gcj02(float(lng), float(lat))
    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            resp = await client.get(
                _REGEO_URL,
                params={
                    "key": key,
                    "location": f"{lng_gcj},{lat_gcj}",
                    "extensions": "all",
                    "roadlevel": "0",
                },
            )
            data = resp.json()
    except Exception as exc:  # noqa: BLE001
        _logger.warning("高德逆地理请求失败: %s", exc)
        return None

    if not isinstance(data, dict) or str(data.get("status")) != "1":
        _logger.warning(
            "高德逆地理失败 status=%s info=%s",
            (data or {}).get("status") if isinstance(data, dict) else None,
            (data or {}).get("info") if isinstance(data, dict) else None,
        )
        _mem_cache[ck] = ""
        return None

    address = _parse_regeo_body(data if isinstance(data, dict) else {})
    _mem_cache[ck] = address or ""
    if address:
        await asyncio.to_thread(save_jt808_address_cache, lat, lng, address)
    return address


async def resolve_address_wgs84(
    db: AsyncSession,
    lat: float | None,
    lng: float | None,
    *,
    existing: str | None = None,
) -> str:
    if (existing or "").strip():
        return str(existing).strip()[:_MAX_ADDRESS_LEN]
    if not jt808_address_enabled():
        pass  # 808 关闭时仍允许 CESG 侧补地址
    if not _valid_coord(lat, lng):
        return ""

    # 先走本地/808 地址缓存，避免无谓消耗 Key
    jt808_cached = await asyncio.to_thread(lookup_jt808_address_cache, float(lat), float(lng))
    if jt808_cached:
        return jt808_cached[:_MAX_ADDRESS_LEN]

    async def _call(key: str) -> str | None:
        # 绕过 regeo_wgs84 内失败写空缓存导致二次刷新无法再试：直接请求
        ck = _cache_key(float(lat), float(lng))
        _mem_cache.pop(ck, None)
        return await regeo_wgs84(key, float(lat), float(lng))

    resolved, _key, source = await with_web_service_key(
        db,
        _call,
        is_success=lambda addr: bool(addr and str(addr).strip()),
    )
    if resolved:
        _logger.debug("逆地理成功 source=%s", source)
        return str(resolved).strip()[:_MAX_ADDRESS_LEN]
    _logger.debug("无可用逆地理结果（web_service_key / 808 appkey1）")
    return ""

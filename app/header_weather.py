"""顶栏天气：根据访问者 IP 定位城市并查询实况（优先高德 Web 服务）。"""

from __future__ import annotations

import ipaddress
import logging
import re
import time
from threading import Lock
from typing import Any

import httpx
from fastapi import Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import MapApiConfig

_logger = logging.getLogger(__name__)

_cache: dict[str, tuple[dict[str, Any], float]] = {}
_lock = Lock()
_CACHE_TTL = 15 * 60
_DEFAULT_ADCODE = "500000"
_DEFAULT_CITY = "重庆"
_HTTP_TIMEOUT = 8.0


def _cache_get(key: str) -> dict[str, Any] | None:
    now = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit and (now - hit[1]) < _CACHE_TTL:
            return dict(hit[0])
    return None


def _cache_set(key: str, payload: dict[str, Any]) -> None:
    with _lock:
        if len(_cache) > 2000:
            _cache.clear()
        _cache[key] = (dict(payload), time.time())


def client_ip_from_request(request: Request) -> str:
    for header in ("x-forwarded-for", "x-real-ip", "cf-connecting-ip"):
        raw = (request.headers.get(header) or "").strip()
        if raw:
            return raw.split(",")[0].strip()
    if request.client and request.client.host:
        return str(request.client.host).strip()
    return ""


def _is_public_ip(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip.strip())
        return not (
            addr.is_private
            or addr.is_loopback
            or addr.is_reserved
            or addr.is_link_local
            or addr.is_multicast
        )
    except ValueError:
        return False


def _short_city_name(city: str) -> str:
    s = (city or "").strip()
    if not s:
        return _DEFAULT_CITY
    s = re.sub(r"(市|省|自治区|特别行政区)$", "", s)
    return s or _DEFAULT_CITY


def weather_icon_char(weather: str) -> str:
    w = (weather or "").strip()
    if not w:
        return "☁"
    if "晴" in w and "云" not in w and "阴" not in w:
        return "☀"
    if any(x in w for x in ("雷", "电")):
        return "⚡"
    if any(x in w for x in ("雪", "冰", "冻")):
        return "❄"
    if any(x in w for x in ("雨", "阵雨", "毛毛雨")):
        return "🌧"
    if any(x in w for x in ("雾", "霾", "沙", "尘")):
        return "🌫"
    if "阴" in w or "云" in w:
        return "☁"
    return "☁"


def _build_display(weather: str, temperature: str, city: str) -> str:
    w = (weather or "—").strip()
    t = (temperature or "").strip()
    c = _short_city_name(city)
    temp_part = f"{t}℃" if t else ""
    parts = [w]
    if temp_part:
        parts.append(temp_part)
    parts.append(c)
    return " ".join(parts)


def _wmo_to_zh(code: int) -> str:
    mapping = {
        0: "晴",
        1: "晴",
        2: "多云",
        3: "阴",
        45: "雾",
        48: "雾",
        51: "小雨",
        53: "中雨",
        55: "大雨",
        61: "小雨",
        63: "中雨",
        65: "大雨",
        71: "小雪",
        73: "中雪",
        75: "大雪",
        80: "阵雨",
        95: "雷阵雨",
    }
    return mapping.get(code, "多云")


async def _amap_key(db: AsyncSession) -> str:
    row = await db.scalar(select(MapApiConfig).where(MapApiConfig.provider == "amap").limit(1))
    return (row.api_key if row else "") or ""


async def _discover_egress_public_ip(client: httpx.AsyncClient) -> str:
    for url in (
        "https://api.ipify.org?format=json",
        "http://ip-api.com/json/?fields=query",
    ):
        try:
            res = await client.get(url)
            res.raise_for_status()
            data = res.json()
            ip = str(data.get("ip") or data.get("query") or "").strip()
            if _is_public_ip(ip):
                return ip
        except Exception as e:
            _logger.debug("egress ip discover failed (%s): %s", url, e)
    return ""


def resolve_lookup_ip(request: Request, client_ip_hint: str | None, egress_ip: str = "") -> str:
    hint = (client_ip_hint or "").strip()
    if _is_public_ip(hint):
        return hint
    req_ip = client_ip_from_request(request)
    if _is_public_ip(req_ip):
        return req_ip
    if _is_public_ip(egress_ip):
        return egress_ip
    return req_ip or ""


async def _fetch_amap_weather_by_ip(db: AsyncSession, ip: str) -> dict[str, Any]:
    key = (await _amap_key(db)).strip()
    if not key:
        return {"error": "未配置高德 Web 服务 Key"}

    adcode = _DEFAULT_ADCODE
    city = _DEFAULT_CITY
    province = ""
    ip_param = ip if _is_public_ip(ip) else ""

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        ip_data: dict[str, Any] = {}
        try:
            ip_params: dict[str, str] = {"key": key, "output": "JSON"}
            if ip_param:
                ip_params["ip"] = ip_param
            res = await client.get("https://restapi.amap.com/v3/ip", params=ip_params)
            res.raise_for_status()
            ip_data = res.json()
        except Exception as e:
            _logger.warning("amap ip 失败: %s", e)

        if str(ip_data.get("status") or "") == "1":
            adcode = str(ip_data.get("adcode") or "").strip() or adcode
            city = str(ip_data.get("city") or ip_data.get("province") or city).strip() or city
            province = str(ip_data.get("province") or "").strip()

        if not adcode:
            adcode = _DEFAULT_ADCODE

        try:
            res = await client.get(
                "https://restapi.amap.com/v3/weather/weatherInfo",
                params={
                    "key": key,
                    "city": adcode,
                    "extensions": "base",
                    "output": "JSON",
                },
            )
            res.raise_for_status()
            w_data = res.json()
        except Exception as e:
            _logger.warning("amap weather 失败: %s", e)
            return {"error": f"天气查询失败：{e}"}

    if str(w_data.get("status") or "") != "1":
        return {"error": str(w_data.get("info") or "天气接口异常")}

    live = (w_data.get("lives") or [{}])[0]
    weather = str(live.get("weather") or "").strip()
    temp = str(live.get("temperature") or "").strip()
    live_city = str(live.get("city") or city).strip() or city
    reporttime = str(live.get("reporttime") or "").strip()
    icon = weather_icon_char(weather)
    display = _build_display(weather, temp, live_city)
    return {
        "weather": weather,
        "temperature": temp,
        "city": _short_city_name(live_city),
        "province": province or str(live.get("province") or "").strip(),
        "adcode": adcode,
        "icon": icon,
        "display": display,
        "report_time": reporttime,
        "source": "amap",
        "client_ip": ip_param or ip or "local",
    }


async def _fetch_open_meteo_fallback(ip: str) -> dict[str, Any] | None:
    lat, lon, city = None, None, _DEFAULT_CITY
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        try:
            if _is_public_ip(ip):
                res = await client.get(
                    f"http://ip-api.com/json/{ip}",
                    params={"lang": "zh-CN", "fields": "status,city,lat,lon"},
                )
                res.raise_for_status()
                geo = res.json()
                if str(geo.get("status") or "").lower() == "success":
                    lat, lon = geo.get("lat"), geo.get("lon")
                    city = str(geo.get("city") or city).strip() or city
        except Exception as e:
            _logger.debug("ip-api fallback: %s", e)

        if lat is None or lon is None:
            lat, lon = 29.563, 106.551
            city = _DEFAULT_CITY

        try:
            res = await client.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": str(lat),
                    "longitude": str(lon),
                    "current": "temperature_2m,weather_code",
                    "timezone": "Asia/Shanghai",
                },
            )
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            _logger.warning("open-meteo fallback: %s", e)
            return None

    cur = data.get("current") or {}
    code = int(cur.get("weather_code") or 0)
    temp = str(cur.get("temperature_2m") or "").strip()
    if temp and "." in temp:
        try:
            temp = str(int(round(float(temp))))
        except ValueError:
            pass
    weather = _wmo_to_zh(code)
    icon = weather_icon_char(weather)
    display = _build_display(weather, temp, city)
    return {
        "weather": weather,
        "temperature": temp,
        "city": _short_city_name(city),
        "icon": icon,
        "display": display,
        "source": "open-meteo",
        "client_ip": ip or "local",
    }


async def _fetch_weather_by_coords(lat: float, lon: float, city_hint: str = "") -> dict[str, Any] | None:
    try:
        flat, flon = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    if not (-90 <= flat <= 90 and -180 <= flon <= 180):
        return None
    city = (city_hint or "").strip()
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        try:
            res = await client.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": str(flat),
                    "longitude": str(flon),
                    "current": "temperature_2m,weather_code",
                    "timezone": "Asia/Shanghai",
                },
            )
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            _logger.warning("open-meteo coords: %s", e)
            return None

    cur = data.get("current") or {}
    code = int(cur.get("weather_code") or 0)
    temp = str(cur.get("temperature_2m") or "").strip()
    if temp and "." in temp:
        try:
            temp = str(int(round(float(temp))))
        except ValueError:
            pass
    weather = _wmo_to_zh(code)
    if not city:
        city = _DEFAULT_CITY
    icon = weather_icon_char(weather)
    display = _build_display(weather, temp, city)
    return {
        "weather": weather,
        "temperature": temp,
        "city": _short_city_name(city),
        "icon": icon,
        "display": display,
        "source": "open-meteo-coords",
        "located_by": "coords",
    }


async def get_header_weather_for_request(
    request: Request,
    db: AsyncSession,
    *,
    client_ip_hint: str | None = None,
    lng: float | None = None,
    lat: float | None = None,
) -> dict[str, Any]:
    egress_ip = ""
    if not _is_public_ip((client_ip_hint or "").strip()):
        req_ip = client_ip_from_request(request)
        if not _is_public_ip(req_ip):
            async with httpx.AsyncClient(timeout=6.0) as client:
                egress_ip = await _discover_egress_public_ip(client)

    lookup_ip = resolve_lookup_ip(request, client_ip_hint, egress_ip)
    cache_key = f"ip:{lookup_ip or 'local'}:{lng or ''}:{lat or ''}"
    cached = _cache_get(cache_key)
    if cached:
        cached["cached"] = True
        return cached

    if lng is not None and lat is not None:
        coord_fb = await _fetch_weather_by_coords(lat, lng)
        if coord_fb:
            coord_fb["client_ip"] = lookup_ip or "local"
            coord_fb["cached"] = False
            _cache_set(cache_key, coord_fb)
            return coord_fb

    result = await _fetch_amap_weather_by_ip(db, lookup_ip)
    if result.get("error"):
        fb = await _fetch_open_meteo_fallback(lookup_ip)
        if fb:
            result = fb
        else:
            result = {
                "weather": "—",
                "temperature": "",
                "city": _DEFAULT_CITY,
                "icon": "☁",
                "display": f"— {_DEFAULT_CITY}",
                "source": "default",
                "client_ip": lookup_ip or "local",
                "error": result.get("error"),
            }

    result["located_by"] = "public_ip" if _is_public_ip(lookup_ip) else "fallback"
    result["client_ip"] = lookup_ip or result.get("client_ip") or "local"
    result["cached"] = False
    _cache_set(cache_key, result)
    return result

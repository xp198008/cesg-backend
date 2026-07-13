"""高德轨迹纠偏（GraspRoad Web 服务 /v4/grasproad/driving）。

将可能偏离道路的 GPS 轨迹点吸附到实际道路上，供 OBD 规则几何命中前使用。
输入坐标须为 GCJ02（高德坐标系）。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.geo_utils import bearing_deg, offset_point_m
from app.models import MapApiConfig

_logger = logging.getLogger(__name__)

_GRASP_URL = "https://restapi.amap.com/v4/grasproad/driving"
_HTTP_TIMEOUT = 8.0
_MIN_TRAIL_POINTS = 2
_MAX_TRAIL_POINTS = 10
_SYNTH_POINT_COUNT = 3


@dataclass
class GraspTrailPoint:
    lng: float
    lat: float
    speed_kmh: float
    angle: float | None
    at: datetime


@dataclass
class GraspRoadResult:
    lng: float | None = None
    lat: float | None = None
    errcode: int | str | None = None
    errmsg: str | None = None
    key_source: str | None = None


async def get_amap_web_key(db: AsyncSession) -> str:
    """JS API Key（仅前端地图 SDK）。"""
    row = await db.scalar(select(MapApiConfig).where(MapApiConfig.provider == "amap").limit(1))
    return (row.api_key if row else "") or ""


async def get_amap_grasp_keys(db: AsyncSession) -> list[tuple[str, str]]:
    """纠偏可用 Key：优先 CESG 库 web_service_key，空则从 808 同步。"""
    from app.amap_web_service_key import ensure_web_service_key

    key, source = await ensure_web_service_key(db)
    if key:
        return [(key, source)]
    return []


def _normalize_angle(angle: float | None, prev: GraspTrailPoint | None, cur: GraspTrailPoint) -> float:
    # 高德文档：ag=0 会大概率导致纠偏失败
    if angle is not None:
        try:
            a = float(angle)
            if 0 < a < 360:
                return a
        except (TypeError, ValueError):
            pass
    if prev is not None:
        b = bearing_deg(prev.lng, prev.lat, cur.lng, cur.lat)
        if b and 0 < b < 360:
            return b
    return 90.0


def _ensure_min_trail(trail: list[GraspTrailPoint]) -> list[GraspTrailPoint]:
    """点数不足时沿行驶方向合成前置点，提高单点纠偏成功率。"""
    if len(trail) >= _MIN_TRAIL_POINTS:
        return trail[-_MAX_TRAIL_POINTS:]
    cur = trail[-1]
    angle = _normalize_angle(cur.angle, None, cur)
    speed = max(float(cur.speed_kmh or 0), 10.0)
    step_m = max(20.0, min(speed / 3.6 * 5.0, 120.0))
    out: list[GraspTrailPoint] = []
    for i in range(_SYNTH_POINT_COUNT, 0, -1):
        lng, lat = offset_point_m(cur.lng, cur.lat, (angle + 180) % 360, step_m * i)
        out.append(
            GraspTrailPoint(
                lng,
                lat,
                speed,
                angle,
                cur.at - timedelta(seconds=5 * i),
            )
        )
    out.append(cur)
    return out


def _build_request_body(trail: list[GraspTrailPoint]) -> list[dict[str, Any]]:
    points = _ensure_min_trail(trail)
    body: list[dict[str, Any]] = []
    base_at = points[0].at
    prev: GraspTrailPoint | None = None
    for p in points:
        angle = _normalize_angle(p.angle, prev, p)
        sp = max(1, int(round(p.speed_kmh or 10)))
        tm = int(p.at.timestamp()) if not body else max(1, int((p.at - base_at).total_seconds()))
        ag = int(angle) % 360
        if ag == 0:
            ag = 90
        body.append(
            {
                "x": round(p.lng, 6),
                "y": round(p.lat, 6),
                "sp": sp,
                "ag": ag,
                "tm": tm,
            }
        )
        prev = p
    return body


async def grasp_road_request(api_key: str, trail: list[GraspTrailPoint]) -> GraspRoadResult:
    """纠偏轨迹并返回最后一个点的道路坐标（GCJ02）及高德原始错误信息。"""
    key = (api_key or "").strip()
    if not key or not trail:
        return GraspRoadResult(errcode="local", errmsg="missing_key_or_trail")
    body = _build_request_body(trail)
    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            res = await client.post(_GRASP_URL, params={"key": key}, json=body)
            res.raise_for_status()
            data = res.json()
    except Exception as exc:  # noqa: BLE001
        _logger.warning("高德轨迹纠偏请求失败: %s", exc)
        return GraspRoadResult(errcode="http", errmsg=str(exc))

    errcode = data.get("errcode")
    errmsg = str(data.get("errmsg") or "")
    if errcode not in (0, "0", None):
        _logger.warning("高德轨迹纠偏失败 errcode=%s errmsg=%s", errcode, errmsg)
        return GraspRoadResult(errcode=errcode, errmsg=errmsg)

    payload = data.get("data") if isinstance(data.get("data"), dict) else {}
    points = payload.get("points") if isinstance(payload.get("points"), list) else []
    if not points:
        _logger.warning("高德轨迹纠偏无返回点 errcode=%s errmsg=%s", errcode, errmsg)
        return GraspRoadResult(errcode=errcode or 30001, errmsg=errmsg or "empty_points")

    last = points[-1]
    if not isinstance(last, dict):
        return GraspRoadResult(errcode=30001, errmsg="invalid_point_shape")
    try:
        lng = float(last.get("x"))
        lat = float(last.get("y"))
    except (TypeError, ValueError):
        return GraspRoadResult(errcode=30001, errmsg="invalid_point_coord")
    if not lng or not lat:
        return GraspRoadResult(errcode=30001, errmsg="zero_coord")
    return GraspRoadResult(lng=lng, lat=lat, errcode=0, errmsg=errmsg)


async def grasp_road_last_point(api_key: str, trail: list[GraspTrailPoint]) -> tuple[float, float] | None:
    result = await grasp_road_request(api_key, trail)
    if result.lng is None or result.lat is None:
        return None
    return result.lng, result.lat


async def grasp_road_with_keys(
    db: AsyncSession,
    trail: list[GraspTrailPoint],
) -> GraspRoadResult:
    """库内 Web 服务 Key 优先；失败则强制从 808 刷新后再调一次。"""
    from app.amap_web_service_key import with_web_service_key

    async def _call(key: str) -> GraspRoadResult:
        return await grasp_road_request(key, trail)

    result, _key, source = await with_web_service_key(
        db,
        _call,
        is_success=lambda r: r is not None and r.lng is not None and r.lat is not None,
    )
    if result is None:
        return GraspRoadResult(errcode="local", errmsg="no_key", key_source=source)
    result.key_source = source
    return result

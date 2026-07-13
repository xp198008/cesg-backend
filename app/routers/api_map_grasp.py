"""实时监控地图轨迹纠偏（高德 GraspRoad）。"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.amap_grasp_road import GraspTrailPoint, grasp_road_with_keys
from app.database import get_db
from app.geo_utils import wgs84_to_gcj02
from app.timeutil import china_now_naive

router = APIRouter(prefix="/api", tags=["map-grasp"])


class MapGraspRoadSnapIn(BaseModel):
    lng: float = Field(..., description="WGS84 经度")
    lat: float = Field(..., description="WGS84 纬度")
    speed_kmh: float | None = Field(None, ge=0, le=300)
    angle: float | None = Field(None, ge=0, lt=360)


@router.post("/map/grasp-road/snap")
async def map_grasp_road_snap(body: MapGraspRoadSnapIn, db: AsyncSession = Depends(get_db)):
    """将单点 GPS 吸附到道路（GCJ02）。失败时回落为坐标转换结果。"""
    lng_gcj, lat_gcj = wgs84_to_gcj02(float(body.lng), float(body.lat))
    speed = float(body.speed_kmh or 0)
    if speed < 3:
        return {
            "ok": True,
            "lng": lng_gcj,
            "lat": lat_gcj,
            "corrected": False,
            "reason": "low_speed",
        }

    trail = [
        GraspTrailPoint(
            lng_gcj,
            lat_gcj,
            max(speed, 10.0),
            body.angle,
            china_now_naive(),
        )
    ]
    result = await grasp_road_with_keys(db, trail)
    if result.lng is not None and result.lat is not None:
        return {
            "ok": True,
            "lng": result.lng,
            "lat": result.lat,
            "corrected": True,
            "key_source": result.key_source,
        }
    return {
        "ok": True,
        "lng": lng_gcj,
        "lat": lat_gcj,
        "corrected": False,
        "reason": "grasp_failed",
        "key_source": result.key_source,
        "grasp_errcode": result.errcode,
        "grasp_errmsg": result.errmsg,
    }

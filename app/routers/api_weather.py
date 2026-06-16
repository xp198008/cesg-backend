"""顶栏天气 API：后端按 IP 定位城市并缓存，避免前端页面加载被外部接口拖慢。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.header_weather import get_header_weather_for_request

router = APIRouter(prefix="/api/weather", tags=["weather"])


@router.get("/header")
async def header_weather(
    request: Request,
    client_ip: str | None = Query(
        None,
        description="浏览器探测的公网 IP（本机 127.0.0.1 访问时用于定位真实城市）",
    ),
    lng: float | None = Query(None, description="可选：经度"),
    lat: float | None = Query(None, description="可选：纬度"),
    db: AsyncSession = Depends(get_db),
):
    data = await get_header_weather_for_request(
        request,
        db,
        client_ip_hint=client_ip,
        lng=lng,
        lat=lat,
    )
    return {"ok": True, "data": data}

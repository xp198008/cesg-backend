"""地图配置与公用地图规则（公用限速管理）本地接口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import (
    MapApiConfig,
    MapRuleCategory,
    OrgCompany,
    PrivateMapRule,
    PrivateMapRuleWeather,
    PublicMapRule,
    Vehicle,
)

router = APIRouter(prefix="/api", tags=["map-rules"])


WEATHER_TYPE_OPTIONS = [
    {"code": "sunny", "label": "晴"},
    {"code": "cloudy", "label": "多云"},
    {"code": "overcast", "label": "阴"},
    {"code": "rain", "label": "雨"},
    {"code": "snow", "label": "雪"},
    {"code": "fog", "label": "雾"},
    {"code": "wind", "label": "大风"},
    {"code": "other", "label": "其他"},
]


async def _resolve_company_id(db: AsyncSession, x_org_id: str | None = None) -> int:
    raw = (x_org_id or "").strip()
    if raw:
        try:
            cid = int(raw)
        except ValueError:
            raise HTTPException(status_code=400, detail="X-Org-Id 无效")
        exists = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == cid).limit(1))
        if exists:
            return int(exists)
    cid = await db.scalar(select(OrgCompany.id).order_by(OrgCompany.id).limit(1))
    if not cid:
        raise HTTPException(status_code=400, detail="请先维护公司信息")
    return int(cid)


class MapApiConfigBody(BaseModel):
    provider: str = "amap"
    api_key: str | None = None
    secret_key: str | None = None
    default_zoom: int | None = Field(None, ge=1, le=20)
    default_center_lng: float | None = None
    default_center_lat: float | None = None
    remark: str | None = None


def _map_config_out(row: MapApiConfig) -> dict:
    return {
        "id": row.id,
        "provider": row.provider,
        "api_key": row.api_key,
        "secret_key": row.secret_key,
        "default_zoom": row.default_zoom,
        "default_center_lng": row.default_center_lng,
        "default_center_lat": row.default_center_lat,
        "remark": row.remark,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@router.get("/map-api-config")
async def map_api_config_get(
    provider: str = Query("amap"),
    db: AsyncSession = Depends(get_db),
):
    row = await db.scalar(select(MapApiConfig).where(MapApiConfig.provider == (provider or "amap")).limit(1))
    if row is not None:
        await db.refresh(row)
    return {"ok": True, "data": _map_config_out(row) if row else None}


@router.put("/map-api-config")
async def map_api_config_put(body: MapApiConfigBody, db: AsyncSession = Depends(get_db)):
    provider = (body.provider or "amap").strip() or "amap"
    row = await db.scalar(select(MapApiConfig).where(MapApiConfig.provider == provider).limit(1))
    if row is None:
        row = MapApiConfig(provider=provider)
        db.add(row)
    row.api_key = (body.api_key or "").strip() or None
    row.secret_key = (body.secret_key or "").strip() or None
    if body.default_zoom is not None:
        row.default_zoom = body.default_zoom
    if body.default_center_lng is not None:
        row.default_center_lng = body.default_center_lng
    if body.default_center_lat is not None:
        row.default_center_lat = body.default_center_lat
    row.remark = (body.remark or "").strip() or None
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _map_config_out(row)}


class PublicMapRuleCreateBody(BaseModel):
    rule_code: str = Field(..., min_length=1, max_length=64)
    rule_name: str = Field(..., min_length=1, max_length=200)
    rule_type_code: str = Field(..., min_length=1, max_length=32)
    draw_shape_type: str = Field(..., min_length=1, max_length=32)
    geometry_json: dict[str, Any] | list[Any]
    remark: str | None = Field(None, max_length=255)


class PublicMapRuleUpdateBody(BaseModel):
    rule_name: str | None = Field(None, min_length=1, max_length=200)
    geometry_json: dict[str, Any] | list[Any] | None = None
    remark: str | None = Field(None, max_length=255)


def _rule_out(row: PublicMapRule) -> dict:
    return {
        "id": row.id,
        "rule_code": row.rule_code,
        "rule_name": row.rule_name,
        "rule_type_code": row.rule_type_code,
        "draw_shape_type": row.draw_shape_type,
        "is_public": row.is_public,
        "geometry_json": row.geometry_json,
        "remark": row.remark,
        "created_at": row.created_at.strftime("%Y-%m-%d %H:%M:%S") if row.created_at else None,
        "updated_at": row.updated_at.strftime("%Y-%m-%d %H:%M:%S") if row.updated_at else None,
    }


@router.get("/public-map-rules")
async def public_map_rules_list(
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    is_public: int | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(PublicMapRule)
    if is_public is not None:
        stmt = stmt.where(PublicMapRule.is_public == is_public)
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        await db.execute(stmt.order_by(PublicMapRule.id.desc()).offset(offset).limit(limit))
    ).scalars().all()
    return {"ok": True, "items": [_rule_out(x) for x in rows], "total": total}


@router.get("/public-map-rules/{rid}")
async def public_map_rule_get(rid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(PublicMapRule).where(PublicMapRule.id == rid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": _rule_out(row)}


@router.post("/public-map-rules")
async def public_map_rule_create(body: PublicMapRuleCreateBody, db: AsyncSession = Depends(get_db)):
    rule_code = body.rule_code.strip()
    if await db.scalar(select(PublicMapRule.id).where(PublicMapRule.rule_code == rule_code).limit(1)):
        raise HTTPException(status_code=400, detail="规则编号已存在")
    row = PublicMapRule(
        rule_code=rule_code,
        rule_name=body.rule_name.strip(),
        rule_type_code=body.rule_type_code.strip(),
        draw_shape_type=body.draw_shape_type.strip(),
        geometry_json=body.geometry_json,
        is_public=1,
        remark=(body.remark or "").strip() or None,
    )
    db.add(row)
    await db.flush()
    return {"ok": True, "id": row.id}


@router.put("/public-map-rules/{rid}")
async def public_map_rule_update(
    rid: int,
    body: PublicMapRuleUpdateBody,
    db: AsyncSession = Depends(get_db),
):
    row = await db.scalar(select(PublicMapRule).where(PublicMapRule.id == rid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    data = body.model_dump(exclude_unset=True)
    if "rule_name" in data and body.rule_name is not None:
        row.rule_name = body.rule_name.strip()
    if "geometry_json" in data:
        row.geometry_json = body.geometry_json
    if "remark" in data:
        row.remark = (body.remark or "").strip() or None
    await db.flush()
    return {"ok": True}


@router.delete("/public-map-rules/{rid}")
async def public_map_rule_delete(rid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(PublicMapRule).where(PublicMapRule.id == rid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    await db.delete(row)
    await db.flush()
    return {"ok": True}


class PrivateMapRuleCreateBody(BaseModel):
    rule_code: str = Field(..., min_length=1, max_length=64)
    rule_name: str = Field(..., min_length=1, max_length=200)
    rule_type_code: str = Field(..., min_length=1, max_length=32)
    draw_shape_type: str = Field(..., min_length=1, max_length=32)
    geometry_json: dict[str, Any] | list[Any]
    speed_limit_kmh: int = Field(0, ge=0, le=500)
    ref_public_rule_id: int | None = None
    remark: str | None = Field(None, max_length=255)


class PrivateMapRuleUpdateBody(BaseModel):
    rule_name: str | None = Field(None, min_length=1, max_length=200)
    geometry_json: dict[str, Any] | list[Any] | None = None
    speed_limit_kmh: int | None = Field(None, ge=0, le=500)
    ref_public_rule_id: int | None = None
    remark: str | None = Field(None, max_length=255)


class PrivateRuleCategoryAssignBody(BaseModel):
    category_ids: list[int] = Field(default_factory=list)


def _private_rule_out(row: PrivateMapRule) -> dict:
    return {
        "id": row.id,
        "company_id": row.company_id,
        "rule_code": row.rule_code,
        "rule_name": row.rule_name,
        "rule_type_code": row.rule_type_code,
        "draw_shape_type": row.draw_shape_type,
        "geometry_json": row.geometry_json,
        "speed_limit_kmh": row.speed_limit_kmh,
        "ref_public_rule_id": row.ref_public_rule_id,
        "category_ids": _normalize_vehicle_ids(row.category_ids if isinstance(row.category_ids, list) else []),
        "remark": row.remark,
        "created_at": row.created_at.strftime("%Y-%m-%d %H:%M:%S") if row.created_at else None,
        "updated_at": row.updated_at.strftime("%Y-%m-%d %H:%M:%S") if row.updated_at else None,
    }


@router.get("/private-map-rules")
async def private_map_rules_list(
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    stmt = select(PrivateMapRule).where(PrivateMapRule.company_id == cid)
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        await db.execute(stmt.order_by(PrivateMapRule.id.desc()).offset(offset).limit(limit))
    ).scalars().all()
    return {"ok": True, "items": [_private_rule_out(x) for x in rows], "total": total}


@router.get("/private-map-rules/weather-type-options")
async def private_map_rule_weather_type_options():
    return {"ok": True, "items": WEATHER_TYPE_OPTIONS}


class MapRuleCategoryCreateBody(BaseModel):
    type_name: str = Field(..., min_length=1, max_length=128)
    speed_limit_kmh: int = Field(0, ge=0, le=500)
    weather_rule_id: int | None = None
    weather_types: list[str] = Field(default_factory=lambda: ["sunny"])
    weather_speed_limits: dict[str, int] = Field(default_factory=dict)
    assigned_vehicle_ids: list[int] = Field(default_factory=list)
    remark: str | None = Field(None, max_length=255)


class MapRuleCategoryUpdateBody(BaseModel):
    type_name: str | None = Field(None, min_length=1, max_length=128)
    speed_limit_kmh: int | None = Field(None, ge=0, le=500)
    weather_rule_id: int | None = None
    weather_types: list[str] | None = None
    weather_speed_limits: dict[str, int] | None = None
    assigned_vehicle_ids: list[int] | None = None
    remark: str | None = Field(None, max_length=255)


def _weather_type_label(code: str | None) -> str:
    c = (code or "").strip().lower()
    for x in WEATHER_TYPE_OPTIONS:
        if x.get("code") == c:
            return str(x.get("label") or c)
    return c or "—"


def _normalize_vehicle_ids(values: list[int] | None) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for x in values or []:
        try:
            n = int(x)
        except (TypeError, ValueError):
            continue
        if n < 1 or n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def _normalize_weather_types(values: list[str] | None) -> list[str]:
    allowed = {str(x["code"]) for x in WEATHER_TYPE_OPTIONS}
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        code = str(raw or "").strip().lower()
        if not code or code not in allowed or code in seen:
            continue
        seen.add(code)
        out.append(code)
    if "sunny" not in seen:
        out.insert(0, "sunny")
    return out


def _normalize_weather_speed_limits(values: dict[str, Any] | None, weather_types: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    allowed = set(weather_types)
    for code in allowed:
        raw = (values or {}).get(code)
        try:
            n = int(raw)
        except (TypeError, ValueError):
            n = 0
        out[code] = max(0, min(500, n))
    return out


async def _weather_rule_belongs_to_company(db: AsyncSession, weather_rule_id: int | None, company_id: int) -> bool:
    if weather_rule_id is None:
        return True
    row = await db.scalar(
        select(PrivateMapRuleWeather.id)
        .join(PrivateMapRule, PrivateMapRule.id == PrivateMapRuleWeather.private_map_rule_id)
        .where(PrivateMapRuleWeather.id == weather_rule_id, PrivateMapRule.company_id == company_id)
        .limit(1)
    )
    return row is not None


async def _category_out(db: AsyncSession, row: MapRuleCategory) -> dict:
    vehicle_ids = _normalize_vehicle_ids(row.assigned_vehicle_ids if isinstance(row.assigned_vehicle_ids, list) else [])
    weather_types = _normalize_weather_types(row.weather_types if isinstance(row.weather_types, list) else [])
    weather_speed_limits = _normalize_weather_speed_limits(
        row.weather_speed_limits if isinstance(row.weather_speed_limits, dict) else {},
        weather_types,
    )
    plate_map: dict[int, str] = {}
    if vehicle_ids:
        vrows = (
            await db.execute(select(Vehicle.id, Vehicle.plate_no).where(Vehicle.id.in_(vehicle_ids)))
        ).all()
        plate_map = {int(a): (b or "") for a, b in vrows}
    vehicle_plates = [plate_map[x] for x in vehicle_ids if plate_map.get(x)]

    weather_type_code = None
    weather_type_label = None
    weather_speed_limit_kmh = None
    weather_rule_label = None
    if row.weather_rule_id is not None:
        wr_pair = (
            await db.execute(
                select(PrivateMapRuleWeather, PrivateMapRule)
                .join(PrivateMapRule, PrivateMapRule.id == PrivateMapRuleWeather.private_map_rule_id)
                .where(PrivateMapRuleWeather.id == int(row.weather_rule_id))
                .limit(1)
            )
        ).first()
        if wr_pair:
            wr, pr = wr_pair
            weather_type_code = (wr.weather_type_code or "").strip().lower()
            weather_type_label = _weather_type_label(weather_type_code)
            weather_speed_limit_kmh = int(wr.speed_limit_kmh)
            weather_rule_label = f"#{wr.id} {weather_type_label} {weather_speed_limit_kmh}km/h（{pr.rule_name or ''}）"

    return {
        "id": int(row.id),
        "type_name": row.type_name,
        "company_id": int(row.company_id),
        "speed_limit_kmh": int(row.speed_limit_kmh or 0),
        "weather_rule_id": int(row.weather_rule_id) if row.weather_rule_id is not None else None,
        "weather_types": weather_types,
        "weather_speed_limits": weather_speed_limits,
        "weather_type_labels": [_weather_type_label(x) for x in weather_types],
        "weather_type_code": weather_type_code,
        "weather_type_label": weather_type_label,
        "weather_speed_limit_kmh": weather_speed_limit_kmh,
        "weather_rule_label": weather_rule_label,
        "assigned_vehicle_ids": vehicle_ids,
        "assigned_vehicle_plates": vehicle_plates,
        "remark": row.remark,
        "created_at": row.created_at.strftime("%Y-%m-%d %H:%M:%S") if row.created_at else None,
        "updated_at": row.updated_at.strftime("%Y-%m-%d %H:%M:%S") if row.updated_at else None,
    }


@router.get("/map-rule-categories/weather-rule-options")
async def map_rule_category_weather_rule_options(
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    rows = (
        await db.execute(
            select(PrivateMapRuleWeather, PrivateMapRule)
            .join(PrivateMapRule, PrivateMapRule.id == PrivateMapRuleWeather.private_map_rule_id)
            .where(PrivateMapRule.company_id == cid)
            .order_by(PrivateMapRuleWeather.id.desc())
        )
    ).all()
    items = []
    for wr, pr in rows:
        wlabel = _weather_type_label(wr.weather_type_code)
        spd = int(wr.speed_limit_kmh)
        items.append(
            {
                "id": int(wr.id),
                "weather_type_code": (wr.weather_type_code or "").strip().lower(),
                "weather_type_label": wlabel,
                "speed_limit_kmh": spd,
                "private_map_rule_id": int(pr.id),
                "private_map_rule_name": pr.rule_name,
                "label": f"#{wr.id} {wlabel} {spd}km/h · {pr.rule_name}",
            }
        )
    return {"ok": True, "items": items}


@router.get("/map-rule-categories")
async def map_rule_categories_list(
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    type_name: str | None = Query(None),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    stmt = select(MapRuleCategory).where(MapRuleCategory.company_id == cid)
    kw = (type_name or "").strip()
    if kw:
        stmt = stmt.where(MapRuleCategory.type_name.ilike(f"%{kw}%"))
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        await db.execute(stmt.order_by(MapRuleCategory.id.desc()).offset(offset).limit(limit))
    ).scalars().all()
    return {"ok": True, "items": [await _category_out(db, x) for x in rows], "total": total}


@router.get("/map-rule-categories/{rid}")
async def map_rule_category_get(
    rid: int,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    row = await db.scalar(
        select(MapRuleCategory).where(MapRuleCategory.id == rid, MapRuleCategory.company_id == cid).limit(1)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": await _category_out(db, row)}


@router.get("/map-rule-categories/{rid}/resolve-speed")
async def map_rule_category_resolve_speed(
    rid: int,
    weather_type_code: str | None = Query(None),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    row = await db.scalar(
        select(MapRuleCategory).where(MapRuleCategory.id == rid, MapRuleCategory.company_id == cid).limit(1)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    default_speed = int(row.speed_limit_kmh or 0)
    cur = (weather_type_code or "").strip().lower()
    weather_types = _normalize_weather_types(row.weather_types if isinstance(row.weather_types, list) else [])
    weather_speed_limits = _normalize_weather_speed_limits(
        row.weather_speed_limits if isinstance(row.weather_speed_limits, dict) else {},
        weather_types,
    )
    if cur and cur in weather_types:
        return {"ok": True, "speed_limit_kmh": int(weather_speed_limits.get(cur, default_speed)), "source": "weather_type"}
    if row.weather_rule_id is not None and cur:
        wr = await db.scalar(
            select(PrivateMapRuleWeather).where(PrivateMapRuleWeather.id == int(row.weather_rule_id)).limit(1)
        )
        if wr and (wr.weather_type_code or "").strip().lower() == cur:
            return {"ok": True, "speed_limit_kmh": int(wr.speed_limit_kmh), "source": "weather"}
    return {"ok": True, "speed_limit_kmh": default_speed, "source": "default"}


@router.post("/map-rule-categories")
async def map_rule_category_create(
    body: MapRuleCategoryCreateBody,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    name = body.type_name.strip()
    if body.weather_rule_id is not None and not await _weather_rule_belongs_to_company(db, body.weather_rule_id, cid):
        raise HTTPException(status_code=400, detail="天气规则不存在或不属于本公司")
    row = MapRuleCategory(
        type_name=name,
        company_id=cid,
        speed_limit_kmh=int(body.speed_limit_kmh or 0),
        weather_rule_id=body.weather_rule_id,
        weather_types=_normalize_weather_types(body.weather_types),
        weather_speed_limits=_normalize_weather_speed_limits(body.weather_speed_limits, _normalize_weather_types(body.weather_types)),
        assigned_vehicle_ids=_normalize_vehicle_ids(body.assigned_vehicle_ids),
        remark=(body.remark or "").strip() or None,
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "id": row.id, "data": await _category_out(db, row)}


@router.put("/map-rule-categories/{rid}")
async def map_rule_category_update(
    rid: int,
    body: MapRuleCategoryUpdateBody,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    row = await db.scalar(
        select(MapRuleCategory).where(MapRuleCategory.id == rid, MapRuleCategory.company_id == cid).limit(1)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    data = body.model_dump(exclude_unset=True)
    if "type_name" in data and body.type_name is not None:
        row.type_name = body.type_name.strip()
    if "speed_limit_kmh" in data:
        row.speed_limit_kmh = int(body.speed_limit_kmh or 0)
    if "weather_rule_id" in data:
        if body.weather_rule_id is not None and not await _weather_rule_belongs_to_company(db, body.weather_rule_id, cid):
            raise HTTPException(status_code=400, detail="天气规则不存在或不属于本公司")
        row.weather_rule_id = body.weather_rule_id
    if "weather_types" in data:
        row.weather_types = _normalize_weather_types(body.weather_types)
    if "weather_speed_limits" in data or "weather_types" in data:
        wtypes = _normalize_weather_types(body.weather_types) if body.weather_types is not None else _normalize_weather_types(row.weather_types if isinstance(row.weather_types, list) else [])
        row.weather_speed_limits = _normalize_weather_speed_limits(body.weather_speed_limits, wtypes)
    if "assigned_vehicle_ids" in data:
        row.assigned_vehicle_ids = _normalize_vehicle_ids(body.assigned_vehicle_ids)
    if "remark" in data:
        row.remark = (body.remark or "").strip() or None
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": await _category_out(db, row)}


@router.delete("/map-rule-categories/{rid}")
async def map_rule_category_delete(
    rid: int,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    row = await db.scalar(
        select(MapRuleCategory).where(MapRuleCategory.id == rid, MapRuleCategory.company_id == cid).limit(1)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    await db.delete(row)
    await db.flush()
    return {"ok": True}


@router.get("/private-map-rules/{rid}")
async def private_map_rule_get(
    rid: int,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    row = await db.scalar(
        select(PrivateMapRule).where(PrivateMapRule.id == rid, PrivateMapRule.company_id == cid).limit(1)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": _private_rule_out(row)}


@router.get("/private-map-rules/{rid}/categories")
async def private_map_rule_categories_get(
    rid: int,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    row = await db.scalar(
        select(PrivateMapRule).where(PrivateMapRule.id == rid, PrivateMapRule.company_id == cid).limit(1)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    selected_ids = _normalize_vehicle_ids(row.category_ids if isinstance(row.category_ids, list) else [])
    rows = (
        await db.execute(select(MapRuleCategory).where(MapRuleCategory.company_id == cid).order_by(MapRuleCategory.id.desc()))
    ).scalars().all()
    return {
        "ok": True,
        "selected_category_ids": selected_ids,
        "items": [await _category_out(db, x) for x in rows],
    }


@router.put("/private-map-rules/{rid}/categories")
async def private_map_rule_categories_put(
    rid: int,
    body: PrivateRuleCategoryAssignBody,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    row = await db.scalar(
        select(PrivateMapRule).where(PrivateMapRule.id == rid, PrivateMapRule.company_id == cid).limit(1)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    ids = _normalize_vehicle_ids(body.category_ids)
    if ids:
        rows = (
            await db.execute(
                select(MapRuleCategory.id, MapRuleCategory.type_name, MapRuleCategory.assigned_vehicle_ids)
                .where(MapRuleCategory.company_id == cid, MapRuleCategory.id.in_(ids))
            )
        ).all()
        existing = {int(rid) for rid, _, _ in rows}
        missing = [x for x in ids if x not in existing]
        if missing:
            raise HTTPException(status_code=400, detail="包含不存在或不属于本公司的规则类别")
        no_vehicle = [
            int(rid)
            for rid, _, vehicle_ids in rows
            if not _normalize_vehicle_ids(vehicle_ids if isinstance(vehicle_ids, list) else [])
        ]
        if no_vehicle:
            raise HTTPException(status_code=400, detail="包含未分配车辆的规则类别，不能分配")
        vehicle_owner: dict[int, str] = {}
        conflicts: list[str] = []
        for rid, type_name, vehicle_ids in rows:
            label = str(type_name or f"类别#{rid}")
            for vid in _normalize_vehicle_ids(vehicle_ids if isinstance(vehicle_ids, list) else []):
                if vid in vehicle_owner:
                    conflicts.append(f"规则「{vehicle_owner[vid]}」与规则「{label}」车辆有交集")
                    break
                else:
                    vehicle_owner[vid] = label
            if conflicts:
                break
        if conflicts:
            raise HTTPException(status_code=400, detail=conflicts[0])
    row.category_ids = ids
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "selected_category_ids": ids, "data": _private_rule_out(row)}


@router.post("/private-map-rules")
async def private_map_rule_create(
    body: PrivateMapRuleCreateBody,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    rule_code = body.rule_code.strip()
    if await db.scalar(select(PrivateMapRule.id).where(PrivateMapRule.rule_code == rule_code).limit(1)):
        raise HTTPException(status_code=400, detail="规则编号已存在")
    row = PrivateMapRule(
        company_id=cid,
        rule_code=rule_code,
        rule_name=body.rule_name.strip(),
        rule_type_code=body.rule_type_code.strip(),
        draw_shape_type=body.draw_shape_type.strip(),
        geometry_json=body.geometry_json,
        speed_limit_kmh=body.speed_limit_kmh,
        ref_public_rule_id=body.ref_public_rule_id,
        remark=(body.remark or "").strip() or None,
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "id": row.id, "data": _private_rule_out(row)}


@router.put("/private-map-rules/{rid}")
async def private_map_rule_update(
    rid: int,
    body: PrivateMapRuleUpdateBody,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    row = await db.scalar(
        select(PrivateMapRule).where(PrivateMapRule.id == rid, PrivateMapRule.company_id == cid).limit(1)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    data = body.model_dump(exclude_unset=True)
    if "rule_name" in data and body.rule_name is not None:
        row.rule_name = body.rule_name.strip()
    if "geometry_json" in data:
        row.geometry_json = body.geometry_json
    if "speed_limit_kmh" in data and body.speed_limit_kmh is not None:
        row.speed_limit_kmh = body.speed_limit_kmh
    if "ref_public_rule_id" in data:
        row.ref_public_rule_id = body.ref_public_rule_id
    if "remark" in data:
        row.remark = (body.remark or "").strip() or None
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _private_rule_out(row)}


@router.delete("/private-map-rules/{rid}")
async def private_map_rule_delete(
    rid: int,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    row = await db.scalar(
        select(PrivateMapRule).where(PrivateMapRule.id == rid, PrivateMapRule.company_id == cid).limit(1)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    await db.execute(delete(PrivateMapRuleWeather).where(PrivateMapRuleWeather.private_map_rule_id == rid))
    await db.delete(row)
    await db.flush()
    return {"ok": True}


class BatchFromPublicBody(BaseModel):
    public_rule_ids: list[int] = Field(default_factory=list)


@router.post("/private-map-rules/batch-from-public")
async def private_map_rules_batch_from_public(
    body: BatchFromPublicBody,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    created = updated = skipped = 0
    for public_id in list(dict.fromkeys(body.public_rule_ids)):
        pub = await db.scalar(select(PublicMapRule).where(PublicMapRule.id == public_id).limit(1))
        if pub is None:
            skipped += 1
            continue
        code = f"PRV-C{cid}-P{public_id}"
        row = await db.scalar(
            select(PrivateMapRule).where(PrivateMapRule.company_id == cid, PrivateMapRule.ref_public_rule_id == public_id).limit(1)
        )
        if row is None:
            row = PrivateMapRule(company_id=cid, rule_code=code)
            db.add(row)
            created += 1
        else:
            updated += 1
        row.rule_name = pub.rule_name
        row.rule_type_code = pub.rule_type_code
        row.draw_shape_type = pub.draw_shape_type
        row.geometry_json = pub.geometry_json
        row.speed_limit_kmh = pub.speed_limit_kmh or 0
        row.ref_public_rule_id = public_id
        row.remark = pub.remark
    await db.flush()
    return {"ok": True, "created": created, "updated": updated, "skipped": skipped}


class WeatherRuleBody(BaseModel):
    weather_type_code: str
    speed_limit_kmh: int = Field(..., ge=0, le=500)
    remark: str | None = Field(None, max_length=255)


class WeatherRulesPutBody(BaseModel):
    items: list[WeatherRuleBody] = Field(default_factory=list)


def _weather_rule_out(row: PrivateMapRuleWeather) -> dict:
    label = next((x["label"] for x in WEATHER_TYPE_OPTIONS if x["code"] == row.weather_type_code), row.weather_type_code)
    return {
        "id": row.id,
        "private_map_rule_id": row.private_map_rule_id,
        "weather_type_code": row.weather_type_code,
        "weather_type_label": label,
        "speed_limit_kmh": row.speed_limit_kmh,
        "sort_order": row.sort_order,
        "remark": row.remark,
    }


@router.get("/private-map-rules/{rid}/weather-rules")
async def private_map_rule_weather_rules_list(
    rid: int,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    exists = await db.scalar(select(PrivateMapRule.id).where(PrivateMapRule.id == rid, PrivateMapRule.company_id == cid).limit(1))
    if not exists:
        raise HTTPException(status_code=404, detail="记录不存在")
    rows = (
        await db.execute(
            select(PrivateMapRuleWeather)
            .where(PrivateMapRuleWeather.private_map_rule_id == rid)
            .order_by(PrivateMapRuleWeather.sort_order, PrivateMapRuleWeather.id)
        )
    ).scalars().all()
    return {"ok": True, "items": [_weather_rule_out(x) for x in rows]}


@router.put("/private-map-rules/{rid}/weather-rules")
async def private_map_rule_weather_rules_save(
    rid: int,
    body: WeatherRulesPutBody | list[WeatherRuleBody],
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    cid = await _resolve_company_id(db, x_org_id)
    exists = await db.scalar(select(PrivateMapRule.id).where(PrivateMapRule.id == rid, PrivateMapRule.company_id == cid).limit(1))
    if not exists:
        raise HTTPException(status_code=404, detail="记录不存在")
    items = body if isinstance(body, list) else body.items
    await db.execute(delete(PrivateMapRuleWeather).where(PrivateMapRuleWeather.private_map_rule_id == rid))
    for idx, item in enumerate(items):
        db.add(
            PrivateMapRuleWeather(
                private_map_rule_id=rid,
                weather_type_code=item.weather_type_code.strip().lower(),
                speed_limit_kmh=item.speed_limit_kmh,
                sort_order=idx,
                remark=(item.remark or "").strip() or None,
            )
        )
    await db.flush()
    rows = (
        await db.execute(
            select(PrivateMapRuleWeather)
            .where(PrivateMapRuleWeather.private_map_rule_id == rid)
            .order_by(PrivateMapRuleWeather.sort_order, PrivateMapRuleWeather.id)
        )
    ).scalars().all()
    return {"ok": True, "items": [_weather_rule_out(x) for x in rows]}

"""停车超时报表：读 808 cesg_park_alarm，按组织/车辆权限过滤并分页。"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.amap_regeo import resolve_address_gcj02
from app.database import get_db
from app.jt808_park_alarm_sync import TABLE_NAME, _connect, _ensure_table
from app.models import PrivateMapRule, Vehicle
from app.vehicle_alloc_scope import parse_user_id_header, resolve_allowed_plate_nos

logger = logging.getLogger(__name__)

router = APIRouter(tags=["park-alarm-report"])


def _parse_day_start(raw: str | None) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        try:
            return datetime.strptime(digits[:8], "%Y%m%d")
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], fmt)
        except ValueError:
            continue
    return None


def _fmt_dt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    text = str(value).strip()
    return text[:19] if text else ""


@router.get("/api/park-alarm/report")
async def park_alarm_report(
    start_date: str | None = Query(None, description="开始日期 yyyy-MM-dd / yyyyMMdd"),
    end_date: str | None = Query(None, description="结束日期 yyyy-MM-dd / yyyyMMdd"),
    plates: str | None = Query(None, description="车牌，逗号分隔；空=权限范围内全部"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=500),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    """停车超时报表列表（808 cesg_park_alarm）。"""
    user_id = parse_user_id_header(x_user_id)
    allowed = await resolve_allowed_plate_nos(db, user_id)
    if allowed is not None and not allowed:
        return {"ok": True, "total": 0, "items": [], "page": page, "page_size": page_size}

    selected = [p.strip() for p in (plates or "").split(",") if p.strip()]
    if selected:
        if allowed is not None:
            selected = [p for p in selected if p in allowed]
            if not selected:
                return {"ok": True, "total": 0, "items": [], "page": page, "page_size": page_size}
        plate_filter = selected
    else:
        plate_filter = sorted(allowed) if allowed is not None else None

    start_dt = _parse_day_start(start_date)
    end_dt = _parse_day_start(end_date)
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1) - timedelta(seconds=1)

    conn = _connect()
    if conn is None:
        raise HTTPException(status_code=503, detail="808 MySQL 不可用")

    where = ["1=1"]
    params: list[Any] = []
    if start_dt is not None:
        where.append("(`end_time` IS NULL OR `end_time` >= %s)")
        params.append(start_dt)
    if end_dt is not None:
        where.append("(`start_time` IS NULL OR `start_time` <= %s)")
        params.append(end_dt)
    if plate_filter is not None:
        placeholders = ", ".join(["%s"] * len(plate_filter))
        where.append(f"`plate_no` IN ({placeholders})")
        params.extend(plate_filter)

    where_sql = " AND ".join(where)
    offset = (page - 1) * page_size
    items: list[dict[str, Any]] = []
    total = 0
    try:
        with conn.cursor() as cur:
            _ensure_table(cur, TABLE_NAME)
            cur.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}` WHERE {where_sql}", params)
            row = cur.fetchone()
            total = int(row[0] or 0) if row else 0
            cur.execute(
                f"""
                SELECT `id`, `plate_no`, `device_no`, `lng`, `lat`, `address`,
                       `start_time`, `end_time`, `duration_min`, `limit_min`,
                       `rule_id`, `rule_name`, `day`, `created_at`
                FROM `{TABLE_NAME}`
                WHERE {where_sql}
                ORDER BY `end_time` DESC, `id` DESC
                LIMIT %s OFFSET %s
                """,
                [*params, page_size, offset],
            )
            cols = [d[0] for d in (cur.description or [])]
            for raw in cur.fetchall() or []:
                items.append(dict(zip(cols, raw)))
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("停车超时报表查询失败: %s", exc)
        raise HTTPException(status_code=500, detail=f"查询失败: {exc}") from exc
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    need_limit_ids = {
        int(it["rule_id"])
        for it in items
        if it.get("rule_id") is not None and it.get("limit_min") is None
    }
    limit_by_rule: dict[int, int] = {}
    if need_limit_ids:
        rows = (
            await db.execute(
                select(PrivateMapRule.id, PrivateMapRule.park_stop_limit_minutes).where(
                    PrivateMapRule.id.in_(list(need_limit_ids))
                )
            )
        ).all()
        for rid, lim in rows:
            limit_by_rule[int(rid)] = int(lim or 0)

    plates_in_page = sorted({str(it.get("plate_no") or "").strip() for it in items if it.get("plate_no")})
    org_by_plate: dict[str, dict[str, str]] = {}
    if plates_in_page:
        vrows = (
            await db.execute(
                select(Vehicle)
                .options(selectinload(Vehicle.company), selectinload(Vehicle.fleet))
                .where(Vehicle.plate_no.in_(plates_in_page))
            )
        ).scalars().all()
        for v in vrows:
            plate = (v.plate_no or "").strip()
            if not plate:
                continue
            company_name = ""
            if getattr(v, "company", None) is not None:
                company_name = (v.company.name or "").strip()
            fleet_name = ""
            if getattr(v, "fleet", None) is not None:
                fleet_name = (v.fleet.name or "").strip()
            org_by_plate[plate] = {"company": company_name, "fleet": fleet_name}

    out_items: list[dict[str, Any]] = []
    for it in items:
        plate = str(it.get("plate_no") or "").strip()
        org = org_by_plate.get(plate) or {}
        address = str(it.get("address") or "").strip()
        lng = it.get("lng")
        lat = it.get("lat")
        if not address and lng is not None and lat is not None:
            try:
                address = await resolve_address_gcj02(
                    db, float(lat), float(lng), existing=address
                ) or ""
            except Exception:  # noqa: BLE001
                address = ""
        limit_min = it.get("limit_min")
        if limit_min is None and it.get("rule_id") is not None:
            limit_min = limit_by_rule.get(int(it["rule_id"]))
        out_items.append(
            {
                "id": it.get("id"),
                "plate_no": plate,
                "company": org.get("company") or "",
                "fleet": org.get("fleet") or "",
                "address": address,
                "start_time": _fmt_dt(it.get("start_time")),
                "end_time": _fmt_dt(it.get("end_time")),
                "rule_name": str(it.get("rule_name") or "").strip(),
                "limit_min": int(limit_min) if limit_min is not None else None,
                "duration_min": int(it.get("duration_min") or 0),
                "lng": float(lng) if lng is not None else None,
                "lat": float(lat) if lat is not None else None,
                "day": str(it.get("day") or ""),
            }
        )

    return {
        "ok": True,
        "total": total,
        "items": out_items,
        "page": page,
        "page_size": page_size,
    }

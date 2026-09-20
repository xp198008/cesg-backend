"""CESG 自有 OBD 日里程报表。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.obd_mileage_daily import (
    query_obd_mileage_daily,
    query_obd_mileage_monthly,
    query_live_mileage,
    _summarize_company,
    _summarize_driver,
    _summarize_vehicle,
)

router = APIRouter(prefix="/api/obd-mileage", tags=["obd-mileage"])


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    out: list[str] = []
    for part in str(value).replace("，", ",").split(","):
        item = part.strip()
        if item and item not in out:
            out.append(item)
    return out


def _split_ints(value: str | None) -> list[int]:
    out: list[int] = []
    for part in _split_csv(value):
        try:
            out.append(int(part))
        except ValueError:
            continue
    return out


@router.get("/daily")
async def list_obd_mileage_daily(
    stime: str = Query(..., description="开始日 yyyyMMdd"),
    etime: str = Query(..., description="结束日 yyyyMMdd"),
    plates: str | None = Query(None),
    device_nos: str | None = Query(None),
    company_ids: str | None = Query(None),
    companies: str | None = Query(None),
    drivers: str | None = Query(None),
    backfill: bool = Query(True),
    db: AsyncSession = Depends(get_db),
):
    items = await query_obd_mileage_daily(
        db,
        start_day=stime,
        end_day=etime,
        plates=_split_csv(plates),
        device_nos=_split_csv(device_nos),
        company_ids=_split_ints(company_ids) or None,
        company_names=_split_csv(companies) or None,
        driver_names=_split_csv(drivers) or None,
        backfill=backfill,
    )
    return {"ok": True, "items": items, "total": len(items), "source": "obd_mileage_daily"}


@router.get("/monthly")
async def list_obd_mileage_monthly(
    stime: str = Query(..., description="开始日 yyyyMMdd"),
    etime: str = Query(..., description="结束日 yyyyMMdd"),
    plates: str | None = Query(None),
    device_nos: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    items = await query_obd_mileage_monthly(
        db,
        start_day=stime,
        end_day=etime,
        plates=_split_csv(plates),
        device_nos=_split_csv(device_nos),
    )
    return {"ok": True, "items": items, "total": len(items), "source": "obd_mileage_daily"}


@router.get("/summary")
async def list_obd_mileage_summary(
    stime: str = Query(...),
    etime: str = Query(...),
    group: str = Query("vehicle", description="vehicle / company / driver"),
    plates: str | None = Query(None),
    device_nos: str | None = Query(None),
    company_ids: str | None = Query(None),
    companies: str | None = Query(None),
    drivers: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    daily = await query_obd_mileage_daily(
        db,
        start_day=stime,
        end_day=etime,
        plates=_split_csv(plates),
        device_nos=_split_csv(device_nos),
        company_ids=_split_ints(company_ids) or None,
        company_names=_split_csv(companies) or None,
        driver_names=_split_csv(drivers) or None,
        backfill=True,
    )
    kind = str(group or "vehicle").strip().lower()
    if kind in ("company", "公司"):
        items = _summarize_company(daily)
    elif kind in ("driver", "司机"):
        items = _summarize_driver(daily, stime, etime)
    else:
        items = _summarize_vehicle(daily)
    return {"ok": True, "items": items, "total": len(items), "source": "obd_mileage_daily", "group": kind}


@router.get("/live")
async def list_obd_mileage_live(
    day: str | None = Query(None, description="自然日 yyyyMMdd，默认今天"),
    plates: str | None = Query(None),
    device_nos: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """监控列表 / 看板 / AI 共用的统一当日行驶里程。"""
    items = await query_live_mileage(
        db,
        day=day,
        plates=_split_csv(plates) or None,
        device_nos=_split_csv(device_nos) or None,
    )
    return {"ok": True, "items": items, "total": len(items), "source": "obd_mileage_daily"}

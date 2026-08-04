"""OBD 日油耗报表 API（读 obd_fuel_daily；必要时从 obd_energy_snapshot 回填）。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, or_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.jt808_obd_fuel_sync import notify_obd_fuel_daily_by_keys
from app.models import ObdEnergySnapshot, ObdFuelDaily
from app.timeutil import china_now_naive

router = APIRouter(prefix="/api/obd-fuel", tags=["obd-fuel"])


def _norm_day(value: str | None) -> str:
    text = str(value or "").strip().replace("-", "").replace("/", "")[:8]
    return text if len(text) == 8 and text.isdigit() else ""


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    out: list[str] = []
    for part in str(value).replace("，", ",").split(","):
        item = part.strip()
        if item and item not in out:
            out.append(item)
    return out


def _row_dict(row: ObdFuelDaily) -> dict:
    fuel = float(row.fuel_l) if row.fuel_l is not None else None
    drive = float(row.drive_km) if row.drive_km is not None else None
    fuel100 = float(row.fuel_per_100km) if row.fuel_per_100km is not None else None
    if fuel100 is None and fuel is not None and drive is not None and drive > 0.1:
        fuel100 = round(fuel / drive * 100.0, 2)
    day = str(row.day or "")
    day_dash = f"{day[:4]}-{day[4:6]}-{day[6:8]}" if len(day) == 8 else day
    return {
        "id": row.id,
        "device_no": row.device_no,
        "deviceId": row.device_no,
        "plate_no": row.plate_no,
        "carno": row.plate_no,
        "vehicle_id": row.vehicle_id,
        "company_id": row.company_id,
        "day": day,
        "dayDash": day_dash,
        "stime": f"{day}000000" if len(day) == 8 else "",
        "etime": f"{day}235959" if len(day) == 8 else "",
        "oil": fuel,
        "fuel_l": fuel,
        "lc": drive,
        "drive_km": drive,
        "startMileage": row.start_mileage,
        "endMileage": row.end_mileage,
        "ave_oil": fuel100,
        "fuel100": fuel100,
        "source": row.source or "obd_fdjrlll",
        "report_time": row.report_time.isoformat(sep=" ") if row.report_time else None,
        "updated_at": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


async def _backfill_from_snapshot(
    db: AsyncSession,
    *,
    stime: str,
    etime: str,
) -> int:
    """把看板用的 oil 快照补进日油耗表（仅补缺失行，不覆盖已有估算）。"""
    from app.jt808_alarm_sync import _vehicle_by_terminal

    rows = (
        await db.execute(
            select(ObdEnergySnapshot).where(
                ObdEnergySnapshot.energy_type == "oil",
                ObdEnergySnapshot.day >= stime,
                ObdEnergySnapshot.day <= etime,
                ObdEnergySnapshot.fuel.is_not(None),
            )
        )
    ).scalars().all()
    if not rows:
        return 0
    now = china_now_naive()
    wrote = 0
    for snap in rows:
        device_no = str(snap.device_no or "").strip()
        day = str(snap.day or "").strip()
        if not device_no or len(day) != 8:
            continue
        exists = (
            await db.execute(
                select(ObdFuelDaily.id).where(
                    ObdFuelDaily.device_no == device_no,
                    ObdFuelDaily.day == day,
                ).limit(1)
            )
        ).scalar_one_or_none()
        if exists:
            continue
        vehicle_id = None
        plate_no = None
        company_id = None
        try:
            vehicle = await _vehicle_by_terminal(db, device_no)
            if vehicle is not None:
                vehicle_id = vehicle.id
                plate_no = vehicle.plate_no
                company_id = getattr(vehicle, "company_id", None)
        except Exception:  # noqa: BLE001
            pass
        stmt = sqlite_insert(ObdFuelDaily).values(
            device_no=device_no,
            plate_no=plate_no,
            vehicle_id=vehicle_id,
            company_id=company_id,
            day=day,
            fuel_l=float(snap.fuel) if snap.fuel is not None else None,
            drive_km=None,
            start_mileage=None,
            end_mileage=None,
            fuel_per_100km=None,
            source="obd_energy_snapshot",
            report_time=snap.report_time,
            updated_at=now,
            created_at=now,
        )
        stmt = stmt.on_conflict_do_nothing(index_elements=["device_no", "day"])
        await db.execute(stmt)
        wrote += 1
        try:
            await db.flush()
            await notify_obd_fuel_daily_by_keys(db, device_no=device_no, day=day)
        except Exception:  # noqa: BLE001
            pass
    if wrote:
        await db.commit()
    return wrote


@router.get("/daily")
async def list_obd_fuel_daily(
    stime: str = Query(..., description="开始日 yyyyMMdd / yyyy-MM-dd"),
    etime: str = Query(..., description="结束日 yyyyMMdd / yyyy-MM-dd"),
    plates: str | None = Query(None, description="车牌，逗号分隔"),
    device_nos: str | None = Query(None, description="设备号，逗号分隔"),
    backfill: bool = Query(True, description="是否从 obd_energy_snapshot 补缺失行"),
    db: AsyncSession = Depends(get_db),
):
    start = _norm_day(stime)
    end = _norm_day(etime)
    if not start or not end:
        return {"ok": False, "detail": "stime/etime 格式应为 yyyyMMdd", "items": [], "total": 0}
    if start > end:
        start, end = end, start

    if backfill:
        try:
            await _backfill_from_snapshot(db, stime=start, etime=end)
        except Exception:  # noqa: BLE001
            pass

    plate_list = _split_csv(plates)
    device_list = _split_csv(device_nos)
    conds = [
        ObdFuelDaily.day >= start,
        ObdFuelDaily.day <= end,
    ]
    if plate_list or device_list:
        parts = []
        if plate_list:
            parts.append(ObdFuelDaily.plate_no.in_(plate_list))
        if device_list:
            parts.append(ObdFuelDaily.device_no.in_(device_list))
        conds.append(or_(*parts))

    rows = (
        await db.execute(
            select(ObdFuelDaily)
            .where(and_(*conds))
            .order_by(ObdFuelDaily.day.asc(), ObdFuelDaily.plate_no.asc(), ObdFuelDaily.id.asc())
        )
    ).scalars().all()

    items = [_row_dict(r) for r in rows]
    return {"ok": True, "items": items, "total": len(items), "stime": start, "etime": end}

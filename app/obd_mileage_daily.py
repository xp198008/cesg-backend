"""CESG 自有日里程：按 OBD 总里程（zlc）一车一日记起止，不再依赖 808 1302 结转。"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import pymysql
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.amap_regeo import resolve_address_wgs84
from app.config import settings
from app.db_upsert import upsert_stmt
from app.jt808_alarm_sync import _vehicle_by_terminal
from app.jt808_vehicle import _terminal_variants
from app.models import Driver, ObdMileageDaily, OrgCompany, Vehicle, VehicleDevice
from app.timeutil import china_now_naive

logger = logging.getLogger(__name__)

_MAX_DAY_KM = 800.0
_MIN_ZLC = 1.0


def _norm_day(value: Any) -> str:
    text = str(value or "").strip().replace("-", "").replace("/", "").replace(" ", "")[:8]
    return text if len(text) == 8 and text.isdigit() else ""


def _parse_ts(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    s = str(raw).strip()
    if len(s) >= 14 and s[:14].isdigit() and s[:2] in ("19", "20"):
        try:
            return datetime.strptime(s[:14], "%Y%m%d%H%M%S")
        except ValueError:
            return None
    return None


def _to_float(raw: Any) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        n = float(raw)
    except (TypeError, ValueError):
        return None
    if n != n:  # NaN
        return None
    return n


def _drive_km(start: float | None, end: float | None) -> float | None:
    if start is None or end is None:
        return None
    diff = float(end) - float(start)
    if diff < 0:
        return None
    if diff > _MAX_DAY_KM:
        return None
    return round(diff, 2)


def _fmt_dt(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parse_day(day: str) -> datetime | None:
    text = _norm_day(day)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return None


def _day_diff(start_day: str, end_day: str) -> int | None:
    a, b = _parse_day(start_day), _parse_day(end_day)
    if a is None or b is None:
        return None
    return (b - a).days


def _fmt_day_dash(day: str) -> str:
    if len(day) != 8:
        return day
    return f"{day[:4]}-{day[4:6]}-{day[6:8]}"


def _connect_jt808(*, read_timeout: int = 60):
    return pymysql.connect(
        host=settings.jt808_mysql_host,
        port=int(settings.jt808_mysql_port),
        user=settings.jt808_mysql_user,
        password=settings.jt808_mysql_password,
        database=settings.jt808_mysql_database,
        charset="utf8mb4",
        connect_timeout=5,
        read_timeout=read_timeout,
        write_timeout=8,
    )


async def _load_org_names(db: AsyncSession) -> dict[int, str]:
    rows = (await db.execute(select(OrgCompany.id, OrgCompany.name, OrgCompany.parent_id))).all()
    by_id = {int(r[0]): {"name": r[1] or "", "parent_id": r[2]} for r in rows}
    return by_id


def _org_chain(orgs: dict[int, dict[str, Any]], company_id: int | None) -> list[str]:
    names: list[str] = []
    seen: set[int] = set()
    cid = int(company_id) if company_id else None
    while cid and cid not in seen and cid in orgs:
        seen.add(cid)
        names.append(str(orgs[cid].get("name") or ""))
        parent = orgs[cid].get("parent_id")
        cid = int(parent) if parent else None
    names.reverse()
    while len(names) < 4:
        names.append("")
    return names[:4]


async def _vehicle_meta(db: AsyncSession, device_no: str) -> dict[str, Any]:
    out = {
        "device_no": device_no,
        "vehicle_id": None,
        "plate_no": None,
        "company_id": None,
        "company_name": None,
        "driver_name": None,
    }
    if not device_no:
        return out
    try:
        vehicle = await _vehicle_by_terminal(db, device_no)
    except Exception:  # noqa: BLE001
        vehicle = None
    if vehicle is None:
        return out
    company_name = None
    if vehicle.company_id:
        company_name = (
            await db.execute(select(OrgCompany.name).where(OrgCompany.id == vehicle.company_id))
        ).scalar_one_or_none()
    out.update(
        {
            "vehicle_id": vehicle.id,
            "plate_no": vehicle.plate_no,
            "company_id": vehicle.company_id,
            "company_name": company_name,
            "driver_name": vehicle.driver_name,
        }
    )
    return out


async def upsert_obd_mileage_tick(
    db: AsyncSession,
    *,
    device_no: str,
    zlc: float,
    report_time: datetime,
    lat: float | None = None,
    lng: float | None = None,
    plate_no: str | None = None,
) -> None:
    """实时 OBD 帧：写入当天起止，并用本帧下延关闭上一未结束日。"""
    device_no = str(device_no or "").strip()
    if not device_no or zlc is None or zlc < _MIN_ZLC:
        return
    day = report_time.strftime("%Y%m%d")
    meta = await _vehicle_meta(db, device_no)
    if plate_no:
        meta["plate_no"] = plate_no or meta["plate_no"]

    prev = (
        await db.execute(
            select(ObdMileageDaily)
            .where(ObdMileageDaily.device_no == device_no, ObdMileageDaily.day < day)
            .order_by(ObdMileageDaily.day.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if prev is not None:
        gap = _day_diff(prev.day, day)
        now_close = china_now_naive()
        if gap is not None and gap == 1 and not prev.closed:
            # 相邻日：用本帧下延关闭昨天
            if prev.end_mileage is None or float(prev.end_mileage) < float(zlc):
                prev.end_mileage = float(zlc)
            prev.closed = True
            prev.drive_km = _drive_km(prev.start_mileage, prev.end_mileage)
            prev.updated_at = now_close
        elif gap is not None and gap > 1:
            # 跨天中间的自然日行驶里程记 0，不把空档加到上一有数据日
            prev.closed = True
            if prev.end_mileage is None:
                prev.end_mileage = prev.start_mileage
                prev.drive_km = _drive_km(prev.start_mileage, prev.end_mileage)
            prev.updated_at = now_close
            await _insert_zero_gap_days(
                db,
                prev,
                until_day=day,
                meta=meta,
            )

    row = (
        await db.execute(
            select(ObdMileageDaily).where(
                ObdMileageDaily.device_no == device_no,
                ObdMileageDaily.day == day,
            )
        )
    ).scalar_one_or_none()
    now = china_now_naive()
    if row is None:
        db.add(
            ObdMileageDaily(
                device_no=device_no,
                plate_no=meta.get("plate_no"),
                vehicle_id=meta.get("vehicle_id"),
                company_id=meta.get("company_id"),
                company_name=meta.get("company_name"),
                driver_name=meta.get("driver_name"),
                day=day,
                start_mileage=float(zlc),
                end_mileage=float(zlc),
                drive_km=0.0,
                start_time=report_time,
                end_time=report_time,
                start_lng=lng,
                start_lat=lat,
                end_lng=lng,
                end_lat=lat,
                closed=False,
                source="obd_zlc",
                updated_at=now,
                created_at=now,
            )
        )
        return
    if row.start_mileage is None:
        row.start_mileage = float(zlc)
        row.start_time = report_time
        if lat is not None and lng is not None:
            row.start_lat = lat
            row.start_lng = lng
    row.end_mileage = float(zlc)
    row.end_time = report_time
    if lat is not None and lng is not None:
        row.end_lat = lat
        row.end_lng = lng
    row.drive_km = _drive_km(row.start_mileage, row.end_mileage)
    if meta.get("plate_no"):
        row.plate_no = meta["plate_no"]
    if meta.get("company_id"):
        row.company_id = meta["company_id"]
        row.company_name = meta.get("company_name") or row.company_name
    if meta.get("driver_name"):
        row.driver_name = meta["driver_name"]
    row.updated_at = now


def _load_808_car_map(cur) -> dict[int, dict[str, Any]]:
    cur.execute("SELECT id, carno, tid FROM tgps_car")
    out: dict[int, dict[str, Any]] = {}
    for car_id, carno, tid in cur.fetchall() or []:
        out[int(car_id)] = {"car_id": int(car_id), "plate_no": carno, "tid": str(tid or "").strip()}
    return out


def _obd_day_bounds(cur, table: str, start_ts: str, end_ts: str) -> list[dict[str, Any]]:
    if not table:
        return []
    try:
        cur.execute(f"SHOW COLUMNS FROM `{table}` LIKE 'zlc'")
        if not cur.fetchone():
            return []
    except Exception:  # noqa: BLE001
        return []
    # ts 现网是 14 位串（如 20260915105328）。禁止 DATE_FORMAT('%Y')：
    # PyMySQL 会把 %Y 当格式符，回填直接空跑。
    day_expr = "LEFT(CAST(ts AS CHAR), 8)"
    sql = (
        f"SELECT t.car_id, {day_expr} AS d, t.zlc, t.ts "
        f"FROM `{table}` t "
        f"INNER JOIN ("
        f"  SELECT car_id, {day_expr} AS d, MIN(ts) AS mints, MAX(ts) AS maxts "
        f"  FROM `{table}` WHERE ts>=%s AND ts<=%s AND zlc IS NOT NULL AND zlc>1 "
        f"  GROUP BY car_id, {day_expr}"
        f") x ON t.car_id=x.car_id AND LEFT(CAST(t.ts AS CHAR), 8)=x.d "
        f"AND (t.ts=x.mints OR t.ts=x.maxts) "
        f"WHERE t.zlc IS NOT NULL AND t.zlc>1"
    )
    try:
        cur.execute(sql, (start_ts, end_ts))
    except Exception as exc:  # noqa: BLE001
        logger.warning("读 808 %s 日里程失败: %s", table, exc)
        return []
    by_key: dict[tuple[int, str], dict[str, Any]] = {}
    for car_id, day, zlc, ts in cur.fetchall() or []:
        day = _norm_day(day)
        if not day:
            continue
        key = (int(car_id), day)
        item = by_key.setdefault(key, {"car_id": int(car_id), "day": day})
        ts_s = str(ts or "")
        z = _to_float(zlc)
        if z is None:
            continue
        if "start_ts" not in item or ts_s < item["start_ts"]:
            item["start_ts"] = ts_s
            item["start_mileage"] = z
        if "end_ts" not in item or ts_s > item["end_ts"]:
            item["end_ts"] = ts_s
            item["end_mileage"] = z
    return list(by_key.values())


def _gps_day_bounds(cur, day: str) -> dict[int, dict[str, Any]]:
    table = f"tgps_data_{day}"
    try:
        cur.execute("SHOW TABLES LIKE %s", (table,))
        if not cur.fetchone():
            return {}
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        cols = {str(r[0]).lower() for r in cur.fetchall() or []}
    except Exception:  # noqa: BLE001
        return {}
    lat_col = "lat" if "lat" in cols else ("latitude" if "latitude" in cols else None)
    lng_col = "lng" if "lng" in cols else ("longitude" if "longitude" in cols else None)
    time_col = "gpstime" if "gpstime" in cols else ("ts" if "ts" in cols else None)
    if not lat_col or not lng_col or not time_col:
        return {}
    sql = (
        f"SELECT t.car_id, t.{lat_col}, t.{lng_col}, t.{time_col} FROM `{table}` t "
        f"INNER JOIN ("
        f"  SELECT car_id, MIN({time_col}) AS mints, MAX({time_col}) AS maxts "
        f"  FROM `{table}` GROUP BY car_id"
        f") x ON t.car_id=x.car_id AND (t.{time_col}=x.mints OR t.{time_col}=x.maxts)"
    )
    try:
        cur.execute(sql)
    except Exception as exc:  # noqa: BLE001
        logger.warning("读 808 %s 定位失败: %s", table, exc)
        return {}
    out: dict[int, dict[str, Any]] = {}
    for car_id, lat, lng, ts in cur.fetchall() or []:
        cid = int(car_id)
        item = out.setdefault(cid, {})
        ts_s = str(ts or "")
        la, ln = _to_float(lat), _to_float(lng)
        if la is None or ln is None:
            continue
        if "start_ts" not in item or ts_s < item["start_ts"]:
            item["start_ts"] = ts_s
            item["start_lat"] = la
            item["start_lng"] = ln
        if "end_ts" not in item or ts_s > item["end_ts"]:
            item["end_ts"] = ts_s
            item["end_lat"] = la
            item["end_lng"] = ln
    return out


def _merge_808_days(yc: list[dict[str, Any]], dc: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[int, str], dict[str, Any]] = {}
    for item in yc + dc:
        key = (int(item["car_id"]), str(item["day"]))
        old = by_key.get(key)
        if old is None:
            by_key[key] = dict(item)
            continue
        if str(item.get("end_ts") or "") > str(old.get("end_ts") or ""):
            old["end_ts"] = item.get("end_ts")
            old["end_mileage"] = item.get("end_mileage")
        if str(item.get("start_ts") or "") and (
            not old.get("start_ts") or str(item.get("start_ts")) < str(old.get("start_ts"))
        ):
            old["start_ts"] = item.get("start_ts")
            old["start_mileage"] = item.get("start_mileage")
    return list(by_key.values())


async def backfill_obd_mileage_range(db: AsyncSession, start_day: str, end_day: str) -> dict[str, Any]:
    """从 808 油/电 OBD 表回填区间内日里程，并用次日首帧下延未结束日。"""
    start_day = _norm_day(start_day)
    end_day = _norm_day(end_day)
    if not start_day or not end_day:
        return {"ok": False, "wrote": 0}
    if start_day > end_day:
        start_day, end_day = end_day, start_day
    start_ts = start_day + "000000"
    end_ts = end_day + "235959"

    def _load():
        conn = _connect_jt808(read_timeout=180)
        try:
            with conn.cursor() as cur:
                cars = _load_808_car_map(cur)
                yc = _obd_day_bounds(cur, "tgps_obd_yc", start_ts, end_ts)
                dc = _obd_day_bounds(cur, "tgps_obd_dc", start_ts, end_ts)
                merged = _merge_808_days(yc, dc)
                logger.info("808 OBD 日界 yc=%s dc=%s merged=%s %s~%s", len(yc), len(dc), len(merged), start_day, end_day)
                gps_by_day: dict[str, dict[int, dict[str, Any]]] = {}
                for key in sorted({str(item.get("day") or "") for item in merged if item.get("day")}):
                    gps_by_day[key] = _gps_day_bounds(cur, key)
                return cars, merged, gps_by_day
        finally:
            conn.close()

    import asyncio

    cars, days, gps_by_day = await asyncio.to_thread(_load)
    if not days:
        return {"ok": True, "wrote": 0, "days": 0}

    devices = (
        await db.execute(select(VehicleDevice.device_no, VehicleDevice.vehicle_id))
    ).all()
    vehicles = (
        await db.execute(
            select(
                Vehicle.id,
                Vehicle.plate_no,
                Vehicle.company_id,
                Vehicle.driver_name,
            )
        )
    ).all()
    orgs = {r[0]: r[1] for r in (await db.execute(select(OrgCompany.id, OrgCompany.name))).all()}
    by_vehicle = {
        int(r[0]): {
            "plate_no": r[1],
            "company_id": r[2],
            "company_name": orgs.get(r[2]),
            "driver_name": r[3],
        }
        for r in vehicles
    }
    device_to_vehicle: dict[str, int] = {}
    for device_no, vid in devices:
        device_to_vehicle[str(device_no)] = int(vid)
        for variant in _terminal_variants(str(device_no)) or []:
            device_to_vehicle[str(variant)] = int(vid)

    wrote = 0
    now = china_now_naive()
    for item in days:
        info = cars.get(int(item["car_id"])) or {}
        tid = str(info.get("tid") or "")
        vid = device_to_vehicle.get(tid)
        if vid is None:
            for variant in _terminal_variants(tid) or []:
                vid = device_to_vehicle.get(str(variant))
                if vid is not None:
                    tid = str(variant)
                    break
        meta = by_vehicle.get(int(vid)) if vid is not None else {}
        gps = (gps_by_day.get(item["day"]) or {}).get(int(item["car_id"])) or {}
        start_m = _to_float(item.get("start_mileage"))
        end_m = _to_float(item.get("end_mileage"))
        values = {
            "device_no": tid or str(item["car_id"]),
            "plate_no": meta.get("plate_no") or info.get("plate_no"),
            "vehicle_id": vid,
            "company_id": meta.get("company_id"),
            "company_name": meta.get("company_name"),
            "driver_name": meta.get("driver_name"),
            "day": item["day"],
            "start_mileage": start_m,
            "end_mileage": end_m,
            "drive_km": _drive_km(start_m, end_m),
            "start_time": _parse_ts(item.get("start_ts")),
            "end_time": _parse_ts(item.get("end_ts")),
            "start_lng": gps.get("start_lng"),
            "start_lat": gps.get("start_lat"),
            "end_lng": gps.get("end_lng"),
            "end_lat": gps.get("end_lat"),
            "closed": True,
            "source": "obd_zlc_808",
            "updated_at": now,
            "created_at": now,
        }
        stmt = upsert_stmt(
            ObdMileageDaily,
            values,
            ["device_no", "day"],
            [
                "plate_no",
                "vehicle_id",
                "company_id",
                "company_name",
                "driver_name",
                "start_mileage",
                "end_mileage",
                "drive_km",
                "start_time",
                "end_time",
                "start_lng",
                "start_lat",
                "end_lng",
                "end_lat",
                "closed",
                "source",
                "updated_at",
            ],
        )
        await db.execute(stmt)
        wrote += 1
    await _apply_carry_forward(db, start_day, end_day)
    await _fill_gap_zero_days(db, start_day, end_day)
    await db.commit()
    await _fill_missing_addresses(db, start_day, end_day, limit=200)
    return {"ok": True, "wrote": wrote, "days": len(days)}


async def _insert_zero_gap_days(
    db: AsyncSession,
    prev: ObdMileageDaily,
    *,
    until_day: str,
    meta: dict[str, Any] | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> int:
    """把 prev 与 until_day 之间的自然日补成起止相同、行驶 0。"""
    start_dt = _parse_day(prev.day)
    end_dt = _parse_day(until_day)
    if start_dt is None or end_dt is None or (end_dt - start_dt).days <= 1:
        return 0
    anchor = prev.end_mileage if prev.end_mileage is not None else prev.start_mileage
    if anchor is None:
        return 0
    existing = {
        str(r)
        for r in (
            await db.execute(
                select(ObdMileageDaily.day).where(
                    ObdMileageDaily.device_no == prev.device_no,
                    ObdMileageDaily.day > prev.day,
                    ObdMileageDaily.day < until_day,
                )
            )
        ).scalars().all()
    }
    now = china_now_naive()
    meta = meta or {}
    window_start = _norm_day(window_start) if window_start else ""
    window_end = _norm_day(window_end) if window_end else ""
    wrote = 0
    day = start_dt + timedelta(days=1)
    while day < end_dt:
        key = day.strftime("%Y%m%d")
        day += timedelta(days=1)
        if window_start and key < window_start:
            continue
        if window_end and key > window_end:
            continue
        if key in existing:
            continue
        db.add(
            ObdMileageDaily(
                device_no=prev.device_no,
                plate_no=meta.get("plate_no") or prev.plate_no,
                vehicle_id=meta.get("vehicle_id") or prev.vehicle_id,
                company_id=meta.get("company_id") or prev.company_id,
                company_name=meta.get("company_name") or prev.company_name,
                driver_name=meta.get("driver_name") or prev.driver_name,
                day=key,
                start_mileage=float(anchor),
                end_mileage=float(anchor),
                drive_km=0.0,
                start_time=None,
                end_time=None,
                start_lng=None,
                start_lat=None,
                end_lng=None,
                end_lat=None,
                start_address=None,
                end_address=None,
                closed=True,
                source="obd_zlc_gap",
                updated_at=now,
                created_at=now,
            )
        )
        wrote += 1
    return wrote


async def _fill_gap_zero_days(db: AsyncSession, start_day: str, end_day: str) -> int:
    """区间内：两段有 OBD 的日期中间，按自然日补 0 公里。"""
    start_day = _norm_day(start_day)
    end_day = _norm_day(end_day)
    if not start_day or not end_day:
        return 0
    devices = [
        str(r)
        for r in (
            await db.execute(
                select(ObdMileageDaily.device_no)
                .where(ObdMileageDaily.day >= start_day, ObdMileageDaily.day <= end_day)
                .distinct()
            )
        ).scalars().all()
        if r
    ]
    if not devices:
        return 0
    rows = (
        await db.execute(
            select(ObdMileageDaily)
            .where(ObdMileageDaily.device_no.in_(devices), ObdMileageDaily.day >= start_day)
            .order_by(ObdMileageDaily.device_no, ObdMileageDaily.day)
        )
    ).scalars().all()
    by_dev: dict[str, list[ObdMileageDaily]] = {}
    for row in rows:
        by_dev.setdefault(str(row.device_no), []).append(row)
    wrote = 0
    for seq in by_dev.values():
        active = [r for r in seq if (r.source or "") != "obd_zlc_gap"]
        for i, row in enumerate(active):
            nxt = active[i + 1] if i + 1 < len(active) else None
            if nxt is None:
                continue
            wrote += await _insert_zero_gap_days(
                db,
                row,
                until_day=nxt.day,
                window_start=start_day,
                window_end=end_day,
            )
    return wrote


async def _apply_carry_forward(db: AsyncSession, start_day: str, end_day: str) -> None:
    """相邻日：结束里程空时用次日开始里程下延。跨天中间不把里程并过来。"""
    rows = (
        await db.execute(
            select(ObdMileageDaily)
            .where(ObdMileageDaily.day >= start_day, ObdMileageDaily.day <= end_day)
            .order_by(ObdMileageDaily.device_no, ObdMileageDaily.day)
        )
    ).scalars().all()
    by_dev: dict[str, list[ObdMileageDaily]] = {}
    for row in rows:
        by_dev.setdefault(str(row.device_no), []).append(row)
    now = china_now_naive()
    for seq in by_dev.values():
        for i, row in enumerate(seq):
            if (row.source or "") == "obd_zlc_gap":
                row.drive_km = 0.0
                continue
            nxt = seq[i + 1] if i + 1 < len(seq) else None
            if row.end_mileage is None and nxt is not None and nxt.start_mileage is not None:
                if _day_diff(row.day, nxt.day) == 1:
                    row.end_mileage = float(nxt.start_mileage)
                    row.closed = True
                    row.updated_at = now
            if row.end_mileage is not None:
                row.drive_km = _drive_km(row.start_mileage, row.end_mileage)


async def _fill_missing_addresses(db: AsyncSession, start_day: str, end_day: str, *, limit: int = 40) -> None:
    rows = (
        await db.execute(
            select(ObdMileageDaily)
            .where(ObdMileageDaily.day >= start_day, ObdMileageDaily.day <= end_day)
            .order_by(ObdMileageDaily.updated_at.desc())
        )
    ).scalars().all()
    n = 0
    for row in rows:
        if n >= limit:
            break
        changed = False
        if not (row.start_address or "").strip() and row.start_lat is not None and row.start_lng is not None:
            addr = await resolve_address_wgs84(db, row.start_lat, row.start_lng)
            if addr:
                row.start_address = addr
                changed = True
        if not (row.end_address or "").strip() and row.end_lat is not None and row.end_lng is not None:
            addr = await resolve_address_wgs84(db, row.end_lat, row.end_lng)
            if addr:
                row.end_address = addr
                changed = True
        if changed:
            n += 1
            row.updated_at = china_now_naive()
    if n:
        await db.commit()


def _iter_days(start_day: str, end_day: str) -> list[str]:
    start_dt = _parse_day(start_day)
    end_dt = _parse_day(end_day)
    if start_dt is None or end_dt is None or start_dt > end_dt:
        return []
    out: list[str] = []
    cur = start_dt
    while cur <= end_dt:
        out.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
        if len(out) > 93:
            break
    return out


def _placeholder_daily(meta: dict[str, Any], day: str, orgs: dict[int, dict[str, Any]] | None = None) -> dict[str, Any]:
    """查询日没有 OBD 日结时补一行：里程 0，无起止位置。"""
    levels = _org_chain(orgs or {}, meta.get("company_id")) if orgs else ["", "", "", ""]
    plate = meta.get("plate_no") or ""
    company = meta.get("company_name") or ""
    device_no = meta.get("device_no") or ""
    return {
        "device_no": device_no,
        "deviceId": device_no,
        "plate_no": plate,
        "carno": plate,
        "vehicle": plate,
        "vehicle_id": meta.get("vehicle_id"),
        "company_id": meta.get("company_id"),
        "company": company,
        "companyName": company,
        "group": company,
        "team": company,
        "driver": meta.get("driver_name") or "",
        "date": _fmt_day_dash(day),
        "day": day,
        "startAt": "",
        "endAt": "",
        "start_time": "",
        "end_time": "",
        "startMileage": None,
        "endMileage": None,
        "start_mileage": None,
        "end_mileage": None,
        "mileage": 0,
        "driveMileage": 0,
        "drive_km": 0,
        "startAddress": "",
        "endAddress": "",
        "startLocation": "",
        "endLocation": "",
        "stopAddress": "",
        "levelOneOrg": levels[0] or company,
        "levelTwoOrg": levels[1] or "",
        "levelThreeOrg": levels[2] or "",
        "levelFourOrg": levels[3] or "",
        "closed": True,
        "source": "obd_zlc_gap",
        "gap": True,
        "_fromObd": True,
        "_from1302": True,
        "_pendingSettle": False,
    }


def _plate_match_clause(column, plates: list[str]):
    parts = []
    exact = [p for p in plates if p]
    if exact:
        parts.append(column.in_(exact))
        for plate in exact:
            if len(plate) >= 4:
                parts.append(column.like(f"%{plate}"))
    return or_(*parts) if parts else None


def _descendant_company_ids(orgs: dict[int, dict[str, Any]], roots: set[int]) -> list[int]:
    out = set(int(x) for x in roots if x is not None)
    changed = True
    while changed:
        changed = False
        for oid, info in orgs.items():
            parent = info.get("parent_id")
            if parent in out and int(oid) not in out:
                out.add(int(oid))
                changed = True
    return list(out)


async def _resolve_scope_vehicles(
    db: AsyncSession,
    *,
    plates: list[str] | None = None,
    device_nos: list[str] | None = None,
    company_ids: list[int] | None = None,
    company_names: list[str] | None = None,
    driver_names: list[str] | None = None,
) -> list[dict[str, Any]]:
    """司机/公司先落到车牌，再拿车牌去日里程表取数。"""
    if not any([plates, device_nos, company_ids, company_names, driver_names]):
        return []
    orgs = await _load_org_names(db)
    stmt = (
        select(
            Vehicle.id,
            Vehicle.plate_no,
            Vehicle.company_id,
            Vehicle.driver_name,
            VehicleDevice.device_no,
            OrgCompany.name,
            Driver.name,
        )
        .select_from(Vehicle)
        .outerjoin(VehicleDevice, VehicleDevice.vehicle_id == Vehicle.id)
        .outerjoin(OrgCompany, OrgCompany.id == Vehicle.company_id)
        .outerjoin(Driver, Driver.id == Vehicle.driver_id)
    )
    if driver_names:
        stmt = stmt.where(or_(Vehicle.driver_name.in_(driver_names), Driver.name.in_(driver_names)))
    elif company_ids or company_names:
        roots: set[int] = set(int(x) for x in (company_ids or []) if x is not None)
        if company_names:
            for oid, info in orgs.items():
                if (info.get("name") or "") in company_names:
                    roots.add(int(oid))
        ids = _descendant_company_ids(orgs, roots)
        if not ids:
            return []
        stmt = stmt.where(Vehicle.company_id.in_(ids))
    else:
        identity = []
        plate_clause = _plate_match_clause(Vehicle.plate_no, plates or [])
        if plate_clause is not None:
            identity.append(plate_clause)
        if device_nos:
            variants: list[str] = []
            for d in device_nos:
                variants.append(str(d))
                variants.extend(_terminal_variants(str(d)) or [])
            identity.append(VehicleDevice.device_no.in_(list(dict.fromkeys(variants))))
        if identity:
            stmt = stmt.where(or_(*identity))
        else:
            return []
    found: dict[int, dict[str, Any]] = {}
    for vid, plate, company_id, driver_name, device_no, company_name, driver_tbl in (await db.execute(stmt)).all():
        item = found.setdefault(
            int(vid),
            {
                "vehicle_id": int(vid),
                "plate_no": plate,
                "company_id": company_id,
                "company_name": company_name,
                "driver_name": driver_name or driver_tbl or "",
                "device_no": str(device_no or ""),
            },
        )
        if device_no and not item.get("device_no"):
            item["device_no"] = str(device_no)
        if driver_tbl and not item.get("driver_name"):
            item["driver_name"] = driver_tbl
    return list(found.values())


def _apply_vehicle_master(item: dict[str, Any], meta: dict[str, Any] | None, orgs: dict[int, dict[str, Any]] | None = None) -> dict[str, Any]:
    if not meta:
        return item
    if meta.get("driver_name"):
        item["driver"] = meta["driver_name"]
    if meta.get("company_name"):
        item["company"] = meta["company_name"]
        item["companyName"] = meta["company_name"]
        item["group"] = meta["company_name"]
        item["team"] = meta["company_name"]
    if meta.get("company_id") is not None:
        item["company_id"] = meta["company_id"]
        levels = _org_chain(orgs or {}, meta["company_id"]) if orgs else ["", "", "", ""]
        item["levelOneOrg"] = levels[0] or meta.get("company_name") or item.get("levelOneOrg") or ""
        item["levelTwoOrg"] = levels[1] or ""
        item["levelThreeOrg"] = levels[2] or ""
        item["levelFourOrg"] = levels[3] or ""
    if meta.get("plate_no") and not item.get("plate_no"):
        item["plate_no"] = meta["plate_no"]
        item["carno"] = meta["plate_no"]
        item["vehicle"] = meta["plate_no"]
    return item


def _pad_missing_days(
    items: list[dict[str, Any]],
    vehicles: list[dict[str, Any]],
    start_day: str,
    end_day: str,
    orgs: dict[int, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if not vehicles:
        by_key: dict[str, dict[str, Any]] = {}
        for item in items:
            plate = str(item.get("plate_no") or item.get("vehicle") or "")
            if not plate:
                continue
            by_key[plate] = {
                "vehicle_id": item.get("vehicle_id"),
                "plate_no": plate,
                "company_id": item.get("company_id"),
                "company_name": item.get("company") or item.get("companyName"),
                "driver_name": item.get("driver"),
                "device_no": item.get("device_no") or "",
            }
        vehicles = list(by_key.values())
    if not vehicles:
        return items
    have = {
        (str(item.get("plate_no") or item.get("vehicle") or ""), str(item.get("day") or ""))
        for item in items
        if item.get("day")
    }
    out = list(items)
    for meta in vehicles:
        plate = str(meta.get("plate_no") or "")
        driver = str(meta.get("driver_name") or "")
        ident = plate or (f"driver:{driver}" if driver else "")
        if not ident:
            continue
        for day in _iter_days(start_day, end_day):
            key = (plate or ident, day)
            if key in have:
                continue
            out.append(_placeholder_daily(meta, day, orgs))
            have.add(key)
    out.sort(key=lambda x: (str(x.get("plate_no") or ""), str(x.get("day") or "")))
    return out


def serialize_daily_row(row: ObdMileageDaily, orgs: dict[int, dict[str, Any]] | None = None) -> dict[str, Any]:
    day = str(row.day or "")
    levels = _org_chain(orgs or {}, row.company_id) if orgs else ["", "", "", ""]
    is_gap = (row.source or "") == "obd_zlc_gap"
    start_m = None if is_gap else row.start_mileage
    end_m = None if is_gap else row.end_mileage
    drive = 0.0 if is_gap else (row.drive_km if row.drive_km is not None else _drive_km(row.start_mileage, row.end_mileage))
    return {
        "device_no": row.device_no,
        "deviceId": row.device_no,
        "plate_no": row.plate_no,
        "carno": row.plate_no,
        "vehicle": row.plate_no,
        "vehicle_id": row.vehicle_id,
        "company_id": row.company_id,
        "company": row.company_name or "",
        "companyName": row.company_name or "",
        "group": row.company_name or "",
        "team": row.company_name or "",
        "driver": row.driver_name or "",
        "date": _fmt_day_dash(day),
        "day": day,
        "startAt": "" if is_gap else (_fmt_dt(row.start_time) or f"{_fmt_day_dash(day)} 00:00:00"),
        "endAt": "" if is_gap else (_fmt_dt(row.end_time) or ""),
        "start_time": "" if is_gap else _fmt_dt(row.start_time),
        "end_time": "" if is_gap else _fmt_dt(row.end_time),
        "startMileage": start_m,
        "endMileage": end_m,
        "start_mileage": start_m,
        "end_mileage": end_m,
        "mileage": drive if drive is not None else 0,
        "driveMileage": drive if drive is not None else 0,
        "drive_km": drive,
        "startAddress": "" if is_gap else (row.start_address or ""),
        "endAddress": "" if is_gap else (row.end_address or ""),
        "startLocation": "" if is_gap else (row.start_address or ""),
        "endLocation": "" if is_gap else (row.end_address or ""),
        "stopAddress": "" if is_gap else (row.end_address or ""),
        "levelOneOrg": levels[0] or row.company_name or "",
        "levelTwoOrg": levels[1] or "",
        "levelThreeOrg": levels[2] or "",
        "levelFourOrg": levels[3] or "",
        "closed": bool(row.closed),
        "source": row.source,
        "gap": is_gap,
        "_fromObd": True,
        "_from1302": True,
        "_pendingSettle": False,
    }


async def query_obd_mileage_daily(
    db: AsyncSession,
    *,
    start_day: str,
    end_day: str,
    plates: list[str] | None = None,
    device_nos: list[str] | None = None,
    company_ids: list[int] | None = None,
    company_names: list[str] | None = None,
    driver_names: list[str] | None = None,
    backfill: bool = True,
) -> list[dict[str, Any]]:
    start_day = _norm_day(start_day)
    end_day = _norm_day(end_day)
    if not start_day or not end_day:
        return []
    today = china_now_naive().strftime("%Y%m%d")
    if backfill:
        existing = (
            await db.execute(
                select(func.count()).select_from(ObdMileageDaily).where(
                    ObdMileageDaily.day >= start_day,
                    ObdMileageDaily.day <= end_day,
                )
            )
        ).scalar() or 0
        try:
            if existing == 0:
                await backfill_obd_mileage_range(db, start_day, end_day)
            elif end_day >= today:
                # 今天由实时 OBD 续写；只补昨天便于下延结束里程，避免每次报表全表扫描。
                yday = (datetime.strptime(today, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
                latest = (
                    await db.execute(
                        select(func.max(ObdMileageDaily.updated_at)).where(
                            ObdMileageDaily.day >= yday
                        )
                    )
                ).scalar()
                stale = latest is None or (china_now_naive() - latest).total_seconds() > 600
                if stale:
                    await backfill_obd_mileage_range(db, yday, today)
        except Exception as exc:  # noqa: BLE001
            logger.warning("OBD 日里程回填失败: %s", exc)
    try:
        filled = await _fill_gap_zero_days(db, start_day, end_day)
        if filled:
            await db.commit()
            logger.info("OBD 日里程中间日补零 %s~%s wrote=%s", start_day, end_day, filled)
    except Exception as exc:  # noqa: BLE001
        logger.warning("OBD 日里程中间日补零失败: %s", exc)

    orgs = await _load_org_names(db)
    scope = await _resolve_scope_vehicles(
        db,
        plates=plates,
        device_nos=device_nos,
        company_ids=company_ids,
        company_names=company_names,
        driver_names=driver_names,
    )
    scope_plates = [str(v.get("plate_no") or "") for v in scope if v.get("plate_no")]
    scope_vids = [int(v["vehicle_id"]) for v in scope if v.get("vehicle_id") is not None]
    scope_devs = [str(v.get("device_no") or "") for v in scope if v.get("device_no")]
    by_plate = {str(v.get("plate_no") or ""): v for v in scope if v.get("plate_no")}
    by_vid = {int(v["vehicle_id"]): v for v in scope if v.get("vehicle_id") is not None}

    stmt = select(ObdMileageDaily).where(
        ObdMileageDaily.day >= start_day,
        ObdMileageDaily.day <= end_day,
    )
    identity = []
    plate_clause = _plate_match_clause(ObdMileageDaily.plate_no, scope_plates or (plates or []))
    if plate_clause is not None:
        identity.append(plate_clause)
    if scope_vids:
        identity.append(ObdMileageDaily.vehicle_id.in_(scope_vids))
    if scope_devs:
        identity.append(ObdMileageDaily.device_no.in_(scope_devs))
    if not identity and device_nos:
        variants: list[str] = []
        for d in device_nos:
            variants.append(str(d))
            variants.extend(_terminal_variants(str(d)) or [])
        identity.append(ObdMileageDaily.device_no.in_(list(dict.fromkeys(variants))))
    if identity:
        stmt = stmt.where(or_(*identity))
    elif driver_names or company_names or company_ids:
        rows = []
        items = []
        return _pad_missing_days(items, scope, start_day, end_day, orgs)
    rows = (await db.execute(stmt.order_by(ObdMileageDaily.plate_no, ObdMileageDaily.day))).scalars().all()
    items = [serialize_daily_row(r, orgs) for r in rows]
    for item in items:
        meta = by_vid.get(int(item["vehicle_id"])) if item.get("vehicle_id") is not None else None
        if meta is None:
            meta = by_plate.get(str(item.get("plate_no") or ""))
        _apply_vehicle_master(item, meta, orgs)
    return _pad_missing_days(items, scope, start_day, end_day, orgs)


def _summarize_vehicle(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_plate: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        key = str(item.get("plate_no") or item.get("device_no") or "")
        if not key:
            continue
        by_plate.setdefault(key, []).append(item)
    out = []
    for plate, days in by_plate.items():
        days.sort(key=lambda x: str(x.get("day") or ""))
        first, last = days[0], days[-1]
        mileage = round(sum(float(d.get("mileage") or 0) for d in days), 2)
        out.append(
            {
                **last,
                "vehicle": plate,
                "carno": plate,
                "mileage": mileage,
                "driveMileage": mileage,
                "monthMileage": mileage,
                "workDays": sum(1 for d in days if float(d.get("mileage") or 0) > 0.05),
                "monthOnlineDays": sum(1 for d in days if not d.get("gap")),
                "ccts": sum(1 for d in days if not d.get("gap")),
                "startAt": first.get("startAt"),
                "endAt": last.get("endAt"),
                "startMileage": first.get("startMileage"),
                "endMileage": last.get("endMileage"),
                "startAddress": first.get("startAddress"),
                "endAddress": last.get("endAddress"),
                "startLocation": first.get("startLocation"),
                "endLocation": last.get("endLocation"),
                "_fromObd": True,
                "_from1302": True,
            }
        )
    out.sort(key=lambda x: (-float(x.get("mileage") or 0), str(x.get("vehicle") or "")))
    return out


def _summarize_company(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_co: dict[str, dict[str, Any]] = {}
    for item in items:
        name = str(item.get("company") or "未分组").strip() or "未分组"
        bucket = by_co.setdefault(name, {"company": name, "mileage": 0.0, "plates": set()})
        bucket["mileage"] += float(item.get("mileage") or 0)
        if item.get("plate_no"):
            bucket["plates"].add(item["plate_no"])
    rows = [
        {
            "company": name,
            "companyName": name,
            "group": name,
            "mileage": round(val["mileage"], 2),
            "vehicleCount": len(val["plates"]),
            "_fromObd": True,
        }
        for name, val in by_co.items()
    ]
    rows.sort(key=lambda x: (-float(x["mileage"]), x["company"]))
    return rows


def _query_bound_times(start_day: str, end_day: str) -> tuple[str, str]:
    start = _fmt_day_dash(_norm_day(start_day) or str(start_day or ""))
    end = _fmt_day_dash(_norm_day(end_day) or str(end_day or ""))
    return (
        f"{start} 00:00:00" if len(start) >= 10 else "",
        f"{end} 23:59:59" if len(end) >= 10 else "",
    )


def _summarize_driver(
    items: list[dict[str, Any]],
    start_day: str = "",
    end_day: str = "",
) -> list[dict[str, Any]]:
    start_at, end_at = _query_bound_times(start_day, end_day)
    by_drv: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        name = str(item.get("driver") or "").strip() or "未绑定司机"
        by_drv.setdefault(name, []).append(item)
    out = []
    for name, days in by_drv.items():
        days.sort(key=lambda x: str(x.get("day") or ""))
        mileage = round(sum(float(d.get("mileage") or 0) for d in days), 2)
        last = days[-1]
        start_loc = next((d.get("startLocation") or d.get("startAddress") or "" for d in days if d.get("startLocation") or d.get("startAddress")), "")
        end_loc = next((d.get("endLocation") or d.get("endAddress") or "" for d in reversed(days) if d.get("endLocation") or d.get("endAddress")), "")
        out.append(
            {
                "driver": name,
                "company": last.get("company") or "",
                "mileage": mileage,
                "startAt": start_at or last.get("startAt") or "",
                "endAt": end_at or last.get("endAt") or "",
                "startLocation": start_loc,
                "endLocation": end_loc,
                "_fromObd": True,
                "_from1302": True,
            }
        )
    out.sort(key=lambda x: (-float(x["mileage"]), x["driver"]))
    return out


async def query_obd_mileage_monthly(
    db: AsyncSession,
    *,
    start_day: str,
    end_day: str,
    plates: list[str] | None = None,
    device_nos: list[str] | None = None,
) -> list[dict[str, Any]]:
    items = await query_obd_mileage_daily(
        db,
        start_day=start_day,
        end_day=end_day,
        plates=plates,
        device_nos=device_nos,
    )
    rows = _summarize_vehicle(items)
    for row in rows:
        row["monthDrivingMinutes"] = "--"
        row["monthOnlineDays"] = row.get("monthOnlineDays") or row.get("workDays") or 0
    return rows

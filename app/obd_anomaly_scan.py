"""OBD 异常巡检。

两类都会列出：
1. 近 15 分钟 GPS 正在跑（5 < 车速 < 120），但 OBD 超过 15 分钟没收到；
2. 当天轨迹里出现过正常 GPS 车速，但 OBD 还停在昨天或更早。

GPS 现势来自 ``tgps_car``，当天轨迹来自 ``tgps_data_YYYYMMDD``。
只认 5 < GPS车速 < 120。
OBD 最后时间取 Redis ``{tid}_OBD``、CESG 能耗快照、808 油/电 OBD 表中最新的一条。
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

import pymysql
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.jt808_vehicle import _terminal_variants
from app.models import OrgCompany, Vehicle, VehicleDevice
from app.obd_speed_monitor import _new_redis, _parse_ts, parse_obd_payload
from app.timeutil import china_now_naive

logger = logging.getLogger(__name__)

GPS_MIN_KMH = 5.0
GPS_MAX_KMH = 120.0
OBD_STALE_MINUTES = 15
GPS_FRESH_MINUTES = 15


def _parse_compact_ts(raw: Any) -> datetime | None:
    """808 常用 14 位 ``YYYYMMDDHHMMSS``；不能先当 epoch 毫秒。"""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    s = str(raw).strip()
    if len(s) >= 14 and s[:14].isdigit() and s[:2] in ("19", "20"):
        try:
            return datetime.strptime(s[:14], "%Y%m%d%H%M%S")
        except ValueError:
            pass
    return _parse_ts(raw)


def _fmt_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _minutes_ago(now: datetime, value: datetime | None) -> float | None:
    if value is None:
        return None
    return round(max(0.0, (now - value).total_seconds() / 60.0), 1)


def _connect_jt808(*, read_timeout: int = 45):
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


def _latest_obd_by_car(cur, table: str, car_ids: list[int]) -> dict[int, tuple[Any, Any]]:
    out: dict[int, tuple[Any, Any]] = {}
    if not car_ids or not table:
        return out
    # 分批，避免 IN 过长
    for i in range(0, len(car_ids), 400):
        chunk = car_ids[i : i + 400]
        placeholders = ",".join(["%s"] * len(chunk))
        try:
            cur.execute(
                f"SELECT t.car_id, t.speed, t.ts FROM {table} t "
                f"INNER JOIN (SELECT car_id, MAX(ts) AS mts FROM {table} "
                f"WHERE car_id IN ({placeholders}) GROUP BY car_id) x "
                f"ON t.car_id=x.car_id AND t.ts=x.mts",
                chunk,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("读 808 %s 失败: %s", table, exc)
            return out
        for car_id, speed, ts in cur.fetchall() or []:
            cid = int(car_id)
            if cid not in out:
                out[cid] = (speed, ts)
    return out


def _table_exists(cur, name: str) -> bool:
    cur.execute("SHOW TABLES LIKE %s", (name,))
    return cur.fetchone() is not None


def _cars_by_ids(cur, car_ids: list[int]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    if not car_ids:
        return out
    for i in range(0, len(car_ids), 400):
        chunk = car_ids[i : i + 400]
        placeholders = ",".join(["%s"] * len(chunk))
        cur.execute(
            f"SELECT id, carno, tid, online, speed, gpstime, systime FROM tgps_car "
            f"WHERE id IN ({placeholders})",
            chunk,
        )
        for car_id, carno, tid, online, speed, gpstime, systime in cur.fetchall() or []:
            gps_at = _parse_compact_ts(gpstime) or _parse_compact_ts(systime)
            try:
                gps_speed = float(speed) if speed is not None else None
            except (TypeError, ValueError):
                gps_speed = None
            out[int(car_id)] = {
                "car_id": int(car_id),
                "plate_no": str(carno or "").strip(),
                "device_no": str(tid or "").strip(),
                "online": int(online or 0),
                "gps_speed_kmh": None if gps_speed is None else round(gps_speed, 1),
                "gps_time": gps_at,
            }
    return out


def _load_today_moving_gps(
    cur,
    now: datetime,
    gps_min: float,
    gps_max: float,
    car_ids: list[int] | None = None,
) -> dict[int, dict[str, Any]]:
    """当天轨迹里出现过正常 GPS 车速的车：最后一次合格点。"""
    table = f"tgps_data_{now.strftime('%Y%m%d')}"
    if not _table_exists(cur, table):
        return {}
    extra_sql = ""
    params: list[Any] = [gps_min, gps_max]
    if car_ids:
        placeholders = ",".join(["%s"] * len(car_ids))
        extra_sql = f" AND car_id IN ({placeholders})"
        params.extend(int(x) for x in car_ids)
    params.extend([gps_min, gps_max])
    try:
        cur.execute(
            f"SELECT t.car_id, t.speed, t.gpstime FROM {table} t "
            f"INNER JOIN (SELECT car_id, MAX(gpstime) AS mts FROM {table} "
            f"WHERE speed > %s AND speed < %s{extra_sql} GROUP BY car_id) x "
            f"ON t.car_id=x.car_id AND t.gpstime=x.mts "
            f"WHERE t.speed > %s AND t.speed < %s",
            params,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("读当天轨迹 %s 失败: %s", table, exc)
        return {}
    out: dict[int, dict[str, Any]] = {}
    for car_id, speed, gpstime in cur.fetchall() or []:
        try:
            cid = int(car_id)
            gps_speed = float(speed)
        except (TypeError, ValueError):
            continue
        gps_at = _parse_compact_ts(gpstime)
        if gps_at is None or not (gps_min < gps_speed < gps_max):
            continue
        out[cid] = {"gps_speed_kmh": round(gps_speed, 1), "gps_time": gps_at}
    return out


def _pack_808_obd(yc: dict[int, tuple[Any, Any]], dc: dict[int, tuple[Any, Any]]) -> dict[int, dict[str, Any]]:
    obd_808: dict[int, dict[str, Any]] = {}
    for cid in set(yc) | set(dc):
        best_at: datetime | None = None
        best_speed = None
        best_src = None
        for src, row in (("yc", yc.get(cid)), ("dc", dc.get(cid))):
            if not row:
                continue
            at = _parse_compact_ts(row[1])
            if at is None:
                continue
            if best_at is None or at > best_at:
                best_at = at
                best_speed = row[0]
                best_src = src
        if best_at is None:
            continue
        try:
            spd = float(best_speed) if best_speed is not None else None
        except (TypeError, ValueError):
            spd = None
        obd_808[cid] = {"at": best_at, "speed": spd, "source": f"808_{best_src}"}
    return obd_808


def _load_gps_candidates(
    now: datetime,
    *,
    gps_min: float,
    gps_max: float,
    gps_fresh_minutes: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[int, dict[str, Any]]]:
    cutoff = now - timedelta(minutes=max(1, int(gps_fresh_minutes)))
    conn = _connect_jt808()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, carno, tid, online, speed, gpstime, systime FROM tgps_car "
                "WHERE speed IS NOT NULL AND speed > %s AND speed < %s",
                (gps_min, gps_max),
            )
            live_cars: list[dict[str, Any]] = []
            for car_id, carno, tid, online, speed, gpstime, systime in cur.fetchall() or []:
                gps_at = _parse_compact_ts(gpstime) or _parse_compact_ts(systime)
                if gps_at is None or gps_at < cutoff:
                    continue
                try:
                    gps_speed = float(speed)
                except (TypeError, ValueError):
                    continue
                if not (gps_min < gps_speed < gps_max):
                    continue
                live_cars.append(
                    {
                        "car_id": int(car_id),
                        "plate_no": str(carno or "").strip(),
                        "device_no": str(tid or "").strip(),
                        "online": int(online or 0),
                        "gps_speed_kmh": round(gps_speed, 1),
                        "gps_time": gps_at,
                        "kind": "live",
                    }
                )
            today_hits = _load_today_moving_gps(cur, now, gps_min, gps_max)
            today_ids = [cid for cid in today_hits if cid not in {c["car_id"] for c in live_cars}]
            archive = _cars_by_ids(cur, today_ids)
            today_cars: list[dict[str, Any]] = []
            for cid, hit in today_hits.items():
                if cid not in today_ids:
                    continue
                info = archive.get(cid)
                if not info:
                    continue
                today_cars.append(
                    {
                        **info,
                        "gps_speed_kmh": hit["gps_speed_kmh"],
                        "gps_time": hit["gps_time"],
                        "kind": "today",
                    }
                )
            car_ids = list({c["car_id"] for c in live_cars + today_cars})
            yc = _latest_obd_by_car(cur, "tgps_obd_yc", car_ids)
            dc = _latest_obd_by_car(cur, "tgps_obd_dc", car_ids)
    finally:
        conn.close()
    return live_cars, today_cars, _pack_808_obd(yc, dc)


async def _load_redis_obd(device_nos: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not device_nos:
        return out
    redis = _new_redis()
    try:
        keys: list[str] = []
        owners: list[str] = []
        seen: set[str] = set()
        for device_no in device_nos:
            for variant in _terminal_variants(device_no) or [device_no]:
                key = f"{variant}_OBD"
                if key in seen:
                    continue
                seen.add(key)
                keys.append(key)
                owners.append(device_no)
        if not keys:
            return out
        values = await redis.mget(keys)
        for device_no, raw in zip(owners, values):
            if raw is None:
                continue
            text = raw if isinstance(raw, str) else raw.decode("utf-8", "ignore")
            reading = parse_obd_payload(device_no, text)
            if reading is None:
                continue
            old = out.get(device_no)
            if old is None or (reading.report_at and reading.report_at > (old.get("at") or datetime.min)):
                out[device_no] = {
                    "at": reading.report_at,
                    "speed": reading.speed_kmh,
                    "source": "redis",
                }
    finally:
        try:
            await redis.aclose()
        except Exception:  # noqa: BLE001
            pass
    return out


async def _load_energy_obd(db: AsyncSession, device_nos: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not device_nos:
        return out
    variants: set[str] = set()
    owner_by_variant: dict[str, str] = {}
    for device_no in device_nos:
        for variant in _terminal_variants(device_no) or [device_no]:
            variants.add(variant)
            owner_by_variant[variant] = device_no
    if not variants:
        return out
    from app.models import ObdEnergySnapshot

    day = china_now_naive().strftime("%Y%m%d")
    rows = (
        await db.execute(
            select(
                ObdEnergySnapshot.device_no,
                ObdEnergySnapshot.raw,
                ObdEnergySnapshot.report_time,
            ).where(
                ObdEnergySnapshot.day == day,
                ObdEnergySnapshot.device_no.in_(list(variants)),
            )
        )
    ).all()
    for device_no, raw, report_time in rows:
        owner = owner_by_variant.get(str(device_no or "").strip()) or str(device_no or "").strip()
        reading = parse_obd_payload(str(device_no or ""), raw or "") if raw else None
        at = (reading.report_at if reading else None) or report_time
        if at is None:
            continue
        speed = reading.speed_kmh if reading else None
        old = out.get(owner)
        if old is None or at > (old.get("at") or datetime.min):
            out[owner] = {"at": at, "speed": speed, "source": "energy"}
    return out


async def _load_cesg_meta(db: AsyncSession, plates: list[str], device_nos: list[str]) -> dict[str, dict[str, Any]]:
    meta: dict[str, dict[str, Any]] = {}
    plate_set = {p.strip() for p in plates if p and p.strip()}
    device_set: set[str] = set()
    for device_no in device_nos:
        for variant in _terminal_variants(device_no) or [device_no]:
            if variant:
                device_set.add(variant)
    if not plate_set and not device_set:
        return meta
    stmt = (
        select(
            Vehicle.id,
            Vehicle.plate_no,
            VehicleDevice.device_no,
            OrgCompany.name,
        )
        .outerjoin(VehicleDevice, VehicleDevice.vehicle_id == Vehicle.id)
        .outerjoin(OrgCompany, OrgCompany.id == Vehicle.company_id)
    )
    conds = []
    if plate_set:
        conds.append(Vehicle.plate_no.in_(list(plate_set)))
    if device_set:
        conds.append(VehicleDevice.device_no.in_(list(device_set)))
    if conds:
        from sqlalchemy import or_

        stmt = stmt.where(or_(*conds))
    rows = (await db.execute(stmt)).all()
    for vid, plate, device_no, company in rows:
        item = {
            "vehicle_id": int(vid) if vid is not None else None,
            "company_name": str(company or "").strip() or None,
            "device_no": str(device_no or "").strip() or None,
        }
        if plate:
            meta[f"plate:{str(plate).strip()}"] = item
        if device_no:
            meta[f"dev:{str(device_no).strip()}"] = item
    return meta


def _pick_meta(meta: dict[str, dict[str, Any]], plate: str, device_no: str) -> dict[str, Any]:
    hit = meta.get(f"plate:{plate}") if plate else None
    if hit:
        return hit
    for variant in _terminal_variants(device_no) or [device_no]:
        hit = meta.get(f"dev:{variant}")
        if hit:
            return hit
    return {}


def _judge_obd_status(
    *,
    now: datetime,
    today_start: datetime,
    stale_after: timedelta,
    stale_minutes: int,
    kind: str,
    obd: dict[str, Any] | None,
) -> tuple[bool, str]:
    """返回 (是否异常, 原因)。"""
    obd_at = obd.get("at") if obd else None
    if kind == "today":
        if obd_at is not None and obd_at >= today_start:
            return False, ""
        return True, "当天有过正常GPS车速，但OBD停在昨天或更早"
    if obd_at is not None and now - obd_at <= stale_after:
        return False, ""
    if obd_at is None:
        return True, "未收到OBD车速"
    if obd_at < today_start:
        return True, "当天有过正常GPS车速，但OBD停在昨天或更早"
    return True, f"OBD车速已停滞超过{stale_minutes}分钟"


def _pick_obd_for_car(
    car: dict[str, Any],
    redis_obd: dict[str, dict[str, Any]],
    energy_obd: dict[str, dict[str, Any]],
    obd_808: dict[int, dict[str, Any]],
) -> dict[str, Any] | None:
    device_no = car.get("device_no") or ""
    obd = _better_obd(
        redis_obd.get(device_no),
        energy_obd.get(device_no),
        obd_808.get(car.get("car_id")),
    )
    if obd is None:
        for variant in _terminal_variants(device_no):
            obd = _better_obd(obd, redis_obd.get(variant), energy_obd.get(variant))
    return obd


def _item_from_car(
    car: dict[str, Any],
    obd: dict[str, Any] | None,
    now: datetime,
    reason: str,
    *,
    abnormal: bool,
    meta: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    info = _pick_meta(meta or {}, car.get("plate_no") or "", car.get("device_no") or "")
    obd_at = obd.get("at") if obd else None
    return {
        "car_id": car.get("car_id"),
        "plate_no": car.get("plate_no"),
        "device_no": car.get("device_no"),
        "vehicle_id": info.get("vehicle_id"),
        "company_name": info.get("company_name"),
        "online": bool(car.get("online")),
        "kind": car.get("kind") or "live",
        "abnormal": abnormal,
        "gps_speed_kmh": car.get("gps_speed_kmh"),
        "gps_time": _fmt_dt(car.get("gps_time")),
        "gps_age_minutes": _minutes_ago(now, car.get("gps_time")),
        "obd_speed_kmh": None if not obd else obd.get("speed"),
        "obd_time": _fmt_dt(obd_at),
        "obd_age_minutes": _minutes_ago(now, obd_at),
        "obd_source": None if not obd else obd.get("source"),
        "reason": reason,
        "message": "OBD检测接口存在异常" if abnormal else "",
    }


def _better_obd(*cands: dict[str, Any] | None) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    for item in cands:
        if not item or not item.get("at"):
            continue
        if best is None or item["at"] > best["at"]:
            best = item
    return best


async def scan_obd_anomaly(
    db: AsyncSession,
    *,
    stale_minutes: int = OBD_STALE_MINUTES,
    gps_min_kmh: float = GPS_MIN_KMH,
    gps_max_kmh: float = GPS_MAX_KMH,
    gps_fresh_minutes: int = GPS_FRESH_MINUTES,
) -> dict[str, Any]:
    now = china_now_naive()
    stale_after = timedelta(minutes=max(1, int(stale_minutes)))
    gps_min = float(gps_min_kmh)
    gps_max = float(gps_max_kmh)

    live_cars, today_cars, obd_808 = await asyncio.to_thread(
        _load_gps_candidates,
        now,
        gps_min=gps_min,
        gps_max=gps_max,
        gps_fresh_minutes=int(gps_fresh_minutes),
    )
    cars = live_cars + today_cars
    device_nos = [c["device_no"] for c in cars if c.get("device_no")]
    redis_obd = await _load_redis_obd(device_nos)
    energy_obd = await _load_energy_obd(db, device_nos)
    meta = await _load_cesg_meta(db, [c["plate_no"] for c in cars], device_nos)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    items: list[dict[str, Any]] = []
    for car in cars:
        obd = _pick_obd_for_car(car, redis_obd, energy_obd, obd_808)
        abnormal, reason = _judge_obd_status(
            now=now,
            today_start=today_start,
            stale_after=stale_after,
            stale_minutes=int(stale_minutes),
            kind=car.get("kind") or "live",
            obd=obd,
        )
        if not abnormal:
            continue
        items.append(_item_from_car(car, obd, now, reason, abnormal=True, meta=meta))

    items.sort(key=lambda x: (-(x.get("obd_age_minutes") or 9999), -(x.get("gps_speed_kmh") or 0)))
    return {
        "now": _fmt_dt(now),
        "gps_min_kmh": gps_min,
        "gps_max_kmh": gps_max,
        "gps_fresh_minutes": int(gps_fresh_minutes),
        "stale_minutes": int(stale_minutes),
        "gps_moving_count": len(live_cars),
        "today_gps_moving_count": len(today_cars),
        "anomaly_count": len(items),
        "items": items,
    }


def _load_cars_for_ids(
    now: datetime,
    car_ids: list[int],
    *,
    gps_min: float,
    gps_max: float,
    gps_fresh_minutes: int,
) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]]]:
    """按 808 车辆 id 取现势/当天 GPS + OBD，供实时监控点选检测。"""
    ids = [int(x) for x in car_ids if x is not None]
    if not ids:
        return [], {}
    cutoff = now - timedelta(minutes=max(1, int(gps_fresh_minutes)))
    conn = _connect_jt808()
    try:
        with conn.cursor() as cur:
            archive = _cars_by_ids(cur, ids)
            today_hits = _load_today_moving_gps(cur, now, gps_min, gps_max, car_ids=ids)
            cars: list[dict[str, Any]] = []
            for cid in ids:
                info = archive.get(cid)
                if not info:
                    continue
                gps_speed = info.get("gps_speed_kmh")
                gps_at = info.get("gps_time")
                live = (
                    gps_speed is not None
                    and gps_at is not None
                    and gps_at >= cutoff
                    and gps_min < float(gps_speed) < gps_max
                )
                today = today_hits.get(cid)
                if live:
                    cars.append({**info, "kind": "live"})
                elif today:
                    cars.append(
                        {
                            **info,
                            "gps_speed_kmh": today["gps_speed_kmh"],
                            "gps_time": today["gps_time"],
                            "kind": "today",
                        }
                    )
            yc = _latest_obd_by_car(cur, "tgps_obd_yc", ids)
            dc = _latest_obd_by_car(cur, "tgps_obd_dc", ids)
    finally:
        conn.close()
    return cars, _pack_808_obd(yc, dc)


async def check_obd_anomaly_for_car_ids(
    db: AsyncSession,
    car_ids: list[int],
    *,
    stale_minutes: int = OBD_STALE_MINUTES,
    gps_min_kmh: float = GPS_MIN_KMH,
    gps_max_kmh: float = GPS_MAX_KMH,
    gps_fresh_minutes: int = GPS_FRESH_MINUTES,
) -> dict[str, Any]:
    """给实时监控：只判用户选中的车，返回每台是否 OBD 接口异常。"""
    now = china_now_naive()
    ids = []
    seen: set[int] = set()
    for raw in car_ids:
        try:
            cid = int(raw)
        except (TypeError, ValueError):
            continue
        if cid <= 0 or cid in seen:
            continue
        seen.add(cid)
        ids.append(cid)
        if len(ids) >= 20:
            break
    cars, obd_808 = await asyncio.to_thread(
        _load_cars_for_ids,
        now,
        ids,
        gps_min=float(gps_min_kmh),
        gps_max=float(gps_max_kmh),
        gps_fresh_minutes=int(gps_fresh_minutes),
    )
    device_nos = [c["device_no"] for c in cars if c.get("device_no")]
    redis_obd = await _load_redis_obd(device_nos)
    energy_obd = await _load_energy_obd(db, device_nos)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    stale_after = timedelta(minutes=max(1, int(stale_minutes)))
    by_id = {int(c["car_id"]): c for c in cars}
    items: list[dict[str, Any]] = []
    for cid in ids:
        car = by_id.get(cid)
        if car is None:
            items.append(
                {
                    "car_id": cid,
                    "abnormal": False,
                    "message": "",
                    "reason": "",
                }
            )
            continue
        obd = _pick_obd_for_car(car, redis_obd, energy_obd, obd_808)
        abnormal, reason = _judge_obd_status(
            now=now,
            today_start=today_start,
            stale_after=stale_after,
            stale_minutes=int(stale_minutes),
            kind=car.get("kind") or "live",
            obd=obd,
        )
        items.append(_item_from_car(car, obd, now, reason, abnormal=abnormal))
    return {
        "now": _fmt_dt(now),
        "items": items,
        "abnormal_count": sum(1 for x in items if x.get("abnormal")),
    }

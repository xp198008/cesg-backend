"""停车超限报警调度：与历史回放同源取停车点，再按车辆/围栏/时长过滤写 808。

数据源：OpenAPI 1202（= 轨迹回放 1105 的 stops[]），字段 stopTime/stime/etime/lng/lat。
流程：规则范围内车辆 → 拉停车点 → 时长≥停止限时 → 命中圆/矩/多边形围栏 → 写 cesg_park_alarm。
重叠时只取最高优先级一条（纯私有范围 > 继承集团范围）；同级随机取一条。
"""
from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.amap_regeo import resolve_address_wgs84
from app.config import settings
from app.database import AsyncSessionLocal
from app.geo_utils import geometry_hit, wgs84_to_gcj02
from app.jt808_openapi_client import jt808_openapi_client
from app.jt808_park_alarm_sync import (
    ensure_and_insert,
    ensure_park_alarm_table,
    probe_park_alarm_table,
    round_coord,
)
from app.models import MapRuleCategory, ParkAlarmScanCursor, PrivateMapRule, Vehicle, VehicleDevice
from app.timeutil import china_now_naive

logger = logging.getLogger(__name__)

# 与电子围栏一致：圆 / 矩形 / 多边形同等参与停车命中与优先级（折线除外）
_FENCE_SHAPES = frozenset({"circle", "rectangle", "polygon"})


@dataclass
class ParkAlarmRunResult:
    rules: int = 0
    vehicles: int = 0
    pulled: int = 0
    over_limit: int = 0
    fence_hits: int = 0
    inserted: int = 0
    duplicates: int = 0
    skipped: int = 0
    skipped_cursor: int = 0
    skipped_no_coord: int = 0
    skipped_short: int = 0
    skipped_no_fence: int = 0
    errors: int = 0
    error: str | None = None
    detail: list[dict[str, Any]] = field(default_factory=list)


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    s = str(value).strip()
    if not s:
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 14:
        try:
            return datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return None


def _fmt_hms(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def _pick(item: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for k in keys:
        if k in item and item[k] is not None and str(item[k]).strip() != "":
            return item[k]
    return None


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _duration_minutes(item: dict[str, Any], start: datetime | None, end: datetime | None) -> int:
    """与轨迹回放一致：优先 stopTime（秒），再起止差。"""
    stop_sec = _to_float(_pick(item, ("stopTime", "stop_time", "time2", "duration")))
    if stop_sec is not None and stop_sec > 0:
        # 回放 stopTime 为秒；若异常大按毫秒
        if stop_sec > 24 * 3600 * 7:
            stop_sec = stop_sec / 1000.0
        return max(0, int(round(stop_sec / 60.0)))
    if start and end and end >= start:
        return max(0, int(round((end - start).total_seconds() / 60.0)))
    return 0


def _extract_lng_lat(item: dict[str, Any]) -> tuple[float | None, float | None]:
    """轨迹回放停车点：段起点 lng/lat（WGS84）。"""
    lng = _to_float(_pick(item, ("lng", "longitude", "lon", "elng", "slng")))
    lat = _to_float(_pick(item, ("lat", "latitude", "elat", "slat")))
    return lng, lat


def _extract_address(item: dict[str, Any]) -> str:
    text = str(_pick(item, ("address", "stopInfo", "stop_address", "location")) or "").strip()
    if text in ("", "0", "1", "--"):
        return ""
    # stopInfo 是「X分钟」可读时长，不是地址
    if text.endswith("分钟") or text.endswith("秒") or text.endswith("小时"):
        return ""
    return text[:512]


async def _load_device_map(db: AsyncSession, vehicle_ids: set[int]) -> dict[int, tuple[str, str]]:
    """vehicle_id -> (device_no, plate_no)。"""
    out: dict[int, tuple[str, str]] = {}
    if not vehicle_ids:
        return out
    rows = (
        await db.execute(
            select(Vehicle.id, Vehicle.plate_no, VehicleDevice.device_no)
            .outerjoin(VehicleDevice, VehicleDevice.vehicle_id == Vehicle.id)
            .where(Vehicle.id.in_(list(vehicle_ids)))
        )
    ).all()
    for vid, plate, device_no in rows:
        try:
            iv = int(vid)
        except (TypeError, ValueError):
            continue
        device = str(device_no or "").strip()
        if not device:
            continue
        # 优先保留已有；同车多设备取第一条主设备即可
        if iv not in out:
            out[iv] = (device, str(plate or "").strip())
    return out


def _to_gcj(lng_wgs: float, lat_wgs: float) -> tuple[float, float]:
    """1240/JT808 停车坐标为 WGS84，围栏几何为 GCJ02。"""
    return wgs84_to_gcj02(float(lng_wgs), float(lat_wgs))


def _rule_hit(lng_gcj: float, lat_gcj: float, rule: PrivateMapRule) -> bool:
    shape = str(rule.draw_shape_type or "").strip().lower()
    if shape not in _FENCE_SHAPES:
        return False
    geom = rule.geometry_json if isinstance(rule.geometry_json, dict) else {}
    return geometry_hit(lng_gcj, lat_gcj, shape, geom, 0.0)


def _park_priority_rank(rule: PrivateMapRule) -> int:
    """范围围栏优先级（数值越小越优先）。

    圆 / 矩形 / 多边形同档；仅区分来源：纯私有范围 > 继承集团范围。
    """
    return 0 if getattr(rule, "ref_public_rule_id", None) is None else 1


def _pick_park_hit(
    hits: list[tuple[PrivateMapRule, int, int]],
) -> tuple[PrivateMapRule, int, int] | None:
    """重叠命中只产一条：先取最高优先级档，同级随机一条。"""
    if not hits:
        return None
    best_rank = min(_park_priority_rank(rule) for rule, _, _ in hits)
    top = [h for h in hits if _park_priority_rank(h[0]) == best_rank]
    return random.choice(top)


async def _get_cursor(db: AsyncSession, device_no: str) -> str:
    row = await db.scalar(
        select(ParkAlarmScanCursor).where(ParkAlarmScanCursor.device_no == device_no).limit(1)
    )
    return str(row.last_etime or "").strip() if row else ""


async def _set_cursor(db: AsyncSession, device_no: str, etime: str) -> None:
    text = str(etime or "").strip()
    if not text:
        return
    row = await db.scalar(
        select(ParkAlarmScanCursor).where(ParkAlarmScanCursor.device_no == device_no).limit(1)
    )
    now = china_now_naive()
    if row is None:
        db.add(ParkAlarmScanCursor(device_no=device_no, last_etime=text, updated_at=now))
    else:
        # 仅前进
        if text > str(row.last_etime or ""):
            row.last_etime = text
            row.updated_at = now


async def _pull_stops(
    device_no: str,
    stime: str,
    etime: str,
    *,
    time_stop_minutes: int,
) -> list[dict[str, Any]]:
    """与历史回放轨迹「停车点」同源：1202/1105 stops。失败向上抛，便于计入 errors。"""
    return await jt808_openapi_client.list_history_stops(
        device_id=device_no,
        stime=stime,
        etime=etime,
        time_stop_minutes=max(0, int(time_stop_minutes)),
    )


async def run_park_alarm_once() -> ParkAlarmRunResult:
    result = ParkAlarmRunResult()
    # 无论是否配置停止限时，都先确保 808 业务表存在
    try:
        ensured = await asyncio.to_thread(ensure_park_alarm_table)
        if not ensured.get("ok") and ensured.get("error"):
            logger.warning("停车报警表预建失败: %s", ensured.get("error"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("停车报警表预建异常: %s", exc)

    if not jt808_openapi_client.configured():
        result.error = "JT808 OpenAPI 未配置"
        return result

    now = china_now_naive()
    lookback = timedelta(hours=max(1, int(settings.park_alarm_lookback_hours)))
    default_stime = _fmt_hms(now - lookback)
    etime = _fmt_hms(now)

    async with AsyncSessionLocal() as db:
        # 规则分配上配置的停止限时：挂在私有围栏规则上
        enabled_rules = (
            await db.execute(
                select(PrivateMapRule).where(PrivateMapRule.park_stop_limit_minutes > 0)
            )
        ).scalars().all()
        fence_rules = [
            r
            for r in enabled_rules
            if str(r.draw_shape_type or "").strip().lower() in _FENCE_SHAPES
            and isinstance(r.category_ids, list)
            and r.category_ids
        ]
        result.rules = len(fence_rules)
        if not fence_rules:
            return result

        cat_ids: set[int] = set()
        for rule in fence_rules:
            for cid in rule.category_ids or []:
                try:
                    cat_ids.add(int(cid))
                except (TypeError, ValueError):
                    continue
        cat_rows = (
            (
                await db.execute(select(MapRuleCategory).where(MapRuleCategory.id.in_(list(cat_ids))))
            ).scalars().all()
            if cat_ids
            else []
        )
        cat_by_id = {int(c.id): c for c in cat_rows}

        # vehicle_id -> [(rule, limit, category_id), ...]
        vehicle_rules: dict[int, list[tuple[PrivateMapRule, int, int]]] = {}
        all_vids: set[int] = set()
        for rule in fence_rules:
            limit = max(0, min(10000, int(getattr(rule, "park_stop_limit_minutes", 0) or 0)))
            if limit <= 0:
                continue
            for cid in rule.category_ids or []:
                try:
                    icid = int(cid)
                except (TypeError, ValueError):
                    continue
                cat = cat_by_id.get(icid)
                if cat is None:
                    continue
                vids = cat.assigned_vehicle_ids if isinstance(cat.assigned_vehicle_ids, list) else []
                for vid in vids:
                    try:
                        iv = int(vid)
                    except (TypeError, ValueError):
                        continue
                    all_vids.add(iv)
                    vehicle_rules.setdefault(iv, []).append((rule, limit, icid))

        device_map = await _load_device_map(db, all_vids)
        result.vehicles = len(device_map)

        for vid, (device_no, plate) in device_map.items():
            targets = vehicle_rules.get(vid) or []
            if not targets or not plate:
                continue
            # 同车取最严（最小分钟）作为 1202 timeStop，与回放「停车时长≥」一致
            min_limit = min(x[1] for x in targets)
            cursor = await _get_cursor(db, device_no)
            stime = cursor if cursor and len(cursor) >= 14 else default_stime
            try:
                stops = await _pull_stops(
                    device_no,
                    stime,
                    etime,
                    time_stop_minutes=int(min_limit),
                )
            except Exception as exc:  # noqa: BLE001
                result.errors += 1
                logger.warning("停车扫描失败 device=%s: %s", device_no, exc)
                continue

            result.pulled += len(stops)
            max_etime_seen = cursor

            for item in stops:
                start = _parse_dt(_pick(item, ("stime", "gpstime", "start_time", "startTime")))
                end = _parse_dt(_pick(item, ("etime", "end_time", "endTime")))
                if end is None:
                    continue
                end_hms = _fmt_hms(end)
                if end_hms > max_etime_seen:
                    max_etime_seen = end_hms
                if cursor and end_hms <= cursor:
                    result.skipped += 1
                    result.skipped_cursor += 1
                    continue

                duration_min = _duration_minutes(item, start, end)
                lng_raw, lat_raw = _extract_lng_lat(item)
                if lng_raw is None or lat_raw is None:
                    result.skipped += 1
                    result.skipped_no_coord += 1
                    continue

                if abs(float(lng_raw)) > 180 or abs(float(lat_raw)) > 90:
                    result.skipped += 1
                    result.skipped_no_coord += 1
                    continue

                # 与 History.vue 一致：轨迹/停车点为 WGS84，围栏为 GCJ02
                lng_wgs, lat_wgs = float(lng_raw), float(lat_raw)
                lng_gcj, lat_gcj = _to_gcj(lng_wgs, lat_wgs)

                if duration_min < min_limit:
                    result.skipped_short += 1
                    continue
                result.over_limit += 1

                # 圆/矩/多边形同等命中；重叠只取最高优先级一条，同级随机
                hits: list[tuple[PrivateMapRule, int, int]] = []
                for rule, limit, cat_id in targets:
                    if duration_min < limit:
                        continue
                    if not _rule_hit(lng_gcj, lat_gcj, rule):
                        continue
                    hits.append((rule, limit, cat_id))

                picked = _pick_park_hit(hits)
                if picked is None:
                    result.skipped_no_fence += 1
                    continue
                matched_rule, matched_limit, matched_cat_id = picked
                result.fence_hits += 1

                day = end.strftime("%Y%m%d")
                address = _extract_address(item)
                if not address:
                    try:
                        address = await resolve_address_wgs84(db, lat_wgs, lng_wgs) or ""
                    except Exception:  # noqa: BLE001
                        address = ""

                rule_name = str(getattr(matched_rule, "rule_name", None) or "").strip()
                write = await asyncio.to_thread(
                    ensure_and_insert,
                    {
                        "plate_no": plate,
                        "device_no": device_no,
                        "lng": lng_gcj,
                        "lat": lat_gcj,
                        "lng_r": round_coord(lng_gcj),
                        "lat_r": round_coord(lat_gcj),
                        "address": address,
                        "start_time": start,
                        "end_time": end,
                        "duration_min": duration_min,
                        "limit_min": matched_limit,
                        "day": day,
                        "category_id": matched_cat_id or None,
                        "rule_id": int(matched_rule.id),
                        "rule_name": rule_name,
                        "created_at": now,
                    },
                )
                if write == "inserted":
                    result.inserted += 1
                    result.detail.append(
                        {
                            "plate_no": plate,
                            "duration_min": duration_min,
                            "limit_min": matched_limit,
                            "rule_id": int(matched_rule.id),
                            "rule_name": rule_name,
                            "day": day,
                        }
                    )
                elif write == "duplicate":
                    result.duplicates += 1
                elif write == "error":
                    result.errors += 1
                else:
                    result.skipped += 1

            if max_etime_seen and max_etime_seen > (cursor or ""):
                await _set_cursor(db, device_no, max_etime_seen)

        try:
            await db.commit()
        except Exception as exc:  # noqa: BLE001
            result.error = f"commit failed: {exc}"
            await db.rollback()

    return result


class ParkAlarmScheduler:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_result: dict[str, Any] | None = None
        self._last_error: str | None = None
        self._last_run_at: datetime | None = None

    @property
    def running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    def status(self) -> dict[str, Any]:
        mirror = {}
        try:
            mirror = probe_park_alarm_table()
        except Exception as exc:  # noqa: BLE001
            mirror = {"error": str(exc), "mysql_ok": False}
        return {
            "enabled": bool(settings.park_alarm_enabled),
            "running": self.running,
            "interval_seconds": int(settings.park_alarm_interval_seconds),
            "lookback_hours": int(settings.park_alarm_lookback_hours),
            "jt808_openapi_configured": jt808_openapi_client.configured(),
            "jt808_mirror": mirror,
            "last_run_at": self._last_run_at.isoformat(sep=" ", timespec="seconds")
            if self._last_run_at
            else None,
            "last_result": self._last_result,
            "last_error": self._last_error,
        }

    def start(self, *, force: bool = False) -> None:
        if not force and not bool(settings.park_alarm_enabled):
            logger.info("停车超限报警调度未启用（park_alarm_enabled=False）")
            return
        if self.running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="park-alarm-scheduler")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_once(self) -> dict[str, Any]:
        result = await run_park_alarm_once()
        self._last_run_at = china_now_naive()
        payload = {
            "rules": result.rules,
            "vehicles": result.vehicles,
            "pulled": result.pulled,
            "over_limit": result.over_limit,
            "fence_hits": result.fence_hits,
            "inserted": result.inserted,
            "duplicates": result.duplicates,
            "skipped": result.skipped,
            "skipped_cursor": result.skipped_cursor,
            "skipped_no_coord": result.skipped_no_coord,
            "skipped_short": result.skipped_short,
            "skipped_no_fence": result.skipped_no_fence,
            "errors": result.errors,
            "error": result.error,
            "detail": result.detail[:20],
        }
        self._last_result = payload
        self._last_error = result.error
        if result.inserted or result.error or result.pulled:
            logger.info(
                "停车超限扫描：规则%s 车辆%s 拉取%s 超限%s 围栏命中%s 入库%s "
                "无围栏%s 无坐标%s 过短%s 游标跳过%s",
                result.rules,
                result.vehicles,
                result.pulled,
                result.over_limit,
                result.fence_hits,
                result.inserted,
                result.skipped_no_fence,
                result.skipped_no_coord,
                result.skipped_short,
                result.skipped_cursor,
            )
        return payload

    async def reset_cursors(self) -> dict[str, Any]:
        """清空扫描游标，便于修复逻辑后重扫近 lookback 窗口。"""
        async with AsyncSessionLocal() as db:
            res = await db.execute(delete(ParkAlarmScanCursor))
            await db.commit()
            deleted = int(res.rowcount or 0)
        return {"ok": True, "deleted": deleted}

    async def _loop(self) -> None:
        logger.info("停车超限报警调度已启动")
        try:
            ensured = await asyncio.to_thread(ensure_park_alarm_table)
            if ensured.get("created"):
                logger.info("808 停车报警表已预建: %s", ensured.get("table"))
            elif not ensured.get("ok"):
                logger.warning("808 停车报警表预建未成功: %s", ensured.get("error"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("808 停车报警表预建异常: %s", exc)
        while self._running:
            try:
                await self.run_once()
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("停车超限报警执行失败: %s", exc)
            await asyncio.sleep(max(30, int(settings.park_alarm_interval_seconds)))


park_alarm_scheduler = ParkAlarmScheduler()

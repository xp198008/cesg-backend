"""防拆报警：从 808 tgps_car_alarm 入库，不要求图片/视频证据。"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select

from app.alarm_type_gate import evaluate_alarm_type_ingest, log_alarm_type_gate
from app.database import AsyncSessionLocal
from app.jt808_alarm_sync import (
    SyncResult,
    _parse_api_time,
    _stable_biz_no,
    _upsert_state,
    _vehicle_by_plate,
    _vehicle_by_terminal,
)
from app.jt808_vehicle import _connect_mysql
from app.jt808_violation_sync import lookup_company_name, notify_violation_created
from app.models import Jt808AlarmSyncState, VehicleViolation
from app.timeutil import china_now_naive
from app.violation_filters import is_tamper_alarm_name

logger = logging.getLogger(__name__)

_SOURCE = "jt808_tamper"
_INITIAL_LOOKBACK = timedelta(hours=24)
_OVERLAP = timedelta(seconds=30)


def _now() -> datetime:
    return china_now_naive()


def _fetch_tamper_rows(start_at: datetime) -> list[dict[str, Any]] | None:
    """读 808 防拆记录。MySQL 不可用时返回 None，不推进同步水位。"""
    conn = _connect_mysql()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.id, a.car_id, a.name, a.gpstime, a.lat, a.lng, a.speed, a.address,
                       c.tid, c.carno
                FROM tgps_car_alarm a
                LEFT JOIN tgps_car c ON c.id = a.car_id
                WHERE a.name LIKE %s AND a.gpstime >= %s
                ORDER BY a.gpstime ASC, a.id ASC
                LIMIT 500
                """,
                ("%防拆%", start_at.strftime("%Y%m%d%H%M%S")),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


async def _window_start(db) -> datetime:
    row = await db.scalar(select(Jt808AlarmSyncState).where(Jt808AlarmSyncState.source == _SOURCE).limit(1))
    if row is None or row.last_window_end_at is None:
        return _now() - _INITIAL_LOOKBACK
    return row.last_window_end_at - _OVERLAP


async def sync_tamper_alarms_once() -> SyncResult:
    result = SyncResult(_SOURCE)
    async with AsyncSessionLocal() as db:
        start_at = await _window_start(db)
        end_at = _now()
        try:
            rows = await asyncio.to_thread(_fetch_tamper_rows, start_at)
        except Exception as exc:  # noqa: BLE001
            result.error = str(exc)
            logger.warning("防拆报警读取 808 失败: %s", exc)
            await _upsert_state(db, _SOURCE, start_at, None, result)
            await db.commit()
            return result
        if rows is None:
            result.error = "jt808 mysql unavailable"
            await _upsert_state(db, _SOURCE, start_at, None, result)
            await db.commit()
            return result

        result.total = len(rows)
        company_name_cache: dict[int, str | None] = {}
        for item in rows:
            type_name = str(item.get("name") or "").strip()
            if not is_tamper_alarm_name(type_name):
                continue
            ext_id = f"jt808:tamper:{item.get('id')}"
            exists = await db.scalar(
                select(VehicleViolation.id).where(VehicleViolation.external_alarm_id == ext_id).limit(1)
            )
            if exists:
                continue
            alarm_time = _parse_api_time(item.get("gpstime")) or end_at
            tid = str(item.get("tid") or "").strip()
            plate = str(item.get("carno") or "").strip()
            vehicle = await _vehicle_by_terminal(db, tid) if tid else None
            if vehicle is None and plate:
                vehicle = await _vehicle_by_plate(db, plate)
            if vehicle is None:
                result.skipped_no_vehicle += 1
                logger.info("防拆报警未匹配车辆，跳过 car_id=%s tid=%s plate=%s", item.get("car_id"), tid, plate)
                continue
            gate = await evaluate_alarm_type_ingest(
                db,
                type_name=type_name,
                vehicle_id=vehicle.id,
                alarm_time=alarm_time,
            )
            if not gate.get("allow"):
                reason = str(gate.get("reason") or "filtered")
                if reason == "interval":
                    result.skipped_interval += 1
                else:
                    result.skipped_filtered += 1
                log_alarm_type_gate(
                    source=_SOURCE,
                    external_id=ext_id,
                    alarm_type_name=type_name,
                    reason=reason,
                    plate=vehicle.plate_no or plate,
                )
                continue
            company_name = await lookup_company_name(db, vehicle.company_id, company_name_cache)
            lat = item.get("lat")
            lng = item.get("lng")
            try:
                lat_f = float(lat) if lat is not None and lat != "" else None
                lng_f = float(lng) if lng is not None and lng != "" else None
            except (TypeError, ValueError):
                lat_f, lng_f = None, None
            row = VehicleViolation(
                biz_no=_stable_biz_no(_SOURCE, ext_id, alarm_time),
                external_alarm_id=ext_id,
                terminal_id=(tid or plate or str(item.get("car_id") or ""))[:32],
                vehicle_id=vehicle.id,
                plate_no=(vehicle.plate_no or plate or "")[:16],
                company_id=vehicle.company_id,
                company_name=company_name,
                violation_type_name=type_name[:64],
                risk_level=str(gate.get("risk_level") or "high"),
                violation_time=alarm_time,
                lat=lat_f,
                lng=lng_f,
                address=str(item.get("address") or "")[:512],
                source=_SOURCE,
                status="待处理",
                ai_queried=False,
            )
            db.add(row)
            await db.flush()
            await notify_violation_created(db, row)
            result.inserted += 1
        await _upsert_state(db, _SOURCE, start_at, end_at, result)
        await db.commit()
    if result.inserted:
        logger.info("防拆报警入库 %s 条（扫描 %s）", result.inserted, result.total)
    return result


class TamperAlarmScheduler:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._running = False

    @property
    def running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    def start(self) -> None:
        if self.running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="tamper-alarm-sync")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        logger.info("防拆报警同步已启动")
        while self._running:
            try:
                result = await sync_tamper_alarms_once()
                if result.error:
                    logger.warning("防拆报警同步未完成: %s", result.error)
            except Exception as exc:  # noqa: BLE001
                logger.warning("防拆报警同步失败: %s", exc)
            await asyncio.sleep(60)


tamper_alarm_scheduler = TamperAlarmScheduler()

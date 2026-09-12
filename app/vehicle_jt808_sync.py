"""车辆档案 → 808 定时同步。

车辆新增/修改只把 vehicle.jt808_sync_status 标成 pending，不立刻调 808。
定时器只扫非 success 的行；1251 成功后改 success，之后不再扫。
删除车辆时本行已不在，仍立即走 1216。
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

from sqlalchemy import or_, select

from app import jt808_vehicle
from app.config import settings
from app.database import AsyncSessionLocal
from app.scheduler_lock import should_run_schedulers
from app.models import Vehicle
from app.timeutil import china_now_naive

logger = logging.getLogger(__name__)

STATUS_PENDING = "pending"
STATUS_SUCCESS = "success"


def _pending_filter():
    return or_(
        Vehicle.jt808_sync_status.is_(None),
        Vehicle.jt808_sync_status != STATUS_SUCCESS,
    )


async def queue_snapshot() -> dict[str, Any]:
    """待同步 / 已同步台数，给 OBD-STATUS 监控用。"""
    from sqlalchemy import func

    async with AsyncSessionLocal() as db:
        total = int((await db.scalar(select(func.count()).select_from(Vehicle))) or 0)
        success = int(
            (
                await db.scalar(
                    select(func.count())
                    .select_from(Vehicle)
                    .where(Vehicle.jt808_sync_status == STATUS_SUCCESS)
                )
            )
            or 0
        )
        pending = int(
            (await db.scalar(select(func.count()).select_from(Vehicle).where(_pending_filter())))
            or 0
        )
    return {
        "total": total,
        "pending": pending,
        "success": success,
        "scheduler": vehicle_jt808_sync_scheduler.status(),
    }


def mark_pending(vehicle: Vehicle, old_device_no: str | None = None) -> None:
    """车辆有改动：立刻标待同步，等定时器扫。"""
    vehicle.jt808_sync_status = STATUS_PENDING
    vehicle.jt808_sync_error = None
    if old_device_no:
        prev = (vehicle.jt808_sync_old_device_no or "").strip()
        if not prev:
            vehicle.jt808_sync_old_device_no = old_device_no.strip()


class VehicleJt808SyncScheduler:
    def __init__(self) -> None:
        self._running = False
        self._task: asyncio.Task | None = None
        self._last_run_at: datetime | None = None
        self._last_result: dict[str, Any] | None = None
        self._last_error: str | None = None

    @property
    def running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    def status(self) -> dict[str, Any]:
        return {
            "enabled": bool(settings.vehicle_jt808_sync_enabled),
            "running": self.running if should_run_schedulers() else bool(settings.vehicle_jt808_sync_enabled),
            "interval_seconds": int(settings.vehicle_jt808_sync_interval_seconds),
            "batch_size": int(settings.vehicle_jt808_sync_batch_size),
            "last_run_at": self._last_run_at.isoformat(sep=" ", timespec="seconds")
            if self._last_run_at
            else None,
            "last_result": self._last_result,
            "last_error": self._last_error,
        }

    def start(self, *, force: bool = False) -> None:
        if not force and not bool(settings.vehicle_jt808_sync_enabled):
            logger.info("车辆 808 同步调度未启用（vehicle_jt808_sync_enabled=False）")
            return
        if self.running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="vehicle-jt808-sync")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_once(self) -> dict[str, Any]:
        from sqlalchemy import func

        batch = max(1, int(settings.vehicle_jt808_sync_batch_size))
        pending_filter = _pending_filter()
        async with AsyncSessionLocal() as db:
            rows = (
                await db.execute(
                    select(Vehicle.id, Vehicle.jt808_sync_old_device_no, Vehicle.plate_no)
                    .where(pending_filter)
                    .order_by(Vehicle.id.asc())
                    .limit(batch)
                )
            ).all()
            pending_left = int(
                (await db.scalar(select(func.count()).select_from(Vehicle).where(pending_filter)))
                or 0
            )

        ok = 0
        fail = 0
        skipped = 0
        errors: list[str] = []
        for vid, old_dev, plate in rows:
            err = ""
            try:
                result = await jt808_vehicle.upsert_now(int(vid), old_dev)
            except Exception as exc:  # noqa: BLE001
                result = False
                err = str(exc)[:240]

            async with AsyncSessionLocal() as db:
                vehicle = await db.get(Vehicle, int(vid))
                if vehicle is None:
                    skipped += 1
                    continue
                vehicle.jt808_sync_try_count = int(vehicle.jt808_sync_try_count or 0) + 1
                vehicle.jt808_sync_at = china_now_naive()
                if result is True:
                    vehicle.jt808_sync_status = STATUS_SUCCESS
                    vehicle.jt808_sync_error = None
                    vehicle.jt808_sync_old_device_no = None
                    ok += 1
                else:
                    vehicle.jt808_sync_status = STATUS_PENDING
                    vehicle.jt808_sync_error = err or "808 未返回成功"
                    fail += 1
                    if len(errors) < 20:
                        errors.append(f"{plate or vid}: {vehicle.jt808_sync_error}")
                await db.commit()

        payload = {
            "scanned": len(rows),
            "success": ok,
            "failed": fail,
            "skipped": skipped,
            "pending_left": max(0, pending_left - ok),
            "errors": errors,
        }
        self._last_run_at = china_now_naive()
        self._last_result = payload
        self._last_error = errors[0] if fail and errors else None
        if ok or fail:
            logger.info(
                "车辆 808 同步：本轮扫%s 成功%s 失败%s 剩余待同步约%s",
                len(rows),
                ok,
                fail,
                payload["pending_left"],
            )
        return payload

    async def _loop(self) -> None:
        logger.info(
            "车辆 808 同步调度已启动，间隔 %ss，每轮最多 %s 台",
            int(settings.vehicle_jt808_sync_interval_seconds),
            int(settings.vehicle_jt808_sync_batch_size),
        )
        while self._running:
            try:
                await self.run_once()
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("车辆 808 同步执行失败: %s", exc)
            await asyncio.sleep(max(5, int(settings.vehicle_jt808_sync_interval_seconds)))


vehicle_jt808_sync_scheduler = VehicleJt808SyncScheduler()

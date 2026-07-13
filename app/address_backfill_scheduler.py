"""报警地址定时回填：持续为有坐标但 address 为空的记录补逆地理地址。"""
from __future__ import annotations

import asyncio
import logging

from app.database import AsyncSessionLocal
from app.violation_address_backfill import (
    backfill_vehicle_location_addresses,
    backfill_violation_addresses,
)

logger = logging.getLogger(__name__)

_INTERVAL_SEC = 90
_BATCH_VIOLATIONS = 40
_BATCH_LOCATIONS = 20


class AddressBackfillScheduler:
    def __init__(self) -> None:
        self._running = False
        self._task: asyncio.Task | None = None
        self._last_run_at: str | None = None
        self._last_violation_updated = 0
        self._last_location_updated = 0
        self._last_error: str | None = None

    @property
    def running(self) -> bool:
        return self._running

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="address-backfill")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_once(self) -> dict[str, int]:
        async with AsyncSessionLocal() as db:
            v = await backfill_violation_addresses(db, limit=_BATCH_VIOLATIONS)
            l = await backfill_vehicle_location_addresses(db, limit=_BATCH_LOCATIONS)
            await db.commit()
        self._last_violation_updated = v
        self._last_location_updated = l
        self._last_error = None
        if v or l:
            logger.info("地址定时回填：违章 %s 条，车辆位置 %s 条", v, l)
        return {"violations": v, "locations": l}

    def status(self) -> dict:
        return {
            "running": self._running,
            "interval_sec": _INTERVAL_SEC,
            "last_violation_updated": self._last_violation_updated,
            "last_location_updated": self._last_location_updated,
            "last_error": self._last_error,
        }

    async def _loop(self) -> None:
        logger.info("报警地址定时回填已启动（每 %s 秒）", _INTERVAL_SEC)
        while self._running:
            try:
                await self.run_once()
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("报警地址定时回填失败: %s", exc)
            await asyncio.sleep(_INTERVAL_SEC)


address_backfill_scheduler = AddressBackfillScheduler()

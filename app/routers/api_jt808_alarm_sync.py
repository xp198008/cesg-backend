"""JT808 主动安全同步管理接口。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.jt808_alarm_sync import jt808_alarm_scheduler
from app.models import Jt808AlarmSyncState

router = APIRouter(prefix="/api/jt808-alarm-sync", tags=["jt808-alarm-sync"])


def _dt(v):
    return v.isoformat(sep=" ", timespec="seconds") if v else None


def _state_out(row: Jt808AlarmSyncState) -> dict:
    return {
        "source": row.source,
        "last_window_start_at": _dt(row.last_window_start_at),
        "last_window_end_at": _dt(row.last_window_end_at),
        "last_success_at": _dt(row.last_success_at),
        "last_error": row.last_error,
        "last_total": row.last_total,
        "last_inserted": row.last_inserted,
        "updated_at": _dt(row.updated_at),
    }


@router.get("/status")
async def jt808_alarm_sync_status(db: AsyncSession = Depends(get_db)):
    rows = (await db.execute(select(Jt808AlarmSyncState).order_by(Jt808AlarmSyncState.source))).scalars().all()
    return {"ok": True, "scheduler": jt808_alarm_scheduler.status(), "states": [_state_out(x) for x in rows]}


@router.post("/run-once")
async def jt808_alarm_sync_run_once():
    try:
        results = await jt808_alarm_scheduler.run_once()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "results": [x.__dict__ for x in results]}


@router.post("/backfill")
async def jt808_alarm_sync_backfill(
    lookback_minutes: int = Query(120, ge=1, le=1440),
    reset_state: bool = Query(True),
):
    """按时间窗口回补历史主动安全报警（首次部署或排查时可用）。"""
    try:
        results = await jt808_alarm_scheduler.run_backfill(
            lookback_minutes=lookback_minutes,
            reset_state=reset_state,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "lookback_minutes": lookback_minutes, "reset_state": reset_state, "results": [x.__dict__ for x in results]}


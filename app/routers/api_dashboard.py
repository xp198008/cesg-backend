"""快捷桌面看板 API。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dashboard_stats import _FAULT_LEVEL_MAP, _fmt_dt, build_board_stats, build_home_stats
from app.database import AsyncSessionLocal, get_db
from app.models import VehicleFaultLive
from app.redis_queue_consumer import peek_queue, redis_queue_scheduler

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/home-stats")
async def dashboard_home_stats(
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    """快捷桌面业务指标：待处理任务、今日完成任务（808 在线/报警由前端用登录 token 调 /api）。"""
    return await build_home_stats(db, x_org_id)


@router.get("/board-stats")
async def dashboard_board_stats(
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    """智慧看板聚合指标：车辆情况、AI 预警、设备故障、司机画像、能耗统计。"""
    return await build_board_stats(db, x_org_id)


@router.get("/redis-peek")
async def dashboard_redis_peek(
    key: str = Query(..., description="QUEUE_GZM / QUEUE_OBD_YC / QUEUE_OBD_DC"),
    count: int = Query(3, ge=1, le=20),
):
    """只读抓取 Redis 队列样例（LRANGE，不移除数据），用于部署后校准字段别名。

    访问：/api/dashboard/redis-peek?key=QUEUE_GZM&count=3
    """
    from app.config import settings

    allowed = {settings.redis_queue_gzm, settings.redis_queue_obd_yc, settings.redis_queue_obd_dc}
    if key not in allowed:
        raise HTTPException(status_code=400, detail=f"key must be one of {sorted(allowed)}")
    return await peek_queue(key, count)


@router.get("/redis-queue/status")
async def dashboard_redis_queue_status():
    """Redis 队列消费器运行状态（用于诊断调度是否在跑、最近一轮消费了多少条）。"""
    return redis_queue_scheduler.status()


@router.get("/fault-live/{tid}")
async def dashboard_fault_live_detail(tid: int, db: AsyncSession = Depends(get_db)):
    """智慧看板跳转：系统实时故障详情（vehicle_fault_live）。"""
    row = await db.get(VehicleFaultLive, tid)
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {
        "ok": True,
        "data": {
            "id": row.id,
            "source": "live",
            "biz_no": f"SYS{row.id:08d}",
            "plate_no": row.plate_no or "—",
            "device_no": row.device_no or "—",
            "fault_code": row.fault_code or "—",
            "level": _FAULT_LEVEL_MAP.get(str(row.fault_level or "中"), "二级故障"),
            "status": "已处理" if row.handled else "待处理",
            "report_time": _fmt_dt(row.report_time, "%Y-%m-%d %H:%M:%S"),
        },
    }


@router.get("/redis-queue/recent")
async def dashboard_redis_queue_recent(limit: int = Query(20, ge=1, le=100)):
    """最近落库的故障 / OBD 能耗记录，用于在调试页面验证消费是否生效。"""
    from app.models import ObdEnergySnapshot

    async with AsyncSessionLocal() as db:
        try:
            fault_rows = (
                await db.execute(
                    select(
                        VehicleFaultLive.id,
                        VehicleFaultLive.device_no,
                        VehicleFaultLive.plate_no,
                        VehicleFaultLive.fault_code,
                        VehicleFaultLive.fault_level,
                        VehicleFaultLive.report_time,
                        VehicleFaultLive.handled,
                        VehicleFaultLive.created_at,
                    ).order_by(VehicleFaultLive.id.desc()).limit(limit)
                )
            ).all()
        except Exception as exc:  # noqa: BLE001
            fault_rows = []
        try:
            obd_rows = (
                await db.execute(
                    select(
                        ObdEnergySnapshot.id,
                        ObdEnergySnapshot.device_no,
                        ObdEnergySnapshot.energy_type,
                        ObdEnergySnapshot.fuel,
                        ObdEnergySnapshot.mileage,
                        ObdEnergySnapshot.day,
                        ObdEnergySnapshot.report_time,
                        ObdEnergySnapshot.created_at,
                    ).order_by(ObdEnergySnapshot.id.desc()).limit(limit)
                )
            ).all()
        except Exception as exc:  # noqa: BLE001
            obd_rows = []

    def _dt(v):
        return v.isoformat(sep=" ", timespec="seconds") if v else None

    return {
        "faults": [
            {
                "id": r[0],
                "device_no": r[1],
                "plate_no": r[2],
                "fault_code": r[3],
                "fault_level": r[4],
                "report_time": _dt(r[5]),
                "handled": bool(r[6]),
                "created_at": _dt(r[7]),
            }
            for r in fault_rows
        ],
        "obd": [
            {
                "id": r[0],
                "device_no": r[1],
                "energy_type": r[2],
                "fuel": r[3],
                "mileage": r[4],
                "day": r[5],
                "report_time": _dt(r[6]),
                "created_at": _dt(r[7]),
            }
            for r in obd_rows
        ],
    }

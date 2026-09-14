"""实时监控用：按选中车辆检测 OBD 接口是否异常。"""
from __future__ import annotations

from fastapi import APIRouter, Query

from app.database import AsyncSessionLocal
from app.obd_anomaly_scan import check_obd_anomaly_for_car_ids

router = APIRouter(prefix="/api/obd-anomaly", tags=["obd-anomaly"])


@router.get("/check")
async def obd_anomaly_check(car_ids: str = Query("", description="808 车辆 id，逗号分隔")):
    ids: list[int] = []
    for part in str(car_ids or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    async with AsyncSessionLocal() as db:
        data = await check_obd_anomaly_for_car_ids(db, ids)
    return {"ok": True, **data}

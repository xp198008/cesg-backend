"""主动安全风险画像 API。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.risk_profile_service import (
    default_weekly_end_date,
    load_vehicle_obd_indicators,
    query_risk_profile,
)

router = APIRouter(prefix="/api/risk-profile", tags=["risk-profile"])


def _parse_car_ids(raw: str | None) -> list[int] | None:
    if not raw or not str(raw).strip():
        return None
    ids: list[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"car_ids 无效: {part}") from exc
    return ids or None


def _parse_plates(raw: str | None) -> list[str] | None:
    if not raw or not str(raw).strip():
        return None
    plates = [part.strip() for part in str(raw).split(",") if part.strip()]
    return plates or None


@router.get("/summary")
async def risk_profile_summary(
    dimension: str = Query("vehicle", description="vehicle / company / driver"),
    mode: str = Query("weekly", description="weekly / monthly（月报官方不可用时由周报拼接）"),
    weekly_end_date: str | None = Query(None, description="周结束日 yyyyMMdd / yyyy-MM-dd"),
    report_month: str | None = Query(None, description="月份 yyyyMM / yyyy-MM"),
    car_ids: str | None = Query(None, description="风险侧 car_id 列表，逗号分隔"),
    plates: str | None = Query(None, description="与 car_ids 等长的车牌列表，逗号分隔"),
    car_plates: str | None = Query(
        None, description="car_id:车牌 映射，如 105:渝DX7610,276:渝A12345"
    ),
    company_id: int | None = Query(None),
    company_ids: str | None = Query(None, description="公司 id 列表，逗号分隔（与组织树 gids 一致）"),
    driver_id: int | None = Query(None, description="本地司机 id（司机风险画像优先）"),
    driver_name: str | None = Query(None),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    dim = (dimension or "vehicle").strip().lower()
    if dim not in ("vehicle", "company", "driver"):
        raise HTTPException(status_code=400, detail="dimension 须为 vehicle / company / driver")
    try:
        return await query_risk_profile(
            db,
            dimension=dim,
            mode=mode,
            weekly_end_date=weekly_end_date or default_weekly_end_date(),
            report_month=report_month,
            car_ids=_parse_car_ids(car_ids),
            plates=_parse_plates(plates),
            car_plates=car_plates,
            company_id=company_id,
            company_ids=_parse_car_ids(company_ids),
            driver_id=driver_id,
            driver_name=driver_name,
            x_org_id=x_org_id,
            x_user_id=x_user_id,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"风险数据查询失败: {exc}") from exc


@router.get("/obd")
async def risk_profile_obd(
    plate: str = Query(..., description="车牌号"),
    db: AsyncSession = Depends(get_db),
):
    """车辆风险画像核心指标：按车牌从 Redis `{终端号}_OBD` 取实时 OBD。"""
    plate_no = str(plate or "").strip()
    if not plate_no:
        raise HTTPException(status_code=400, detail="plate 不能为空")
    try:
        return await load_vehicle_obd_indicators(db, plate_no)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"OBD 读取失败: {exc}") from exc

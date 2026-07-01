"""人工报障：录入与处理。"""
from __future__ import annotations

import secrets
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.device_fault_service import update_manual_fault_report_handle
from app.models import (
    FaultTypeDict,
    JtDeviceFault,
    ManualFaultReport,
    OrgCompany,
    Vehicle,
    VehicleDevice,
    VehicleLocation,
    VehicleViolation,
)
from app.org_scope import collect_org_company_subtree_ids, require_x_org_id_header
from app.plate_util import norm_plate
from app.timeutil import china_now_naive

router = APIRouter(prefix="/api/manual-fault", tags=["manual-fault"])

_ALLOWED_LEVEL = frozenset({"高", "中", "低"})


def _gen_biz_no() -> str:
    return f"BZ{china_now_naive().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(3).upper()}"


def _parse_discovery_time(raw: str | datetime | None) -> datetime:
    if isinstance(raw, datetime):
        return raw
    s = (raw or "").strip()
    if not s:
        return china_now_naive()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail="发现时间格式无效，应为 yyyy-MM-dd HH:mm:ss")


async def _resolve_terminal_bind_no(db: AsyncSession, vehicle_id: int | None, plate: str) -> str | None:
    vid = vehicle_id
    p = norm_plate(plate)
    if vid is None and p:
        r = await db.execute(select(Vehicle).where(Vehicle.plate_no == p))
        v = r.scalar_one_or_none()
        if v is None:
            r = await db.execute(select(Vehicle).where(func.upper(Vehicle.plate_no) == p.upper()))
            v = r.scalar_one_or_none()
        vid = v.id if v else None
    if vid is not None:
        rd = await db.execute(
            select(VehicleDevice)
            .where(VehicleDevice.vehicle_id == vid)
            .order_by(VehicleDevice.is_main.desc(), VehicleDevice.id.asc())
        )
        for dev in rd.scalars().all():
            for attr in ("device_no", "device_sn", "sim_no", "actual_sim"):
                val = getattr(dev, attr, None)
                if val is not None and str(val).strip():
                    return str(val).strip()[:64]
        lr = await db.execute(select(VehicleLocation).where(VehicleLocation.vehicle_id == vid))
        loc = lr.scalar_one_or_none()
        if loc is not None and (loc.terminal_id or "").strip():
            return str(loc.terminal_id).strip()[:64]

    stmt_vio = select(VehicleViolation).where(
        VehicleViolation.terminal_id.isnot(None),
        VehicleViolation.terminal_id != "",
    )
    if vid is not None:
        if p:
            stmt_vio = stmt_vio.where(
                or_(
                    VehicleViolation.vehicle_id == vid,
                    func.upper(func.trim(VehicleViolation.plate_no)) == p.upper(),
                )
            )
        else:
            stmt_vio = stmt_vio.where(VehicleViolation.vehicle_id == vid)
    elif p:
        stmt_vio = stmt_vio.where(func.upper(func.trim(VehicleViolation.plate_no)) == p.upper())
    else:
        stmt_vio = None
    if stmt_vio is not None:
        stmt_vio = stmt_vio.order_by(VehicleViolation.violation_time.desc()).limit(1)
        hv = (await db.execute(stmt_vio)).scalar_one_or_none()
        if hv is not None and (hv.terminal_id or "").strip():
            return str(hv.terminal_id).strip()[:64]

    stmt = select(JtDeviceFault).where(JtDeviceFault.terminal_id.isnot(None)).where(JtDeviceFault.terminal_id != "")
    if vid is not None:
        if p:
            stmt = stmt.where(
                or_(
                    JtDeviceFault.vehicle_id == vid,
                    func.upper(func.trim(JtDeviceFault.plate_no)) == p.upper(),
                )
            )
        else:
            stmt = stmt.where(JtDeviceFault.vehicle_id == vid)
    elif p:
        stmt = stmt.where(func.upper(func.trim(JtDeviceFault.plate_no)) == p.upper())
    else:
        return None
    stmt = stmt.order_by(JtDeviceFault.fault_time.desc()).limit(1)
    rf = await db.execute(stmt)
    row = rf.scalar_one_or_none()
    if row and (row.terminal_id or "").strip():
        return str(row.terminal_id).strip()[:64]
    return None


class ManualFaultCreateIn(BaseModel):
    plate_no: str = Field(..., min_length=1, max_length=16)
    vehicle_id: int | None = Field(None, ge=1)
    fault_type_dict_id: int = Field(..., ge=1)
    discovery_time: str | datetime | None = Field(None)
    discoverer: str = Field(..., min_length=1, max_length=64)
    fault_level: str = Field(..., min_length=1, max_length=16)
    fault_devices: str | None = Field(None, max_length=4000)
    fault_phenomenon: str | None = Field(None, max_length=4000)
    fault_location: str | None = Field(None, max_length=256)
    affect_service: int = Field(1, ge=0, le=1)
    terminal_no: str | None = Field(None, max_length=64)


class ManualFaultHandleIn(BaseModel):
    handle_status: str
    handler_remark: str | None = None
    handler_name: str | None = None
    auditor_name: str | None = None
    audit_remark: str | None = None


@router.post("")
async def manual_fault_create(
    body: ManualFaultCreateIn,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    root = require_x_org_id_header(x_org_id)
    co = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
    if co is None:
        raise HTTPException(status_code=400, detail="X-Org-Id 对应公司不存在")
    subtree = await collect_org_company_subtree_ids(db, root)

    ft = await db.get(FaultTypeDict, int(body.fault_type_dict_id))
    if ft is None:
        raise HTTPException(status_code=400, detail="所选故障类型不存在，请刷新页面后重新选择")
    level = (body.fault_level or "").strip()
    if level not in _ALLOWED_LEVEL:
        raise HTTPException(status_code=400, detail="故障级别须为高、中、低")
    dict_level = (ft.fault_level or "").strip() or "中"
    if dict_level != level:
        raise HTTPException(status_code=400, detail="故障级别与所选故障类型不一致，请重新选择故障类型")

    plate = norm_plate(body.plate_no)
    if not plate:
        raise HTTPException(status_code=400, detail="车牌不能为空")
    v: Vehicle | None = None
    if body.vehicle_id is not None:
        r_id = await db.execute(select(Vehicle).where(Vehicle.id == int(body.vehicle_id)))
        v_pick = r_id.scalar_one_or_none()
        if v_pick is None:
            raise HTTPException(status_code=400, detail="所选车辆不存在，请重新从列表选择")
        if norm_plate(v_pick.plate_no) != plate:
            raise HTTPException(status_code=400, detail="所选车辆与当前填写车牌不一致，请重新从列表选择")
        v = v_pick
    else:
        vr = await db.execute(select(Vehicle).where(Vehicle.plate_no == plate))
        v = vr.scalar_one_or_none()
        if v is None:
            vr2 = await db.execute(select(Vehicle).where(func.upper(Vehicle.plate_no) == plate.upper()))
            v = vr2.scalar_one_or_none()

    if v is not None and v.company_id is not None and int(v.company_id) not in subtree:
        raise HTTPException(status_code=403, detail="该车辆不属于您所在公司及下级公司，无法报障")

    vehicle_id = int(v.id) if v else None
    company_id = int(v.company_id) if v is not None and v.company_id is not None else root

    disc_t = _parse_discovery_time(body.discovery_time)
    type_name = (ft.type_name or "").strip()[:64] or None
    override_tid = (body.terminal_no or "").strip()[:64] if body.terminal_no else ""
    terminal_snap = override_tid if override_tid else await _resolve_terminal_bind_no(db, vehicle_id, plate)

    row = ManualFaultReport(
        biz_no=_gen_biz_no(),
        plate_no=plate,
        terminal_bind_no=terminal_snap or None,
        vehicle_id=vehicle_id,
        company_id=company_id,
        fault_type_dict_id=int(body.fault_type_dict_id),
        fault_type_name=type_name,
        fault_level=level,
        discovery_time=disc_t,
        discoverer=(body.discoverer or "").strip()[:64],
        fault_devices=(body.fault_devices or "").strip() or None,
        fault_phenomenon=(body.fault_phenomenon or "").strip() or None,
        fault_location=(body.fault_location or "").strip()[:256] or None,
        affect_service=int(body.affect_service),
        handle_status="未处理",
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return {"ok": True, "id": row.id, "biz_no": row.biz_no}


@router.put("/{fault_id}/handle")
async def manual_fault_handle_put(
    fault_id: int,
    body: ManualFaultHandleIn,
    db: AsyncSession = Depends(get_db),
):
    ok, err = await update_manual_fault_report_handle(
        db,
        fault_id,
        handle_status=body.handle_status,
        handler_remark=body.handler_remark,
        handler_name=body.handler_name,
        auditor_name=body.auditor_name,
        audit_remark=body.audit_remark,
    )
    if not ok:
        raise HTTPException(status_code=400, detail=err or "更新失败")
    return {"ok": True}

"""人工报障：录入、审核与单据上传（待审核 → 审核通过 → 已完结 / 驳回）。"""
from __future__ import annotations

import secrets
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.device_fault_service import (
    device_fault_receipt_safe_ext,
    gen_receipt_stored_name,
    get_manual_fault_by_id,
    get_manual_fault_image_by_id,
    get_manual_fault_receipt_by_id,
    guess_image_media_type,
    insert_manual_fault_image,
    insert_manual_fault_receipt,
    jt_device_fault_receipt_eligible,
    list_manual_fault_images,
    list_manual_fault_receipts,
    manual_fault_image_safe_ext,
    manual_fault_images_root,
    manual_fault_receipts_root,
    mark_fault_awaiting_final_review,
    resolve_manual_fault_image_file_path,
    resolve_manual_fault_receipt_file_path,
    update_manual_fault_report_handle,
)
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
_MANUAL_FAULT_RECEIPT_MAX_BYTES = 10 * 1024 * 1024
_MANUAL_FAULT_IMAGE_MAX_BYTES = 10 * 1024 * 1024
_MANUAL_FAULT_IMAGE_MAX_COUNT = 12


def _gen_biz_no() -> str:
    return f"BZ{china_now_naive().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(3).upper()}"


def _parse_discovery_time(raw: str | datetime | None) -> datetime:
    if isinstance(raw, datetime):
        parsed = raw.replace(tzinfo=None) if raw.tzinfo else raw
    else:
        s = (raw or "").strip().replace("T", " ")
        if not s:
            return china_now_naive()
        parsed = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            raise HTTPException(status_code=400, detail="发现时间格式无效，应为 yyyy-MM-dd HH:mm:ss")
    now = china_now_naive()
    # 只选了日期、时分秒被写成 00:00:00 时，当天用当前时间，避免审核页全是零点
    if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0 and parsed.date() == now.date():
        return now
    return parsed


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
    # 等级以故障类型字典为准，避免前端树选/映射偏差导致无法提交
    dict_level = (ft.fault_level or "").strip() or "中"
    level = dict_level if dict_level in _ALLOWED_LEVEL else "中"

    plate = norm_plate(body.plate_no)
    if not plate:
        raise HTTPException(status_code=400, detail="车牌不能为空")
    v: Vehicle | None = None
    if body.vehicle_id is not None:
        r_id = await db.execute(select(Vehicle).where(Vehicle.id == int(body.vehicle_id)))
        v_pick = r_id.scalar_one_or_none()
        # 前端车辆树常传 JT808 car_id，可能对不上本地 vehicle.id；仅在 id+车牌都匹配时采用
        if v_pick is not None and norm_plate(v_pick.plate_no) == plate:
            v = v_pick
    if v is None:
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
        handle_status="待审核",
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


@router.get("/receipts/list")
async def api_list_manual_fault_receipts(
    fault_id: int | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows, total = await list_manual_fault_receipts(db, fault_id=fault_id, page=page, page_size=page_size)
    return {"ok": True, "items": rows, "total": total}


@router.get("/receipts/{receipt_id}/download")
async def download_manual_fault_receipt_file(receipt_id: int, db: AsyncSession = Depends(get_db)):
    meta = await get_manual_fault_receipt_by_id(db, receipt_id)
    if not meta:
        raise HTTPException(status_code=404, detail="单据不存在")
    p = resolve_manual_fault_receipt_file_path(int(meta["fault_id"]), str(meta["stored_name"]))
    if p is None:
        raise HTTPException(status_code=404, detail="文件不存在")
    name = str(meta.get("original_name") or Path(p).name)
    media = guess_image_media_type(name, meta.get("mime_type") or "application/octet-stream")
    return FileResponse(path=p, filename=name, media_type=media)


@router.get("/{fault_id}/images")
async def api_list_manual_fault_images(fault_id: int, db: AsyncSession = Depends(get_db)):
    row = await get_manual_fault_by_id(db, fault_id)
    if not row:
        raise HTTPException(status_code=404, detail="报障记录不存在")
    items = await list_manual_fault_images(db, fault_id=fault_id)
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/images/{image_id}/view")
async def view_manual_fault_image(image_id: int, db: AsyncSession = Depends(get_db)):
    meta = await get_manual_fault_image_by_id(db, image_id)
    if not meta:
        raise HTTPException(status_code=404, detail="图片不存在")
    p = resolve_manual_fault_image_file_path(int(meta["fault_id"]), str(meta["stored_name"]))
    if p is None:
        raise HTTPException(status_code=404, detail="图片文件不存在")
    name = str(meta.get("original_name") or Path(p).name)
    media = guess_image_media_type(name, meta.get("mime_type"))
    return FileResponse(path=p, filename=name, media_type=media)


@router.post("/{fault_id}/images")
async def upload_manual_fault_images(
    fault_id: int,
    files: list[UploadFile] = File(...),
    uploader_name: str | None = Form(None),
    db: AsyncSession = Depends(get_db),
):
    row = await get_manual_fault_by_id(db, fault_id)
    if not row:
        raise HTTPException(status_code=404, detail="报障记录不存在")
    if not files:
        raise HTTPException(status_code=400, detail="请选择至少一张图片")
    existing = await list_manual_fault_images(db, fault_id=fault_id)
    if len(existing) + len(files) > _MANUAL_FAULT_IMAGE_MAX_COUNT:
        raise HTTPException(status_code=400, detail=f"设备图片最多 {_MANUAL_FAULT_IMAGE_MAX_COUNT} 张")

    root = manual_fault_images_root()
    fault_dir = root / str(fault_id)
    fault_dir.mkdir(parents=True, exist_ok=True)
    biz = row.get("biz_no") or ""
    uname = (uploader_name or "").strip()[:64] or None
    saved: list[dict] = []

    for uf in files:
        ext = manual_fault_image_safe_ext(uf.filename)
        if not ext:
            raise HTTPException(status_code=400, detail=f"仅支持图片文件: {uf.filename}")
        orig = (uf.filename or "image").replace("\\", "/").split("/")[-1][:255]
        content = await uf.read()
        if not content:
            raise HTTPException(status_code=400, detail=f"文件为空: {orig}")
        if len(content) > _MANUAL_FAULT_IMAGE_MAX_BYTES:
            raise HTTPException(status_code=400, detail=f"图片 {orig} 超过 10MB")
        stored = gen_receipt_stored_name(ext)
        dest = fault_dir / stored
        dest.write_bytes(content)
        mime = guess_image_media_type(orig, uf.content_type)
        iid = await insert_manual_fault_image(
            db,
            fault_id=fault_id,
            biz_no=biz,
            stored_name=stored,
            original_name=orig,
            file_size=len(content),
            mime_type=mime,
            uploader_name=uname,
        )
        one = await get_manual_fault_image_by_id(db, iid)
        if one:
            saved.append(one)

    await db.commit()
    return {"ok": True, "saved": saved, "items": saved}


@router.post("/{fault_id}/receipts")
async def upload_manual_fault_receipts(
    fault_id: int,
    files: list[UploadFile] = File(...),
    uploader_name: str | None = Form(None),
    db: AsyncSession = Depends(get_db),
):
    row = await get_manual_fault_by_id(db, fault_id)
    if not row:
        raise HTTPException(status_code=404, detail="报障记录不存在")
    if not jt_device_fault_receipt_eligible(row.get("handle_status")):
        raise HTTPException(status_code=400, detail="仅审核通过后的报障可上传单据")
    if not files:
        raise HTTPException(status_code=400, detail="请选择至少一个文件")

    root = manual_fault_receipts_root()
    fault_dir = root / str(fault_id)
    fault_dir.mkdir(parents=True, exist_ok=True)
    biz = row.get("biz_no") or ""
    uname = (uploader_name or "").strip()[:64] or None
    saved: list[dict] = []

    for uf in files:
        ext = device_fault_receipt_safe_ext(uf.filename)
        if not ext:
            raise HTTPException(status_code=400, detail=f"不支持的文件类型: {uf.filename}")
        orig = (uf.filename or "file").replace("\\", "/").split("/")[-1][:255]
        content = await uf.read()
        if len(content) > _MANUAL_FAULT_RECEIPT_MAX_BYTES:
            raise HTTPException(status_code=400, detail=f"文件 {orig} 超过 10MB")
        stored = gen_receipt_stored_name(ext)
        dest = fault_dir / stored
        dest.write_bytes(content)
        rid = await insert_manual_fault_receipt(
            db,
            fault_id=fault_id,
            biz_no=biz,
            stored_name=stored,
            original_name=orig,
            file_size=len(content),
            mime_type=uf.content_type,
            uploader_name=uname,
        )
        one = await get_manual_fault_receipt_by_id(db, rid)
        if one:
            saved.append(one)

    orm = await db.get(ManualFaultReport, int(fault_id))
    if orm is not None:
        await mark_fault_awaiting_final_review(orm)
    await db.commit()
    return {"ok": True, "saved": saved}

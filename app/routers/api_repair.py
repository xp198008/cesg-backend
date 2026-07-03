"""设备报修：录入、列表、审核、完成与单据上传。"""
from __future__ import annotations

import secrets
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.device_fault_service import device_fault_receipt_safe_ext, gen_receipt_stored_name
from app.models import OrgCompany, Vehicle, VehicleRepair, VehicleRepairReceipt
from app.org_scope import collect_org_company_subtree_ids, require_x_org_id_header
from app.plate_util import norm_plate
from app.timeutil import china_now_naive

router = APIRouter(prefix="/api/repair", tags=["repair"])

_RECEIPT_MAX_BYTES = 10 * 1024 * 1024
_REVIEW_PENDING = "待审核"
_REVIEW_APPROVED = "审核通过"
_REVIEW_REJECTED = "审核驳回"
_REPAIR_STATUSES = frozenset({"待处理", "处理中", "已完成"})


def _gen_biz_no() -> str:
    return f"XB{china_now_naive().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(3).upper()}"


def _parse_dt(raw: str | datetime | None, field_label: str, default_now: bool = False) -> datetime | None:
    if isinstance(raw, datetime):
        return raw
    s = (raw or "").strip()
    if not s:
        return china_now_naive() if default_now else None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail=f"{field_label}格式无效，应为 yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss")


def _fmt_dt(v: datetime | None) -> str | None:
    return v.strftime("%Y-%m-%d %H:%M:%S") if v else None


def _repair_receipts_root() -> Path:
    d = Path(__file__).resolve().parent.parent.parent / "data" / "repair_receipts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _repair_to_dict(row: VehicleRepair) -> dict:
    return {
        "id": row.id,
        "biz_no": row.biz_no,
        "plate_no": row.plate_no,
        "vehicle_id": row.vehicle_id,
        "company_id": row.company_id,
        "repair_type": row.repair_type,
        "repair_time": _fmt_dt(row.repair_time),
        "repairer": row.repairer,
        "phone": row.phone,
        "expected_at": _fmt_dt(row.expected_at),
        "main_device": row.main_device,
        "device_model": row.device_model,
        "device_no": row.device_no,
        "description": row.description,
        "repair_address": row.repair_address,
        "estimated_cost": float(row.estimated_cost) if row.estimated_cost is not None else None,
        "remark": row.remark,
        "review_status": row.review_status,
        "reviewer": row.reviewer,
        "review_remark": row.review_remark,
        "reviewed_at": _fmt_dt(row.reviewed_at),
        "repair_status": row.repair_status,
        "completed_at": _fmt_dt(row.completed_at),
        "created_at": _fmt_dt(row.created_at),
    }


def _receipt_to_dict(row: VehicleRepairReceipt, repair: VehicleRepair | None = None) -> dict:
    return {
        "id": row.id,
        "repair_id": row.repair_id,
        "biz_no": row.biz_no,
        "original_name": row.original_name,
        "file_size": row.file_size,
        "mime_type": row.mime_type,
        "uploader_name": row.uploader_name,
        "remark": row.remark,
        "created_at": _fmt_dt(row.created_at),
        "plate_no": repair.plate_no if repair else None,
        "main_device": repair.main_device if repair else None,
        "repairer": repair.repairer if repair else None,
    }


class RepairCreateIn(BaseModel):
    plate_no: str = Field(..., min_length=1, max_length=16)
    vehicle_id: int | None = Field(None, ge=1)
    repair_type: str = Field("设备报修", max_length=32)
    repair_time: str | datetime | None = None
    repairer: str = Field(..., min_length=1, max_length=64)
    phone: str | None = Field(None, max_length=32)
    expected_at: str | datetime | None = None
    main_device: str | None = Field(None, max_length=64)
    device_model: str | None = Field(None, max_length=64)
    device_no: str | None = Field(None, max_length=64)
    description: str | None = Field(None, max_length=4000)
    repair_address: str | None = Field(None, max_length=256)
    estimated_cost: float | None = Field(None, ge=0)
    initial_status: str | None = Field(None, max_length=32)
    remark: str | None = Field(None, max_length=4000)


class RepairReviewIn(BaseModel):
    result: str = Field(..., description="approve / reject")
    reviewer_name: str | None = Field(None, max_length=64)
    remark: str | None = Field(None, max_length=255)


class RepairStatusIn(BaseModel):
    repair_status: str = Field(..., max_length=32)
    operator_name: str | None = Field(None, max_length=64)


@router.post("")
async def repair_create(
    body: RepairCreateIn,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    root = require_x_org_id_header(x_org_id)
    co = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
    if co is None:
        raise HTTPException(status_code=400, detail="X-Org-Id 对应公司不存在")
    subtree = await collect_org_company_subtree_ids(db, root)

    plate = norm_plate(body.plate_no)
    if not plate:
        raise HTTPException(status_code=400, detail="报修车辆不能为空")

    v: Vehicle | None = None
    if body.vehicle_id is not None:
        v = (await db.execute(select(Vehicle).where(Vehicle.id == int(body.vehicle_id)))).scalar_one_or_none()
        if v is None:
            raise HTTPException(status_code=400, detail="所选车辆不存在，请重新从列表选择")
    else:
        v = (await db.execute(select(Vehicle).where(Vehicle.plate_no == plate))).scalar_one_or_none()
        if v is None:
            v = (
                await db.execute(select(Vehicle).where(func.upper(Vehicle.plate_no) == plate.upper()))
            ).scalar_one_or_none()
    if v is not None and v.company_id is not None and int(v.company_id) not in subtree:
        raise HTTPException(status_code=403, detail="该车辆不属于您所在公司及下级公司，无法报修")

    status = (body.initial_status or "").strip() or "待处理"
    if status not in _REPAIR_STATUSES:
        raise HTTPException(status_code=400, detail="初始维修状态须为 待处理 / 处理中 / 已完成")

    row = VehicleRepair(
        biz_no=_gen_biz_no(),
        plate_no=plate,
        vehicle_id=int(v.id) if v else None,
        company_id=int(v.company_id) if v is not None and v.company_id is not None else root,
        repair_type=(body.repair_type or "").strip()[:32] or "设备报修",
        repair_time=_parse_dt(body.repair_time, "报修时间", default_now=True),
        repairer=(body.repairer or "").strip()[:64],
        phone=(body.phone or "").strip()[:32] or None,
        expected_at=_parse_dt(body.expected_at, "期望完成时间"),
        main_device=(body.main_device or "").strip()[:64] or None,
        device_model=(body.device_model or "").strip()[:64] or None,
        device_no=(body.device_no or "").strip()[:64] or None,
        description=(body.description or "").strip() or None,
        repair_address=(body.repair_address or "").strip()[:256] or None,
        estimated_cost=body.estimated_cost,
        remark=(body.remark or "").strip() or None,
        review_status=_REVIEW_PENDING,
        repair_status=status,
        completed_at=china_now_naive() if status == "已完成" else None,
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return {"ok": True, "id": row.id, "biz_no": row.biz_no}


@router.get("/list")
async def repair_list(
    plate_no: str | None = None,
    biz_no: str | None = None,
    repairer: str | None = None,
    review_status: str | None = None,
    repair_status: str | None = None,
    approved_only: bool = Query(False),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(VehicleRepair)
    if plate_no and plate_no.strip():
        stmt = stmt.where(VehicleRepair.plate_no.like(f"%{plate_no.strip()}%"))
    if biz_no and biz_no.strip():
        stmt = stmt.where(VehicleRepair.biz_no.like(f"%{biz_no.strip()}%"))
    if repairer and repairer.strip():
        stmt = stmt.where(VehicleRepair.repairer.like(f"%{repairer.strip()}%"))
    if review_status and review_status.strip():
        stmt = stmt.where(VehicleRepair.review_status == review_status.strip())
    if repair_status and repair_status.strip():
        stmt = stmt.where(VehicleRepair.repair_status == repair_status.strip())
    if approved_only:
        stmt = stmt.where(VehicleRepair.review_status == _REVIEW_APPROVED)

    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        (
            await db.execute(
                stmt.order_by(VehicleRepair.repair_time.desc(), VehicleRepair.id.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        )
        .scalars()
        .all()
    )
    return {"ok": True, "items": [_repair_to_dict(r) for r in rows], "total": int(total)}


@router.get("/receipts/list")
async def repair_receipt_list(
    repair_id: int | None = None,
    plate_no: str | None = None,
    biz_no: str | None = None,
    repairer: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(VehicleRepairReceipt, VehicleRepair).join(
        VehicleRepair, VehicleRepair.id == VehicleRepairReceipt.repair_id, isouter=True
    )
    if repair_id is not None:
        stmt = stmt.where(VehicleRepairReceipt.repair_id == int(repair_id))
    if biz_no and biz_no.strip():
        stmt = stmt.where(VehicleRepairReceipt.biz_no.like(f"%{biz_no.strip()}%"))
    if plate_no and plate_no.strip():
        stmt = stmt.where(VehicleRepair.plate_no.like(f"%{plate_no.strip()}%"))
    if repairer and repairer.strip():
        stmt = stmt.where(VehicleRepair.repairer.like(f"%{repairer.strip()}%"))

    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    pairs = (
        await db.execute(
            stmt.order_by(VehicleRepairReceipt.id.desc()).offset((page - 1) * page_size).limit(page_size)
        )
    ).all()
    return {
        "ok": True,
        "items": [_receipt_to_dict(rc, rp) for rc, rp in pairs],
        "total": int(total),
    }


@router.get("/receipts/{receipt_id}/download")
async def repair_receipt_download(receipt_id: int, db: AsyncSession = Depends(get_db)):
    rc = await db.get(VehicleRepairReceipt, receipt_id)
    if rc is None:
        raise HTTPException(status_code=404, detail="单据不存在")
    sn = (rc.stored_name or "").strip()
    if not sn or "/" in sn or "\\" in sn or ".." in sn:
        raise HTTPException(status_code=404, detail="文件不存在")
    p = _repair_receipts_root() / str(rc.repair_id) / sn
    if not p.exists():
        raise HTTPException(status_code=404, detail="文件不存在")
    return FileResponse(path=p, filename=rc.original_name or p.name, media_type="application/octet-stream")


@router.get("/{repair_id}")
async def repair_detail(repair_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.get(VehicleRepair, repair_id)
    if row is None:
        raise HTTPException(status_code=404, detail="报修记录不存在")
    return {"ok": True, "data": _repair_to_dict(row)}


@router.put("/{repair_id}/review")
async def repair_review(repair_id: int, body: RepairReviewIn, db: AsyncSession = Depends(get_db)):
    row = await db.get(VehicleRepair, repair_id)
    if row is None:
        raise HTTPException(status_code=404, detail="报修记录不存在")
    result = (body.result or "").strip().lower()
    if result not in {"approve", "reject"}:
        raise HTTPException(status_code=400, detail="result 须为 approve 或 reject")
    row.review_status = _REVIEW_APPROVED if result == "approve" else _REVIEW_REJECTED
    row.reviewer = (body.reviewer_name or "").strip()[:64] or None
    row.review_remark = (body.remark or "").strip()[:255] or None
    row.reviewed_at = china_now_naive()
    await db.commit()
    return {"ok": True}


@router.put("/{repair_id}/status")
async def repair_status_update(repair_id: int, body: RepairStatusIn, db: AsyncSession = Depends(get_db)):
    row = await db.get(VehicleRepair, repair_id)
    if row is None:
        raise HTTPException(status_code=404, detail="报修记录不存在")
    status = (body.repair_status or "").strip()
    if status not in _REPAIR_STATUSES:
        raise HTTPException(status_code=400, detail="维修状态须为 待处理 / 处理中 / 已完成")
    row.repair_status = status
    row.completed_at = china_now_naive() if status == "已完成" else None
    await db.commit()
    return {"ok": True}


@router.delete("/{repair_id}")
async def repair_delete(repair_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.get(VehicleRepair, repair_id)
    if row is None:
        raise HTTPException(status_code=404, detail="报修记录不存在")
    receipts = (
        (await db.execute(select(VehicleRepairReceipt).where(VehicleRepairReceipt.repair_id == repair_id)))
        .scalars()
        .all()
    )
    for rc in receipts:
        await db.delete(rc)
    await db.delete(row)
    await db.commit()
    return {"ok": True}


@router.post("/{repair_id}/receipts")
async def repair_receipt_upload(
    repair_id: int,
    files: list[UploadFile] = File(...),
    uploader_name: str | None = Form(None),
    remark: str | None = Form(None),
    db: AsyncSession = Depends(get_db),
):
    row = await db.get(VehicleRepair, repair_id)
    if row is None:
        raise HTTPException(status_code=404, detail="报修记录不存在")
    if not files:
        raise HTTPException(status_code=400, detail="请选择至少一个文件")

    repair_dir = _repair_receipts_root() / str(repair_id)
    repair_dir.mkdir(parents=True, exist_ok=True)
    uname = (uploader_name or "").strip()[:64] or None
    rmk = (remark or "").strip()[:255] or None
    saved: list[dict] = []

    for uf in files:
        ext = device_fault_receipt_safe_ext(uf.filename)
        if not ext:
            raise HTTPException(status_code=400, detail=f"不支持的文件类型: {uf.filename}")
        orig = (uf.filename or "file").replace("\\", "/").split("/")[-1][:255]
        content = await uf.read()
        if len(content) > _RECEIPT_MAX_BYTES:
            raise HTTPException(status_code=400, detail=f"文件 {orig} 超过 10MB")
        stored = gen_receipt_stored_name(ext)
        (repair_dir / stored).write_bytes(content)
        rc = VehicleRepairReceipt(
            repair_id=repair_id,
            biz_no=row.biz_no,
            stored_name=stored,
            original_name=orig,
            file_size=len(content),
            mime_type=uf.content_type,
            uploader_name=uname,
            remark=rmk,
        )
        db.add(rc)
        await db.flush()
        saved.append(_receipt_to_dict(rc, row))
    await db.commit()
    return {"ok": True, "saved": saved}

"""设备报修：录入、列表、审核、完成与单据上传。

对齐报障三段流：录入(待审核) → 审核通过/驳回 → 上传单据 → 已完成。
"""
from __future__ import annotations

import secrets
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.device_fault_service import (
    device_fault_receipt_safe_ext,
    gen_receipt_stored_name,
    guess_image_media_type,
    manual_fault_image_safe_ext,
)
from app.models import OrgCompany, Vehicle, VehicleRepair, VehicleRepairImage, VehicleRepairReceipt
from app.org_scope import collect_org_company_subtree_ids, require_x_org_id_header
from app.plate_util import norm_plate
from app.timeutil import china_now_naive

router = APIRouter(prefix="/api/repair", tags=["repair"])

_RECEIPT_MAX_BYTES = 10 * 1024 * 1024
_IMAGE_MAX_BYTES = 10 * 1024 * 1024
_IMAGE_MAX_COUNT = 12
_REVIEW_PENDING = "待审核"
_REVIEW_APPROVED = "审核通过"
_REVIEW_REJECTED = "驳回"
_REVIEW_REJECTED_LEGACY = "审核驳回"
_REVIEW_REJECTED_ALL = frozenset({_REVIEW_REJECTED, _REVIEW_REJECTED_LEGACY})
_REPAIR_STATUSES = frozenset({"待处理", "处理中", "已完成"})
_REPAIR_COMPLETED = "已完成"


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


def _repair_images_root() -> Path:
    d = Path(__file__).resolve().parent.parent.parent / "data" / "repair_images"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _normalize_review_status(raw: str | None) -> str:
    st = (raw or "").strip()
    if st == _REVIEW_REJECTED_LEGACY:
        return _REVIEW_REJECTED
    return st


def _image_view_url(image_id: int) -> str:
    return f"/cmapi/repair/images/{int(image_id)}/view"


def _image_to_dict(row: VehicleRepairImage) -> dict:
    return {
        "id": row.id,
        "repair_id": row.repair_id,
        "biz_no": row.biz_no,
        "name": row.original_name,
        "original_name": row.original_name,
        "file_size": row.file_size,
        "mime_type": row.mime_type,
        "uploader_name": row.uploader_name,
        "url": _image_view_url(int(row.id)),
        "created_at": _fmt_dt(row.created_at),
    }


def _resolve_image_path(repair_id: int, stored_name: str) -> Path | None:
    sn = (stored_name or "").strip()
    if not sn or "/" in sn or "\\" in sn or ".." in sn:
        return None
    root = _repair_images_root().resolve()
    p = (root / str(int(repair_id)) / sn).resolve()
    try:
        p.relative_to(root)
    except ValueError:
        return None
    return p if p.is_file() else None


async def _count_receipts(db: AsyncSession, repair_id: int) -> int:
    return int(
        await db.scalar(
            select(func.count()).select_from(VehicleRepairReceipt).where(VehicleRepairReceipt.repair_id == int(repair_id))
        )
        or 0
    )


async def _list_images(db: AsyncSession, repair_id: int) -> list[dict]:
    rows = (
        await db.execute(
            select(VehicleRepairImage)
            .where(VehicleRepairImage.repair_id == int(repair_id))
            .order_by(VehicleRepairImage.created_at.asc(), VehicleRepairImage.id.asc())
        )
    ).scalars().all()
    return [_image_to_dict(r) for r in rows]


async def _list_images_by_ids(db: AsyncSession, repair_ids: list[int]) -> dict[int, list[dict]]:
    ids = [int(x) for x in repair_ids if x is not None]
    if not ids:
        return {}
    rows = (
        await db.execute(
            select(VehicleRepairImage)
            .where(VehicleRepairImage.repair_id.in_(ids))
            .order_by(VehicleRepairImage.created_at.asc(), VehicleRepairImage.id.asc())
        )
    ).scalars().all()
    out: dict[int, list[dict]] = {}
    for r in rows:
        out.setdefault(int(r.repair_id), []).append(_image_to_dict(r))
    return out


async def _receipt_counts_by_ids(db: AsyncSession, repair_ids: list[int]) -> dict[int, int]:
    ids = [int(x) for x in repair_ids if x is not None]
    if not ids:
        return {}
    rows = (
        await db.execute(
            select(VehicleRepairReceipt.repair_id, func.count())
            .where(VehicleRepairReceipt.repair_id.in_(ids))
            .group_by(VehicleRepairReceipt.repair_id)
        )
    ).all()
    return {int(rid): int(cnt or 0) for rid, cnt in rows}


async def _company_names_by_ids(db: AsyncSession, company_ids: list[int]) -> dict[int, str]:
    ids = [int(x) for x in company_ids if x is not None]
    if not ids:
        return {}
    rows = (await db.execute(select(OrgCompany.id, OrgCompany.name).where(OrgCompany.id.in_(ids)))).all()
    return {int(i): (n or "").strip() for i, n in rows if n}


def _repair_to_dict(
    row: VehicleRepair,
    *,
    receipt_count: int = 0,
    images: list[dict] | None = None,
    company_name: str | None = None,
) -> dict:
    count = max(0, int(receipt_count or 0))
    imgs = images if images is not None else []
    return {
        "id": row.id,
        "biz_no": row.biz_no,
        "plate_no": row.plate_no,
        "vehicle_id": row.vehicle_id,
        "company_id": row.company_id,
        "company_name": company_name or None,
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
        "review_status": _normalize_review_status(row.review_status),
        "reviewer": row.reviewer,
        "review_remark": row.review_remark,
        "reviewed_at": _fmt_dt(row.reviewed_at),
        "repair_status": row.repair_status,
        "completed_at": _fmt_dt(row.completed_at),
        "created_at": _fmt_dt(row.created_at),
        "receipt_count": count,
        "has_receipt": count > 0,
        "image_count": len(imgs),
        "images": imgs,
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


def _is_receipt_eligible(row: VehicleRepair) -> bool:
    return (
        _normalize_review_status(row.review_status) == _REVIEW_APPROVED
        and (row.repair_status or "").strip() != _REPAIR_COMPLETED
    )


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
        v_pick = (
            await db.execute(select(Vehicle).where(Vehicle.id == int(body.vehicle_id)))
        ).scalar_one_or_none()
        # 前端车辆树常传 JT808 car_id；仅当本地 id 与车牌一致时采用
        if v_pick is not None and norm_plate(v_pick.plate_no) == plate:
            v = v_pick
    if v is None:
        v = (await db.execute(select(Vehicle).where(Vehicle.plate_no == plate))).scalar_one_or_none()
        if v is None:
            v = (
                await db.execute(select(Vehicle).where(func.upper(Vehicle.plate_no) == plate.upper()))
            ).scalar_one_or_none()
    if v is not None and v.company_id is not None and int(v.company_id) not in subtree:
        raise HTTPException(status_code=403, detail="该车辆不属于您所在公司及下级公司，无法报修")

    # 三段流：创建固定待处理，完结由上传单据触发
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
        repair_status="待处理",
        completed_at=None,
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
    receipt_eligible_only: bool = Query(False),
    archive_visible: bool = Query(False),
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

    rs = (review_status or "").strip()
    if rs in ("审核待办", "待办"):
        stmt = stmt.where(VehicleRepair.review_status == _REVIEW_PENDING)
    elif rs in ("查询可见", "档案可见"):
        # 档案页：完结（审核通过+已完成）+ 驳回
        stmt = stmt.where(
            or_(
                VehicleRepair.review_status.in_(list(_REVIEW_REJECTED_ALL)),
                and_(
                    VehicleRepair.review_status == _REVIEW_APPROVED,
                    VehicleRepair.repair_status == _REPAIR_COMPLETED,
                ),
            )
        )
    elif rs in ("完结", "已完结"):
        stmt = stmt.where(
            VehicleRepair.review_status == _REVIEW_APPROVED,
            VehicleRepair.repair_status == _REPAIR_COMPLETED,
        )
    elif rs in _REVIEW_REJECTED_ALL:
        stmt = stmt.where(VehicleRepair.review_status.in_(list(_REVIEW_REJECTED_ALL)))
    elif rs:
        stmt = stmt.where(VehicleRepair.review_status == rs)

    if repair_status and repair_status.strip():
        stmt = stmt.where(VehicleRepair.repair_status == repair_status.strip())

    # 单据上传列表：审核通过且未完结
    if approved_only or receipt_eligible_only:
        stmt = stmt.where(VehicleRepair.review_status == _REVIEW_APPROVED)
        if receipt_eligible_only:
            stmt = stmt.where(VehicleRepair.repair_status != _REPAIR_COMPLETED)

    # 档案列表开关（与 review_status=查询可见 等价，便于前端固定传参）
    if archive_visible and rs not in ("查询可见", "档案可见"):
        stmt = stmt.where(
            or_(
                VehicleRepair.review_status.in_(list(_REVIEW_REJECTED_ALL)),
                and_(
                    VehicleRepair.review_status == _REVIEW_APPROVED,
                    VehicleRepair.repair_status == _REPAIR_COMPLETED,
                ),
            )
        )

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
    ids = [int(r.id) for r in rows]
    receipt_map = await _receipt_counts_by_ids(db, ids)
    image_map = await _list_images_by_ids(db, ids)
    company_map = await _company_names_by_ids(
        db, [int(r.company_id) for r in rows if r.company_id is not None]
    )
    items = [
        _repair_to_dict(
            r,
            receipt_count=receipt_map.get(int(r.id), 0),
            images=image_map.get(int(r.id), []),
            company_name=company_map.get(int(r.company_id)) if r.company_id is not None else None,
        )
        for r in rows
    ]
    return {"ok": True, "items": items, "total": int(total)}


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


@router.get("/images/{image_id}/view")
async def repair_image_view(image_id: int, db: AsyncSession = Depends(get_db)):
    meta = await db.get(VehicleRepairImage, int(image_id))
    if meta is None:
        raise HTTPException(status_code=404, detail="图片不存在")
    p = _resolve_image_path(int(meta.repair_id), str(meta.stored_name or ""))
    if p is None:
        raise HTTPException(status_code=404, detail="图片文件不存在")
    name = str(meta.original_name or p.name)
    media = guess_image_media_type(name, meta.mime_type)
    return FileResponse(path=p, filename=name, media_type=media)


@router.get("/{repair_id}/images")
async def repair_list_images(repair_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.get(VehicleRepair, repair_id)
    if row is None:
        raise HTTPException(status_code=404, detail="报修记录不存在")
    items = await _list_images(db, repair_id)
    return {"ok": True, "items": items}


@router.post("/{repair_id}/images")
async def repair_upload_images(
    repair_id: int,
    files: list[UploadFile] = File(...),
    uploader_name: str | None = Form(None),
    db: AsyncSession = Depends(get_db),
):
    row = await db.get(VehicleRepair, repair_id)
    if row is None:
        raise HTTPException(status_code=404, detail="报修记录不存在")
    if not files:
        raise HTTPException(status_code=400, detail="请选择至少一张图片")
    existing = await _list_images(db, repair_id)
    if len(existing) + len(files) > _IMAGE_MAX_COUNT:
        raise HTTPException(status_code=400, detail=f"设备图片最多 {_IMAGE_MAX_COUNT} 张")

    repair_dir = _repair_images_root() / str(repair_id)
    repair_dir.mkdir(parents=True, exist_ok=True)
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
        if len(content) > _IMAGE_MAX_BYTES:
            raise HTTPException(status_code=400, detail=f"图片 {orig} 超过 10MB")
        stored = gen_receipt_stored_name(ext)
        (repair_dir / stored).write_bytes(content)
        mime = guess_image_media_type(orig, uf.content_type)
        img = VehicleRepairImage(
            repair_id=repair_id,
            biz_no=row.biz_no,
            stored_name=stored,
            original_name=orig,
            file_size=len(content),
            mime_type=mime,
            uploader_name=uname,
        )
        db.add(img)
        await db.flush()
        saved.append(_image_to_dict(img))

    await db.commit()
    return {"ok": True, "saved": saved, "items": saved}


@router.get("/{repair_id}")
async def repair_detail(repair_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.get(VehicleRepair, repair_id)
    if row is None:
        raise HTTPException(status_code=404, detail="报修记录不存在")
    receipt_count = await _count_receipts(db, repair_id)
    images = await _list_images(db, repair_id)
    company_name = None
    if row.company_id is not None:
        company_name = (
            await db.scalar(select(OrgCompany.name).where(OrgCompany.id == int(row.company_id)).limit(1))
        )
    return {
        "ok": True,
        "data": _repair_to_dict(
            row,
            receipt_count=receipt_count,
            images=images,
            company_name=(company_name or "").strip() or None,
        ),
    }


@router.put("/{repair_id}/review")
async def repair_review(repair_id: int, body: RepairReviewIn, db: AsyncSession = Depends(get_db)):
    row = await db.get(VehicleRepair, repair_id)
    if row is None:
        raise HTTPException(status_code=404, detail="报修记录不存在")
    if (row.review_status or "").strip() != _REVIEW_PENDING:
        raise HTTPException(status_code=400, detail="仅「待审核」记录可审核")
    result = (body.result or "").strip().lower()
    if result not in {"approve", "reject"}:
        raise HTTPException(status_code=400, detail="result 须为 approve 或 reject")
    remark = (body.remark or "").strip()
    if result == "reject" and not remark:
        raise HTTPException(status_code=400, detail="驳回须填写意见")
    row.review_status = _REVIEW_APPROVED if result == "approve" else _REVIEW_REJECTED
    row.reviewer = (body.reviewer_name or "").strip()[:64] or None
    row.review_remark = remark[:255] or None
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
    row.completed_at = china_now_naive() if status == _REPAIR_COMPLETED else None
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
    images = (
        (await db.execute(select(VehicleRepairImage).where(VehicleRepairImage.repair_id == repair_id)))
        .scalars()
        .all()
    )
    for img in images:
        await db.delete(img)
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
    if not _is_receipt_eligible(row):
        raise HTTPException(status_code=400, detail="仅审核通过且未完结的报修可上传单据")
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

    # 上传单据后自动完结（对齐报障「已完结」）
    row.repair_status = _REPAIR_COMPLETED
    row.completed_at = china_now_naive()
    await db.commit()
    return {"ok": True, "saved": saved}

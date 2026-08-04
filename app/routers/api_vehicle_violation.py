"""车辆报修 → 违章管理 专用接口。

与安全管理的 /api/violation 隔离：
- 仅处理 source=manual 的人工录入违章
- 列表不做「必须有图片/视频证据」过滤
- 状态机：录入→待审核 →(通过)→待处理 →完结/申诉
"""
from __future__ import annotations

import json
import logging
import secrets
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.alarm_type_gate import load_alarm_type_risk_map
from app.amap_regeo import resolve_address_wgs84
from app.database import get_db
from app.jt808_violation_sync import lookup_company_name, notify_violation_created
from app.models import OrgCompany, Vehicle, VehicleDevice, VehicleLocation, VehicleViolation, ViolationTypeDict
from app.org_scope import collect_org_company_subtree_ids, require_x_org_id_header
from app.plate_util import norm_plate
from app.routers.api_violation import _row_out_enriched
from app.timeutil import china_now_naive
from app.vehicle_alloc_scope import parse_user_id_header, resolve_allowed_plate_nos
from app.violation_manual_ocr import run_violation_manual_ocr
from app.violation_risk import resolve_risk_level

router = APIRouter(prefix="/api/vehicle-violation", tags=["vehicle-violation"])
logger = logging.getLogger(__name__)

SOURCE_MANUAL = "manual"
KIND_MANUAL_ENTRY = "manual_entry"
_SNAPSHOT_ROOT = Path(__file__).resolve().parents[2] / "data" / "violation_snapshots"
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp"}
_MAX_OCR_IMAGE_BYTES = 10 * 1024 * 1024


def _json_loads(raw: str | None, default):
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _safe_image_ext(filename: str | None, content_type: str | None) -> str:
    name = (filename or "").replace("\\", "/").split("/")[-1]
    suffix = Path(name).suffix.lower()
    if suffix in _IMAGE_EXTS:
        return ".jpg" if suffix == ".jpeg" else suffix
    ct = (content_type or "").lower()
    if "png" in ct:
        return ".png"
    if "webp" in ct:
        return ".webp"
    if "bmp" in ct:
        return ".bmp"
    if "gif" in ct:
        return ".gif"
    return ".jpg"


async def _attach_manual_ocr_image(
    db: AsyncSession,
    row: VehicleViolation,
    *,
    content: bytes,
    filename: str | None,
    content_type: str | None,
) -> dict:
    if not content:
        raise HTTPException(status_code=400, detail="图片内容为空")
    if len(content) > _MAX_OCR_IMAGE_BYTES:
        raise HTTPException(status_code=400, detail="图片不能超过 10MB")
    ext = _safe_image_ext(filename, content_type)
    rel = f"manual/{int(row.id)}_{secrets.token_hex(8)}{ext}"
    dest = _SNAPSHOT_ROOT / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(content)

    # 浏览器经 /cmmedia → 后端 /media
    browser_url = f"/cmmedia/violation-snapshots/{rel}"
    display_name = ((filename or "OCR单据").replace("\\", "/").split("/")[-1] or "OCR单据")[:120]

    evidence = _json_loads(row.ttx_evidence_refs, {})
    if not isinstance(evidence, dict):
        evidence = {}
    images = evidence.get("images") if isinstance(evidence.get("images"), list) else []
    images.append({"url": browser_url, "name": display_name, "source": "manual_ocr"})
    evidence["images"] = images
    row.ttx_evidence_refs = json.dumps(evidence, ensure_ascii=False)

    snaps = _json_loads(row.stream_snapshot_refs, [])
    if not isinstance(snaps, list):
        snaps = []
    if rel not in snaps:
        snaps.append(rel)
    row.stream_snapshot_refs = json.dumps(snaps, ensure_ascii=False)

    await db.commit()
    await db.refresh(row)
    return {"url": browser_url, "name": display_name, "path": rel}


class ManualCreateIn(BaseModel):
    plate_no: str = Field(..., min_length=1, max_length=16)
    violation_type_dict_id: int | None = Field(None, ge=1)
    violation_type_name: str | None = Field(None, max_length=64)
    violation_time: datetime | None = None
    address: str | None = Field(None, max_length=500)
    terminal_id: str | None = Field(None, max_length=32)
    vehicle_id: int | None = Field(None, ge=1)
    remark: str | None = Field(None, max_length=2000)


class AuditIn(BaseModel):
    result: str = Field(..., max_length=32)
    remark: str | None = Field(None, max_length=2000)
    auditor_name: str | None = Field(None, max_length=64)


class HandleIn(BaseModel):
    action: str = Field(..., max_length=32)
    remark: str | None = Field(None, max_length=2000)
    handler_name: str | None = Field(None, max_length=64)


def _gen_biz_no() -> str:
    return f"WZ{china_now_naive().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(3).upper()}"


def _now() -> datetime:
    return china_now_naive()


async def _read_main_terminal_id(db: AsyncSession, vehicle_id: int) -> str:
    row = await db.scalar(
        select(VehicleDevice)
        .where(VehicleDevice.vehicle_id == int(vehicle_id))
        .order_by(VehicleDevice.id.asc())
        .limit(1)
    )
    return (row.device_no or "").strip()[:32] if row else ""


async def _scoped_manual_query(
    db: AsyncSession,
    *,
    x_org_id: str | None,
    x_user_id: str | None,
):
    """仅人工录入；不做安全管理那套媒体证据过滤。"""
    q = select(VehicleViolation).where(VehicleViolation.source == SOURCE_MANUAL)
    if x_org_id:
        root = require_x_org_id_header(x_org_id)
        exists = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
        if exists:
            subtree = await collect_org_company_subtree_ids(db, root)
            q = q.where(or_(VehicleViolation.company_id.in_(subtree), VehicleViolation.company_id.is_(None)))
    allowed_plates = await resolve_allowed_plate_nos(db, parse_user_id_header(x_user_id))
    if allowed_plates is not None:
        if not allowed_plates:
            q = q.where(VehicleViolation.id < 0)
        else:
            q = q.where(VehicleViolation.plate_no.in_(sorted(allowed_plates)))
    return q


async def _get_manual_or_404(db: AsyncSession, violation_id: int) -> VehicleViolation:
    row = await db.get(VehicleViolation, int(violation_id))
    if row is None or (row.source or "").strip() != SOURCE_MANUAL:
        raise HTTPException(status_code=404, detail="违章记录不存在")
    return row


@router.get("/list")
async def list_manual_violations(
    status: str | None = Query(None),
    plate_no: str | None = Query(None),
    biz_no: str | None = Query(None),
    appeal_status: str | None = Query(None),
    violation_type_dict_id: int | None = Query(None, ge=1),
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    q = await _scoped_manual_query(db, x_org_id=x_org_id, x_user_id=x_user_id)
    if status:
        # 查询页语义：完结/驳回最终都落在 status=已处理，靠 audit_reject_remark 区分
        st = status.strip()
        reject_nonempty = and_(
            VehicleViolation.audit_reject_remark.isnot(None),
            func.trim(VehicleViolation.audit_reject_remark) != "",
        )
        reject_empty = or_(
            VehicleViolation.audit_reject_remark.is_(None),
            func.trim(VehicleViolation.audit_reject_remark) == "",
        )
        if st in ("查询可见", "已审核"):
            q = q.where(VehicleViolation.status == "已处理")
        elif st in ("完结", "已完结"):
            q = q.where(VehicleViolation.status == "已处理", reject_empty)
        elif st in ("驳回", "审核驳回"):
            q = q.where(VehicleViolation.status == "已处理", reject_nonempty)
        else:
            q = q.where(VehicleViolation.status == st)
    if plate_no:
        plates = [p.strip() for p in str(plate_no).replace("，", ",").split(",") if p.strip()]
        if len(plates) > 1:
            q = q.where(VehicleViolation.plate_no.in_(plates))
        elif plates:
            q = q.where(VehicleViolation.plate_no.ilike(f"%{plates[0]}%"))
    if biz_no:
        q = q.where(VehicleViolation.biz_no.ilike(f"%{biz_no.strip()}%"))
    if appeal_status:
        q = q.where(VehicleViolation.appeal_status == appeal_status.strip())
    if violation_type_dict_id is not None:
        vt_row = await db.get(ViolationTypeDict, int(violation_type_dict_id))
        if vt_row is not None and (vt_row.type_name or "").strip():
            q = q.where(VehicleViolation.violation_type_name == (vt_row.type_name or "").strip())
    if start_time:
        try:
            q = q.where(VehicleViolation.violation_time >= datetime.fromisoformat(start_time.replace("T", " ")))
        except ValueError:
            pass
    if end_time:
        try:
            q = q.where(VehicleViolation.violation_time <= datetime.fromisoformat(end_time.replace("T", " ")))
        except ValueError:
            pass

    total = await db.scalar(select(func.count()).select_from(q.subquery())) or 0
    rows = (
        await db.execute(
            q.order_by(VehicleViolation.violation_time.desc(), VehicleViolation.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).scalars().all()
    items = []
    for row in rows:
        items.append(await _row_out_enriched(db, row))
    return {"ok": True, "total": int(total), "items": items, "page": page, "page_size": page_size}


@router.post("/manual/ocr")
async def manual_ocr(
    file: UploadFile = File(...),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    content = await file.read()
    return await run_violation_manual_ocr(
        db,
        filename=file.filename or "upload.jpg",
        content=content,
        content_type=file.content_type,
        x_user_id=x_user_id,
    )


@router.post("/manual")
async def manual_create(
    body: ManualCreateIn,
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
        raise HTTPException(status_code=400, detail="车牌不能为空")

    v: Vehicle | None = None
    if body.vehicle_id is not None:
        v_pick = await db.scalar(select(Vehicle).where(Vehicle.id == int(body.vehicle_id)).limit(1))
        if v_pick is not None and norm_plate(v_pick.plate_no) == plate:
            v = v_pick
    if v is None:
        v = await db.scalar(select(Vehicle).where(Vehicle.plate_no == plate).limit(1))
        if v is None:
            v = await db.scalar(
                select(Vehicle).where(func.upper(Vehicle.plate_no) == plate.upper()).limit(1)
            )

    if v is not None and v.company_id is not None and int(v.company_id) not in subtree:
        raise HTTPException(status_code=403, detail="该车辆不属于您所在公司及下级公司，无法录入")

    vehicle_id = int(v.id) if v else None
    company_id = int(v.company_id) if v is not None and v.company_id is not None else root

    if body.violation_type_dict_id is not None:
        vt_row = await db.get(ViolationTypeDict, int(body.violation_type_dict_id))
        if vt_row is None:
            raise HTTPException(status_code=400, detail="所选违章类型不存在")
        vtype_name = (vt_row.type_name or "").strip()[:64]
    else:
        vtype_name = (body.violation_type_name or "").strip()[:64]
    if not vtype_name:
        raise HTTPException(status_code=400, detail="未识别到违章类型，请上传清晰违章单据图片")

    tid = (body.terminal_id or "").strip()[:32]
    if not tid and v:
        tid = await _read_main_terminal_id(db, int(v.id))

    vt = body.violation_time or china_now_naive()
    lat_out: float | None = None
    lng_out: float | None = None
    addr_out = (body.address or "").strip()[:500] or None
    if v is not None:
        loc_row = await db.scalar(
            select(VehicleLocation).where(VehicleLocation.vehicle_id == int(v.id)).limit(1)
        )
        if loc_row is not None:
            if loc_row.lat is not None and loc_row.lng is not None:
                lat_out, lng_out = float(loc_row.lat), float(loc_row.lng)
            if not addr_out and (loc_row.current_position or "").strip():
                addr_out = str(loc_row.current_position).strip()[:500]
            if not addr_out and lat_out is not None and lng_out is not None:
                addr_out = await resolve_address_wgs84(db, lat_out, lng_out) or None

    risk_map = await load_alarm_type_risk_map(db)
    row = VehicleViolation(
        biz_no=_gen_biz_no(),
        terminal_id=tid or "",
        vehicle_id=vehicle_id,
        plate_no=plate,
        company_id=company_id,
        company_name=await lookup_company_name(db, company_id),
        violation_type_code=None,
        violation_type_name=vtype_name,
        risk_level=resolve_risk_level(type_name=vtype_name, risk_map=risk_map),
        violation_time=vt,
        lat=lat_out,
        lng=lng_out,
        address=addr_out,
        source=SOURCE_MANUAL,
        transparent_type=None,
        raw_preview=(body.remark or "").strip() or None,
        status="待审核",
        pre_audit_kind=KIND_MANUAL_ENTRY,
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    try:
        await notify_violation_created(db, row)
    except Exception as exc:
        logger.warning("notify_violation_created failed: %s", exc)
    return {"ok": True, "id": row.id, "biz_no": row.biz_no}


@router.get("/{violation_id}")
async def detail(violation_id: int, db: AsyncSession = Depends(get_db)):
    row = await _get_manual_or_404(db, violation_id)
    return {"ok": True, "data": await _row_out_enriched(db, row)}


@router.post("/{violation_id}/images")
async def upload_manual_images(
    violation_id: int,
    files: list[UploadFile] = File(...),
    db: AsyncSession = Depends(get_db),
):
    """保存录入时的 OCR 单据图片，供审核/查看弹窗展示。"""
    row = await _get_manual_or_404(db, violation_id)
    if not files:
        raise HTTPException(status_code=400, detail="请上传图片")
    saved = []
    for uf in files[:3]:
        content = await uf.read()
        item = await _attach_manual_ocr_image(
            db,
            row,
            content=content,
            filename=uf.filename,
            content_type=uf.content_type,
        )
        saved.append(item)
        await db.refresh(row)
    return {
        "ok": True,
        "saved": saved,
        "data": await _row_out_enriched(db, row),
    }


@router.patch("/{violation_id}/audit")
async def audit(violation_id: int, body: AuditIn, db: AsyncSession = Depends(get_db)):
    """待审核：通过 → 待处理；驳回 → 已处理。"""
    row = await _get_manual_or_404(db, violation_id)
    if (row.status or "").strip() != "待审核":
        raise HTTPException(status_code=400, detail="仅「待审核」记录可审核")
    result = (body.result or "").strip().lower()
    auditor = (body.auditor_name or "系统用户").strip()[:64] or "系统用户"
    if result in ("approve", "approved", "同意", "通过", "agree"):
        row.status = "待处理"
        row.pre_audit_kind = None
        row.auditor_name = auditor
        row.audited_at = _now()
        row.audit_reject_remark = None
    elif result in ("reject", "rejected", "驳回"):
        rr = (body.remark or "").strip()
        if not rr:
            raise HTTPException(status_code=400, detail="驳回须填写意见")
        row.status = "已处理"
        row.pre_audit_kind = None
        row.auditor_name = auditor
        row.audited_at = _now()
        row.audit_reject_remark = rr[:500]
    else:
        raise HTTPException(status_code=400, detail="result 须为 approve 或 reject")
    await db.commit()
    await db.refresh(row)
    return {"ok": True, "data": await _row_out_enriched(db, row)}


@router.patch("/{violation_id}/handle")
async def handle(violation_id: int, body: HandleIn, db: AsyncSession = Depends(get_db)):
    """待处理：完结 → 已处理；申诉 → 已处理+申诉中。"""
    row = await _get_manual_or_404(db, violation_id)
    if (row.status or "").strip() != "待处理":
        raise HTTPException(status_code=400, detail="仅「待处理」记录可操作完结/申诉")
    action = (body.action or "").strip()
    row.handler_name = (body.handler_name or "系统用户").strip()[:64] or "系统用户"
    row.handled_at = _now()
    row.handler_remark = (body.remark or "").strip()[:2000] or None
    if action in ("complete", "finish", "完结", "done"):
        row.status = "已处理"
        row.pre_audit_kind = None
    elif action in ("appeal", "申诉"):
        rm = (body.remark or "").strip()
        if not rm:
            raise HTTPException(status_code=400, detail="申诉须填写说明")
        row.status = "已处理"
        row.pre_audit_kind = None
        row.appeal_status = "申诉中"
        row.appeal_reason = rm[:2000]
        row.appeal_submitted_at = _now()
    else:
        raise HTTPException(status_code=400, detail="请选择「完结」或「申诉」")
    await db.commit()
    await db.refresh(row)
    return {"ok": True, "data": await _row_out_enriched(db, row)}

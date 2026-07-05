"""主动安全/违章报警兼容接口。

为旧版 carManagerV11 安全管理页面提供最小可用的列表、处理、审核和状态流转能力。
"""
from __future__ import annotations

import json
import logging
import secrets
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from starlette.datastructures import UploadFile as StarletteUploadFile
from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.jt808_follow import expand_terminal_id_variants, fetch_followed_device_ids
from app.media_url import normalize_evidence_payload
from app.models import Driver, Fleet, OrgCompany, SysUser, Vehicle, VehicleDevice, VehicleLocation, VehicleViolation, ViolationTicket, ViolationTypeDict
from app.org_scope import collect_org_company_subtree_ids, require_x_org_id_header
from app.plate_util import norm_plate
from app.routers.api_vehicle import _vehicle_list_company_fleet_names
from app.timeutil import china_now_naive
from app.violation_ai_assessment import (
    get_violation_ai_assessment,
    run_violation_ai_assessment,
    stream_violation_ai_assessment,
)
from app.violation_filters import violation_list_visibility

router = APIRouter(prefix="/api/violation", tags=["violation"])
logger = logging.getLogger(__name__)


class ViolationHandleIn(BaseModel):
    action: str = Field("confirm", max_length=32)
    remark: str | None = Field(None, max_length=2000)
    handler_name: str | None = Field(None, max_length=64)


class ViolationAuditIn(BaseModel):
    result: str = Field(..., max_length=32)
    remark: str | None = Field(None, max_length=2000)
    auditor_name: str | None = Field(None, max_length=64)


class ViolationManualIn(BaseModel):
    plate_no: str = Field(..., min_length=1, max_length=16)
    violation_type_dict_id: int | None = Field(None, ge=1)
    violation_type_name: str | None = Field(None, max_length=64)
    violation_time: datetime | None = None
    address: str | None = Field(None, max_length=500)
    terminal_id: str | None = Field(None, max_length=32)
    vehicle_id: int | None = Field(None, ge=1)
    remark: str | None = Field(None, max_length=2000)


def _gen_biz_no() -> str:
    return f"WZ{china_now_naive().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(3).upper()}"


async def _read_main_terminal_id_for_vehicle(db: AsyncSession, vehicle_id: int) -> str:
    rd = await db.execute(
        select(VehicleDevice)
        .where(VehicleDevice.vehicle_id == int(vehicle_id))
        .order_by(VehicleDevice.is_main.desc(), VehicleDevice.id.asc())
    )
    for dev in rd.scalars().all():
        for attr in ("device_no", "device_sn", "sim_no", "actual_sim"):
            val = getattr(dev, attr, None)
            if val is not None and str(val).strip():
                return str(val).strip()[:32]
    return ""


class TicketProcessIn(BaseModel):
    remark: str | None = Field(None, max_length=2000)


class TicketAppealIn(BaseModel):
    remark: str | None = Field(None, max_length=2000)


_TICKET_APPEAL_ALLOWED_EXTS = {".xls", ".xlsx", ".doc", ".docx", ".pdf", ".jpg", ".jpeg", ".bmp", ".png", ".txt"}
_TICKET_APPEAL_MAX_FILE_BYTES = 20 * 1024 * 1024
_TICKET_APPEAL_FORM_FILE_KEYS = ("files", "file", "attachments")


def _ext_from_content_type(content_type: str | None) -> str:
    ct = (content_type or "").lower().split(";", 1)[0].strip()
    return {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/bmp": ".bmp",
        "application/pdf": ".pdf",
        "text/plain": ".txt",
        "application/msword": ".doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    }.get(ct, "")


def _collect_ticket_appeal_upload_files(form) -> list[StarletteUploadFile]:
    found: list[StarletteUploadFile] = []
    seen: set[int] = set()
    for key, value in form.multi_items():
        if key not in _TICKET_APPEAL_FORM_FILE_KEYS or not isinstance(value, StarletteUploadFile):
            continue
        obj_id = id(value)
        if obj_id in seen:
            continue
        seen.add(obj_id)
        found.append(value)
    return found


def _resolve_ticket_appeal_filename(file: StarletteUploadFile) -> str:
    original = (file.filename or "").strip().replace("\\", "/").split("/")[-1]
    if original:
        return original[:255]
    ext = _ext_from_content_type(file.content_type) or ".bin"
    return f"attachment_{uuid.uuid4().hex[:8]}{ext}"


def _ticket_appeal_attachment_base_dir() -> Path:
    return Path(__file__).resolve().parent.parent.parent / "data" / "ticket_appeal_attachments"


def _now() -> datetime:
    return datetime.now()


def _json_loads(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except Exception:
        return fallback


async def _save_ticket_appeal_upload(violation_id: int, file: StarletteUploadFile) -> dict[str, Any]:
    original = _resolve_ticket_appeal_filename(file)
    ext = Path(original).suffix.lower()
    if ext not in _TICKET_APPEAL_ALLOWED_EXTS:
        raise HTTPException(status_code=400, detail="申诉附件仅支持 EXCEL、WORD、PDF、JPG、BMP、PNG、TXT")

    chunks: list[bytes] = []
    total = 0
    while True:
        piece = await file.read(1024 * 1024)
        if not piece:
            break
        total += len(piece)
        if total > _TICKET_APPEAL_MAX_FILE_BYTES:
            raise HTTPException(status_code=400, detail=f"附件 {original} 超过 20MB")
        chunks.append(piece)
    if total <= 0:
        raise HTTPException(status_code=400, detail=f"附件 {original} 为空")

    root = _ticket_appeal_attachment_base_dir()
    sub = root / str(int(violation_id))
    sub.mkdir(parents=True, exist_ok=True)
    stored = f"{uuid.uuid4().hex}{ext}"
    dest = sub / stored
    dest.write_bytes(b"".join(chunks))
    rel = f"{int(violation_id)}/{stored}"
    return {
        "name": original[:255],
        "size": total,
        "rel": rel,
        "url": f"/cmmedia/ticket-appeal-attachments/{rel}",
    }


def _dt_text(v: datetime | None) -> str | None:
    return v.strftime("%Y-%m-%d %H:%M:%S") if v else None


async def _load_org_company_maps(db: AsyncSession) -> tuple[dict[int, str | None], dict[int, int | None], dict[int, str | None]]:
    company_map: dict[int, str | None] = {}
    parent_map: dict[int, int | None] = {}
    for cid, cname, pid in (await db.execute(select(OrgCompany.id, OrgCompany.name, OrgCompany.parent_id))).all():
        company_map[cid] = cname
        parent_map[cid] = pid
    fleet_map: dict[int, str | None] = {}
    for fid, fname in (await db.execute(select(Fleet.id, Fleet.name))).all():
        fleet_map[fid] = fname
    return company_map, parent_map, fleet_map


def _apply_vehicle_display_fields(
    out: dict[str, Any],
    *,
    vehicle: Vehicle | None,
    company_id: int | None,
    company_map: dict[int, str | None],
    parent_map: dict[int, int | None],
    fleet_map: dict[int, str | None],
    drivers: dict[int, Driver],
) -> dict[str, Any]:
    """与车辆列表一致：所属公司展示上级真实公司名，而非叶子机构编号。"""
    if vehicle is not None:
        display_company, display_fleet = _vehicle_list_company_fleet_names(
            vehicle.company_id, vehicle.fleet_id, company_map, parent_map, fleet_map
        )
        out["company_name"] = display_company or "—"
        out["fleet_name"] = display_fleet or ""
        out["vehicle_type"] = vehicle.vehicle_type or ""
        out["resolved_vehicle_id"] = vehicle.id
        out["driver_id"] = vehicle.driver_id
        driver_name = (vehicle.driver_name or "").strip()
        if not driver_name and vehicle.driver_id and vehicle.driver_id in drivers:
            driver_name = (drivers[vehicle.driver_id].name or "").strip()
        out["driver_name"] = driver_name
        return out

    display_company, display_fleet = _vehicle_list_company_fleet_names(
        company_id, None, company_map, parent_map, fleet_map
    )
    out["company_name"] = display_company or "—"
    out["fleet_name"] = display_fleet or ""
    out["vehicle_type"] = ""
    out["driver_name"] = ""
    return out


async def _rows_out(
    db: AsyncSession,
    rows: list[VehicleViolation],
    ticket_by_biz: dict[str, ViolationTicket] | None = None,
) -> list[dict[str, Any]]:
    ticket_by_biz = ticket_by_biz or {}
    vehicle_ids = [int(x.vehicle_id) for x in rows if x.vehicle_id]
    vehicles: dict[int, Vehicle] = {}
    if vehicle_ids:
        for vehicle in (await db.execute(select(Vehicle).where(Vehicle.id.in_(vehicle_ids)))).scalars().all():
            vehicles[int(vehicle.id)] = vehicle

    driver_ids = [int(v.driver_id) for v in vehicles.values() if v.driver_id]
    drivers: dict[int, Driver] = {}
    if driver_ids:
        for driver in (await db.execute(select(Driver).where(Driver.id.in_(driver_ids)))).scalars().all():
            drivers[int(driver.id)] = driver

    company_map, parent_map, fleet_map = await _load_org_company_maps(db)
    items: list[dict[str, Any]] = []
    for row in rows:
        out = _row_out(row, ticket_by_biz)
        vehicle = vehicles.get(int(row.vehicle_id)) if row.vehicle_id else None
        company_id = vehicle.company_id if vehicle is not None else row.company_id
        _apply_vehicle_display_fields(
            out,
            vehicle=vehicle,
            company_id=company_id,
            company_map=company_map,
            parent_map=parent_map,
            fleet_map=fleet_map,
            drivers=drivers,
        )
        items.append(out)
    return items


async def _row_out_enriched(db: AsyncSession, row: VehicleViolation, ticket_by_biz: dict[str, ViolationTicket] | None = None) -> dict[str, Any]:
    items = await _rows_out(db, [row], ticket_by_biz)
    return items[0]


def _row_out(row: VehicleViolation, ticket_by_biz: dict[str, ViolationTicket] | None = None) -> dict:
    ticket_by_biz = ticket_by_biz or {}
    ticket = ticket_by_biz.get(row.biz_no or "")
    evidence = _json_loads(row.ttx_evidence_refs, {})
    evidence_norm = normalize_evidence_payload(evidence) if isinstance(evidence, dict) else {"images": [], "videos": []}
    return {
        "id": row.id,
        "biz_no": row.biz_no,
        "external_alarm_id": row.external_alarm_id,
        "terminal_id": row.terminal_id,
        "vehicle_id": row.vehicle_id,
        "plate_no": row.plate_no,
        "company_id": row.company_id,
        "violation_type_code": row.violation_type_code,
        "violation_type_name": row.violation_type_name,
        "violation_time": _dt_text(row.violation_time),
        "lat": row.lat,
        "lng": row.lng,
        "address": row.address,
        "source": row.source,
        "transparent_type": row.transparent_type,
        "raw_preview": row.raw_preview,
        "stream_snapshot_refs": _json_loads(row.stream_snapshot_refs, []),
        "stream_snapshot_paths": _json_loads(row.stream_snapshot_refs, []),
        "ttx_evidence_refs": _json_loads(row.ttx_evidence_refs, []),
        "evidence_images": evidence_norm.get("images", []),
        "evidence_videos": evidence_norm.get("videos", []),
        "status": row.status,
        "pre_audit_kind": row.pre_audit_kind,
        "ticket_appeal_remark": row.ticket_appeal_remark,
        "ticket_appeal_attachments": _json_loads(row.ticket_appeal_attachment_refs, []),
        "handler_remark": row.handler_remark,
        "handler_name": row.handler_name,
        "handled_at": _dt_text(row.handled_at),
        "auditor_name": row.auditor_name,
        "audited_at": _dt_text(row.audited_at),
        "audit_reject_remark": row.audit_reject_remark,
        "appeal_reason": row.appeal_reason,
        "appeal_submitted_at": _dt_text(row.appeal_submitted_at),
        "appeal_status": row.appeal_status,
        "created_at": _dt_text(row.created_at),
        "has_ticket": ticket is not None,
        "has_violation_ticket": ticket is not None,
        "ticket_process_type": ticket.process_type if ticket else None,
        "ticket_amount": ticket.amount if ticket else None,
        "ticket_remark": ticket.remark if ticket else None,
        "ticket_status": ticket.status if ticket else None,
        "ticket_created_by_name": ticket.created_by_name if ticket else None,
        "ticket_created_at": _dt_text(ticket.created_at) if ticket else None,
        "ai_queried": 1 if bool(getattr(row, "ai_queried", False)) else 0,
        "source_label": {
            "jt808_adas": "JT808 ADAS",
            "jt808_dsm": "JT808 DSM",
            "manual": "人工录入",
        }.get((row.source or "").strip(), row.source or ""),
    }


async def _scoped_query(db: AsyncSession, x_org_id: str | None):
    q = select(VehicleViolation).where(violation_list_visibility())
    if x_org_id:
        root = require_x_org_id_header(x_org_id)
        exists = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
        if exists:
            subtree = await collect_org_company_subtree_ids(db, root)
            q = q.where(or_(VehicleViolation.company_id.in_(subtree), VehicleViolation.company_id.is_(None)))
    return q


def _resolve_follow_user_id(user_id: int | None, x_user_id: str | None) -> int | None:
    if user_id is not None:
        return int(user_id)
    raw = (x_user_id or "").strip()
    if raw.isdigit():
        return int(raw)
    return None


async def _apply_followed_only_filter(
    db: AsyncSession,
    q,
    *,
    followed_only: bool,
    user_id: int | None,
    x_user_id: str | None,
):
    if not followed_only:
        return q

    uid = _resolve_follow_user_id(user_id, x_user_id)
    if uid is None:
        return q.where(VehicleViolation.id == -1)

    user = await db.scalar(select(SysUser).where(SysUser.id == uid).limit(1))
    if user is None or not user.is_active:
        return q.where(VehicleViolation.id == -1)

    username = (user.username or "").strip()
    pwd_plain = (getattr(user, "password_plain", None) or "").strip()
    if not username or not pwd_plain:
        raise HTTPException(status_code=400, detail="当前用户未存储808登录凭据，无法筛选关注车辆，请重新登录")

    try:
        device_ids = await fetch_followed_device_ids(username, pwd_plain)
    except Exception as exc:  # noqa: BLE001
        logger.warning("fetch followed devices failed user_id=%s account=%s: %s", uid, username, exc)
        raise HTTPException(status_code=502, detail=f"获取关注车辆失败：{exc}") from exc

    if not device_ids:
        return q.where(VehicleViolation.id == -1)

    match_ids = expand_terminal_id_variants(device_ids)
    device_match = or_(
        VehicleDevice.device_no.in_(match_ids),
        VehicleDevice.device_sn.in_(match_ids),
        VehicleDevice.sim_no.in_(match_ids),
        VehicleDevice.actual_sim.in_(match_ids),
    )
    vehicle_ids_subq = select(VehicleDevice.vehicle_id).where(device_match).distinct()
    return q.where(
        or_(
            VehicleViolation.terminal_id.in_(match_ids),
            VehicleViolation.vehicle_id.in_(vehicle_ids_subq),
        )
    )


@router.get("/list")
async def violation_list(
    status: str | None = Query(None),
    plate_no: str | None = Query(None),
    biz_no: str | None = Query(None),
    terminal_id: str | None = Query(None),
    source: str | None = Query(None),
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    followed_only: bool = Query(False),
    user_id: int | None = Query(None, ge=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    limit: int | None = Query(None, ge=1, le=2000),
    offset: int | None = Query(None, ge=0),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    q = await _scoped_query(db, x_org_id)
    q = await _apply_followed_only_filter(
        db,
        q,
        followed_only=followed_only,
        user_id=user_id,
        x_user_id=x_user_id,
    )
    if status:
        if status == "pending":
            q = q.where(
                or_(
                    VehicleViolation.status == "待处理",
                    and_(VehicleViolation.status == "待审核", VehicleViolation.pre_audit_kind == "preprocess"),
                )
            )
        elif status == "handled":
            q = q.where(VehicleViolation.status == "已处理")
        else:
            q = q.where(VehicleViolation.status == status.strip())
    if plate_no:
        q = q.where(VehicleViolation.plate_no.ilike(f"%{plate_no.strip()}%"))
    if biz_no:
        q = q.where(VehicleViolation.biz_no.ilike(f"%{biz_no.strip()}%"))
    if terminal_id:
        q = q.where(VehicleViolation.terminal_id.ilike(f"%{terminal_id.strip()}%"))
    if source:
        q = q.where(VehicleViolation.source == source.strip())
    if start_time:
        try:
            q = q.where(VehicleViolation.violation_time >= datetime.fromisoformat(start_time.replace("/", "-")))
        except ValueError:
            pass
    if end_time:
        try:
            q = q.where(VehicleViolation.violation_time <= datetime.fromisoformat(end_time.replace("/", "-")))
        except ValueError:
            pass

    total = await db.scalar(select(func.count()).select_from(q.subquery())) or 0
    lim = limit or page_size
    off = offset if offset is not None else (page - 1) * page_size
    rows = (await db.execute(q.order_by(VehicleViolation.violation_time.desc(), VehicleViolation.id.desc()).offset(off).limit(lim))).scalars().all()
    biz = [x.biz_no for x in rows if x.biz_no]
    ticket_by_biz: dict[str, ViolationTicket] = {}
    if biz:
        for ticket in (await db.execute(select(ViolationTicket).where(ViolationTicket.biz_no.in_(biz)))).scalars().all():
            if ticket.biz_no:
                ticket_by_biz[ticket.biz_no] = ticket
    return {
        "ok": True,
        "total": total,
        "items": await _rows_out(db, list(rows), ticket_by_biz),
        "page": page,
        "page_size": page_size,
    }


@router.post("/manual")
async def violation_manual(
    body: ViolationManualIn,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    """人工录入违章（车辆管理-手动违章录入）。"""
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
        r_id = await db.execute(select(Vehicle).where(Vehicle.id == int(body.vehicle_id)))
        v_pick = r_id.scalar_one_or_none()
        if v_pick is None:
            raise HTTPException(status_code=400, detail="所选车辆不存在")
        if norm_plate(v_pick.plate_no) != plate:
            raise HTTPException(status_code=400, detail="所选车辆与车牌不一致")
        v = v_pick
    else:
        vr = await db.execute(select(Vehicle).where(Vehicle.plate_no == plate))
        v = vr.scalar_one_or_none()

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
        raise HTTPException(status_code=400, detail="请选择违章类型")

    tid = (body.terminal_id or "").strip()[:32]
    if not tid and v:
        tid = await _read_main_terminal_id_for_vehicle(db, int(v.id))

    vt = body.violation_time or china_now_naive()
    lat_out: float | None = None
    lng_out: float | None = None
    addr_out = (body.address or "").strip()[:500] or None
    if v is not None:
        lr = await db.execute(select(VehicleLocation).where(VehicleLocation.vehicle_id == int(v.id)))
        loc_row = lr.scalar_one_or_none()
        if loc_row is not None:
            if loc_row.lat is not None and loc_row.lng is not None:
                lat_out, lng_out = float(loc_row.lat), float(loc_row.lng)
            if not addr_out and (loc_row.current_position or "").strip():
                addr_out = str(loc_row.current_position).strip()[:500]

    row = VehicleViolation(
        biz_no=_gen_biz_no(),
        terminal_id=tid or "",
        vehicle_id=vehicle_id,
        plate_no=plate,
        company_id=company_id,
        violation_type_code=None,
        violation_type_name=vtype_name,
        violation_time=vt,
        lat=lat_out,
        lng=lng_out,
        address=addr_out,
        source="manual",
        transparent_type=None,
        raw_preview=(body.remark or "").strip() or None,
        status="待处理",
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return {"ok": True, "id": row.id, "biz_no": row.biz_no}


@router.get("/{violation_id}/detail")
async def violation_detail(violation_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": await _row_out_enriched(db, row)}


@router.post("/{violation_id}/fetch-device-media")
async def violation_fetch_device_media(violation_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    evidence = normalize_evidence_payload(_json_loads(row.ttx_evidence_refs, {}))
    return {
        "ok": True,
        "message": "已读取本地同步的 JT808 证据",
        "images": evidence.get("images", []),
        "videos": evidence.get("videos", []),
        "downlink": [],
    }


@router.patch("/{violation_id}/handle")
async def violation_handle(violation_id: int, body: ViolationHandleIn, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    action = (body.action or "confirm").strip()
    row.handler_remark = body.remark
    row.handler_name = body.handler_name or "系统用户"
    row.handled_at = _now()
    if action in ("false_alarm", "false", "误报"):
        row.status = "误报"
        row.pre_audit_kind = "false_alarm"
    else:
        row.status = "待审核"
        row.pre_audit_kind = "preprocess"
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": await _row_out_enriched(db, row)}


@router.patch("/{violation_id}/audit")
async def violation_audit(violation_id: int, body: ViolationAuditIn, db: AsyncSession = Depends(get_db)):
    """待审核 → 已处理 / 罚单待处理（生成罚单路径）/ 待处理（打回）。

    依据 ``pre_audit_kind`` 分流：
    - approve + ``ticket``        → 罚单待处理（进入罚单处理页）
    - approve + ``ticket_appeal`` → 已处理，并将关联罚单结案为「完成」
    - approve + 其它（确认/误报）  → 已处理
    - reject  + ``ticket_appeal`` → 退回「罚单待处理」（pre_audit_kind 恢复为 ticket）
    - reject  + 其它              → 退回「待处理」
    """
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if (row.status or "").strip() != "待审核":
        raise HTTPException(status_code=400, detail="仅「待审核」记录可进行审核确认或打回")

    result = (body.result or "").strip().lower()
    auditor = (body.auditor_name or "系统用户").strip()[:64]

    if result in ("approve", "approved", "同意", "通过", "agree"):
        pak = (row.pre_audit_kind or "").strip()
        if pak == "ticket":
            row.status = "罚单待处理"
        elif pak == "ticket_appeal":
            row.status = "已处理"
            row.pre_audit_kind = None
            ticket = await db.scalar(select(ViolationTicket).where(ViolationTicket.biz_no == row.biz_no).limit(1))
            if ticket:
                ticket.status = "完成"
        else:
            row.status = "已处理"
        row.auditor_name = auditor
        row.audited_at = _now()
        row.audit_reject_remark = None
    elif result in ("reject", "rejected", "驳回"):
        rr = (body.remark or "").strip()
        if not rr:
            raise HTTPException(status_code=400, detail="打回须填写打回意见")
        prev_pak = (row.pre_audit_kind or "").strip()
        row.handler_name = None
        row.handled_at = None
        row.handler_remark = None
        row.auditor_name = auditor
        row.audited_at = _now()
        row.audit_reject_remark = rr[:500]
        if prev_pak == "ticket_appeal":
            row.status = "罚单待处理"
            row.pre_audit_kind = "ticket"
        else:
            row.status = "待处理"
            row.pre_audit_kind = None
    else:
        raise HTTPException(status_code=400, detail="result 须为 approve（同意）或 reject（驳回）")

    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": await _row_out_enriched(db, row)}


@router.patch("/{violation_id}/ticket-process-complete")
async def violation_ticket_complete(violation_id: int, body: TicketProcessIn, db: AsyncSession = Depends(get_db)):
    """罚单待处理 → 已处理，并将关联罚单结案为「完成」。"""
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if (row.status or "").strip() != "罚单待处理":
        raise HTTPException(status_code=400, detail="仅「罚单待处理」记录可操作处理完成")
    row.status = "已处理"
    row.handler_remark = body.remark or row.handler_remark
    row.handled_at = _now()
    ticket = await db.scalar(select(ViolationTicket).where(ViolationTicket.biz_no == row.biz_no).limit(1))
    if ticket:
        ticket.status = "完成"
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": await _row_out_enriched(db, row)}


@router.patch("/{violation_id}/ticket-appeal-submit")
async def violation_ticket_appeal(violation_id: int, body: TicketAppealIn, db: AsyncSession = Depends(get_db)):
    """罚单待处理 → 待审核（罚单岗发起申诉），写入申诉说明并回到审核队列。"""
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if (row.status or "").strip() != "罚单待处理":
        raise HTTPException(status_code=400, detail="仅「罚单待处理」记录可提交申诉")
    rm = (body.remark or "").strip()
    if not rm:
        raise HTTPException(status_code=400, detail="申诉说明不能为空")
    row.status = "待审核"
    row.pre_audit_kind = "ticket_appeal"
    row.ticket_appeal_remark = rm[:2000]
    row.audit_reject_remark = None
    row.auditor_name = None
    row.audited_at = None
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": await _row_out_enriched(db, row)}


@router.post("/{violation_id}/ticket-appeal-submit-with-attachments")
async def violation_ticket_appeal_with_attachments(
    violation_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """罚单待处理 → 待审核（罚单岗申诉），支持上传申诉附件。"""
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if (row.status or "").strip() != "罚单待处理":
        raise HTTPException(status_code=400, detail="仅「罚单待处理」记录可提交申诉")

    ctype = (request.headers.get("content-type") or "").lower()
    if "multipart/form-data" not in ctype:
        raise HTTPException(status_code=400, detail="申诉附件须以 multipart/form-data 提交")

    form = await request.form()
    rm = str(form.get("remark") or "").strip()
    if not rm:
        raise HTTPException(status_code=400, detail="申诉说明不能为空")

    upload_files = _collect_ticket_appeal_upload_files(form)
    refs: list[dict[str, Any]] = []
    for f in upload_files:
        refs.append(await _save_ticket_appeal_upload(violation_id, f))

    row.status = "待审核"
    row.pre_audit_kind = "ticket_appeal"
    row.ticket_appeal_remark = rm[:2000]
    row.ticket_appeal_attachment_refs = json.dumps(refs, ensure_ascii=False) if refs else None
    row.audit_reject_remark = None
    row.auditor_name = None
    row.audited_at = None
    await db.flush()
    await db.refresh(row)
    return {
        "ok": True,
        "data": await _row_out_enriched(db, row),
        "attachment_count": len(refs),
        "files_received": len(upload_files),
    }


@router.patch("/{violation_id}/false-alarm-reopen")
async def violation_false_alarm_reopen(violation_id: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    row.status = "待处理"
    row.pre_audit_kind = None
    row.appeal_status = None
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": await _row_out_enriched(db, row)}


@router.get("/{violation_id}/ai-assessment")
async def violation_ai_assessment_get(violation_id: int, db: AsyncSession = Depends(get_db)):
    return await get_violation_ai_assessment(db, violation_id)


@router.post("/{violation_id}/ai-assessment/analyze")
async def violation_ai_assessment_analyze(
    violation_id: int,
    force: bool = Query(False, description="为 true 时强制重新咨询 AI"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    user_id = (x_user_id or "cesg_anonymous").strip() or "cesg_anonymous"
    return await run_violation_ai_assessment(db, violation_id=violation_id, user_id=user_id, force=force)


@router.post("/{violation_id}/ai-assessment/analyze-stream")
async def violation_ai_assessment_analyze_stream(
    violation_id: int,
    force: bool = Query(False, description="为 true 时强制重新咨询 AI"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    """SSE 流式 AI 评估：status / content(delta) / assessment / skip / error 事件。"""
    user_id = (x_user_id or "cesg_anonymous").strip() or "cesg_anonymous"
    return StreamingResponse(
        stream_violation_ai_assessment(violation_id=violation_id, user_id=user_id, force=force),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


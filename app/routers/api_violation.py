"""主动安全/违章报警兼容接口。

为旧版 carManagerV11 安全管理页面提供最小可用的列表、处理、审核和状态流转能力。
"""
from __future__ import annotations

import json
import logging
import re
import secrets
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from starlette.datastructures import UploadFile as StarletteUploadFile
from fastapi import APIRouter, Depends, File, HTTPException, Header, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.jt808_alarm_sync import _strip_alarm_level_suffix
from app.jt808_follow import expand_terminal_id_variants, fetch_followed_device_ids
from app.media_url import normalize_evidence_payload
from app.models import Driver, Fleet, OrgCompany, SysUser, Vehicle, VehicleDevice, VehicleLocation, VehicleViolation, ViolationTicket, ViolationTypeDict
from app.org_scope import collect_org_company_subtree_ids, require_x_org_id_header
from app.plate_util import norm_plate
from app.vehicle_alloc_scope import (
    parse_user_id_header,
    resolve_allowed_vehicle_ids,
    user_has_vehicle_alloc_rules,
)
from app.routers.api_vehicle import _vehicle_list_company_fleet_names
from app.timeutil import china_now_naive
from app.ttl_cache import ttl_get_or_set_async
from app.jt808_violation_sync import lookup_company_name, notify_violation_created
from app.violation_alert_cache import get_alerts_after
from app.alarm_type_gate import (
    expand_disabled_alarm_type_names,
    expand_violation_type_query_names,
    load_alarm_type_risk_map,
    load_disabled_alarm_type_names,
)
from app.violation_risk import resolve_risk_level, risk_level_label
from app.amap_regeo import resolve_address_wgs84
from app.violation_address_fill import ensure_violation_address
from app.violation_ai_assessment import (
    AiRefusalError,
    get_violation_ai_assessment,
    run_violation_ai_assessment,
    stream_violation_ai_assessment,
)
from app.violation_filters import violation_list_visibility, violation_row_is_page_visible
from app.violation_manual_ocr import run_violation_manual_ocr

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


class ViolationAppealResolveIn(BaseModel):
    result: str = Field(..., max_length=32)
    remark: str | None = Field(None, max_length=2000)
    handler_name: str | None = Field(None, max_length=64)


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


def _parse_violation_query_time(raw: str | None, *, end_of_day: bool = False) -> datetime | None:
    """解析列表时间筛选；兼容空格/`+` 分隔与仅日期（Mac 浏览器偶发把空格编成 +）。"""
    text = str(raw or "").strip()
    if not text:
        return None
    text = text.replace("/", "-").replace("T", " ")
    # application/x-www-form-urlencoded 场景下空格可能变成 +
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}\+\d{2}:\d{2}(:\d{2})?", text):
        text = text.replace("+", " ", 1)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}$", text):
        text = f"{text} {'23:59:59' if end_of_day else '00:00:00'}"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _china_today_bounds() -> tuple[datetime, datetime]:
    now = china_now_naive()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return today_start, today_end


def _clamp_violation_query_range(
    start_dt: datetime | None,
    end_dt: datetime | None,
) -> tuple[datetime | None, datetime | None, dict[str, Any]]:
    """以东八区「今天」为准校正客户端时间：未来日期钳回当天，起止颠倒时重置为今日。"""
    today_start, today_end = _china_today_bounds()
    meta: dict[str, Any] = {
        "clamped": False,
        "server_today": today_start.strftime("%Y-%m-%d"),
        "reason": None,
    }
    if start_dt is None and end_dt is None:
        return start_dt, end_dt, meta

    reasons: list[str] = []
    out_start, out_end = start_dt, end_dt

    # 结束时间落到未来日历日 → 钳到今天结束
    if out_end is not None and out_end > today_end:
        out_end = today_end
        reasons.append("end_in_future")

    # 开始时间本身已是未来（典型：Mac 把「今日」算成明天）→ 整段改查服务器今天
    if out_start is not None and out_start > today_end:
        out_start = today_start
        out_end = today_end
        reasons.append("start_in_future")

    # 起止颠倒或钳完后无效
    if out_start is not None and out_end is not None and out_start > out_end:
        out_start = today_start
        out_end = today_end
        reasons.append("start_after_end")

    if reasons:
        meta["clamped"] = True
        meta["reason"] = ",".join(reasons)
        meta["effective_start"] = out_start.strftime("%Y-%m-%d %H:%M:%S") if out_start else None
        meta["effective_end"] = out_end.strftime("%Y-%m-%d %H:%M:%S") if out_end else None
        logger.info(
            "violation list time clamped: reasons=%s server_today=%s start=%s end=%s",
            meta["reason"],
            meta["server_today"],
            meta.get("effective_start"),
            meta.get("effective_end"),
        )
    return out_start, out_end, meta


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
    return china_now_naive()


def _json_loads(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except Exception:
        return fallback


def _coerce_positive_int(value: Any) -> int | None:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _coerce_positive_number(value: Any) -> float | None:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _preview_number_from_raw(raw: str | None, key: str) -> float | None:
    """从可能被截断的 raw_preview 文本中用正则提取数字字段。"""
    text = raw or ""
    if not text:
        return None
    m = re.search(rf'"{re.escape(key)}"\s*:\s*(-?\d+(?:\.\d+)?)', text)
    if not m:
        return None
    return _coerce_positive_number(m.group(1))


def _obd_preview_dict(row: VehicleViolation) -> dict[str, Any]:
    preview = _json_loads(row.raw_preview, {})
    return preview if isinstance(preview, dict) else {}


def _speed_limit_display_from_row(row: VehicleViolation) -> str | None:
    """列表「限速值」列：OBD 从 raw_preview.limit_kmh 解析（已是天气生效限速）。"""
    preview = _obd_preview_dict(row)
    source = (row.source or "").strip()
    if source == "obd_speed":
        limit = _coerce_positive_number(preview.get("limit_kmh"))
        if limit is None:
            limit = _preview_number_from_raw(row.raw_preview, "limit_kmh")
        if limit is None:
            return None
        return str(int(limit)) if float(limit).is_integer() else str(limit)
    for key in (
        "speed_limit",
        "speedLimit",
        "limit_speed",
        "limitSpeed",
        "speed_limit_kmh",
        "limit_kmh",
        "xssd",
        "xsxz",
        "bjxs",
    ):
        limit = _coerce_positive_int(preview.get(key))
        if limit is not None:
            return str(limit)
        limit_f = _preview_number_from_raw(row.raw_preview, key)
        if limit_f is not None:
            return str(int(limit_f)) if float(limit_f).is_integer() else str(limit_f)
    return None


def _obd_speed_display_from_row(row: VehicleViolation) -> str | None:
    """OBD 当前时速（km/h）。"""
    if (row.source or "").strip() != "obd_speed":
        return None
    preview = _obd_preview_dict(row)
    speed = _coerce_positive_number(preview.get("obd_speed_kmh"))
    if speed is None:
        speed = _preview_number_from_raw(row.raw_preview, "obd_speed_kmh")
    if speed is None:
        return None
    return str(int(speed)) if float(speed).is_integer() else str(round(speed, 1))


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


_ORG_MAPS_TTL = 60.0


async def _load_org_company_maps(db: AsyncSession) -> tuple[dict[int, str | None], dict[int, int | None], dict[int, str | None]]:
    async def _load():
        company_map: dict[int, str | None] = {}
        parent_map: dict[int, int | None] = {}
        for cid, cname, pid in (await db.execute(select(OrgCompany.id, OrgCompany.name, OrgCompany.parent_id))).all():
            company_map[cid] = cname
            parent_map[cid] = pid
        fleet_map: dict[int, str | None] = {}
        for fid, fname in (await db.execute(select(Fleet.id, Fleet.name))).all():
            fleet_map[fid] = fname
        return company_map, parent_map, fleet_map

    cached = await ttl_get_or_set_async("violation:org_company_maps", _ORG_MAPS_TTL, _load)
    company_map, parent_map, fleet_map = cached
    return dict(company_map), dict(parent_map), dict(fleet_map)


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
    risk_map = await load_alarm_type_risk_map(db)
    items: list[dict[str, Any]] = []
    # 列表不做逆地理：地址补全只在详情/处理弹窗，避免列表被高德请求拖慢
    for row in rows:
        out = _row_out(row, ticket_by_biz, risk_map=risk_map)
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
        if not (out.get("company_name") or "").strip() or out.get("company_name") == "—":
            snap = (getattr(row, "company_name", None) or "").strip()
            if snap:
                out["company_name"] = snap
        items.append(out)
    return items


async def _row_out_enriched(db: AsyncSession, row: VehicleViolation, ticket_by_biz: dict[str, ViolationTicket] | None = None) -> dict[str, Any]:
    await ensure_violation_address(db, row)
    # 详情接口必须带上关联罚单，否则前端审批流转只能看到「生成罚单」而无类型/金额/备注
    if ticket_by_biz is None:
        ticket_by_biz = {}
        bn = (row.biz_no or "").strip()
        if bn:
            ticket = await db.scalar(select(ViolationTicket).where(ViolationTicket.biz_no == bn).limit(1))
            if ticket is not None:
                ticket_by_biz[bn] = ticket
    items = await _rows_out(db, [row], ticket_by_biz)
    return items[0]


def _row_out(
    row: VehicleViolation,
    ticket_by_biz: dict[str, ViolationTicket] | None = None,
    *,
    risk_map: dict[str, str] | None = None,
) -> dict:
    ticket_by_biz = ticket_by_biz or {}
    ticket = ticket_by_biz.get(row.biz_no or "")
    evidence = _json_loads(row.ttx_evidence_refs, {})
    evidence_norm = normalize_evidence_payload(evidence) if isinstance(evidence, dict) else {"images": [], "videos": []}
    risk = resolve_risk_level(
        type_name=row.violation_type_name,
        stored=row.risk_level,
        risk_map=risk_map,
    )
    type_name_raw = (row.violation_type_name or "").strip()
    type_name_display = _strip_alarm_level_suffix(type_name_raw) or type_name_raw
    return {
        "id": row.id,
        "biz_no": row.biz_no,
        "external_alarm_id": row.external_alarm_id,
        "terminal_id": row.terminal_id,
        "vehicle_id": row.vehicle_id,
        "plate_no": row.plate_no,
        "company_id": row.company_id,
        "violation_type_code": row.violation_type_code,
        "violation_type_name": type_name_display,
        "risk_level": risk,
        "risk_level_label": risk_level_label(risk),
        "violation_time": _dt_text(row.violation_time),
        "lat": row.lat,
        "lng": row.lng,
        "address": row.address,
        "source": row.source,
        "weather": getattr(row, "weather", None),
        "private_rule_name": getattr(row, "private_rule_name", None),
        "rule_category_name": getattr(row, "rule_category_name", None),
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
            "obd_speed": "OBD超速",
        }.get((row.source or "").strip(), row.source or ""),
        "speed_limit_display": _speed_limit_display_from_row(row),
        "obd_speed_display": _obd_speed_display_from_row(row),
    }


async def _scoped_query(
    db: AsyncSession,
    x_org_id: str | None,
    disabled_alarm_type_names=None,
    x_user_id: str | None = None,
):
    if disabled_alarm_type_names is None:
        disabled_alarm_type_names = await load_disabled_alarm_type_names(db)
    q = select(VehicleViolation).where(violation_list_visibility(disabled_alarm_type_names))
    subtree: set[int] | None = None
    if x_org_id:
        root = require_x_org_id_header(x_org_id)
        exists = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
        if exists:
            subtree = await collect_org_company_subtree_ids(db, root)
            # company_id 为空时，仅保留车辆挂在组织树内的记录，避免脏数据穿透
            q = q.where(
                or_(
                    VehicleViolation.company_id.in_(subtree),
                    and_(
                        VehicleViolation.company_id.is_(None),
                        VehicleViolation.vehicle_id.in_(
                            select(Vehicle.id).where(Vehicle.company_id.in_(subtree))
                        ),
                    ),
                )
            )
    # 仅「绑定了车辆分配规则」时按 vehicle_id 收紧；无规则依赖组织树，避免上千车牌 IN
    uid = parse_user_id_header(x_user_id)
    if uid is not None and await user_has_vehicle_alloc_rules(db, uid):
        allowed_ids = await resolve_allowed_vehicle_ids(db, uid)
        if allowed_ids is not None:
            if not allowed_ids:
                q = q.where(VehicleViolation.id < 0)
            else:
                q = q.where(VehicleViolation.vehicle_id.in_(list(allowed_ids)))
    return q


async def _get_visible_violation_or_404(db: AsyncSession, violation_id: int) -> VehicleViolation:
    """按 id 取记录；非 OBD 且无图片/视频证据时视为不存在（不在页面展示）。"""
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None or not violation_row_is_page_visible(row):
        raise HTTPException(status_code=404, detail="记录不存在")
    return row


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
    appeal_status: str | None = Query(None, description="申诉状态筛选，如：申诉中"),
    violation_type_dict_id: int | None = Query(None, ge=1),
    violation_type_name: str | None = Query(None, description="触发报警类型（基名或带一/二/三级；多个用逗号分隔）"),
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    followed_only: bool = Query(False),
    user_id: int | None = Query(None, ge=1),
    apply_alarm_filter: bool = Query(
        True,
        description="是否按停用报警类型软隐藏历史记录；默认开启",
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    limit: int | None = Query(None, ge=1, le=2000),
    offset: int | None = Query(None, ge=0),
    min_id: int | None = Query(None, ge=0),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    disabled_names = await load_disabled_alarm_type_names(db) if apply_alarm_filter else []
    q = await _scoped_query(db, x_org_id, disabled_names, x_user_id)
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
        plates = [p.strip() for p in str(plate_no).replace("，", ",").split(",") if p.strip()]
        if len(plates) > 1:
            # 选择车辆多选：精确匹配所选车牌
            q = q.where(VehicleViolation.plate_no.in_(plates))
        elif plates:
            q = q.where(VehicleViolation.plate_no.ilike(f"%{plates[0]}%"))
    if biz_no:
        q = q.where(VehicleViolation.biz_no.ilike(f"%{biz_no.strip()}%"))
    if terminal_id:
        q = q.where(VehicleViolation.terminal_id.ilike(f"%{terminal_id.strip()}%"))
    if source:
        q = q.where(VehicleViolation.source == source.strip())
    if appeal_status:
        q = q.where(VehicleViolation.appeal_status == appeal_status.strip())
    if violation_type_dict_id is not None:
        vt_row = await db.get(ViolationTypeDict, int(violation_type_dict_id))
        if vt_row is not None and (vt_row.type_name or "").strip():
            q = q.where(VehicleViolation.violation_type_name == (vt_row.type_name or "").strip())
    type_names = expand_violation_type_query_names(violation_type_name or "")
    if type_names:
        q = q.where(func.trim(VehicleViolation.violation_type_name).in_(type_names))
    start_dt = _parse_violation_query_time(start_time, end_of_day=False)
    end_dt = _parse_violation_query_time(end_time, end_of_day=True)
    start_dt, end_dt, time_clamp_meta = _clamp_violation_query_range(start_dt, end_dt)
    if start_dt is not None:
        q = q.where(VehicleViolation.violation_time >= start_dt)
    if end_dt is not None:
        q = q.where(VehicleViolation.violation_time <= end_dt)
    if min_id is not None and min_id > 0:
        q = q.where(VehicleViolation.id > min_id)

    # count 只统计 id，避免对整行实体做 subquery
    total = (
        await db.scalar(
            select(func.count()).select_from(
                q.with_only_columns(VehicleViolation.id, maintain_column_froms=True)
                .order_by(None)
                .subquery()
            )
        )
    ) or 0
    lim = limit or page_size
    off = offset if offset is not None else (page - 1) * page_size
    order = (
        (VehicleViolation.id.asc(),)
        if min_id is not None and min_id > 0
        else (VehicleViolation.violation_time.desc(), VehicleViolation.id.desc())
    )
    rows = (await db.execute(q.order_by(*order).offset(off).limit(lim))).scalars().all()
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
        "filter_meta": {
            "engine": "alarm-type-gate-v1",
            "apply_alarm_filter": apply_alarm_filter,
            "disabled_types": len(disabled_names),
            "types": [{"type_name": name} for name in disabled_names],
            "time_clamp": time_clamp_meta,
        },
    }


@router.get("/recent-pending")
async def violation_recent_pending(
    after_id: int = Query(0, ge=0),
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    followed_only: bool = Query(False),
    user_id: int | None = Query(None, ge=1),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    """增量拉取待处理报警：id > after_id，供安全监控弹窗轮询（每入库一条触发一次）。"""
    filter_rules = await load_disabled_alarm_type_names(db)
    q = await _scoped_query(db, x_org_id, filter_rules, x_user_id)
    q = await _apply_followed_only_filter(
        db,
        q,
        followed_only=followed_only,
        user_id=user_id,
        x_user_id=x_user_id,
    )
    q = q.where(
        or_(
            VehicleViolation.status == "待处理",
            and_(VehicleViolation.status == "待审核", VehicleViolation.pre_audit_kind == "preprocess"),
        )
    )
    if after_id > 0:
        q = q.where(VehicleViolation.id > after_id)
    start_dt = _parse_violation_query_time(start_time, end_of_day=False)
    end_dt = _parse_violation_query_time(end_time, end_of_day=True)
    start_dt, end_dt, _time_clamp_meta = _clamp_violation_query_range(start_dt, end_dt)
    if start_dt is not None:
        q = q.where(VehicleViolation.violation_time >= start_dt)
    if end_dt is not None:
        q = q.where(VehicleViolation.violation_time <= end_dt)

    rows = (
        await db.execute(
            q.order_by(VehicleViolation.id.asc()).limit(limit)
        )
    ).scalars().all()
    biz = [x.biz_no for x in rows if x.biz_no]
    ticket_by_biz: dict[str, ViolationTicket] = {}
    if biz:
        for ticket in (await db.execute(select(ViolationTicket).where(ViolationTicket.biz_no.in_(biz)))).scalars().all():
            if ticket.biz_no:
                ticket_by_biz[ticket.biz_no] = ticket
    max_id = await db.scalar(select(func.max(VehicleViolation.id)).select_from(q.subquery()))
    max_id = int(max_id or after_id)
    return {
        "ok": True,
        "items": await _rows_out(db, list(rows), ticket_by_biz),
        "after_id": after_id,
        "max_id": max_id,
    }


@router.get("/alert-cache")
async def violation_alert_cache(
    after_seq: int = Query(-1, ge=-1),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    """新增报警缓存增量。after_seq=-1 只取当前水位（登录时调用一次），
    之后带上次返回的 max_seq 轮询，有新条目即弹窗。按 X-Org-Id 过滤可见公司。
    停用报警类型的历史/缓存条目一并隐藏。"""
    alerts, max_seq = get_alerts_after(after_seq)
    disabled = set(expand_disabled_alarm_type_names(await load_disabled_alarm_type_names(db)))
    if alerts and disabled:
        alerts = [
            a
            for a in alerts
            if str(a.get("violation_type_name") or "").strip() not in disabled
        ]
    if alerts and x_org_id:
        try:
            root = require_x_org_id_header(x_org_id)
            exists = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
            if exists:
                subtree = await collect_org_company_subtree_ids(db, root)
                alerts = [a for a in alerts if a.get("company_id") is None or a.get("company_id") in subtree]
        except HTTPException:
            pass
    return {"ok": True, "items": alerts, "max_seq": max_seq}


@router.get("/pending-watermark")
async def violation_pending_watermark(
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    """当前可见范围内待处理报警的最大 id，用于登录后建立弹窗轮询水位。"""
    filter_rules = await load_disabled_alarm_type_names(db)
    q = await _scoped_query(db, x_org_id, filter_rules, x_user_id)
    q = q.where(
        or_(
            VehicleViolation.status == "待处理",
            and_(VehicleViolation.status == "待审核", VehicleViolation.pre_audit_kind == "preprocess"),
        )
    )
    max_id = await db.scalar(select(func.max(VehicleViolation.id)).select_from(q.subquery()))
    return {"ok": True, "max_id": int(max_id or 0)}


@router.post("/manual/ocr")
async def violation_manual_ocr(
    file: UploadFile = File(...),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    """违章手动录入：上传图片经多模态 AI 抽取表单字段。"""
    content = await file.read()
    return await run_violation_manual_ocr(
        db,
        filename=file.filename or "upload.jpg",
        content=content,
        content_type=file.content_type,
        x_user_id=x_user_id,
    )


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
        # 前端车辆树常传 JT808 car_id；仅当本地 id 与车牌一致时采用
        if v_pick is not None and norm_plate(v_pick.plate_no) == plate:
            v = v_pick
    if v is None:
        vr = await db.execute(select(Vehicle).where(Vehicle.plate_no == plate))
        v = vr.scalar_one_or_none()
        if v is None:
            vr2 = await db.execute(
                select(Vehicle).where(func.upper(Vehicle.plate_no) == plate.upper())
            )
            v = vr2.scalar_one_or_none()

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
        source="manual",
        transparent_type=None,
        raw_preview=(body.remark or "").strip() or None,
        # 手动录入：先入审核队列，通过后再进手动处理
        status="待审核",
        pre_audit_kind="manual_entry",
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    await notify_violation_created(db, row)
    return {"ok": True, "id": row.id, "biz_no": row.biz_no}


@router.get("/{violation_id}/detail")
async def violation_detail(violation_id: int, db: AsyncSession = Depends(get_db)):
    row = await _get_visible_violation_or_404(db, violation_id)
    return {"ok": True, "data": await _row_out_enriched(db, row)}


@router.post("/{violation_id}/fetch-device-media")
async def violation_fetch_device_media(violation_id: int, db: AsyncSession = Depends(get_db)):
    row = await _get_visible_violation_or_404(db, violation_id)
    evidence = normalize_evidence_payload(_json_loads(row.ttx_evidence_refs, {}))
    return {
        "ok": True,
        "message": "",
        "images": evidence.get("images", []),
        "videos": evidence.get("videos", []),
        "downlink": [],
    }


@router.patch("/{violation_id}/handle")
async def violation_handle(violation_id: int, body: ViolationHandleIn, db: AsyncSession = Depends(get_db)):
    """处理动作。

    - 设备报警等：confirm → 待审核(preprocess)；误报 → 误报
    - 手动违章（审核通过后的待处理）：完结 → 已处理；申诉 → 已处理+申诉中
    """
    row = await _get_visible_violation_or_404(db, violation_id)
    action = (body.action or "confirm").strip()
    is_manual = (row.source or "").strip() == "manual"
    status = (row.status or "").strip()

    # 手动违章：录入→审核通过后进入本页，可完结或申诉
    if is_manual and status == "待处理":
        row.handler_name = (body.handler_name or "系统用户").strip()[:64] or "系统用户"
        row.handled_at = _now()
        row.handler_remark = (body.remark or "").strip()[:2000] or None
        if action in ("complete", "finish", "完结", "done"):
            row.status = "已处理"
            row.pre_audit_kind = None
        elif action in ("appeal", "申诉"):
            rm = (body.remark or "").strip()
            if not rm:
                raise HTTPException(status_code=400, detail="申诉须填写申诉说明")
            row.status = "已处理"
            row.pre_audit_kind = None
            row.appeal_status = "申诉中"
            row.appeal_reason = rm[:2000]
            row.appeal_submitted_at = _now()
        else:
            raise HTTPException(status_code=400, detail="手动违章处理请选择「完结」或「申诉」")
        await db.flush()
        await db.refresh(row)
        return {"ok": True, "data": await _row_out_enriched(db, row)}

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
    """待审核分流。

    依据 ``pre_audit_kind``：
    - approve + ``manual_entry``  → 待处理（进入违章手动处理）
    - approve + ``ticket``        → 罚单待处理
    - approve + ``ticket_appeal`` → 已处理，罚单结案
    - approve + 其它（确认/误报）  → 已处理
    - reject  + ``manual_entry``  → 已处理（驳回结束，不进处理页）
    - reject  + ``ticket_appeal`` → 罚单待处理
    - reject  + 其它              → 待处理
    """
    row = await _get_visible_violation_or_404(db, violation_id)
    if (row.status or "").strip() != "待审核":
        raise HTTPException(status_code=400, detail="仅「待审核」记录可进行审核确认或打回")

    result = (body.result or "").strip().lower()
    auditor = (body.auditor_name or "系统用户").strip()[:64]

    if result in ("approve", "approved", "同意", "通过", "agree"):
        pak = (row.pre_audit_kind or "").strip()
        if pak == "manual_entry":
            row.status = "待处理"
            row.pre_audit_kind = None
        elif pak == "ticket":
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
        if prev_pak == "manual_entry":
            # 录入审核驳回：不进手动处理队列
            row.status = "已处理"
            row.pre_audit_kind = None
        elif prev_pak == "ticket_appeal":
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


@router.patch("/{violation_id}/appeal-resolve")
async def violation_appeal_resolve(
    violation_id: int,
    body: ViolationAppealResolveIn,
    db: AsyncSession = Depends(get_db),
):
    """违章申诉页：对「申诉中」记录通过/驳回。"""
    row = await _get_visible_violation_or_404(db, violation_id)
    if (row.appeal_status or "").strip() != "申诉中":
        raise HTTPException(status_code=400, detail="仅「申诉中」记录可进行申诉处理")
    result = (body.result or "").strip().lower()
    name = (body.handler_name or "系统用户").strip()[:64] or "系统用户"
    remark = (body.remark or "").strip()
    if result in ("approve", "approved", "同意", "通过", "agree"):
        row.appeal_status = "申诉成功"
    elif result in ("reject", "rejected", "驳回"):
        if not remark:
            raise HTTPException(status_code=400, detail="驳回申诉须填写意见")
        row.appeal_status = "申诉失败"
    else:
        raise HTTPException(status_code=400, detail="result 须为 approve（通过）或 reject（驳回）")
    if remark:
        prev = (row.handler_remark or "").strip()
        note = f"[申诉处理]{remark}"
        row.handler_remark = f"{prev}\n{note}".strip()[:2000] if prev else note[:2000]
    row.handler_name = name
    row.handled_at = _now()
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": await _row_out_enriched(db, row)}


@router.patch("/{violation_id}/ticket-process-complete")
async def violation_ticket_complete(violation_id: int, body: TicketProcessIn, db: AsyncSession = Depends(get_db)):
    """罚单待处理 → 已处理，并将关联罚单结案为「完成」。"""
    row = await _get_visible_violation_or_404(db, violation_id)
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
    row = await _get_visible_violation_or_404(db, violation_id)
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
    row = await _get_visible_violation_or_404(db, violation_id)
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
    row = await _get_visible_violation_or_404(db, violation_id)
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
    try:
        return await run_violation_ai_assessment(db, violation_id=violation_id, user_id=user_id, force=force)
    except AiRefusalError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"AI 多次拒答，未落库，请稍后重试：{str(exc)[:160]}",
        ) from exc


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
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


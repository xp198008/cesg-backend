"""设备/人工报障合并列表、处理与单据（async SQLAlchemy）。"""
from __future__ import annotations

import logging
import secrets
import uuid
from pathlib import Path

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Fleet,
    JtDeviceFault,
    JtDeviceFaultReceipt,
    ManualFaultImage,
    ManualFaultReceipt,
    ManualFaultReport,
    OrgCompany,
    Vehicle,
    VehicleDevice,
    VehicleLocation,
    VehicleViolation,
)
from app.plate_util import norm_plate
from app.timeutil import china_now_naive

_logger = logging.getLogger(__name__)
_BACKEND_ROOT = Path(__file__).resolve().parent.parent

_DEVICE_FAULT_RECEIPT_ALLOWED_EXT = frozenset(
    {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp"}
)
_MANUAL_FAULT_IMAGE_ALLOWED_EXT = frozenset({".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp"})
_IMAGE_MEDIA_TYPES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".bmp": "image/bmp",
    ".gif": "image/gif",
    ".webp": "image/webp",
}

# 三段流：录入(待审核) → 审核通过 → 上传单据 → 已完结；驳回后离开审核队列
_AUDIT_PENDING = frozenset({"待审核", "待预审", "未处理", "待处理"})
# 仅待审核可操作；驳回后审核页不可见、不可再审
_AUDIT_ACTIONS_FROM = _AUDIT_PENDING
# 可上传单据：审核通过；兼容旧状态（预审通过/待终审/终审驳回）
_RECEIPT_ELIGIBLE = frozenset({"审核通过", "预审通过", "待终审", "终审驳回"})
_COMPLETED = frozenset({"已完结", "完结", "已通过", "已处理"})
_REJECTED = frozenset({"驳回", "预审驳回", "审核驳回", "终审驳回"})
# 查询页：通过类 + 完结 + 驳回（含历史状态）
_QUERY_VISIBLE = frozenset(
    {
        "审核通过",
        "预审通过",
        "待终审",
        "已完结",
        "完结",
        "已通过",
        "已处理",
        "驳回",
        "预审驳回",
        "终审驳回",
        "审核驳回",
    }
)


def _handle_status_match_values(handle_status: str) -> list[str] | None:
    """筛选项到库内可能取值的展开；None 表示不按状态筛。"""
    hs = (handle_status or "").strip()
    if not hs:
        return None
    if hs in ("待审核", "待预审", "未处理"):
        return list(_AUDIT_PENDING)
    if hs in ("预审待办", "审核待办"):
        # 审核页只看待审，不含驳回
        return list(_AUDIT_PENDING)
    if hs in ("查询可见", "已审核"):
        return list(_QUERY_VISIBLE)
    if hs in ("已完结", "完结"):
        return list(_COMPLETED)
    if hs in ("审核通过", "预审通过"):
        return ["审核通过", "预审通过"]
    if hs in ("驳回", "审核驳回", "预审驳回"):
        return list(_REJECTED)
    return [hs]


def _apply_handle_status_filter(q, column, handle_status: str | None, *, receipt_eligible_only: bool):
    if receipt_eligible_only:
        return q.where(column.in_(list(_RECEIPT_ELIGIBLE)))
    if not handle_status or not str(handle_status).strip():
        return q
    values = _handle_status_match_values(str(handle_status))
    if not values:
        return q
    if len(values) == 1:
        return q.where(column == values[0])
    return q.where(column.in_(values))


def _dt_text(v) -> str | None:
    if v is None:
        return None
    try:
        return v.isoformat()[:19]
    except AttributeError:
        return str(v)[:19]


async def _load_company_fleet_names(
    db: AsyncSession,
    *,
    vehicle_ids: list[int],
    company_ids: list[int],
) -> tuple[dict[int, tuple[str | None, str | None]], dict[int, str]]:
    """vehicle_id -> (company_name, fleet_name); company_id -> company_name."""
    vehicle_map: dict[int, tuple[str | None, str | None]] = {}
    company_map: dict[int, str] = {}
    vids = sorted({int(x) for x in vehicle_ids if x is not None})
    cids = sorted({int(x) for x in company_ids if x is not None})
    if vids:
        vehicles = (await db.execute(select(Vehicle).where(Vehicle.id.in_(vids)))).scalars().all()
        fleet_ids = sorted({int(v.fleet_id) for v in vehicles if v.fleet_id is not None})
        company_ids_from_v = sorted({int(v.company_id) for v in vehicles if v.company_id is not None})
        cids = sorted(set(cids) | set(company_ids_from_v))
        fleet_name_by_id: dict[int, str] = {}
        if fleet_ids:
            fleets = (await db.execute(select(Fleet).where(Fleet.id.in_(fleet_ids)))).scalars().all()
            fleet_name_by_id = {int(f.id): (f.name or "").strip() for f in fleets}
        if cids:
            companies = (await db.execute(select(OrgCompany).where(OrgCompany.id.in_(cids)))).scalars().all()
            company_map = {int(c.id): (c.name or c.short_name or "").strip() for c in companies}
        for v in vehicles:
            cid = int(v.company_id) if v.company_id is not None else None
            fid = int(v.fleet_id) if v.fleet_id is not None else None
            vehicle_map[int(v.id)] = (
                company_map.get(cid) if cid is not None else None,
                fleet_name_by_id.get(fid) if fid is not None else None,
            )
    elif cids:
        companies = (await db.execute(select(OrgCompany).where(OrgCompany.id.in_(cids)))).scalars().all()
        company_map = {int(c.id): (c.name or c.short_name or "").strip() for c in companies}
    return vehicle_map, company_map


def _attach_company_fleet(
    row: dict,
    *,
    vehicle_map: dict[int, tuple[str | None, str | None]],
    company_map: dict[int, str],
) -> dict:
    vid = row.get("vehicle_id")
    cid = row.get("company_id")
    company_name = None
    fleet_name = None
    if vid is not None and int(vid) in vehicle_map:
        company_name, fleet_name = vehicle_map[int(vid)]
    if not company_name and cid is not None:
        company_name = company_map.get(int(cid))
    row["company_name"] = company_name or ""
    row["fleet_name"] = fleet_name or ""
    return row


def _jt_device_fault_row_dict(r: JtDeviceFault, *, receipt_count: int = 0) -> dict:
    count = max(0, int(receipt_count or 0))
    return {
        "id": r.id,
        "biz_no": r.biz_no,
        "terminal_id": r.terminal_id,
        "vehicle_id": r.vehicle_id,
        "plate_no": r.plate_no or "",
        "company_id": r.company_id,
        "company_name": "",
        "fleet_name": "",
        "fault_bit": r.fault_bit,
        "fault_type_name": r.fault_type_name,
        "fault_time": _dt_text(r.fault_time),
        "alarm_flags": r.alarm_flags,
        "lat": r.lat,
        "lng": r.lng,
        "speed_kmh": r.speed_kmh,
        "direction": r.direction,
        "raw_preview": r.raw_preview,
        "source": r.source,
        "created_at": _dt_text(r.created_at),
        "handle_status": r.handle_status,
        "handled_at": _dt_text(r.handled_at),
        "handler_name": r.handler_name,
        "handler_remark": r.handler_remark,
        "audited_at": _dt_text(r.audited_at),
        "auditor_name": r.auditor_name,
        "audit_remark": r.audit_remark,
        "receipt_count": count,
        "has_receipt": count > 0,
        "audit_stage": "审核",
    }


async def _terminal_id_for_manual_report(db: AsyncSession, row: ManualFaultReport) -> str:
    snap = (getattr(row, "terminal_bind_no", None) or "").strip()
    if snap:
        return snap[:64]
    vid = row.vehicle_id
    plate = norm_plate(row.plate_no)
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
        if plate:
            stmt_vio = stmt_vio.where(
                or_(
                    VehicleViolation.vehicle_id == vid,
                    func.upper(func.trim(VehicleViolation.plate_no)) == plate.upper(),
                )
            )
        else:
            stmt_vio = stmt_vio.where(VehicleViolation.vehicle_id == vid)
    elif plate:
        stmt_vio = stmt_vio.where(func.upper(func.trim(VehicleViolation.plate_no)) == plate.upper())
    else:
        stmt_vio = None
    if stmt_vio is not None:
        stmt_vio = stmt_vio.order_by(VehicleViolation.violation_time.desc()).limit(1)
        hv = (await db.execute(stmt_vio)).scalar_one_or_none()
        if hv is not None and (hv.terminal_id or "").strip():
            return str(hv.terminal_id).strip()[:64]

    stmt = select(JtDeviceFault).where(JtDeviceFault.terminal_id.isnot(None)).where(JtDeviceFault.terminal_id != "")
    if vid is not None:
        if plate:
            stmt = stmt.where(
                or_(
                    JtDeviceFault.vehicle_id == vid,
                    func.upper(func.trim(JtDeviceFault.plate_no)) == plate.upper(),
                )
            )
        else:
            stmt = stmt.where(JtDeviceFault.vehicle_id == vid)
    elif plate:
        stmt = stmt.where(func.upper(func.trim(JtDeviceFault.plate_no)) == plate.upper())
    else:
        return ""
    stmt = stmt.order_by(JtDeviceFault.fault_time.desc()).limit(1)
    rf = (await db.execute(stmt)).scalar_one_or_none()
    if rf and (rf.terminal_id or "").strip():
        return str(rf.terminal_id).strip()[:64]
    return ""


def _manual_fault_image_view_url(image_id: int) -> str:
    return f"/cmapi/manual-fault/images/{int(image_id)}/view"


def _image_row_dict(r: ManualFaultImage) -> dict:
    return {
        "id": r.id,
        "fault_id": r.fault_id,
        "biz_no": r.biz_no,
        "name": r.original_name,
        "original_name": r.original_name,
        "file_size": r.file_size,
        "mime_type": r.mime_type,
        "url": _manual_fault_image_view_url(int(r.id)),
        "created_at": _dt_text(r.created_at),
    }


async def _list_manual_images_by_fault_ids(db: AsyncSession, fault_ids: list[int]) -> dict[int, list[dict]]:
    ids = [int(x) for x in fault_ids if x is not None]
    if not ids:
        return {}
    rows = (
        await db.execute(
            select(ManualFaultImage)
            .where(ManualFaultImage.fault_id.in_(ids))
            .order_by(ManualFaultImage.created_at.asc())
        )
    ).scalars().all()
    out: dict[int, list[dict]] = {}
    for r in rows:
        out.setdefault(int(r.fault_id), []).append(_image_row_dict(r))
    return out


async def _manual_fault_row_dict(
    db: AsyncSession,
    r: ManualFaultReport,
    *,
    receipt_count: int | None = None,
    images: list[dict] | None = None,
) -> dict:
    tid = await _terminal_id_for_manual_report(db, r)
    if receipt_count is None:
        receipt_count = await _count_manual_receipts(db, int(r.id))
    count = max(0, int(receipt_count or 0))
    if images is None:
        image_map = await _list_manual_images_by_fault_ids(db, [int(r.id)])
        images = image_map.get(int(r.id), [])
    return {
        "id": r.id,
        "biz_no": r.biz_no,
        "terminal_id": tid,
        "vehicle_id": r.vehicle_id,
        "plate_no": r.plate_no or "",
        "company_id": r.company_id,
        "company_name": "",
        "fleet_name": "",
        "fault_bit": None,
        "fault_type_name": r.fault_type_name,
        "fault_time": _dt_text(r.discovery_time),
        "alarm_flags": None,
        "lat": None,
        "lng": None,
        "speed_kmh": None,
        "direction": None,
        "raw_preview": r.fault_phenomenon,
        "source": "manual_entry",
        "created_at": _dt_text(r.created_at),
        "handle_status": r.handle_status,
        "handled_at": _dt_text(r.handled_at),
        "handler_name": r.handler_name,
        "handler_remark": r.handler_remark,
        "audited_at": _dt_text(r.audited_at),
        "auditor_name": r.auditor_name,
        "audit_remark": r.audit_remark,
        "report_source": "manual",
        "discoverer": r.discoverer,
        "fault_level": r.fault_level,
        "fault_devices": r.fault_devices,
        "fault_location": r.fault_location,
        "affect_service": r.affect_service,
        "fault_phenomenon": r.fault_phenomenon,
        "receipt_count": count,
        "has_receipt": count > 0,
        "image_count": len(images or []),
        "images": images or [],
        "audit_stage": "审核",
    }


async def _count_device_receipts_by_fault_ids(db: AsyncSession, fault_ids: list[int]) -> dict[int, int]:
    ids = [int(x) for x in fault_ids if x is not None]
    if not ids:
        return {}
    rows = (
        await db.execute(
            select(JtDeviceFaultReceipt.fault_id, func.count())
            .where(JtDeviceFaultReceipt.fault_id.in_(ids))
            .group_by(JtDeviceFaultReceipt.fault_id)
        )
    ).all()
    return {int(fid): int(cnt or 0) for fid, cnt in rows}


async def _count_manual_receipts_by_fault_ids(db: AsyncSession, fault_ids: list[int]) -> dict[int, int]:
    ids = [int(x) for x in fault_ids if x is not None]
    if not ids:
        return {}
    rows = (
        await db.execute(
            select(ManualFaultReceipt.fault_id, func.count())
            .where(ManualFaultReceipt.fault_id.in_(ids))
            .group_by(ManualFaultReceipt.fault_id)
        )
    ).all()
    return {int(fid): int(cnt or 0) for fid, cnt in rows}


async def _count_manual_receipts(db: AsyncSession, fault_id: int) -> int:
    n = (
        await db.execute(
            select(func.count()).select_from(ManualFaultReceipt).where(ManualFaultReceipt.fault_id == int(fault_id))
        )
    ).scalar_one()
    return int(n or 0)


def _manual_passes_terminal_filter(
    terminal_id: str,
    exact: str | None,
    contains: str | None,
) -> bool:
    t = (terminal_id or "").strip()
    if not t:
        return not (exact and exact.strip()) and not (contains and contains.strip())
    if exact and str(exact).strip():
        return t == str(exact).strip()
    if contains and str(contains).strip():
        return str(contains).strip().lower() in t.lower()
    return True


def _apply_device_fault_filters(q, *, terminal_id, terminal_id_contains, plate_no_contains, biz_no_contains,
                                start_time, end_time, handle_status, receipt_eligible_only):
    if terminal_id and (t := terminal_id.strip()):
        q = q.where(JtDeviceFault.terminal_id == t)
    if terminal_id_contains and (tc := terminal_id_contains.strip()):
        q = q.where(JtDeviceFault.terminal_id.like(f"%{tc}%"))
    if plate_no_contains and (pc := plate_no_contains.strip()):
        q = q.where(JtDeviceFault.plate_no.like(f"%{pc}%"))
    if biz_no_contains and (bc := biz_no_contains.strip()):
        q = q.where(JtDeviceFault.biz_no.like(f"%{bc}%"))
    if start_time and start_time.strip():
        q = q.where(JtDeviceFault.fault_time >= start_time.strip()[:26])
    if end_time and end_time.strip():
        q = q.where(JtDeviceFault.fault_time <= end_time.strip()[:26])
    q = _apply_handle_status_filter(
        q, JtDeviceFault.handle_status, handle_status, receipt_eligible_only=receipt_eligible_only
    )
    return q


def _apply_manual_fault_filters(q, *, plate_no_contains, biz_no_contains, start_time, end_time,
                                handle_status, receipt_eligible_only):
    if plate_no_contains and (pc := plate_no_contains.strip()):
        q = q.where(ManualFaultReport.plate_no.like(f"%{pc}%"))
    if biz_no_contains and (bc := biz_no_contains.strip()):
        q = q.where(ManualFaultReport.biz_no.like(f"%{bc}%"))
    if start_time and start_time.strip():
        q = q.where(ManualFaultReport.discovery_time >= start_time.strip()[:26])
    if end_time and end_time.strip():
        q = q.where(ManualFaultReport.discovery_time <= end_time.strip()[:26])
    q = _apply_handle_status_filter(
        q, ManualFaultReport.handle_status, handle_status, receipt_eligible_only=receipt_eligible_only
    )
    return q


async def get_merged_device_manual_fault_list(
    db: AsyncSession,
    *,
    terminal_id: str | None = None,
    terminal_id_contains: str | None = None,
    plate_no_contains: str | None = None,
    biz_no_contains: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    handle_status: str | None = None,
    receipt_eligible_only: bool = False,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[dict], int]:
    qd = select(JtDeviceFault)
    qd = _apply_device_fault_filters(
        qd,
        terminal_id=terminal_id,
        terminal_id_contains=terminal_id_contains,
        plate_no_contains=plate_no_contains,
        biz_no_contains=biz_no_contains,
        start_time=start_time,
        end_time=end_time,
        handle_status=handle_status,
        receipt_eligible_only=receipt_eligible_only,
    )
    qm = select(ManualFaultReport)
    qm = _apply_manual_fault_filters(
        qm,
        plate_no_contains=plate_no_contains,
        biz_no_contains=biz_no_contains,
        start_time=start_time,
        end_time=end_time,
        handle_status=handle_status,
        receipt_eligible_only=receipt_eligible_only,
    )

    dev_rows = (await db.execute(qd.order_by(JtDeviceFault.fault_time.desc()))).scalars().all()
    man_rows = (await db.execute(qm.order_by(ManualFaultReport.discovery_time.desc()))).scalars().all()

    device_counts = await _count_device_receipts_by_fault_ids(db, [int(r.id) for r in dev_rows])
    manual_counts = await _count_manual_receipts_by_fault_ids(db, [int(r.id) for r in man_rows])
    manual_images = await _list_manual_images_by_fault_ids(db, [int(r.id) for r in man_rows])

    dev_list: list[dict] = []
    for r in dev_rows:
        d = _jt_device_fault_row_dict(r, receipt_count=device_counts.get(int(r.id), 0))
        d["report_source"] = "device"
        d["discoverer"] = None
        d["images"] = []
        d["image_count"] = 0
        dev_list.append(d)

    man_list: list[dict] = []
    for r in man_rows:
        d = await _manual_fault_row_dict(
            db,
            r,
            receipt_count=manual_counts.get(int(r.id), 0),
            images=manual_images.get(int(r.id), []),
        )
        if _manual_passes_terminal_filter(d["terminal_id"], terminal_id, terminal_id_contains):
            man_list.append(d)

    vehicle_map, company_map = await _load_company_fleet_names(
        db,
        vehicle_ids=[d.get("vehicle_id") for d in dev_list + man_list],
        company_ids=[d.get("company_id") for d in dev_list + man_list],
    )
    for d in dev_list + man_list:
        _attach_company_fleet(d, vehicle_map=vehicle_map, company_map=company_map)

    merged = sorted(dev_list + man_list, key=lambda x: x.get("fault_time") or "", reverse=True)
    total = len(merged)
    skip = max(0, (max(1, page) - 1) * max(1, page_size))
    return merged[skip : skip + max(1, page_size)], total


async def get_jt_device_fault_by_id(db: AsyncSession, fault_id: int) -> dict | None:
    r = await db.get(JtDeviceFault, int(fault_id))
    if r is None:
        return None
    counts = await _count_device_receipts_by_fault_ids(db, [int(fault_id)])
    d = _jt_device_fault_row_dict(r, receipt_count=counts.get(int(fault_id), 0))
    vehicle_map, company_map = await _load_company_fleet_names(
        db, vehicle_ids=[d.get("vehicle_id")], company_ids=[d.get("company_id")]
    )
    _attach_company_fleet(d, vehicle_map=vehicle_map, company_map=company_map)
    d["report_source"] = "device"
    d["discoverer"] = None
    return d


def jt_device_fault_receipt_eligible(handle_status: str | None) -> bool:
    st = (handle_status or "").strip()
    return st in _RECEIPT_ELIGIBLE


def device_fault_receipts_root() -> Path:
    d = _BACKEND_ROOT / "data" / "device_fault_receipts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def manual_fault_receipts_root() -> Path:
    d = _BACKEND_ROOT / "data" / "manual_fault_receipts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def manual_fault_images_root() -> Path:
    d = _BACKEND_ROOT / "data" / "manual_fault_images"
    d.mkdir(parents=True, exist_ok=True)
    return d


def device_fault_receipt_safe_ext(filename: str | None) -> str:
    raw = (filename or "").replace("\\", "/").rsplit("/", 1)[-1]
    if "." not in raw:
        return ""
    ext = "." + raw.rsplit(".", 1)[-1].lower()
    return ext if ext in _DEVICE_FAULT_RECEIPT_ALLOWED_EXT else ""


def manual_fault_image_safe_ext(filename: str | None) -> str:
    raw = (filename or "").replace("\\", "/").rsplit("/", 1)[-1]
    if "." not in raw:
        return ""
    ext = "." + raw.rsplit(".", 1)[-1].lower()
    return ext if ext in _MANUAL_FAULT_IMAGE_ALLOWED_EXT else ""


def guess_image_media_type(filename: str | None, fallback: str | None = None) -> str:
    raw = (filename or "").replace("\\", "/").rsplit("/", 1)[-1]
    ext = ("." + raw.rsplit(".", 1)[-1].lower()) if "." in raw else ""
    return _IMAGE_MEDIA_TYPES.get(ext) or (fallback or "application/octet-stream")


def _resolve_receipt_file_path(root: Path, fault_id: int, stored_name: str) -> Path | None:
    sn = (stored_name or "").strip()
    if not sn or "/" in sn or "\\" in sn or ".." in sn:
        return None
    root_r = root.resolve()
    p = (root_r / str(int(fault_id)) / sn).resolve()
    try:
        p.relative_to(root_r)
    except ValueError:
        return None
    return p if p.is_file() else None


def resolve_device_fault_receipt_file_path(fault_id: int, stored_name: str) -> Path | None:
    return _resolve_receipt_file_path(device_fault_receipts_root(), fault_id, stored_name)


def resolve_manual_fault_receipt_file_path(fault_id: int, stored_name: str) -> Path | None:
    return _resolve_receipt_file_path(manual_fault_receipts_root(), fault_id, stored_name)


def resolve_manual_fault_image_file_path(fault_id: int, stored_name: str) -> Path | None:
    return _resolve_receipt_file_path(manual_fault_images_root(), fault_id, stored_name)


def _receipt_row_dict(r, *, report_source: str) -> dict:
    return {
        "id": r.id,
        "fault_id": r.fault_id,
        "biz_no": r.biz_no,
        "documentId": r.biz_no,
        "faultId": str(r.fault_id),
        "documentType": "报障单据",
        "documentName": r.original_name,
        "original_name": r.original_name,
        "file_size": r.file_size,
        "fileSize": f"{round(r.file_size / 1024, 1)} KB" if r.file_size else "--",
        "uploader_name": r.uploader_name,
        "uploader": r.uploader_name or "--",
        "uploadAt": _dt_text(r.created_at),
        "created_at": _dt_text(r.created_at),
        "remark": "",
        "report_source": report_source,
    }


async def list_jt_device_fault_receipts(
    db: AsyncSession,
    *,
    fault_id: int | None = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[dict], int]:
    q = select(JtDeviceFaultReceipt)
    if fault_id is not None:
        q = q.where(JtDeviceFaultReceipt.fault_id == int(fault_id))
    total = await db.scalar(select(func.count()).select_from(q.subquery())) or 0
    skip = max(0, (page - 1) * page_size)
    rows = (
        await db.execute(q.order_by(JtDeviceFaultReceipt.created_at.desc()).offset(skip).limit(max(1, page_size)))
    ).scalars().all()
    return [_receipt_row_dict(r, report_source="device") for r in rows], int(total)


async def list_manual_fault_receipts(
    db: AsyncSession,
    *,
    fault_id: int | None = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[dict], int]:
    q = select(ManualFaultReceipt)
    if fault_id is not None:
        q = q.where(ManualFaultReceipt.fault_id == int(fault_id))
    total = await db.scalar(select(func.count()).select_from(q.subquery())) or 0
    skip = max(0, (page - 1) * page_size)
    rows = (
        await db.execute(q.order_by(ManualFaultReceipt.created_at.desc()).offset(skip).limit(max(1, page_size)))
    ).scalars().all()
    return [_receipt_row_dict(r, report_source="manual") for r in rows], int(total)


async def list_merged_fault_receipts(
    db: AsyncSession,
    *,
    fault_id: int | None = None,
    report_source: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[dict], int]:
    src = (report_source or "").strip().lower()
    if src == "manual":
        return await list_manual_fault_receipts(db, fault_id=fault_id, page=page, page_size=page_size)
    if src == "device":
        return await list_jt_device_fault_receipts(db, fault_id=fault_id, page=page, page_size=page_size)

    # 无来源时合并；若带 fault_id 且两边 id 可能碰撞，仍合并（前端宜带 report_source）
    dev, _ = await list_jt_device_fault_receipts(db, fault_id=fault_id, page=1, page_size=500)
    man, _ = await list_manual_fault_receipts(db, fault_id=fault_id, page=1, page_size=500)
    merged = sorted(dev + man, key=lambda x: x.get("created_at") or "", reverse=True)
    total = len(merged)
    skip = max(0, (max(1, page) - 1) * max(1, page_size))
    return merged[skip : skip + max(1, page_size)], total


async def insert_jt_device_fault_receipt(
    db: AsyncSession,
    *,
    fault_id: int,
    biz_no: str,
    stored_name: str,
    original_name: str,
    file_size: int,
    mime_type: str | None,
    uploader_name: str | None,
) -> int:
    row = JtDeviceFaultReceipt(
        fault_id=int(fault_id),
        biz_no=biz_no,
        stored_name=stored_name,
        original_name=original_name,
        file_size=file_size,
        mime_type=mime_type,
        uploader_name=uploader_name,
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return int(row.id)


async def insert_manual_fault_receipt(
    db: AsyncSession,
    *,
    fault_id: int,
    biz_no: str,
    stored_name: str,
    original_name: str,
    file_size: int,
    mime_type: str | None,
    uploader_name: str | None,
) -> int:
    row = ManualFaultReceipt(
        fault_id=int(fault_id),
        biz_no=biz_no,
        stored_name=stored_name,
        original_name=original_name,
        file_size=file_size,
        mime_type=mime_type,
        uploader_name=uploader_name,
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return int(row.id)


async def get_jt_device_fault_receipt_by_id(db: AsyncSession, receipt_id: int) -> dict | None:
    r = await db.get(JtDeviceFaultReceipt, int(receipt_id))
    if r is None:
        return None
    d = {
        "id": r.id,
        "fault_id": r.fault_id,
        "biz_no": r.biz_no,
        "stored_name": r.stored_name,
        "original_name": r.original_name,
        "file_size": r.file_size,
        "uploader_name": r.uploader_name,
        "created_at": _dt_text(r.created_at),
        "report_source": "device",
    }
    return d


async def get_manual_fault_receipt_by_id(db: AsyncSession, receipt_id: int) -> dict | None:
    r = await db.get(ManualFaultReceipt, int(receipt_id))
    if r is None:
        return None
    return {
        "id": r.id,
        "fault_id": r.fault_id,
        "biz_no": r.biz_no,
        "stored_name": r.stored_name,
        "original_name": r.original_name,
        "file_size": r.file_size,
        "mime_type": r.mime_type,
        "uploader_name": r.uploader_name,
        "created_at": _dt_text(r.created_at),
        "report_source": "manual",
    }


async def insert_manual_fault_image(
    db: AsyncSession,
    *,
    fault_id: int,
    biz_no: str,
    stored_name: str,
    original_name: str,
    file_size: int,
    mime_type: str | None,
    uploader_name: str | None,
) -> int:
    row = ManualFaultImage(
        fault_id=int(fault_id),
        biz_no=biz_no,
        stored_name=stored_name,
        original_name=original_name,
        file_size=file_size,
        mime_type=mime_type,
        uploader_name=uploader_name,
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return int(row.id)


async def list_manual_fault_images(db: AsyncSession, *, fault_id: int) -> list[dict]:
    image_map = await _list_manual_images_by_fault_ids(db, [int(fault_id)])
    return image_map.get(int(fault_id), [])


async def get_manual_fault_image_by_id(db: AsyncSession, image_id: int) -> dict | None:
    r = await db.get(ManualFaultImage, int(image_id))
    if r is None:
        return None
    return {
        "id": r.id,
        "fault_id": r.fault_id,
        "biz_no": r.biz_no,
        "stored_name": r.stored_name,
        "original_name": r.original_name,
        "file_size": r.file_size,
        "mime_type": r.mime_type,
        "url": _manual_fault_image_view_url(int(r.id)),
        "created_at": _dt_text(r.created_at),
    }


async def get_manual_fault_by_id(db: AsyncSession, fault_id: int) -> dict | None:
    r = await db.get(ManualFaultReport, int(fault_id))
    if r is None:
        return None
    d = await _manual_fault_row_dict(db, r)
    vehicle_map, company_map = await _load_company_fleet_names(
        db, vehicle_ids=[d.get("vehicle_id")], company_ids=[d.get("company_id")]
    )
    return _attach_company_fleet(d, vehicle_map=vehicle_map, company_map=company_map)


async def mark_fault_completed_after_receipt(row) -> bool:
    """单据上传成功后：审核通过（及兼容旧状态）→ 已完结。"""
    st = (row.handle_status or "").strip()
    if st in _RECEIPT_ELIGIBLE:
        row.handle_status = "已完结"
        row.audited_at = china_now_naive()
        return True
    return False


async def mark_fault_awaiting_final_review(row) -> bool:
    """兼容旧调用名：上传单据后直接完结。"""
    return await mark_fault_completed_after_receipt(row)


async def _apply_fault_handle(
    row,
    *,
    handle_status: str,
    handler_remark: str | None,
    handler_name: str | None,
    auditor_name: str | None,
    audit_remark: str | None,
) -> tuple[bool, str]:
    st = (handle_status or "").strip()
    if not st:
        return False, "处理状态不能为空"
    # 兼容旧前端文案
    if st in ("已处理", "已通过", "完结"):
        st = "已完结"
    if st in ("审核驳回", "预审驳回"):
        st = "驳回"
    if st == "预审通过":
        st = "审核通过"
    now = china_now_naive()
    cur = (row.handle_status or "").strip()

    if st == "审核通过":
        if cur not in _AUDIT_ACTIONS_FROM:
            return False, "仅「待审核」的记录可审核通过"
        row.handle_status = "审核通过"
        row.handled_at = now
        if handler_name is not None:
            row.handler_name = (str(handler_name).strip()[:64] or None)
        row.handler_remark = (handler_remark or "").strip() or None
        if auditor_name is not None:
            row.auditor_name = (str(auditor_name).strip()[:64] or None)
        if audit_remark is not None:
            row.audit_remark = (audit_remark or "").strip() or None
    elif st == "驳回":
        if cur not in _AUDIT_ACTIONS_FROM:
            return False, "仅「待审核」的记录可驳回"
        row.handle_status = "驳回"
        row.handled_at = now
        # 驳回即流程结束，完结时间与审核时间一致
        row.audited_at = now
        if handler_name is not None:
            row.handler_name = (str(handler_name).strip()[:64] or None)
        row.handler_remark = (handler_remark or "").strip() or None
        if auditor_name is not None:
            row.auditor_name = (str(auditor_name).strip()[:64] or None)
        row.audit_remark = (audit_remark or handler_remark or "").strip() or None
    elif st == "已完结":
        # 完结由单据上传自动完成；兼容历史「待终审」手工完结
        if cur not in _RECEIPT_ELIGIBLE | _COMPLETED:
            return False, "仅「审核通过」且可上传单据的记录可完结（请先上传单据）"
        row.handle_status = "已完结"
        row.audited_at = now
        if auditor_name is not None:
            row.auditor_name = (str(auditor_name).strip()[:64] or None)
        row.audit_remark = (audit_remark or "").strip() or None
    elif st in ("终审驳回", "待终审"):
        return False, "已取消终审流程，请使用「审核通过」后上传单据完结"
    elif st in ("待预审", "未处理", "待审核"):
        return False, "不支持直接改回该状态，请使用业务流程"
    else:
        return False, f"不支持的状态: {st}"
    return True, ""


async def update_manual_fault_report_handle(
    db: AsyncSession,
    fault_id: int,
    *,
    handle_status: str,
    handler_remark: str | None = None,
    handler_name: str | None = None,
    auditor_name: str | None = None,
    audit_remark: str | None = None,
) -> tuple[bool, str]:
    row = await db.get(ManualFaultReport, int(fault_id))
    if row is None:
        return False, "记录不存在"
    ok, err = await _apply_fault_handle(
        row,
        handle_status=handle_status,
        handler_remark=handler_remark,
        handler_name=handler_name,
        auditor_name=auditor_name,
        audit_remark=audit_remark,
    )
    if not ok:
        return False, err
    await db.commit()
    return True, ""


async def update_jt_device_fault_handle(
    db: AsyncSession,
    fault_id: int,
    *,
    handle_status: str,
    handler_remark: str | None = None,
    handler_name: str | None = None,
    auditor_name: str | None = None,
    audit_remark: str | None = None,
) -> tuple[bool, str]:
    row = await db.get(JtDeviceFault, int(fault_id))
    if row is None:
        return False, "记录不存在"
    ok, err = await _apply_fault_handle(
        row,
        handle_status=handle_status,
        handler_remark=handler_remark,
        handler_name=handler_name,
        auditor_name=auditor_name,
        audit_remark=audit_remark,
    )
    if not ok:
        return False, err
    await db.commit()
    return True, ""


def gen_device_fault_biz_no() -> str:
    return f"BZ{china_now_naive().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(3).upper()}"


def gen_receipt_stored_name(ext: str) -> str:
    return f"{uuid.uuid4().hex}{ext}"

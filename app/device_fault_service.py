"""设备/人工报障合并列表、处理与单据（async SQLAlchemy）。"""
from __future__ import annotations

import logging
import secrets
import uuid
from pathlib import Path

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    JtDeviceFault,
    JtDeviceFaultReceipt,
    ManualFaultReport,
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
    {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".jpg", ".jpeg", ".png"}
)


def _dt_text(v) -> str | None:
    if v is None:
        return None
    try:
        return v.isoformat()[:19]
    except AttributeError:
        return str(v)[:19]


def _jt_device_fault_row_dict(r: JtDeviceFault) -> dict:
    return {
        "id": r.id,
        "biz_no": r.biz_no,
        "terminal_id": r.terminal_id,
        "vehicle_id": r.vehicle_id,
        "plate_no": r.plate_no or "",
        "company_id": r.company_id,
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


async def _manual_fault_row_dict(db: AsyncSession, r: ManualFaultReport) -> dict:
    tid = await _terminal_id_for_manual_report(db, r)
    return {
        "id": r.id,
        "biz_no": r.biz_no,
        "terminal_id": tid,
        "vehicle_id": r.vehicle_id,
        "plate_no": r.plate_no or "",
        "company_id": r.company_id,
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
    }


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
    if receipt_eligible_only:
        q = q.where(or_(JtDeviceFault.handle_status == "已通过", JtDeviceFault.handle_status == "已处理"))
    elif handle_status and handle_status.strip():
        hs = handle_status.strip()
        if hs == "未处理":
            q = q.where(or_(JtDeviceFault.handle_status == "未处理", JtDeviceFault.handle_status == "待处理"))
        else:
            q = q.where(JtDeviceFault.handle_status == hs)
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
    if receipt_eligible_only:
        q = q.where(or_(ManualFaultReport.handle_status == "已通过", ManualFaultReport.handle_status == "已处理"))
    elif handle_status and handle_status.strip():
        hs = handle_status.strip()
        if hs == "未处理":
            q = q.where(or_(ManualFaultReport.handle_status == "未处理", ManualFaultReport.handle_status == "待处理"))
        else:
            q = q.where(ManualFaultReport.handle_status == hs)
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
    if receipt_eligible_only:
        q = select(JtDeviceFault)
        q = _apply_device_fault_filters(
            q,
            terminal_id=terminal_id,
            terminal_id_contains=terminal_id_contains,
            plate_no_contains=plate_no_contains,
            biz_no_contains=biz_no_contains,
            start_time=start_time,
            end_time=end_time,
            handle_status=handle_status,
            receipt_eligible_only=True,
        )
        total = await db.scalar(select(func.count()).select_from(q.subquery())) or 0
        skip = max(0, (max(1, page) - 1) * max(1, page_size))
        rows = (
            await db.execute(q.order_by(JtDeviceFault.fault_time.desc()).offset(skip).limit(max(1, page_size)))
        ).scalars().all()
        out = []
        for r in rows:
            d = _jt_device_fault_row_dict(r)
            d["report_source"] = "device"
            d["discoverer"] = None
            out.append(d)
        return out, int(total)

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
        receipt_eligible_only=False,
    )
    qm = select(ManualFaultReport)
    qm = _apply_manual_fault_filters(
        qm,
        plate_no_contains=plate_no_contains,
        biz_no_contains=biz_no_contains,
        start_time=start_time,
        end_time=end_time,
        handle_status=handle_status,
        receipt_eligible_only=False,
    )

    dev_rows = (await db.execute(qd.order_by(JtDeviceFault.fault_time.desc()))).scalars().all()
    man_rows = (await db.execute(qm.order_by(ManualFaultReport.discovery_time.desc()))).scalars().all()

    dev_list: list[dict] = []
    for r in dev_rows:
        d = _jt_device_fault_row_dict(r)
        d["report_source"] = "device"
        d["discoverer"] = None
        dev_list.append(d)

    man_list: list[dict] = []
    for r in man_rows:
        d = await _manual_fault_row_dict(db, r)
        if _manual_passes_terminal_filter(d["terminal_id"], terminal_id, terminal_id_contains):
            man_list.append(d)

    merged = sorted(dev_list + man_list, key=lambda x: x.get("fault_time") or "", reverse=True)
    total = len(merged)
    skip = max(0, (max(1, page) - 1) * max(1, page_size))
    return merged[skip : skip + max(1, page_size)], total


async def get_jt_device_fault_by_id(db: AsyncSession, fault_id: int) -> dict | None:
    r = await db.get(JtDeviceFault, int(fault_id))
    if r is None:
        return None
    d = _jt_device_fault_row_dict(r)
    d["report_source"] = "device"
    d["discoverer"] = None
    return d


def jt_device_fault_receipt_eligible(handle_status: str | None) -> bool:
    st = (handle_status or "").strip()
    return st in ("已通过", "已处理")


def device_fault_receipts_root() -> Path:
    d = _BACKEND_ROOT / "data" / "device_fault_receipts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def device_fault_receipt_safe_ext(filename: str | None) -> str:
    raw = (filename or "").replace("\\", "/").rsplit("/", 1)[-1]
    if "." not in raw:
        return ""
    ext = "." + raw.rsplit(".", 1)[-1].lower()
    return ext if ext in _DEVICE_FAULT_RECEIPT_ALLOWED_EXT else ""


def resolve_device_fault_receipt_file_path(fault_id: int, stored_name: str) -> Path | None:
    sn = (stored_name or "").strip()
    if not sn or "/" in sn or "\\" in sn or ".." in sn:
        return None
    root = device_fault_receipts_root().resolve()
    p = (root / str(int(fault_id)) / sn).resolve()
    try:
        p.relative_to(root)
    except ValueError:
        return None
    return p if p.is_file() else None


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
    out = []
    for r in rows:
        out.append(
            {
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
            }
        )
    return out, int(total)


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


async def get_jt_device_fault_receipt_by_id(db: AsyncSession, receipt_id: int) -> dict | None:
    r = await db.get(JtDeviceFaultReceipt, int(receipt_id))
    if r is None:
        return None
    return {
        "id": r.id,
        "fault_id": r.fault_id,
        "biz_no": r.biz_no,
        "stored_name": r.stored_name,
        "original_name": r.original_name,
        "file_size": r.file_size,
        "uploader_name": r.uploader_name,
        "created_at": _dt_text(r.created_at),
    }


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
    if st == "已处理":
        st = "已通过"
    now = china_now_naive()

    if st == "待审核":
        allowed = ("未处理", "审核驳回", "待处理")
        if row.handle_status not in allowed:
            return False, "仅「待处理」或「审核驳回」的记录可提交审核"
        row.handle_status = "待审核"
        row.handled_at = now
        if handler_name is not None:
            row.handler_name = (str(handler_name).strip()[:64] or None)
        row.handler_remark = (handler_remark or "").strip() or None
        row.audited_at = None
        row.auditor_name = None
        row.audit_remark = None
    elif st == "已通过":
        if row.handle_status != "待审核":
            return False, "仅「待审核」记录可通过审核"
        row.handle_status = "已通过"
        row.audited_at = now
        if auditor_name is not None:
            row.auditor_name = (str(auditor_name).strip()[:64] or None)
        row.audit_remark = (audit_remark or "").strip() or None
    elif st == "审核驳回":
        if row.handle_status != "待审核":
            return False, "仅「待审核」记录可驳回"
        row.handle_status = "审核驳回"
        row.audited_at = now
        if auditor_name is not None:
            row.auditor_name = (str(auditor_name).strip()[:64] or None)
        row.audit_remark = (audit_remark or "").strip() or None
    elif st == "未处理":
        return False, "不支持直接改回未处理，请使用业务流程"
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

"""主动安全报警被挡住记录：入库跳过 / 待处理不可见。"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AlarmBlockedRecord, VehicleViolation
from app.timeutil import china_now_naive

REASON_LABELS = {
    "no_evidence": "无图片/视频，未进入平台",
    "no_vehicle": "未匹配到平台车辆，未进入平台",
    "unknown_type": "未知报警类型，未进入平台",
    "type_missing": "报警类型不在字典，未进入平台",
    "type_disabled": "报警类型已停用，未进入平台",
    "interval": "同车同类型间隔未到，未进入平台",
    "auto_false_stopped": "停车接打电话，已入库为误报（待处理看不到）",
    "insufficient_evidence": "证据不足（未满3图+1视频），已入库为误报（待处理看不到）",
}

REASON_CODES = tuple(REASON_LABELS.keys())


def reason_label(code: str) -> str:
    return REASON_LABELS.get(str(code or "").strip(), str(code or "未知原因"))


def _now() -> datetime:
    return china_now_naive()


async def upsert_alarm_blocked(
    db: AsyncSession,
    *,
    external_alarm_id: str,
    reason_code: str,
    plate_no: str = "",
    terminal_id: str | None = None,
    vehicle_id: int | None = None,
    company_name: str | None = None,
    violation_type_name: str | None = None,
    alarm_time: datetime | None = None,
    source: str = "jt808_adas",
    image_count: int = 0,
    video_count: int = 0,
    file_count: int = 0,
    speed: float | None = None,
    visible_on_platform: bool = False,
    reason_text: str | None = None,
) -> AlarmBlockedRecord:
    ext_id = str(external_alarm_id or "").strip()
    if not ext_id:
        raise ValueError("external_alarm_id required")
    code = str(reason_code or "").strip() or "no_evidence"
    text = (reason_text or "").strip() or reason_label(code)
    when = alarm_time or _now()
    row = await db.scalar(
        select(AlarmBlockedRecord).where(AlarmBlockedRecord.external_alarm_id == ext_id).limit(1)
    )
    if row is None:
        row = AlarmBlockedRecord(external_alarm_id=ext_id[:128], created_at=_now())
        db.add(row)
    row.plate_no = str(plate_no or "")[:16]
    row.terminal_id = (str(terminal_id).strip()[:32] if terminal_id else None)
    row.vehicle_id = vehicle_id
    row.company_name = (company_name or None)
    if row.company_name:
        row.company_name = row.company_name[:128]
    row.violation_type_name = (violation_type_name or None)
    if row.violation_type_name:
        row.violation_type_name = row.violation_type_name[:64]
    row.alarm_time = when
    row.source = (source or "jt808_adas")[:32]
    row.reason_code = code[:32]
    row.reason_text = text[:255]
    row.image_count = int(image_count or 0)
    row.video_count = int(video_count or 0)
    row.file_count = int(file_count or 0)
    row.speed = speed
    row.visible_on_platform = bool(visible_on_platform)
    row.last_seen_at = _now()
    await db.flush()
    return row


async def clear_alarm_blocked(db: AsyncSession, external_alarm_id: str) -> None:
    ext_id = str(external_alarm_id or "").strip()
    if not ext_id:
        return
    row = await db.scalar(
        select(AlarmBlockedRecord).where(AlarmBlockedRecord.external_alarm_id == ext_id).limit(1)
    )
    if row is not None:
        await db.delete(row)
        await db.flush()


def _row_out(row: AlarmBlockedRecord) -> dict[str, Any]:
    return {
        "id": row.id,
        "external_alarm_id": row.external_alarm_id,
        "plate_no": row.plate_no,
        "terminal_id": row.terminal_id,
        "vehicle_id": row.vehicle_id,
        "company_name": row.company_name,
        "violation_type_name": row.violation_type_name,
        "alarm_time": row.alarm_time.strftime("%Y-%m-%d %H:%M:%S") if row.alarm_time else None,
        "source": row.source,
        "reason_code": row.reason_code,
        "reason_text": row.reason_text or reason_label(row.reason_code),
        "image_count": row.image_count,
        "video_count": row.video_count,
        "file_count": row.file_count,
        "speed": row.speed,
        "visible_on_platform": bool(row.visible_on_platform),
        "last_seen_at": row.last_seen_at.strftime("%Y-%m-%d %H:%M:%S") if row.last_seen_at else None,
        "created_at": row.created_at.strftime("%Y-%m-%d %H:%M:%S") if row.created_at else None,
    }


async def list_alarm_blocked(
    db: AsyncSession,
    *,
    reason_code: str | None = None,
    plate_no: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    q = select(AlarmBlockedRecord)
    count_q = select(func.count()).select_from(AlarmBlockedRecord)
    code = (reason_code or "").strip()
    if code:
        q = q.where(AlarmBlockedRecord.reason_code == code)
        count_q = count_q.where(AlarmBlockedRecord.reason_code == code)
    plate = (plate_no or "").strip()
    if plate:
        like = f"%{plate}%"
        q = q.where(AlarmBlockedRecord.plate_no.ilike(like))
        count_q = count_q.where(AlarmBlockedRecord.plate_no.ilike(like))
    if start_time is not None:
        q = q.where(AlarmBlockedRecord.alarm_time >= start_time)
        count_q = count_q.where(AlarmBlockedRecord.alarm_time >= start_time)
    if end_time is not None:
        q = q.where(AlarmBlockedRecord.alarm_time <= end_time)
        count_q = count_q.where(AlarmBlockedRecord.alarm_time <= end_time)
    total = int((await db.scalar(count_q)) or 0)
    page = max(1, int(page or 1))
    page_size = max(1, min(200, int(page_size or 50)))
    rows = (
        await db.execute(
            q.order_by(AlarmBlockedRecord.alarm_time.desc(), AlarmBlockedRecord.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).scalars().all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_row_out(r) for r in rows],
        "reasons": [{"code": k, "label": v} for k, v in REASON_LABELS.items()],
    }


async def alarm_blocked_stats(db: AsyncSession, *, hours: int = 24) -> dict[str, Any]:
    since = _now() - timedelta(hours=max(1, int(hours)))
    total = int((await db.scalar(select(func.count()).select_from(AlarmBlockedRecord))) or 0)
    recent = int(
        (await db.scalar(
            select(func.count()).select_from(AlarmBlockedRecord).where(AlarmBlockedRecord.last_seen_at >= since)
        ))
        or 0
    )
    hidden = int(
        (await db.scalar(
            select(func.count()).select_from(AlarmBlockedRecord).where(
                AlarmBlockedRecord.visible_on_platform.is_(False)
            )
        ))
        or 0
    )
    by_reason_rows = (
        await db.execute(
            select(AlarmBlockedRecord.reason_code, func.count())
            .where(AlarmBlockedRecord.last_seen_at >= since)
            .group_by(AlarmBlockedRecord.reason_code)
        )
    ).all()
    by_reason = [
        {"code": code, "label": reason_label(str(code)), "count": int(cnt or 0)}
        for code, cnt in by_reason_rows
    ]
    by_reason.sort(key=lambda x: x["count"], reverse=True)
    return {
        "total": total,
        "recent_hours": int(hours),
        "recent_count": recent,
        "not_on_platform": hidden,
        "by_reason": by_reason,
    }


async def scan_recent_blocked_from_808(db: AsyncSession, *, hours: int = 12) -> dict[str, Any]:
    """对照 808 近 N 小时主动安全：平台没有的按原因记入异常表。"""
    from app.jt808_alarm_sync import inspect_and_record_blocked_items, _fetch_adas_alarm_items, _now as sync_now

    hours = max(1, min(72, int(hours or 12)))
    end_at = sync_now()
    start_at = end_at - timedelta(hours=hours)
    items, total, err = await _fetch_adas_alarm_items(start_at, end_at, max_pages=20)
    recorded, already_in, allowed = await inspect_and_record_blocked_items(db, items, end_at=end_at)
    return {
        "ok": err is None,
        "error": err,
        "hours": hours,
        "fetched": len(items),
        "808_total": total,
        "recorded": recorded,
        "already_in": already_in,
        "would_ingest": allowed,
    }


async def existing_external_ids(db: AsyncSession, ext_ids: list[str]) -> set[str]:
    ids = [x for x in ext_ids if x]
    if not ids:
        return set()
    found: set[str] = set()
    for i in range(0, len(ids), 400):
        chunk = ids[i : i + 400]
        rows = (
            await db.execute(
                select(VehicleViolation.external_alarm_id).where(VehicleViolation.external_alarm_id.in_(chunk))
            )
        ).scalars().all()
        found.update(str(x) for x in rows if x)
    return found

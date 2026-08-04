"""视频预览会话：监控页双写落库 + 报表按用户/时间查询。"""
from __future__ import annotations

from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Vehicle, VehicleDevice, VehicleLocation, VideoPreviewSession
from app.org_scope import (
    list_scoped_usernames,
    org_scope_row_clause,
    require_user_company_subtree_ids,
)
from app.plate_util import norm_plate
from app.timeutil import china_now_naive

router = APIRouter(prefix="/api/video-preview", tags=["video-preview"])


def _fmt_dt(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parse_dt(value: str | datetime | None, *, end_of_day: bool = False) -> datetime | None:
    if isinstance(value, datetime):
        return value
    text = (value or "").strip()
    if not text:
        return None
    if len(text) >= 19:
        try:
            return datetime.strptime(text[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    if len(text) >= 10:
        try:
            d = datetime.strptime(text[:10], "%Y-%m-%d")
            if end_of_day:
                return d.replace(hour=23, minute=59, second=59)
            return d
        except ValueError:
            pass
    return None


class VideoPreviewSessionPayload(BaseModel):
    username: str = Field(..., min_length=1)
    user_id: int | None = None
    real_name: str | None = None
    org_id: int | None = None
    org_name: str | None = None
    car_id: str = Field(..., min_length=1)
    plate_no: str | None = None
    plate_color: str | None = None
    duration_seconds: int = Field(..., ge=1)
    started_at: str | None = None
    ended_at: str | None = None
    session_key: str | None = None
    source: str | None = "realtime"


async def _resolve_plate_fields(
    db: AsyncSession,
    *,
    car_id: str,
    plate_no: str | None,
    plate_color: str | None,
) -> tuple[str, str]:
    plate = norm_plate(plate_no or "") or (plate_no or "").strip()
    color = (plate_color or "").strip()
    vehicle: Vehicle | None = None

    if plate:
        vehicle = await db.scalar(select(Vehicle).where(Vehicle.plate_no == plate).limit(1))
        if vehicle is None:
            vehicle = await db.scalar(
                select(Vehicle).where(func.upper(Vehicle.plate_no) == plate.upper()).limit(1)
            )

    cid = (car_id or "").strip()
    if vehicle is None and cid:
        device = await db.scalar(
            select(VehicleDevice)
            .where(
                or_(
                    VehicleDevice.device_no == cid,
                    VehicleDevice.device_sn == cid,
                    VehicleDevice.sim_no == cid,
                )
            )
            .limit(1)
        )
        if device is not None:
            vehicle = await db.scalar(select(Vehicle).where(Vehicle.id == device.vehicle_id).limit(1))
        if vehicle is None:
            loc = await db.scalar(
                select(VehicleLocation).where(VehicleLocation.terminal_id == cid).limit(1)
            )
            if loc is not None and loc.vehicle_id is not None:
                vehicle = await db.scalar(select(Vehicle).where(Vehicle.id == loc.vehicle_id).limit(1))

    if vehicle is not None:
        if not plate:
            plate = (vehicle.plate_no or "").strip()
        if not color:
            color = (vehicle.plate_color or "").strip()
    return plate, color


def _row_to_item(row: VideoPreviewSession) -> dict:
    return {
        "id": row.id,
        "session_key": row.session_key,
        "account": row.username,
        "username": row.username,
        "user": row.username,
        "name": row.real_name or row.username,
        "user_id": row.user_id,
        "org_id": row.org_id,
        "org_name": row.org_name or "",
        "group": row.org_name or "",
        "company": row.org_name or "",
        "car_id": row.car_id,
        "tid": row.car_id,
        "carno": row.plate_no or "",
        "vehicle": row.plate_no or "",
        "plate": row.plate_no or "",
        "plate_no": row.plate_no or "",
        "plateColor": row.plate_color or "",
        "plate_color": row.plate_color or "",
        "stime": _fmt_dt(row.started_at),
        "etime": _fmt_dt(row.ended_at),
        "startAt": _fmt_dt(row.started_at),
        "endAt": _fmt_dt(row.ended_at),
        "video_time": int(row.duration_seconds or 0),
        "durationSeconds": int(row.duration_seconds or 0),
        "duration": int(row.duration_seconds or 0),
        "source": row.source or "realtime",
    }


@router.post("/session")
async def upsert_video_preview_session(
    payload: VideoPreviewSessionPayload,
    db: AsyncSession = Depends(get_db),
):
    username = (payload.username or "").strip()
    car_id = (payload.car_id or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username不可为空")
    if not car_id:
        raise HTTPException(status_code=400, detail="car_id不可为空")

    duration = max(1, int(payload.duration_seconds or 0))
    ended_at = _parse_dt(payload.ended_at) or china_now_naive()
    started_at = _parse_dt(payload.started_at)
    if started_at is None:
        started_at = ended_at - timedelta(seconds=duration)
    if started_at > ended_at:
        started_at = ended_at - timedelta(seconds=duration)

    session_key = (payload.session_key or "").strip()
    if not session_key:
        session_key = f"{username}|{car_id}|{_fmt_dt(started_at)}"

    plate, color = await _resolve_plate_fields(
        db,
        car_id=car_id,
        plate_no=payload.plate_no,
        plate_color=payload.plate_color,
    )

    row = await db.scalar(
        select(VideoPreviewSession).where(VideoPreviewSession.session_key == session_key).limit(1)
    )
    if row is None:
        row = VideoPreviewSession(
            session_key=session_key[:128],
            username=username[:64],
            user_id=payload.user_id,
            real_name=(payload.real_name or "").strip()[:64] or None,
            org_id=payload.org_id,
            org_name=(payload.org_name or "").strip()[:128] or None,
            car_id=car_id[:64],
            plate_no=plate[:32] if plate else None,
            plate_color=color[:16] if color else None,
            started_at=started_at,
            ended_at=ended_at,
            duration_seconds=duration,
            source=(payload.source or "realtime")[:32],
        )
        db.add(row)
    else:
        # checkpoint / 重复上报：取更大时长与更晚结束时间
        if duration > int(row.duration_seconds or 0):
            row.duration_seconds = duration
        if ended_at > row.ended_at:
            row.ended_at = ended_at
        if started_at < row.started_at:
            row.started_at = started_at
        if payload.user_id is not None:
            row.user_id = payload.user_id
        if (payload.real_name or "").strip():
            row.real_name = (payload.real_name or "").strip()[:64]
        if payload.org_id is not None:
            row.org_id = payload.org_id
        if (payload.org_name or "").strip():
            row.org_name = (payload.org_name or "").strip()[:128]
        if plate:
            row.plate_no = plate[:32]
        if color:
            row.plate_color = color[:16]
        row.car_id = car_id[:64]
        row.source = (payload.source or row.source or "realtime")[:32]

    await db.flush()
    return {"ok": True, "id": row.id, "session_key": row.session_key, "item": _row_to_item(row)}


@router.get("/sessions")
async def list_video_preview_sessions(
    usernames: str | None = Query(default=None),
    username: str | None = Query(default=None),
    org_id: int | None = Query(default=None),
    start_at: str | None = Query(default=None),
    end_at: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=500, ge=1, le=2000),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    _, scoped_ids = await require_user_company_subtree_ids(
        db, x_org_id=x_org_id, x_user_id=x_user_id
    )
    scoped_names = await list_scoped_usernames(db, scoped_ids)
    allowed_lower = {n.lower() for n in scoped_names}

    name_list: list[str] = []
    if usernames:
        name_list.extend([p.strip() for p in str(usernames).split(",") if p.strip()])
    if username and username.strip():
        name_list.append(username.strip())
    # 去重保序，并与可见用户求交（防伪造 usernames 越权）
    seen = set()
    uniq_names = []
    for name in name_list:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        if allowed_lower and key not in allowed_lower:
            continue
        uniq_names.append(name)

    scope_clause = org_scope_row_clause(
        VideoPreviewSession.org_id,
        VideoPreviewSession.username,
        scoped_ids,
        scoped_names,
    )
    stmt = select(VideoPreviewSession).where(scope_clause)
    count_stmt = select(func.count()).select_from(VideoPreviewSession).where(scope_clause)
    if uniq_names:
        lowered = [n.lower() for n in uniq_names]
        stmt = stmt.where(func.lower(VideoPreviewSession.username).in_(lowered))
        count_stmt = count_stmt.where(func.lower(VideoPreviewSession.username).in_(lowered))
    elif name_list:
        # 请求了用户名但全部不在范围内 → 空结果
        stmt = stmt.where(VideoPreviewSession.id.in_([]))
        count_stmt = count_stmt.where(VideoPreviewSession.id.in_([]))
    if org_id is not None:
        if org_id not in scoped_ids:
            raise HTTPException(status_code=403, detail="无权查看该公司视频预览数据")
        stmt = stmt.where(VideoPreviewSession.org_id == org_id)
        count_stmt = count_stmt.where(VideoPreviewSession.org_id == org_id)

    start_dt = _parse_dt(start_at)
    end_dt = _parse_dt(end_at, end_of_day=True)
    # 与查询区间有交集：started_at <= end && ended_at >= start
    if start_dt is not None:
        stmt = stmt.where(VideoPreviewSession.ended_at >= start_dt)
        count_stmt = count_stmt.where(VideoPreviewSession.ended_at >= start_dt)
    if end_dt is not None:
        stmt = stmt.where(VideoPreviewSession.started_at <= end_dt)
        count_stmt = count_stmt.where(VideoPreviewSession.started_at <= end_dt)

    total = int((await db.scalar(count_stmt)) or 0)
    rows = (
        await db.execute(
            stmt.order_by(VideoPreviewSession.ended_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).scalars().all()
    items = [_row_to_item(row) for row in rows]
    return {"ok": True, "items": items, "total": total, "page": page, "page_size": page_size}

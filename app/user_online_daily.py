"""用户按日在线时长汇总。"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time as dt_time, timedelta

from app.timeutil import china_now_naive

from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import OrgCompany, SysRole, SysUser, UserLoginLog, UserOnlineDaily

# 超过该时间无心跳，视为会话已结束（未点退出的关浏览器等）
SESSION_IDLE_TIMEOUT = timedelta(minutes=30)


def iter_day_segments(start: datetime, end: datetime) -> list[tuple[date, int]]:
    if start is None or end is None:
        return []
    start_naive = start.replace(tzinfo=None) if getattr(start, "tzinfo", None) else start
    end_naive = end.replace(tzinfo=None) if getattr(end, "tzinfo", None) else end
    if end_naive <= start_naive:
        return []
    segments: list[tuple[date, int]] = []
    cur = start_naive
    while cur < end_naive:
        next_day = datetime.combine(cur.date() + timedelta(days=1), dt_time.min)
        segment_end = min(end_naive, next_day)
        seconds = int((segment_end - cur).total_seconds())
        if seconds > 0:
            segments.append((cur.date(), seconds))
        cur = segment_end
    return segments


def merge_intervals(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    cleaned = []
    for start, end in intervals:
        if start is None or end is None:
            continue
        start_naive = start.replace(tzinfo=None) if getattr(start, "tzinfo", None) else start
        end_naive = end.replace(tzinfo=None) if getattr(end, "tzinfo", None) else end
        if end_naive <= start_naive:
            continue
        cleaned.append((start_naive, end_naive))
    if not cleaned:
        return []
    cleaned.sort(key=lambda item: item[0])
    merged = [cleaned[0]]
    for start, end in cleaned[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def clip_interval_to_day(start: datetime, end: datetime, stat_date: date) -> tuple[datetime, datetime] | None:
    day_start = datetime.combine(stat_date, dt_time.min)
    day_end = day_start + timedelta(days=1)
    seg_start = max(start, day_start)
    seg_end = min(end, day_end)
    if seg_end <= seg_start:
        return None
    return seg_start, seg_end


def merged_seconds_for_day(sessions: list[tuple[datetime, datetime]], stat_date: date) -> int:
    day_intervals: list[tuple[datetime, datetime]] = []
    for start, end in sessions:
        clipped = clip_interval_to_day(start, end, stat_date)
        if clipped is not None:
            day_intervals.append(clipped)
    total = 0
    for start, end in merge_intervals(day_intervals):
        total += int((end - start).total_seconds())
    return total


def _naive_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=None) if getattr(value, "tzinfo", None) else value


def _resolve_session_end(
    login_row: UserLoginLog,
    *,
    next_login_at: datetime | None,
    now: datetime,
) -> datetime:
    if login_row.logout_at is not None:
        return _naive_dt(login_row.logout_at) or now
    if next_login_at is not None:
        return _naive_dt(next_login_at) or now
    last_active = _naive_dt(login_row.last_heartbeat_at) or _naive_dt(login_row.login_at) or now
    if (now - last_active) <= SESSION_IDLE_TIMEOUT:
        return now
    return last_active


def _normalize_stale_session_clock(login_row: UserLoginLog) -> None:
    """无前端心跳的历史未退出会话，不应把结束时间延伸到当前。"""
    if login_row.logout_at is not None:
        return
    if login_row.online_seconds is not None:
        return
    login_at = _naive_dt(login_row.login_at)
    last_hb = _naive_dt(login_row.last_heartbeat_at)
    if login_at is None:
        return
    if last_hb is None or last_hb > login_at:
        login_row.last_heartbeat_at = login_row.login_at


def _effective_role_code(role: SysRole | None, username: str) -> str:
    un = (username or "").strip().lower()
    if un == "admin":
        return "admin"
    if role is None:
        return ""
    code_raw = (role.code or "").strip()
    if code_raw.lower() == "admin":
        return "admin"
    name = (role.name or "").strip()
    if getattr(role, "is_global", False) and not code_raw and name in ("系统管理员", "管理员"):
        return "admin"
    return code_raw


async def _lookup_org_name(db: AsyncSession, org_id: int | None) -> str | None:
    if org_id is None:
        return None
    co = await db.scalar(select(OrgCompany).where(OrgCompany.id == org_id).limit(1))
    name = (co.name if co else "") or ""
    return name.strip() or None


async def resolve_user_org_profile(
    db: AsyncSession,
    username: str,
) -> tuple[int | None, str | None, int | None, str | None]:
    uname = (username or "").strip()
    if not uname:
        return None, None, None, None
    user = await db.scalar(
        select(SysUser)
        .options(selectinload(SysUser.org), selectinload(SysUser.role))
        .where(SysUser.username == uname)
        .limit(1)
    )
    if user is not None:
        org_id = user.org_id
        org_name = (user.org.name if user.org else None) or None
        if not org_name and org_id is not None:
            org_name = await _lookup_org_name(db, org_id)
        role = user.role
        if org_id is None and role and role.org_id is not None:
            org_id = int(role.org_id)
            org_name = await _lookup_org_name(db, org_id) or org_name
        if org_id is None and _effective_role_code(role, uname).strip().lower() == "admin":
            first_org = await db.scalar(select(OrgCompany).order_by(OrgCompany.id.asc()).limit(1))
            if first_org is not None:
                org_id = int(first_org.id)
                org_name = (first_org.name or "") or org_name
        return (
            user.id,
            user.real_name or user.username,
            org_id,
            org_name,
        )
    log = await db.scalar(
        select(UserLoginLog)
        .where(UserLoginLog.username == uname[:64])
        .order_by(UserLoginLog.login_at.desc())
        .limit(1)
    )
    if log is not None:
        org_name = (log.org_name or "").strip() or None
        org_id = log.org_id
        if not org_name and org_id is not None:
            org_name = await _lookup_org_name(db, org_id)
        return log.user_id, log.real_name or log.username, org_id, org_name
    return None, None, None, None


async def backfill_login_log_org_names(db: AsyncSession) -> int:
    """补全历史登录明细中缺失的 user_id / org_id / org_name。"""
    rows = (
        await db.execute(
            select(UserLoginLog).where(
                or_(
                    UserLoginLog.org_name.is_(None),
                    UserLoginLog.org_name == "",
                    UserLoginLog.org_id.is_(None),
                    UserLoginLog.user_id.is_(None),
                )
            )
        )
    ).scalars().all()
    if not rows:
        return 0
    cache: dict[str, tuple[int | None, str | None, int | None, str | None]] = {}
    updated = 0
    for row in rows:
        uname = (row.username or "").strip()
        if not uname:
            continue
        if uname not in cache:
            cache[uname] = await resolve_user_org_profile(db, uname)
        user_id, _, org_id, org_name = cache[uname]
        changed = False
        if user_id is not None and row.user_id is None:
            row.user_id = user_id
            changed = True
        if org_id is not None and row.org_id is None:
            row.org_id = org_id
            changed = True
        if org_name and not (row.org_name or "").strip():
            row.org_name = org_name[:128]
            changed = True
        if changed:
            updated += 1
    if updated:
        await db.flush()
    return updated


async def get_or_create_daily_row(
    db: AsyncSession,
    *,
    username: str,
    stat_date: date,
    user_id: int | None = None,
    real_name: str | None = None,
    org_id: int | None = None,
    org_name: str | None = None,
) -> UserOnlineDaily:
    uname = (username or "")[:64]
    row = await db.scalar(
        select(UserOnlineDaily)
        .where(UserOnlineDaily.username == uname, UserOnlineDaily.stat_date == stat_date)
        .limit(1)
    )
    if row is not None:
        if user_id is not None and row.user_id is None:
            row.user_id = user_id
        if real_name and not row.real_name:
            row.real_name = real_name[:64]
        if org_id is not None and row.org_id is None:
            row.org_id = org_id
        if org_name and not row.org_name:
            row.org_name = (org_name or "")[:128] or None
        return row
    row = UserOnlineDaily(
        user_id=user_id,
        username=uname,
        real_name=(real_name or "")[:64] or None,
        org_id=org_id,
        org_name=(org_name or "")[:128] or None,
        stat_date=stat_date,
        online_seconds=0,
        login_count=0,
    )
    db.add(row)
    await db.flush()
    return row


async def recompute_user_daily_for_date(
    db: AsyncSession,
    username: str,
    stat_date: date,
) -> UserOnlineDaily | None:
    uname = (username or "").strip()
    if not uname:
        return None

    rows = (
        await db.execute(
            select(UserLoginLog)
            .where(UserLoginLog.username == uname)
            .order_by(UserLoginLog.login_at.asc())
        )
    ).scalars().all()
    if not rows:
        return None

    now = china_now_naive()
    day_start = datetime.combine(stat_date, dt_time.min)
    day_end = day_start + timedelta(days=1)
    sessions: list[tuple[datetime, datetime]] = []
    login_count = 0
    profile: UserLoginLog | None = None

    for index, row in enumerate(rows):
        if row.login_at is None:
            continue
        start = row.login_at.replace(tzinfo=None) if getattr(row.login_at, "tzinfo", None) else row.login_at
        next_login = rows[index + 1].login_at if index + 1 < len(rows) else None
        end = _resolve_session_end(row, next_login_at=next_login, now=now)
        sessions.append((start, end))
        if day_start <= start < day_end:
            login_count += 1
            profile = row

    online_seconds = merged_seconds_for_day(sessions, stat_date)
    existing = await db.scalar(
        select(UserOnlineDaily)
        .where(UserOnlineDaily.username == uname[:64], UserOnlineDaily.stat_date == stat_date)
        .limit(1)
    )
    if online_seconds <= 0 and login_count <= 0:
        if existing is not None:
            await db.delete(existing)
            await db.flush()
        return None
    if existing is None:
        user_id, real_name, org_id, org_name = await resolve_user_org_profile(db, uname)
        existing = await get_or_create_daily_row(
            db,
            username=uname,
            stat_date=stat_date,
            user_id=user_id or (profile.user_id if profile else None),
            real_name=real_name or (profile.real_name if profile else None),
            org_id=org_id or (profile.org_id if profile else None),
            org_name=org_name or (profile.org_name if profile else None),
        )
    user_id, real_name, org_id, org_name = await resolve_user_org_profile(db, uname)
    existing.online_seconds = online_seconds
    existing.login_count = login_count
    if user_id is not None:
        existing.user_id = user_id
    if real_name:
        existing.real_name = real_name[:64]
    if org_id is not None:
        existing.org_id = org_id
    if org_name:
        existing.org_name = (org_name or "")[:128] or None
    elif profile is not None and profile.org_name:
        existing.org_name = (profile.org_name or "")[:128] or None
    await db.flush()
    return existing


async def close_open_sessions_for_user(
    db: AsyncSession,
    username: str,
    *,
    logout_at: datetime | None = None,
    exclude_id: int | None = None,
) -> None:
    now = logout_at or china_now_naive()
    stmt = select(UserLoginLog).where(
        UserLoginLog.username == (username or "")[:64],
        UserLoginLog.logout_at.is_(None),
    )
    if exclude_id is not None:
        stmt = stmt.where(UserLoginLog.id != exclude_id)
    open_rows = (await db.execute(stmt.order_by(UserLoginLog.login_at.asc()))).scalars().all()
    for row in open_rows:
        start = row.login_at or now
        start_naive = start.replace(tzinfo=None) if getattr(start, "tzinfo", None) else start
        if now > start_naive:
            await recompute_user_daily_for_date(db, row.username, start_naive.date())
            if now.date() != start_naive.date():
                await recompute_user_daily_for_date(db, row.username, now.date())
        row.logout_at = now
        row.last_heartbeat_at = now


async def record_login_daily(db: AsyncSession, login_row: UserLoginLog) -> None:
    login_at = login_row.login_at or china_now_naive()
    login_date = login_at.date() if hasattr(login_at, "date") else login_at
    await recompute_user_daily_for_date(db, login_row.username, login_date)


async def sync_login_session_to_daily(
    db: AsyncSession,
    login_row: UserLoginLog,
    *,
    until: datetime | None = None,
) -> None:
    now = until or china_now_naive()
    login_row.last_heartbeat_at = now
    if login_row.login_at is not None:
        login_at = login_row.login_at
        login_naive = login_at.replace(tzinfo=None) if getattr(login_at, "tzinfo", None) else login_at
        await recompute_user_daily_for_date(db, login_row.username, login_naive.date())
        if now.date() != login_naive.date():
            await recompute_user_daily_for_date(db, login_row.username, now.date())


async def finalize_stale_open_sessions(db: AsyncSession, *, now: datetime | None = None) -> int:
    """补全未退出且已长时间无心跳的登录记录结束时间。"""
    current = now or china_now_naive()
    rows = (
        await db.execute(
            select(UserLoginLog)
            .where(UserLoginLog.logout_at.is_(None))
            .order_by(UserLoginLog.username.asc(), UserLoginLog.login_at.asc())
        )
    ).scalars().all()
    if not rows:
        return 0

    by_user: dict[str, list[UserLoginLog]] = defaultdict(list)
    for row in rows:
        key = (row.username or "").strip() or f"id:{row.id}"
        by_user[key].append(row)

    closed = 0
    for user_rows in by_user.values():
        user_rows.sort(key=lambda item: item.login_at or datetime.min)
        for index, row in enumerate(user_rows):
            _normalize_stale_session_clock(row)
            next_login = user_rows[index + 1].login_at if index + 1 < len(user_rows) else None
            if next_login is not None:
                row.logout_at = next_login
                row.last_heartbeat_at = next_login
                closed += 1
                continue
            end = _resolve_session_end(row, next_login_at=None, now=current)
            last_active = _naive_dt(row.last_heartbeat_at) or _naive_dt(row.login_at) or current
            if end <= last_active and (current - last_active) > SESSION_IDLE_TIMEOUT:
                row.logout_at = last_active
                closed += 1
    await db.flush()
    return closed


async def rebuild_daily_from_login_logs(db: AsyncSession) -> int:
    """从登录明细重建按日汇总：同一用户同一天合并重叠时段，不重复累加。"""
    await db.execute(delete(UserOnlineDaily))
    rows = (await db.execute(select(UserLoginLog).order_by(UserLoginLog.login_at.asc()))).scalars().all()
    if not rows:
        await db.flush()
        return 0

    now = china_now_naive()
    await finalize_stale_open_sessions(db, now=now)

    by_user: dict[str, list[UserLoginLog]] = defaultdict(list)
    for row in rows:
        _normalize_stale_session_clock(row)
        key = (row.username or "").strip() or f"id:{row.id}"
        by_user[key].append(row)

    affected_days: set[tuple[str, date]] = set()
    rebuilt = 0
    for user_rows in by_user.values():
        user_rows.sort(key=lambda item: item.login_at or datetime.min)
        for index, row in enumerate(user_rows):
            start = row.login_at
            if start is None:
                continue
            next_login = user_rows[index + 1].login_at if index + 1 < len(user_rows) else None
            end = _resolve_session_end(row, next_login_at=next_login, now=now)
            for stat_date, _ in iter_day_segments(start, end):
                affected_days.add((row.username, stat_date))
            rebuilt += 1

    for username, stat_date in sorted(affected_days, key=lambda item: (item[1], item[0])):
        await recompute_user_daily_for_date(db, username, stat_date)

    await db.flush()
    return rebuilt

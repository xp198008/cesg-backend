"""用户登录/操作审计日志辅助函数。"""
from __future__ import annotations

from datetime import datetime

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import UserLoginLog, UserOperationLog


def client_ip(request: Request) -> str:
    xff = (request.headers.get("x-forwarded-for") or "").strip()
    if xff:
        return (xff.split(",")[0] or "").strip()[:64] or ""
    xri = (request.headers.get("x-real-ip") or "").strip()
    if xri:
        return xri[:64]
    try:
        host = request.client.host if request.client else ""
    except Exception:
        host = ""
    return (host or "")[:64]


def format_duration_seconds(total_seconds: int | None) -> str:
    if total_seconds is None or total_seconds < 0:
        return "--"
    if total_seconds == 0:
        return "0秒"
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}天")
    if hours:
        parts.append(f"{hours}小时")
    if minutes:
        parts.append(f"{minutes}分钟")
    if seconds or not parts:
        parts.append(f"{seconds}秒")
    return "".join(parts)


def duration_seconds_between(
    start: datetime | None,
    end: datetime | None = None,
    *,
    online_seconds: int | None = None,
    now: datetime | None = None,
) -> int | None:
    if online_seconds is not None and online_seconds >= 0:
        return int(online_seconds)
    if start is None:
        return None
    start_naive = start.replace(tzinfo=None) if getattr(start, "tzinfo", None) else start
    if end is None:
        end = now or datetime.now()
    end_naive = end.replace(tzinfo=None) if getattr(end, "tzinfo", None) else end
    if end_naive < start_naive:
        return None
    return int((end_naive - start_naive).total_seconds())


def duration_between(
    start: datetime | None,
    end: datetime | None = None,
    *,
    online_seconds: int | None = None,
    now: datetime | None = None,
) -> str:
    total = duration_seconds_between(start, end, online_seconds=online_seconds, now=now)
    if total is None:
        return "--"
    return format_duration_seconds(total)


async def append_operation_log(
    db: AsyncSession,
    *,
    username: str,
    operation_content: str,
    user_id: int | None = None,
    real_name: str | None = None,
    org_id: int | None = None,
    org_name: str | None = None,
    module: str | None = None,
    menu: str | None = None,
    action: str | None = None,
    operation_ip: str | None = None,
    result: str = "成功",
    vehicle: str | None = None,
    plate_color: str | None = None,
    device_no: str | None = None,
    source: str = "manual",
) -> UserOperationLog:
    row = UserOperationLog(
        user_id=user_id,
        username=(username or "")[:64],
        real_name=(real_name or "")[:64] or None,
        org_id=org_id,
        org_name=(org_name or "")[:128] or None,
        module=(module or "")[:64] or None,
        menu=(menu or "")[:64] or None,
        action=(action or "")[:64] or None,
        operation_content=(operation_content or "")[:2000],
        operation_ip=(operation_ip or "")[:64] or None,
        result=(result or "成功")[:16],
        vehicle=(vehicle or "")[:32] or None,
        plate_color=(plate_color or "")[:16] or None,
        device_no=(device_no or "")[:64] or None,
        source=(source or "manual")[:16],
    )
    db.add(row)
    await db.flush()
    return row

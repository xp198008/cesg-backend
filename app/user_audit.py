"""用户登录/操作审计日志辅助函数。"""
from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal

from app.timeutil import china_now_naive

from fastapi import Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OrgCompany, SysUser, UserLoginLog, UserOperationLog
from app.vehicle_alloc_scope import parse_user_id_header

logger = logging.getLogger(__name__)


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
        end = now or china_now_naive()
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


def op_disp(value) -> str:
    if value is None or value == "":
        return "空"
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, Decimal):
        text = format(value, "f").rstrip("0").rstrip(".")
        return text or "0"
    if isinstance(value, (list, tuple)):
        return "、".join(str(item) for item in value if str(item).strip()) or "空"
    return str(value).strip() or "空"


def compose_op_content(head: str, parts: list[str] | None = None) -> str:
    text = (head or "").strip() or "操作"
    extras = [str(part).strip() for part in (parts or []) if str(part).strip()]
    if extras:
        text = f"{text}；" + "；".join(extras)
    if len(text) > 1900:
        return text[:1900] + f"…（共{len(extras)}项）"
    return text


def format_create_content(head: str, fields: dict[str, str] | None = None) -> str:
    parts = [f"{key}：{value}" for key, value in (fields or {}).items() if value and value != "空"]
    return compose_op_content(head, parts)


def format_update_content(head: str, old: dict[str, str], new: dict[str, str]) -> str:
    parts: list[str] = []
    for key in list(old.keys()) + [k for k in new.keys() if k not in old]:
        before = old.get(key, "空")
        after = new.get(key, "空")
        if before != after:
            parts.append(f"{key}「{before}」→「{after}」")
    return compose_op_content(head, parts)


async def resolve_actor(db: AsyncSession, x_user_id: str | None) -> SysUser | None:
    uid = parse_user_id_header(x_user_id)
    if uid is None:
        return None
    return await db.scalar(select(SysUser).where(SysUser.id == uid).limit(1))


async def write_biz_operation_log(
    db: AsyncSession,
    *,
    request: Request | None,
    x_user_id: str | None,
    action: str,
    content: str,
    module: str = "基础数据管理",
    menu: str,
    vehicle: str | None = None,
    plate_color: str | None = None,
    device_no: str | None = None,
) -> None:
    try:
        user = await resolve_actor(db, x_user_id)
        org_name = None
        if user is not None and user.org_id:
            org_name = await db.scalar(
                select(OrgCompany.name).where(OrgCompany.id == user.org_id).limit(1)
            )
        await append_operation_log(
            db,
            username=(user.username if user else "")[:64] or "未知用户",
            operation_content=content[:2000],
            user_id=user.id if user else None,
            real_name=(user.real_name if user else None),
            org_id=user.org_id if user else None,
            org_name=org_name,
            module=module,
            menu=menu,
            action=action,
            operation_ip=client_ip(request) if request is not None else None,
            result="成功",
            vehicle=(vehicle or "")[:32] or None,
            plate_color=(plate_color or "")[:16] or None,
            device_no=(device_no or "")[:64] or None,
            source="manual",
        )
    except Exception:  # noqa: BLE001
        logger.exception("写入业务操作日志失败")

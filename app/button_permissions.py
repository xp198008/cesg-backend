"""用户信息 / 车辆信息按钮权限。

与「规则转换」(114) 相同：角色树里单独勾选才生效，不随页面父节点继承。
已有该页面权限的角色，启动时补上全部按钮，避免现网账号突然不能操作。
"""
from __future__ import annotations

import json

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import SysRole, SysUser
from app.vehicle_alloc_scope import parse_user_id_header

USER_ADD = "1011"
USER_BATCH_DELETE = "1012"
USER_SET_PASSWORD = "1013"
USER_DELETE = "1014"
USER_EDIT = "1015"
VEHICLE_ADD = "1021"
VEHICLE_BATCH_DELETE = "1022"
VEHICLE_DELETE = "1023"
VEHICLE_EDIT = "1024"

PAGE_USER = "101"
PAGE_VEHICLE = "102"
PAGE_BASE = "10"

USER_BUTTONS = (USER_ADD, USER_BATCH_DELETE, USER_SET_PASSWORD, USER_DELETE, USER_EDIT)
VEHICLE_BUTTONS = (VEHICLE_ADD, VEHICLE_BATCH_DELETE, VEHICLE_DELETE, VEHICLE_EDIT)


def _parse_ids(raw: str | None) -> list[str]:
    try:
        arr = json.loads(raw or "[]")
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(arr, list):
        return []
    out: list[str] = []
    for item in arr:
        if item is None:
            continue
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    return out


async def backfill_button_permissions(db: AsyncSession) -> int:
    """给已经能进用户信息/车辆信息的角色补上对应按钮，保持现有操作不变。"""
    roles = (await db.scalars(select(SysRole))).all()
    updated = 0
    for role in roles:
        ids = _parse_ids(role.permissions)
        if not ids:
            continue
        extra: list[str] = []
        if PAGE_USER in ids or PAGE_BASE in ids:
            extra.extend(item for item in USER_BUTTONS if item not in ids)
        if PAGE_VEHICLE in ids or PAGE_BASE in ids:
            extra.extend(item for item in VEHICLE_BUTTONS if item not in ids)
        if not extra:
            continue
        role.permissions = json.dumps([*ids, *extra], ensure_ascii=False)
        updated += 1
    return updated


async def _owned_permission_ids(db: AsyncSession, x_user_id: str | None) -> set[str] | None:
    """None 表示 admin，放行。"""
    uid = parse_user_id_header(x_user_id)
    if uid is None:
        raise HTTPException(status_code=401, detail="缺少登录用户")
    user = await db.scalar(
        select(SysUser).options(selectinload(SysUser.role)).where(SysUser.id == uid).limit(1)
    )
    if user is None:
        raise HTTPException(status_code=401, detail="用户不存在")
    username = (user.username or "").strip().lower()
    role_code = ((user.role.code if user.role else "") or "").strip().lower()
    if username == "admin" or role_code == "admin":
        return None
    return set(_parse_ids(user.role.permissions if user.role else None))


async def require_button_permission(
    db: AsyncSession,
    x_user_id: str | None,
    perm_ids: tuple[str, ...] | list[str],
    label: str,
) -> None:
    owned = await _owned_permission_ids(db, x_user_id)
    if owned is None:
        return
    if any(pid in owned for pid in perm_ids):
        return
    raise HTTPException(status_code=403, detail=f"需要「{label}」权限")


async def current_permission_ids(db: AsyncSession, x_user_id: str | None) -> list[str]:
    owned = await _owned_permission_ids(db, x_user_id)
    if owned is None:
        user = await db.scalar(
            select(SysUser).options(selectinload(SysUser.role)).where(
                SysUser.id == parse_user_id_header(x_user_id)
            ).limit(1)
        )
        ids = _parse_ids(user.role.permissions if user and user.role else None)
    else:
        ids = list(owned)
    if "1" not in ids:
        ids.append("1")
    return ids

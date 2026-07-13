"""启动时补全角色权限（新菜单项与既有基础数据权限对齐）。"""
from __future__ import annotations

import json
import logging

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import SysRole

logger = logging.getLogger(__name__)

_ALARM_FILTER_PERM = "111"
_BASE_MODULE_PERM = "10"
_ALARM_TYPE_PERM = "107"


def _parse_permissions(raw: str | None) -> list[str]:
    try:
        data = json.loads(raw or "[]")
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(data, list):
        return []
    out: list[str] = []
    for item in data:
        text = str(item).strip()
        if text:
            out.append(text)
    return out


def _should_grant_alarm_filter(perms: list[str]) -> bool:
    if _ALARM_FILTER_PERM in perms:
        return False
    if _ALARM_TYPE_PERM in perms:
        return True
    if _BASE_MODULE_PERM in perms:
        return True
    return False


async def ensure_alarm_filter_rule_permission() -> None:
    """已有报警类型或基础数据模块权限的角色，自动补上报警过滤规则(111)。"""
    updated = 0
    async with AsyncSessionLocal() as db:
        roles = (await db.execute(select(SysRole))).scalars().all()
        for role in roles:
            code = (role.code or "").strip().lower()
            if code == "admin":
                continue
            perms = _parse_permissions(role.permissions)
            if not _should_grant_alarm_filter(perms):
                continue
            perms.append(_ALARM_FILTER_PERM)
            role.permissions = json.dumps(sorted(set(perms), key=lambda x: (len(x), x)))
            updated += 1
        if updated:
            await db.commit()
            logger.info("已为 %s 个角色自动补充权限 id=%s（报警过滤规则）", updated, _ALARM_FILTER_PERM)

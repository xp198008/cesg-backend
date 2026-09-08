"""启动时补全角色权限（新菜单项与既有基础数据权限对齐）。

报警过滤规则(111) 已下线，不再自动补发该权限。
道路类型维护(113)：已有基础数据(10) 或 公用限速(110) 的角色自动补发。
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import SysRole

logger = logging.getLogger(__name__)

_ROAD_TYPE_PERM = "113"
_GRANT_IF_HAS = frozenset({"10", "110"})


def _as_id_list(raw: str | None) -> list:
    try:
        data = json.loads((raw or "").strip() or "[]")
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(data, list):
        return []
    return data


def _norm_id(value) -> str:
    text = str(value).strip()
    if text.isdigit():
        return str(int(text))
    return text


async def grant_road_type_permission() -> int:
    """给已有基础数据/公用限速权限的角色补发道路类型维护(113)。"""
    updated = 0
    async with AsyncSessionLocal() as db:
        roles = (await db.execute(select(SysRole))).scalars().all()
        for role in roles:
            ids = _as_id_list(role.permissions)
            norm = {_norm_id(x) for x in ids}
            if _ROAD_TYPE_PERM in norm:
                continue
            if not (norm & _GRANT_IF_HAS):
                continue
            ids.append(113)
            role.permissions = json.dumps(ids, ensure_ascii=False)
            updated += 1
        if updated:
            await db.commit()
    if updated:
        logger.info("已为 %s 个角色补发道路类型维护权限(113)", updated)
    return updated

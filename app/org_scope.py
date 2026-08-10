"""登录用户组织范围：所属公司及下级公司（org_company.parent_id 树）。"""

from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import OrgCompany, SysUser
from app.ttl_cache import ttl_get_or_set_async
from app.vehicle_alloc_scope import parse_user_id_header

_ORG_SUBTREE_TTL = 60.0


async def collect_org_company_subtree_ids(db: AsyncSession, root_id: int) -> set[int]:
    """返回 root_id 自身及其所有下级公司 id（按 parent_id BFS）。"""
    rid = int(root_id)

    async def _load() -> set[int]:
        out: set[int] = {rid}
        frontier = [rid]
        while frontier:
            r = await db.execute(select(OrgCompany.id).where(OrgCompany.parent_id.in_(frontier)))
            nxt: list[int] = []
            for row in r.all():
                cid = int(row[0])
                if cid not in out:
                    out.add(cid)
                    nxt.append(cid)
            frontier = nxt
        return out

    cached = await ttl_get_or_set_async(f"org:subtree:{rid}", _ORG_SUBTREE_TTL, _load)
    return set(cached)


def wants_org_tree_scope(scope_org_tree: bool, x_org_id: str | None) -> bool:
    if scope_org_tree:
        return True
    return bool((x_org_id or "").strip())


def require_x_org_id_header(x_org_id: str | None) -> int:
    raw = (x_org_id or "").strip()
    if not raw:
        raise HTTPException(
            status_code=400,
            detail="缺少请求头 X-Org-Id。请重新登录以写入所属公司；若已登录仍如此，请在系统管理中为本账号或角色绑定「所属公司」。",
        )
    try:
        n = int(raw, 10)
    except ValueError:
        raise HTTPException(status_code=400, detail="X-Org-Id 无效") from None
    if n < 1:
        raise HTTPException(status_code=400, detail="X-Org-Id 无效")
    return n


def _effective_user_org_id(user: SysUser) -> int | None:
    if user.org_id is not None:
        return int(user.org_id)
    role = user.role
    if role and role.org_id is not None:
        return int(role.org_id)
    return None


async def _load_user_home_org_id(db: AsyncSession, x_user_id: str | None) -> int | None:
    uid = parse_user_id_header(x_user_id)
    if uid is None:
        return None
    user = await db.scalar(
        select(SysUser).options(selectinload(SysUser.role)).where(SysUser.id == uid).limit(1)
    )
    if user is None:
        return None
    return _effective_user_org_id(user)


async def require_user_company_subtree_ids(
    db: AsyncSession,
    *,
    x_org_id: str | None,
    x_user_id: str | None,
) -> tuple[int, set[int]]:
    """当前登录用户可见公司范围（本公司 + 下级）。

    - 优先使用 X-Org-Id（前端 effectiveOrgId），但必须在用户所属公司子树内
    - 若无 X-Org-Id，则回退到登录用户绑定公司
    """
    user_home_org = await _load_user_home_org_id(db, x_user_id)
    requested_root: int | None = None
    if (x_org_id or "").strip():
        requested_root = require_x_org_id_header(x_org_id)

    if requested_root is not None:
        if user_home_org is not None:
            allowed_home = await collect_org_company_subtree_ids(db, user_home_org)
            if requested_root not in allowed_home:
                raise HTTPException(status_code=403, detail="无权查看该公司数据")
        root = requested_root
    elif user_home_org is not None:
        root = user_home_org
    else:
        raise HTTPException(
            status_code=400,
            detail="缺少 X-Org-Id，且无法从登录用户解析所属公司。请重新登录或在用户管理中绑定所属公司。",
        )

    exists = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
    if exists is None:
        raise HTTPException(status_code=400, detail="所属公司不存在")
    return root, await collect_org_company_subtree_ids(db, root)


async def list_scoped_usernames(
    db: AsyncSession,
    scoped_org_ids: set[int],
) -> list[str]:
    """可见公司范围内的用户名（去重保序）。"""
    if not scoped_org_ids:
        return []
    rows = (
        await db.execute(
            select(SysUser.username).where(SysUser.org_id.in_(list(scoped_org_ids))).order_by(SysUser.id)
        )
    ).all()
    seen: set[str] = set()
    out: list[str] = []
    for (name,) in rows:
        text = (name or "").strip()
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def org_scope_row_clause(
    org_id_column,
    username_column,
    scoped_org_ids: set[int],
    scoped_usernames: list[str],
):
    """日志/会话行是否落在可见组织：org_id 命中，或 org_id 为空但用户名在范围内。"""
    if not scoped_org_ids:
        return org_id_column.in_([])
    parts = [org_id_column.in_(list(scoped_org_ids))]
    if scoped_usernames:
        lowered = [n.lower() for n in scoped_usernames]
        parts.append(
            and_(
                org_id_column.is_(None),
                func.lower(username_column).in_(lowered),
            )
        )
    return or_(*parts)

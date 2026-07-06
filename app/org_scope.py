"""登录用户组织范围：所属公司及下级公司（org_company.parent_id 树）。"""

from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import OrgCompany, SysUser
from app.vehicle_alloc_scope import parse_user_id_header


async def collect_org_company_subtree_ids(db: AsyncSession, root_id: int) -> set[int]:
    """返回 root_id 自身及其所有下级公司 id（按 parent_id BFS）。"""
    rid = int(root_id)
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
                raise HTTPException(status_code=403, detail="无权查看该公司及下属司机数据")
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

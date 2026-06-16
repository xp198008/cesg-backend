"""登录用户组织范围：所属公司及下级公司（org_company.parent_id 树）。"""

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OrgCompany


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

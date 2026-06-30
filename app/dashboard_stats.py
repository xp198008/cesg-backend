"""快捷桌面看板指标（仅 CESG 业务库，808 平台数据由前端用登录 token 调用）。"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OrgCompany, VehicleViolation
from app.org_scope import collect_org_company_subtree_ids, require_x_org_id_header, wants_org_tree_scope


def _today_iso_range() -> tuple[str, str]:
    now = datetime.now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


async def _scoped_company_ids(db: AsyncSession, x_org_id: str | None) -> set[int] | None:
    if not wants_org_tree_scope(False, x_org_id):
        return None
    root = require_x_org_id_header(x_org_id)
    co = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
    if co is None:
        return set()
    return await collect_org_company_subtree_ids(db, root)


def _violation_scope_clause(scoped_company_ids: set[int] | None):
    if scoped_company_ids is None:
        return None
    return or_(
        VehicleViolation.company_id.in_(scoped_company_ids),
        VehicleViolation.company_id.is_(None),
    )


async def build_home_stats(db: AsyncSession, x_org_id: str | None) -> dict:
    scoped_company_ids = await _scoped_company_ids(db, x_org_id)
    scope = _violation_scope_clause(scoped_company_ids)

    pending_q = select(func.count()).select_from(VehicleViolation).where(
        or_(
            VehicleViolation.status == "待处理",
            and_(VehicleViolation.status == "待审核", VehicleViolation.pre_audit_kind == "preprocess"),
        )
    )
    if scope is not None:
        pending_q = pending_q.where(scope)

    start_iso, end_iso = _today_iso_range()
    completed_q = select(func.count()).select_from(VehicleViolation).where(VehicleViolation.status == "已处理")
    try:
        completed_q = completed_q.where(
            VehicleViolation.handled_at >= datetime.fromisoformat(start_iso),
            VehicleViolation.handled_at <= datetime.fromisoformat(end_iso),
        )
    except ValueError:
        completed_q = completed_q.where(VehicleViolation.id == -1)
    if scope is not None:
        completed_q = completed_q.where(scope)

    pending_tasks = int((await db.scalar(pending_q)) or 0)
    today_completed = int((await db.scalar(completed_q)) or 0)

    return {
        "ok": True,
        "pending_tasks": pending_tasks,
        "today_completed": today_completed,
    }

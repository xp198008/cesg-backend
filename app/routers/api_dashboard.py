"""快捷桌面看板 API。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.dashboard_stats import build_board_stats, build_home_stats
from app.database import get_db

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/home-stats")
async def dashboard_home_stats(
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    """快捷桌面业务指标：待处理任务、今日完成任务（808 在线/报警由前端用登录 token 调 /api）。"""
    return await build_home_stats(db, x_org_id)


@router.get("/board-stats")
async def dashboard_board_stats(
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    """智慧看板聚合指标：车辆情况、AI 预警、设备故障、司机画像。"""
    return await build_board_stats(db, x_org_id)

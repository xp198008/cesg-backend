"""用户快捷桌面配置。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.dashboard_stats import build_home_stats
from app.models import SysRole, SysUser, SysUserShortcut

router = APIRouter(prefix="/api/shortcut", tags=["shortcut"])

_DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "permission_menu.json"

_SHORTCUT_META: dict[str, dict[str, str]] = {
    "1": {"url": "../board_page/board.html", "icon": "./images/svg/icon-zhihuikanban.svg"},
    "11": {"url": "../board_page/board.html", "icon": "./images/svg/icon-zhihuikanban.svg"},
    "12": {"url": "../board_page/security_board.html", "icon": "./images/svg/icon-zhihuikanban.svg"},
    "15": {"url": "../board_page/obd_board.html", "icon": "./images/svg/icon-zhihuikanban.svg"},
    "2": {"url": "../dashboard_page/dashboard.html", "icon": "./images/svg/icon-zhihuikanban.svg"},
    "3": {"url": "../monitoring_center/index.html", "icon": "./images/svg/icon-jiankong.svg"},
    "31": {"url": "../monitoring_center/index.html", "icon": "./images/svg/icon-jiankong.svg"},
    "32": {"url": "../monitoring_center/history_playback.html", "icon": "./images/svg/icon-jiankong.svg"},
    "33": {"url": "../monitoring_center/group_monitoring.html", "icon": "./images/svg/icon-jiankong.svg"},
    "321": {"url": "../monitoring_center/history_playback.html", "icon": "./images/svg/icon-jiankong.svg"},
    "324": {"url": "../monitoring_center/multi_car_trajectory.html", "icon": "./images/svg/icon-jiankong.svg"},
    "325": {"url": "../monitoring_center/security_monitoring.html", "icon": "./images/svg/icon-jiankong.svg"},
    "4": {"url": "/main/vehicle/fault/manual", "icon": "./images/svg/icon-carmanage.svg"},
    "411": {"url": "/main/vehicle/violation/manual", "icon": "./images/svg/icon-carmanage.svg"},
    "412": {"url": "/main/vehicle/violation/instruction", "icon": "./images/svg/icon-carmanage.svg"},
    "413": {"url": "/main/vehicle/violation/review", "icon": "./images/svg/icon-carmanage.svg"},
    "414": {"url": "/main/vehicle/violation/appeal", "icon": "./images/svg/icon-carmanage.svg"},
    "431": {"url": "/main/vehicle/fault/manual", "icon": "./images/svg/icon-carmanage.svg"},
    "432": {"url": "/main/vehicle/fault/handle", "icon": "./images/svg/icon-carmanage.svg"},
    "433": {"url": "/main/vehicle/fault/review", "icon": "./images/svg/icon-carmanage.svg"},
    "5": {"url": "../map_manage/geofence_manage.html", "icon": "./images/svg/icon-ditu.svg"},
    "51": {"url": "../map_manage/map_api_manage.html", "icon": "./images/svg/icon-ditu.svg"},
    "52": {"url": "../map_manage/geofence_manage.html", "icon": "./images/svg/icon-ditu.svg"},
    "6": {"url": "../stats_report/mileage_report.html", "icon": "./images/svg/icon-tongji.svg"},
    "611": {"url": "../stats_report/mileage_report.html", "icon": "./images/svg/icon-tongji.svg"},
    "626": {"url": "../stats_report/vehicle_work_daily_report.html", "icon": "./images/svg/icon-tongji.svg"},
    "641": {"url": "../stats_report/vehicle_offline_report.html", "icon": "./images/svg/icon-tongji.svg"},
    "7": {"url": "../ops_manage/rule_maintenance.html", "icon": "./images/svg/icon-yunying.svg"},
    "71": {"url": "../ops_manage/rule_maintenance.html", "icon": "./images/svg/icon-yunying.svg"},
    "72": {"url": "../ops_manage/speed_limit_area.html", "icon": "./images/svg/icon-yunying.svg"},
    "73": {"url": "../ops_manage/speed_limit_weather.html", "icon": "./images/svg/icon-yunying.svg"},
    "8": {"url": "../safety_manage/active_safety_alarm.html", "icon": "./images/svg/icon-anquan.svg"},
    "81": {"url": "../safety_manage/active_safety_alarm.html", "icon": "./images/svg/icon-anquan.svg"},
    "82": {"url": "../safety_manage/active_safety_alarm_appeal_audit.html", "icon": "./images/svg/icon-anquan.svg"},
    "83": {"url": "../safety_manage/active_safety_evidence_query.html", "icon": "./images/svg/icon-anquan.svg"},
    "84": {"url": "../safety_manage/alarm_false_positive_query.html", "icon": "./images/svg/icon-anquan.svg"},
    "85": {"url": "../safety_manage/ticket_processing.html", "icon": "./images/svg/icon-anquan.svg"},
    "87": {"url": "../safety_manage/driver_identity_report.html", "icon": "./images/svg/icon-anquan.svg"},
    "88": {"url": "../safety_manage/driver_identity_query.html", "icon": "./images/svg/icon-anquan.svg"},
    "89": {"url": "../safety_manage/comprehensive_key_alarm_process.html", "icon": "./images/svg/icon-anquan.svg"},
    "9": {"url": "../system_manage/system_config.html", "icon": "./images/svg/icon-xitong.svg"},
    "91": {"url": "../system_manage/system_config.html", "icon": "./images/svg/icon-xitong.svg"},
    "92": {"url": "../system_manage/system_config.html", "icon": "./images/svg/icon-xitong.svg"},
    "10": {"url": "../basic_data_manage/org_manage.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "100": {"url": "../basic_data_manage/vehicle_type_manage.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "101": {"url": "../basic_data_manage/user_info.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "102": {"url": "../basic_data_manage/vehicle_info.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "103": {"url": "../basic_data_manage/driver_info.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "105": {"url": "../basic_data_manage/org_manage.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "106": {"url": "../basic_data_manage/team_manage.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "107": {"url": "../basic_data_manage/alarm_type.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "108": {"url": "../basic_data_manage/role_manage.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "109": {"url": "../basic_data_manage/fault_type.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "110": {"url": "../basic_data_manage/public_speed_limit_manage.html", "icon": "./images/svg/icon-jichushuju.svg"},
    "16": {"url": "../knowledge_graph/knowledge_graph.html", "icon": "./images/svg/icon-zhihuikanban.svg"},
}


class ShortcutSavePayload(BaseModel):
    user_id: int = Field(..., ge=1)
    permission_ids: list[str | int] = Field(default_factory=list)


class ShortcutDeletePayload(BaseModel):
    user_id: int = Field(..., ge=1)
    permission_id: str | int


def _read_tree() -> list[dict[str, Any]]:
    if not _DATA_FILE.is_file():
        return []
    raw = json.loads(_DATA_FILE.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and isinstance(raw.get("tree"), list):
        return raw["tree"]
    return []


def _role_code(role: SysRole | None, username: str) -> str:
    if (username or "").strip().lower() == "admin":
        return "admin"
    return ((role.code if role else "") or "").strip().lower()


def _role_permission_ids(role: SysRole | None, username: str) -> set[str] | None:
    if _role_code(role, username) == "admin":
        return None
    raw = (role.permissions if role else "") or "[]"
    try:
        arr = json.loads(raw)
    except Exception:
        arr = []
    if not isinstance(arr, list):
        arr = []
    return {str(x) for x in arr if x is not None and str(x).strip()}


async def _load_user(db: AsyncSession, user_id: int) -> SysUser:
    user = await db.scalar(
        select(SysUser).options(selectinload(SysUser.role)).where(SysUser.id == user_id).limit(1)
    )
    if user is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    return user


def _walk_shortcut_entries(nodes: list[dict[str, Any]], out: dict[str, dict[str, Any]]) -> None:
    for node in nodes:
        pid = str(node.get("id") or "").strip()
        if pid and pid in _SHORTCUT_META:
            meta = _SHORTCUT_META[pid]
            out[pid] = {
                "permission_id": pid,
                "title": str(node.get("title") or ""),
                "url": meta["url"],
                "icon": meta.get("icon") or "./images/svg/icon-zhihuikanban.svg",
            }
        children = node.get("children")
        if isinstance(children, list):
            _walk_shortcut_entries(children, out)


def _all_shortcut_entries() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    _walk_shortcut_entries(_read_tree(), out)
    return out


def _allowed_shortcut_entries(user: SysUser) -> dict[str, dict[str, Any]]:
    entries = _all_shortcut_entries()
    allowed_ids = _role_permission_ids(user.role, user.username)
    if allowed_ids is None:
        return entries
    return {pid: item for pid, item in entries.items() if pid in allowed_ids}


def _filter_tree(
    nodes: list[dict[str, Any]],
    allowed_ids: set[str] | None,
    checked_ids: set[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for node in nodes:
        pid = str(node.get("id") or "").strip()
        children = node.get("children")
        filtered_children = _filter_tree(children, allowed_ids, checked_ids) if isinstance(children, list) else []
        self_allowed = bool(pid) and (allowed_ids is None or pid in allowed_ids)
        selectable = pid in _SHORTCUT_META and self_allowed
        if not selectable and not filtered_children:
            continue
        item: dict[str, Any] = {
            "id": pid,
            "title": str(node.get("title") or ""),
            "spread": bool(node.get("spread", True)),
            "disabled": not selectable,
            "checked": selectable and pid in checked_ids,
            "shortcut_url": _SHORTCUT_META.get(pid, {}).get("url", ""),
        }
        if filtered_children:
            item["children"] = filtered_children
        out.append(item)
    return out


async def _selected_ids(db: AsyncSession, user_id: int) -> set[str]:
    rows = (
        await db.execute(select(SysUserShortcut.permission_id).where(SysUserShortcut.user_id == user_id))
    ).scalars().all()
    return {str(x) for x in rows if x is not None}


@router.get("/home-stats")
async def shortcut_home_stats(
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    """快捷桌面业务指标（待处理、今日完成）。"""
    return await build_home_stats(db, x_org_id)


@router.get("/permission-tree")
async def shortcut_permission_tree(user_id: int = Query(..., ge=1), db: AsyncSession = Depends(get_db)):
    user = await _load_user(db, user_id)
    allowed_ids = _role_permission_ids(user.role, user.username)
    checked_ids = await _selected_ids(db, user_id)
    return {"ok": True, "tree": _filter_tree(_read_tree(), allowed_ids, checked_ids)}


@router.get("/list")
async def shortcut_list(user_id: int = Query(..., ge=1), db: AsyncSession = Depends(get_db)):
    user = await _load_user(db, user_id)
    allowed = _allowed_shortcut_entries(user)
    rows = (
        await db.execute(
            select(SysUserShortcut)
            .where(SysUserShortcut.user_id == user_id)
            .order_by(SysUserShortcut.sort_order.asc(), SysUserShortcut.id.asc())
        )
    ).scalars().all()
    out: list[dict[str, Any]] = []
    for row in rows:
        pid = str(row.permission_id)
        if pid not in allowed:
            continue
        meta = allowed[pid]
        out.append(
            {
                "permission_id": pid,
                "title": row.title or meta["title"],
                "url": row.url or meta["url"],
                "icon": row.icon or meta["icon"],
                "sort_order": row.sort_order,
            }
        )
    return {"ok": True, "list": out, "total": len(out)}


@router.post("/save")
async def shortcut_save(payload: ShortcutSavePayload, db: AsyncSession = Depends(get_db)):
    user = await _load_user(db, payload.user_id)
    allowed = _allowed_shortcut_entries(user)
    selected: list[str] = []
    seen: set[str] = set()
    for x in payload.permission_ids:
        pid = str(x).strip()
        if not pid or pid in seen or pid not in allowed:
            continue
        seen.add(pid)
        selected.append(pid)
    await db.execute(delete(SysUserShortcut).where(SysUserShortcut.user_id == payload.user_id))
    for idx, pid in enumerate(selected):
        item = allowed[pid]
        db.add(
            SysUserShortcut(
                user_id=payload.user_id,
                permission_id=pid,
                title=item["title"],
                url=item["url"],
                icon=item["icon"],
                sort_order=idx,
            )
        )
    await db.flush()
    return {"ok": True, "message": "保存成功", "count": len(selected)}


@router.post("/delete")
async def shortcut_delete(payload: ShortcutDeletePayload, db: AsyncSession = Depends(get_db)):
    pid = str(payload.permission_id).strip()
    if not pid:
        raise HTTPException(status_code=400, detail="permission_id 不能为空")
    await _load_user(db, payload.user_id)
    await db.execute(
        delete(SysUserShortcut).where(
            SysUserShortcut.user_id == payload.user_id,
            SysUserShortcut.permission_id == pid,
        )
    )
    await db.flush()
    return {"ok": True, "message": "删除成功"}

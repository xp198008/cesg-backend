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

# permission_id → Vue 路由（与 jt808-vue3 shortcutRoutes.js 对齐；点击跳转以前端 MAP 为准）
_SHORTCUT_META: dict[str, dict[str, str]] = {
    "1": {"url": "/main/board", "icon": "./images/svg/icon-zhihuikanban.svg"},
    "2": {"url": "/main/home", "icon": "./images/svg/icon-zhihuikanban.svg"},
    "3": {"url": "/main/realtime", "icon": "./images/svg/icon-jiankong.svg"},
    "31": {"url": "/main/realtime", "icon": "./images/svg/icon-jiankong.svg"},
    "311": {"url": "/main/realtimealarm", "icon": "./images/svg/icon-jiankong.svg"},
    "321": {"url": "/main/videohistory", "icon": "./images/svg/icon-jiankong.svg"},
    "322": {"url": "/main/postsitionhistory", "icon": "./images/svg/icon-jiankong.svg"},
    "323": {"url": "/main/history", "icon": "./images/svg/icon-jiankong.svg"},
    "324": {"url": "/main/postsitionhistorygroup", "icon": "./images/svg/icon-jiankong.svg"},
    "33": {"url": "/main/realtimegroup", "icon": "./images/svg/icon-jiankong.svg"},
    "325": {"url": "/main/realtimealarm", "icon": "./images/svg/icon-jiankong.svg"},
    "4": {"url": "/main/vehicle/fault/manual", "icon": "./images/svg/icon-carmanage.svg"},
    "411": {"url": "/main/vehicle/violation/manual", "icon": "./images/svg/icon-carmanage.svg"},
    "412": {"url": "/main/vehicle/violation/instruction", "icon": "./images/svg/icon-carmanage.svg"},
    "413": {"url": "/main/vehicle/violation/review", "icon": "./images/svg/icon-carmanage.svg"},
    "414": {"url": "/main/vehicle/violation/appeal", "icon": "./images/svg/icon-carmanage.svg"},
    "421": {"url": "/main/vehicle/repair/list", "icon": "./images/svg/icon-carmanage.svg"},
    "422": {"url": "/main/vehicle/repair/entry", "icon": "./images/svg/icon-carmanage.svg"},
    "423": {"url": "/main/vehicle/repair/review", "icon": "./images/svg/icon-carmanage.svg"},
    "424": {"url": "/main/vehicle/repair/uploaded", "icon": "./images/svg/icon-carmanage.svg"},
    "431": {"url": "/main/vehicle/fault/manual", "icon": "./images/svg/icon-carmanage.svg"},
    "432": {"url": "/main/vehicle/fault/handle", "icon": "./images/svg/icon-carmanage.svg"},
    "433": {"url": "/main/vehicle/fault/review", "icon": "./images/svg/icon-carmanage.svg"},
    "434": {"url": "/main/vehicle/fault/upload", "icon": "./images/svg/icon-carmanage.svg"},
    "5": {"url": "/main/map/geofence", "icon": "./images/svg/icon-ditu.svg"},
    "51": {"url": "/main/map/config", "icon": "./images/svg/icon-ditu.svg"},
    "52": {"url": "/main/map/geofence", "icon": "./images/svg/icon-ditu.svg"},
    "53": {"url": "/main/map/geofence", "icon": "./images/svg/icon-ditu.svg"},
    "6": {"url": "/main/report/mileage-summary", "icon": "./images/svg/icon-tongji.svg"},
    "611": {"url": "/main/report/mileage-summary", "icon": "./images/svg/icon-tongji.svg"},
    "612": {"url": "/main/report/mileage-daily", "icon": "./images/svg/icon-tongji.svg"},
    "613": {"url": "/main/report/mileage-single", "icon": "./images/svg/icon-tongji.svg"},
    "614": {"url": "/main/report/mileage-monthly", "icon": "./images/svg/icon-tongji.svg"},
    "621": {"url": "/main/report/behavior-daily", "icon": "./images/svg/icon-tongji.svg"},
    "622": {"url": "/main/report/behavior-monthly", "icon": "./images/svg/icon-tongji.svg"},
    "623": {"url": "/main/report/behavior-query", "icon": "./images/svg/icon-tongji.svg"},
    "624": {"url": "/main/report/behavior-statistics", "icon": "./images/svg/icon-tongji.svg"},
    "625": {"url": "/main/report/behavior-track", "icon": "./images/svg/icon-tongji.svg"},
    "626": {"url": "/main/report/travel-work-daily", "icon": "./images/svg/icon-tongji.svg"},
    "631": {"url": "/main/report/travel-stop-summary", "icon": "./images/svg/icon-tongji.svg"},
    "632": {"url": "/main/report/travel-stop-detail", "icon": "./images/svg/icon-tongji.svg"},
    "633": {"url": "/main/report/travel-driving-summary", "icon": "./images/svg/icon-tongji.svg"},
    "634": {"url": "/main/report/travel-driving-detail", "icon": "./images/svg/icon-tongji.svg"},
    "635": {"url": "/main/report/travel-acc-statistics", "icon": "./images/svg/icon-tongji.svg"},
    "636": {"url": "/main/report/travel-acc-query", "icon": "./images/svg/icon-tongji.svg"},
    "637": {"url": "/main/report/travel-acc-daily", "icon": "./images/svg/icon-tongji.svg"},
    "638": {"url": "/main/report/travel-trip-detail", "icon": "./images/svg/icon-tongji.svg"},
    "641": {"url": "/main/report/travel-offline-detail", "icon": "./images/svg/icon-tongji.svg"},
    "642": {"url": "/main/report/travel-fuel", "icon": "./images/svg/icon-tongji.svg"},
    "651": {"url": "/main/report/alarm-statistics", "icon": "./images/svg/icon-tongji.svg"},
    "652": {"url": "/main/report/alarm-key-query", "icon": "./images/svg/icon-tongji.svg"},
    "7": {"url": "/main/rule/fatigue", "icon": "./images/svg/icon-yunying.svg"},
    "71": {"url": "/main/rule/fatigue", "icon": "./images/svg/icon-yunying.svg"},
    "72": {"url": "/main/map/private-speed", "icon": "./images/svg/icon-yunying.svg"},
    "73": {"url": "/main/rule/sensor", "icon": "./images/svg/icon-yunying.svg"},
    "8": {"url": "/main/safety/active-alarm", "icon": "./images/svg/icon-anquan.svg"},
    "81": {"url": "/main/safety/active-alarm", "icon": "./images/svg/icon-anquan.svg"},
    "82": {"url": "/main/safety/alarm-audit", "icon": "./images/svg/icon-anquan.svg"},
    "83": {"url": "/main/safety/evidence-query", "icon": "./images/svg/icon-anquan.svg"},
    "84": {"url": "/main/safety/false-positive", "icon": "./images/svg/icon-anquan.svg"},
    "85": {"url": "/main/safety/ticket-processing", "icon": "./images/svg/icon-anquan.svg"},
    "86": {"url": "/main/safety/ticket-archive", "icon": "./images/svg/icon-anquan.svg"},
    "89": {"url": "/main/safety/key-alarm-process", "icon": "./images/svg/icon-anquan.svg"},
    "9": {"url": "/main/system", "icon": "./images/svg/icon-xitong.svg"},
    "91": {"url": "/main/system", "icon": "./images/svg/icon-xitong.svg"},
    "92": {"url": "/main/system", "icon": "./images/svg/icon-xitong.svg"},
    "10": {"url": "/main/base/org", "icon": "./images/svg/icon-jichushuju.svg"},
    "100": {"url": "/main/base/vehicle-types", "icon": "./images/svg/icon-jichushuju.svg"},
    "101": {"url": "/main/base/users", "icon": "./images/svg/icon-jichushuju.svg"},
    "102": {"url": "/main/base/vehicles", "icon": "./images/svg/icon-jichushuju.svg"},
    "103": {"url": "/main/base/drivers", "icon": "./images/svg/icon-jichushuju.svg"},
    "105": {"url": "/main/base/org", "icon": "./images/svg/icon-jichushuju.svg"},
    "106": {"url": "/main/base/vehicle-assignment", "icon": "./images/svg/icon-jichushuju.svg"},
    "107": {"url": "/main/base/alarms", "icon": "./images/svg/icon-jichushuju.svg"},
    "108": {"url": "/main/base/roles", "icon": "./images/svg/icon-jichushuju.svg"},
    "109": {"url": "/main/base/faults", "icon": "./images/svg/icon-jichushuju.svg"},
    "110": {"url": "/main/base/speed-rules", "icon": "./images/svg/icon-jichushuju.svg"},
    "16": {"url": "http://113.207.68.94:5002", "icon": "./images/svg/icon-zhihuikanban.svg"},
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


async def _migrate_legacy_board_shortcuts(
    db: AsyncSession,
    user_id: int,
    rows: list[SysUserShortcut],
    allowed: dict[str, dict[str, Any]],
) -> None:
    """将已废弃的看板子页快捷项(11/12/15)合并为智慧看板(1)并写回数据库。"""
    legacy_board_ids = {"11", "12", "15"}
    meta_one = allowed.get("1")
    if not meta_one:
        return
    touched = False
    for row in rows:
        if str(row.permission_id) not in legacy_board_ids:
            continue
        row.permission_id = "1"
        row.title = meta_one["title"]
        row.url = meta_one["url"]
        row.icon = meta_one.get("icon") or row.icon
        touched = True
    if not touched:
        return
    await db.flush()
    # 去重：同一用户只保留一条 id=1
    rows_after = (
        await db.execute(
            select(SysUserShortcut)
            .where(SysUserShortcut.user_id == user_id)
            .order_by(SysUserShortcut.sort_order.asc(), SysUserShortcut.id.asc())
        )
    ).scalars().all()
    seen_one = False
    for row in rows_after:
        if str(row.permission_id) != "1":
            continue
        if seen_one:
            await db.delete(row)
        else:
            seen_one = True
    await db.flush()


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
    await _migrate_legacy_board_shortcuts(db, user_id, list(rows), allowed)
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
        if any(item.get("permission_id") == pid for item in out):
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
    legacy_board_ids = {"11", "12", "15"}
    selected: list[str] = []
    seen: set[str] = set()
    for x in payload.permission_ids:
        pid = str(x).strip()
        if pid in legacy_board_ids:
            pid = "1"
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

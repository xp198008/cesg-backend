"""路径规划：下发历史 + 预设记录（公司权限隔离）。"""
from __future__ import annotations

import secrets
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import load_only

from app.database import get_db
from app.models import OrgCompany, RoutePlanHistory, RoutePlanPreset, SysUser, Vehicle
from app.org_scope import (
    collect_org_company_subtree_ids,
    require_user_company_subtree_ids,
    require_x_org_id_header,
)
from app.plate_util import norm_plate
from app.timeutil import china_now_naive

router = APIRouter(prefix="/api/route-plan", tags=["route-plan"])

_SEND_TYPES = frozenset({"voice", "text", "both"})
_SEND_STATUSES = frozenset({"success", "fail"})


def _gen_path_code() -> str:
    """稳定可读唯一编码：RP + yyyyMMddHHmmss + 6 位十六进制。"""
    return f"RP{china_now_naive().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(3).upper()}"


def _fmt_dt(v) -> str | None:
    return v.strftime("%Y-%m-%d %H:%M:%S") if v else None


def _parse_user_id(x_user_id: str | None) -> int | None:
    raw = (x_user_id or "").strip()
    if not raw:
        return None
    try:
        uid = int(raw)
    except ValueError:
        return None
    return uid if uid > 0 else None


async def _resolve_creator_name(db: AsyncSession, user_id: int | None) -> str | None:
    """按登录用户 ID 解析展示名：优先 real_name，其次 username。"""
    if not user_id:
        return None
    pair = (
        await db.execute(
            select(SysUser.real_name, SysUser.username).where(SysUser.id == int(user_id)).limit(1)
        )
    ).first()
    if not pair:
        return None
    real_name, username = pair
    return (real_name or "").strip() or (username or "").strip() or None


async def _creator_name_map(db: AsyncSession, user_ids: list[int | None]) -> dict[int, str]:
    ids = sorted({int(i) for i in user_ids if i})
    if not ids:
        return {}
    rows = (
        await db.execute(
            select(SysUser.id, SysUser.real_name, SysUser.username).where(SysUser.id.in_(ids))
        )
    ).all()
    out: dict[int, str] = {}
    for uid, real_name, username in rows:
        name = (real_name or "").strip() or (username or "").strip()
        if name:
            out[int(uid)] = name
    return out


def _history_to_dict(row: RoutePlanHistory) -> dict:
    return {
        "id": row.id,
        "path_code": row.path_code,
        "plate_no": row.plate_no,
        "terminal_id": row.terminal_id,
        "vehicle_id": row.vehicle_id,
        "company_id": row.company_id,
        "current_address": row.current_address,
        "start_address": row.start_address,
        "dest_address": row.dest_address,
        "route_summary": row.route_summary,
        "send_type": row.send_type,
        "send_status": row.send_status,
        "error_message": row.error_message,
        "created_at": _fmt_dt(row.created_at),
    }


def _preset_to_dict(
    row: RoutePlanPreset,
    *,
    with_snapshot: bool = True,
    creator_name: str | None = None,
) -> dict:
    name = creator_name if creator_name is not None else (row.created_by_name or None)
    data = {
        "id": row.id,
        "path_code": row.path_code,
        "company_id": row.company_id,
        "created_by": row.created_by,
        "created_by_name": name,
        "plan_name": row.plan_name,
        "start_address": row.start_address,
        "dest_address": row.dest_address,
        "route_text": row.route_text,
        "send_count": int(row.send_count or 0),
        "created_at": _fmt_dt(row.created_at),
        "updated_at": _fmt_dt(row.updated_at),
    }
    if with_snapshot:
        data["plan_snapshot"] = row.plan_snapshot or {}
    return data


class RoutePlanHistoryCreateIn(BaseModel):
    plate_no: str | None = Field(None, max_length=32)
    terminal_id: str | None = Field(None, max_length=64)
    vehicle_id: int | None = Field(None, ge=1)
    company_id: int | None = Field(None, ge=1)
    current_address: str | None = Field(None, max_length=512)
    start_address: str | None = Field(None, max_length=512)
    dest_address: str | None = Field(None, max_length=512)
    route_summary: str | None = Field(None, max_length=8000)
    send_type: str = Field("text", max_length=16)
    send_status: str = Field("success", max_length=16)
    error_message: str | None = Field(None, max_length=512)


class RoutePlanPresetCreateIn(BaseModel):
    plan_name: str = Field(..., min_length=1, max_length=30)
    start_address: str | None = Field(None, max_length=512)
    dest_address: str | None = Field(None, max_length=512)
    route_text: str | None = Field(None, max_length=8000)
    plan_snapshot: dict[str, Any] = Field(...)
    # 前端可附带当前登录用户，作为请求头缺失时的兜底（仍以 X-User-Id 优先）
    created_by: int | None = Field(None, ge=1)
    created_by_name: str | None = Field(None, max_length=64)


async def _resolve_vehicle(
    db: AsyncSession,
    *,
    vehicle_id: int | None,
    plate_no: str | None,
    terminal_id: str | None,
) -> Vehicle | None:
    if vehicle_id is not None:
        v = await db.get(Vehicle, int(vehicle_id))
        if v is not None:
            return v
    plate = norm_plate(plate_no or "")
    if plate:
        v = (await db.execute(select(Vehicle).where(Vehicle.plate_no == plate))).scalar_one_or_none()
        if v is None:
            v = (
                await db.execute(select(Vehicle).where(func.upper(Vehicle.plate_no) == plate.upper()))
            ).scalar_one_or_none()
        if v is not None:
            return v
    tid = (terminal_id or "").strip()
    if tid:
        # vehicle 表无独立 terminal 列时，仅按车牌匹配；tid 原样入库
        pass
    return None


@router.post("/history")
async def route_plan_history_create(
    body: RoutePlanHistoryCreateIn,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    root = require_x_org_id_header(x_org_id)
    co = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
    if co is None:
        raise HTTPException(status_code=400, detail="X-Org-Id 对应公司不存在")
    subtree = await collect_org_company_subtree_ids(db, root)

    send_type = (body.send_type or "").strip().lower() or "text"
    if send_type not in _SEND_TYPES:
        raise HTTPException(status_code=400, detail="send_type 须为 voice / text / both")
    send_status = (body.send_status or "").strip().lower() or "success"
    if send_status not in _SEND_STATUSES:
        raise HTTPException(status_code=400, detail="send_status 须为 success / fail")

    plate = norm_plate(body.plate_no or "") or ((body.plate_no or "").strip() or None)
    terminal_id = (body.terminal_id or "").strip() or None
    v = await _resolve_vehicle(
        db, vehicle_id=body.vehicle_id, plate_no=plate, terminal_id=terminal_id
    )
    if v is not None and v.company_id is not None and int(v.company_id) not in subtree:
        raise HTTPException(status_code=403, detail="该车辆不属于您所在公司及下级公司")

    company_id = body.company_id
    if company_id is not None and int(company_id) not in subtree:
        raise HTTPException(status_code=403, detail="company_id 超出可见组织范围")
    if company_id is None:
        company_id = int(v.company_id) if v is not None and v.company_id is not None else root

    # 极端并发下 path_code 冲突时重试
    row: RoutePlanHistory | None = None
    last_err: Exception | None = None
    for _ in range(5):
        try:
            row = RoutePlanHistory(
                path_code=_gen_path_code(),
                plate_no=plate or (v.plate_no if v else None),
                terminal_id=terminal_id,
                vehicle_id=int(v.id) if v else body.vehicle_id,
                company_id=int(company_id) if company_id is not None else None,
                current_address=(body.current_address or "").strip()[:512] or None,
                start_address=(body.start_address or "").strip()[:512] or None,
                dest_address=(body.dest_address or "").strip()[:512] or None,
                route_summary=(body.route_summary or "").strip() or None,
                send_type=send_type,
                send_status=send_status,
                error_message=(body.error_message or "").strip()[:512] or None,
            )
            db.add(row)
            await db.commit()
            await db.refresh(row)
            break
        except Exception as e:
            last_err = e
            await db.rollback()
            row = None
    if row is None:
        raise HTTPException(status_code=500, detail=f"写入历史失败：{last_err}")

    return {"ok": True, "item": _history_to_dict(row)}


@router.get("/history/list")
async def route_plan_history_list(
    plate_no: str | None = None,
    terminal_id: str | None = None,
    send_status: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    db: AsyncSession = Depends(get_db),
):
    root = require_x_org_id_header(x_org_id)
    subtree = await collect_org_company_subtree_ids(db, root)

    stmt = select(RoutePlanHistory).where(
        or_(
            RoutePlanHistory.company_id.in_(subtree),
            RoutePlanHistory.company_id.is_(None),
        )
    )
    if plate_no and plate_no.strip():
        stmt = stmt.where(RoutePlanHistory.plate_no.like(f"%{plate_no.strip()}%"))
    if terminal_id and terminal_id.strip():
        stmt = stmt.where(RoutePlanHistory.terminal_id == terminal_id.strip())
    if send_status and send_status.strip():
        st = send_status.strip().lower()
        if st not in _SEND_STATUSES:
            raise HTTPException(status_code=400, detail="send_status 须为 success / fail")
        stmt = stmt.where(RoutePlanHistory.send_status == st)

    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        (
            await db.execute(
                stmt.order_by(RoutePlanHistory.id.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        )
        .scalars()
        .all()
    )
    return {
        "ok": True,
        "items": [_history_to_dict(r) for r in rows],
        "total": int(total),
        "page": page,
        "page_size": page_size,
    }


@router.post("/preset")
async def route_plan_preset_create(
    body: RoutePlanPresetCreateIn,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    """保存预设：挂到当前登录用户可见公司（X-Org-Id，且须在用户所属公司子树内）。"""
    root, _subtree = await require_user_company_subtree_ids(
        db, x_org_id=x_org_id, x_user_id=x_user_id
    )
    plan_name = (body.plan_name or "").strip()
    if not plan_name:
        raise HTTPException(status_code=400, detail="请输入助记名称")
    if len(plan_name) > 30:
        raise HTTPException(status_code=400, detail="助记名称最多30个汉字")
    snapshot = body.plan_snapshot
    if not isinstance(snapshot, dict) or not snapshot.get("options"):
        raise HTTPException(status_code=400, detail="plan_snapshot 无效，请先完成路径规划")

    # 入库时去掉接驳虚线，恢复时按当前选车重算
    snap = dict(snapshot)
    snap["approachPath"] = []

    created_by = _parse_user_id(x_user_id) or (
        int(body.created_by) if body.created_by is not None else None
    )
    created_by_name = await _resolve_creator_name(db, created_by)
    if not created_by_name:
        created_by_name = (body.created_by_name or "").strip()[:64] or None
    row: RoutePlanPreset | None = None
    last_err: Exception | None = None
    for _ in range(5):
        try:
            row = RoutePlanPreset(
                path_code=_gen_path_code(),
                company_id=int(root),
                created_by=created_by,
                created_by_name=created_by_name,
                plan_name=plan_name,
                start_address=(body.start_address or "").strip()[:512] or None,
                dest_address=(body.dest_address or "").strip()[:512] or None,
                route_text=(body.route_text or "").strip() or None,
                plan_snapshot=snap,
                send_count=0,
            )
            db.add(row)
            await db.commit()
            await db.refresh(row)
            break
        except Exception as e:
            last_err = e
            await db.rollback()
            row = None
    if row is None:
        raise HTTPException(status_code=500, detail=f"保存预设失败：{last_err}")
    return {"ok": True, "item": _preset_to_dict(row)}


@router.get("/preset/list")
async def route_plan_preset_list(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    """预设列表：轻量字段（不含 plan_snapshot）；详情点行后再拉。"""
    _root, subtree = await require_user_company_subtree_ids(
        db, x_org_id=x_org_id, x_user_id=x_user_id
    )
    where_scope = RoutePlanPreset.company_id.in_(subtree)
    total = (
        await db.scalar(select(func.count()).select_from(RoutePlanPreset).where(where_scope))
        or 0
    )
    stmt = (
        select(RoutePlanPreset)
        .options(
            load_only(
                RoutePlanPreset.id,
                RoutePlanPreset.path_code,
                RoutePlanPreset.company_id,
                RoutePlanPreset.created_by,
                RoutePlanPreset.created_by_name,
                RoutePlanPreset.plan_name,
                RoutePlanPreset.start_address,
                RoutePlanPreset.dest_address,
                RoutePlanPreset.route_text,
                RoutePlanPreset.send_count,
                RoutePlanPreset.created_at,
                RoutePlanPreset.updated_at,
            )
        )
        .where(where_scope)
        .order_by(RoutePlanPreset.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    rows = (await db.execute(stmt)).scalars().all()
    # 旧数据可能只有 created_by：按用户 ID 批量补全展示名
    need_ids = [r.created_by for r in rows if r.created_by and not (r.created_by_name or "").strip()]
    name_map = await _creator_name_map(db, need_ids)
    items = []
    for r in rows:
        stored = (r.created_by_name or "").strip() or None
        fallback = name_map.get(int(r.created_by)) if r.created_by else None
        items.append(
            _preset_to_dict(r, with_snapshot=False, creator_name=stored or fallback)
        )
    return {
        "ok": True,
        "items": items,
        "total": int(total),
        "page": page,
        "page_size": page_size,
    }


@router.get("/preset/{preset_id}")
async def route_plan_preset_detail(
    preset_id: int,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    """点击预设行时按需加载完整快照（含 plan_snapshot）。"""
    _root, subtree = await require_user_company_subtree_ids(
        db, x_org_id=x_org_id, x_user_id=x_user_id
    )
    row = await db.get(RoutePlanPreset, int(preset_id))
    if row is None:
        raise HTTPException(status_code=404, detail="预设不存在")
    if int(row.company_id) not in subtree:
        raise HTTPException(status_code=403, detail="无权查看该预设")
    stored = (row.created_by_name or "").strip() or None
    if not stored and row.created_by:
        stored = await _resolve_creator_name(db, int(row.created_by))
    return {
        "ok": True,
        "item": _preset_to_dict(row, with_snapshot=True, creator_name=stored),
    }


@router.delete("/preset/{preset_id}")
async def route_plan_preset_delete(
    preset_id: int,
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    """删除预设记录（须在可见公司范围内）。"""
    _root, subtree = await require_user_company_subtree_ids(
        db, x_org_id=x_org_id, x_user_id=x_user_id
    )
    row = await db.get(RoutePlanPreset, int(preset_id))
    if row is None:
        raise HTTPException(status_code=404, detail="预设不存在")
    if int(row.company_id) not in subtree:
        raise HTTPException(status_code=403, detail="无权删除该预设")
    await db.delete(row)
    await db.commit()
    return {"ok": True, "id": int(preset_id)}


@router.post("/preset/{preset_id}/send-count/inc")
async def route_plan_preset_bump_send(
    preset_id: int,
    delta: int = Query(1, ge=1, le=500),
    x_org_id: str | None = Header(None, alias="X-Org-Id"),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    db: AsyncSession = Depends(get_db),
):
    """下发成功后累加发送次数（按成功车辆数 delta，默认 1）。"""
    _root, subtree = await require_user_company_subtree_ids(
        db, x_org_id=x_org_id, x_user_id=x_user_id
    )
    row = await db.get(RoutePlanPreset, int(preset_id))
    if row is None:
        raise HTTPException(status_code=404, detail="预设不存在")
    if int(row.company_id) not in subtree:
        raise HTTPException(status_code=403, detail="无权操作该预设")
    row.send_count = int(row.send_count or 0) + int(delta)
    row.updated_at = china_now_naive()
    await db.commit()
    await db.refresh(row)
    # 累加次数无需回传巨大 snapshot
    return {"ok": True, "item": _preset_to_dict(row, with_snapshot=False)}

"""角色管理（基础数据）：角色表 CRUD，与「按模块勾选」的权限 JSON 一致。"""
from __future__ import annotations

import json
import secrets
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.models import OrgCompany, SysRole, SysUser
from app.permission_names import permission_ids_to_piped_titles, remark_text_for_stored_role
from app.risk_profile_service import load_org_company_maps, resolve_selected_to_local_org_ids


def _api_role_no_cache(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate, private"
    response.headers["Pragma"] = "no-cache"


router = APIRouter(
    prefix="/api/role",
    tags=["role"],
    dependencies=[Depends(_api_role_no_cache)],
)


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return ""
    if getattr(dt, "tzinfo", None) is not None:
        dt = dt.replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class RoleCreatePayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=20)
    org_id: int | None = None
    is_global: bool = False
    permissions: str = Field(default="[]", max_length=65535)
    perm_summary: str = Field(default="", max_length=512)


class RoleUpdatePayload(BaseModel):
    role_id: int = Field(..., ge=1)
    name: str = Field(..., min_length=1, max_length=20)
    org_id: int | None = None
    is_global: bool = False
    permissions: str = Field(default="[]", max_length=65535)
    perm_summary: str = Field(default="", max_length=512)


class RoleDeleteBody(BaseModel):
    role_id: int = Field(..., ge=1)


def _validate_payload_org(is_global: bool, org_id: int | None) -> None:
    if is_global and org_id is not None:
        raise HTTPException(status_code=400, detail="选择全局共享时不应指定所属公司")
    if not is_global and (org_id is None or org_id < 1):
        raise HTTPException(status_code=400, detail="非全局角色必须选择所属公司")


def _normalize_permissions_json(s: str) -> str:
    s = (s or "").strip() or "[]"
    try:
        arr = json.loads(s)
    except Exception:
        raise HTTPException(status_code=400, detail="permissions 须为合法 JSON 数组")
    if not isinstance(arr, list):
        raise HTTPException(status_code=400, detail="permissions 须为 JSON 数组")
    ids = [str(x) for x in arr if x is not None and str(x).strip()]
    if "1" not in ids:
        ids.append("1")
    return json.dumps(ids, ensure_ascii=False)


async def _gen_unique_code(db: AsyncSession) -> str:
    for _ in range(20):
        c = f"auto_{secrets.token_hex(5)}"
        ex = await db.scalar(select(SysRole.id).where(SysRole.code == c).limit(1))
        if ex is None:
            return c
    return f"auto_{secrets.token_hex(8)}"


def _parse_company_filter_ids(company_id: int | None, company_ids: str | None) -> list[int]:
    """解析公司筛选：支持本地 org id 或 JT808 gid（与组织/车辆公司树一致）。"""
    selected: list[int] = []
    if company_ids:
        for part in str(company_ids).split(","):
            text = part.strip()
            if not text:
                continue
            try:
                selected.append(int(text))
            except (TypeError, ValueError):
                continue
    elif company_id is not None:
        selected.append(int(company_id))
    return selected


@router.get("/list")
async def role_list(
    keyword: str | None = Query(default=None),
    org_id: int | None = Query(default=None, ge=1),
    company_id: int | None = Query(default=None, description="所属公司：本地 id 或 JT808 gid"),
    company_ids: str | None = Query(default=None, description="所属公司：逗号分隔，本地 id 或 JT808 gid"),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(SysRole).options(selectinload(SysRole.org)).order_by(SysRole.id)
    kw = (keyword or "").strip()
    if kw:
        stmt = stmt.where(SysRole.name.contains(kw))
    filter_org_ids: list[int] = []
    if org_id is not None:
        filter_org_ids.append(int(org_id))
    selected_raw = _parse_company_filter_ids(company_id, company_ids)
    if selected_raw:
        parent_map, _, gid_to_local, _ = await load_org_company_maps(db)
        local_selected = resolve_selected_to_local_org_ids(
            selected_raw,
            gid_to_local=gid_to_local,
            known_local_ids=set(parent_map.keys()),
        )
        if not local_selected and org_id is None:
            # 勾选了公司树但无法映射到本地 org：不应退化为“全部”
            return {"ok": True, "list": [], "total": 0}
        filter_org_ids.extend(int(x) for x in local_selected)
    if filter_org_ids:
        uniq_org_ids = list(dict.fromkeys(filter_org_ids))
        if len(uniq_org_ids) == 1:
            stmt = stmt.where(or_(SysRole.org_id == uniq_org_ids[0], SysRole.is_global == True))  # noqa: E712
        else:
            stmt = stmt.where(or_(SysRole.org_id.in_(uniq_org_ids), SysRole.is_global == True))  # noqa: E712
    r = await db.execute(stmt)
    rows = r.scalars().all()
    out: list[dict] = []
    for role in rows:
        org_name = role.org.name if role.org else ""
        jt808_gid = (
            int(role.org.jt808_group_id)
            if role.org is not None and role.org.jt808_group_id is not None
            else None
        )
        if (role.code or "").strip().lower() == "admin":
            perm_preview = "全部模块"
        else:
            perm_preview = permission_ids_to_piped_titles(role.permissions)
        out.append(
            {
                "id": role.id,
                "name": role.name,
                "code": role.code,
                "org_id": role.org_id,
                "org_name": org_name,
                "jt808_group_id": jt808_gid,
                "is_global": bool(role.is_global),
                "global_share": "是" if role.is_global else "否",
                "permissions": role.permissions or "[]",
                "remark": role.remark,
                "perm_preview": perm_preview,
                "created_at": _fmt_dt(role.created_at) if role.created_at else "",
            }
        )
    return {"ok": True, "list": out, "total": len(out)}


@router.get("/detail/{role_id}")
async def role_detail(role_id: int, db: AsyncSession = Depends(get_db)):
    r = await db.execute(
        select(SysRole).options(selectinload(SysRole.org)).where(SysRole.id == role_id).limit(1)
    )
    role = r.scalar_one_or_none()
    if role is None:
        raise HTTPException(status_code=404, detail="角色不存在")
    jt808_gid = (
        int(role.org.jt808_group_id)
        if role.org is not None and role.org.jt808_group_id is not None
        else None
    )
    return {
        "ok": True,
        "data": {
            "id": role.id,
            "name": role.name,
            "code": role.code,
            "org_id": role.org_id,
            "org_name": role.org.name if role.org else "",
            "jt808_group_id": jt808_gid,
            "is_global": bool(role.is_global),
            "permissions": role.permissions or "[]",
            "remark": role.remark,
            "created_at": _fmt_dt(role.created_at) if role.created_at else "",
        },
    }


@router.post("/create")
async def role_create(payload: RoleCreatePayload, db: AsyncSession = Depends(get_db)):
    _validate_payload_org(payload.is_global, payload.org_id)
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="角色名称不能为空")
    dup = await db.scalar(select(SysRole.id).where(SysRole.name == name).limit(1))
    if dup is not None:
        raise HTTPException(status_code=400, detail="已存在同名角色，请修改名称")
    if not payload.is_global:
        org = await db.scalar(select(OrgCompany).where(OrgCompany.id == payload.org_id).limit(1))
        if org is None:
            raise HTTPException(status_code=400, detail="所属公司不存在")
    perms = _normalize_permissions_json(payload.permissions)
    code = await _gen_unique_code(db)
    summary = remark_text_for_stored_role(perms, code)
    role = SysRole(
        name=name,
        code=code,
        remark=summary[:512],
        org_id=None if payload.is_global else payload.org_id,
        is_global=bool(payload.is_global),
        permissions=perms,
    )
    db.add(role)
    await db.flush()
    await db.refresh(role)
    return {
        "ok": True,
        "message": "创建成功",
        "data": {
            "id": role.id,
            "name": role.name,
            "code": role.code,
            "created_at": _fmt_dt(role.created_at) if role.created_at else "",
        },
    }


@router.post("/update")
async def role_update(payload: RoleUpdatePayload, db: AsyncSession = Depends(get_db)):
    _validate_payload_org(payload.is_global, payload.org_id)
    role = await db.scalar(select(SysRole).where(SysRole.id == payload.role_id).limit(1))
    if role is None:
        raise HTTPException(status_code=404, detail="角色不存在")
    if (role.code or "").strip().lower() == "admin":
        raise HTTPException(status_code=400, detail="内置系统管理员不可编辑")
    name = (payload.name or "").strip()
    dup = await db.scalar(
        select(SysRole.id).where(SysRole.name == name, SysRole.id != payload.role_id).limit(1)
    )
    if dup is not None:
        raise HTTPException(status_code=400, detail="已存在同名角色，请修改名称")
    if not payload.is_global:
        org = await db.scalar(select(OrgCompany).where(OrgCompany.id == payload.org_id).limit(1))
        if org is None:
            raise HTTPException(status_code=400, detail="所属公司不存在")
    perms = _normalize_permissions_json(payload.permissions)
    summary = remark_text_for_stored_role(perms, role.code)
    role.name = name
    role.remark = summary[:512]
    role.org_id = None if payload.is_global else payload.org_id
    role.is_global = bool(payload.is_global)
    role.permissions = perms
    await db.flush()
    await db.refresh(role)
    return {
        "ok": True,
        "message": "更新成功",
        "data": {
            "id": role.id,
            "name": role.name,
            "updated_at": _fmt_dt(role.updated_at) if role.updated_at else _fmt_dt(role.created_at) or "",
        },
    }


async def _delete_sys_role_core(db: AsyncSession, role_id: int) -> dict:
    role = await db.scalar(select(SysRole).where(SysRole.id == role_id).limit(1))
    if role is None:
        raise HTTPException(status_code=404, detail="角色不存在")
    if (role.code or "").strip().lower() == "admin":
        raise HTTPException(status_code=400, detail="内置系统管理员不可删除")
    n_users = await db.scalar(
        select(func.count()).select_from(SysUser).where(SysUser.role_id == role_id)
    ) or 0
    if n_users > 0:
        raise HTTPException(status_code=400, detail=f"仍有 {n_users} 个用户关联该角色，请先调整用户角色后再删")
    res = await db.execute(delete(SysRole).where(SysRole.id == role_id))
    if int(getattr(res, "rowcount", 0) or 0) == 0:
        raise HTTPException(status_code=404, detail="角色不存在或已删除")
    await db.flush()
    return {"ok": True, "message": "删除成功", "id": role_id}


@router.post("/delete")
async def role_delete_by_json(payload: RoleDeleteBody, db: AsyncSession = Depends(get_db)):
    return await _delete_sys_role_core(db, payload.role_id)


@router.delete("/{role_id}")
async def role_delete(role_id: int, db: AsyncSession = Depends(get_db)):
    return await _delete_sys_role_core(db, role_id)


@router.post("/{role_id}/delete")
async def role_delete_post(role_id: int, db: AsyncSession = Depends(get_db)):
    return await _delete_sys_role_core(db, role_id)

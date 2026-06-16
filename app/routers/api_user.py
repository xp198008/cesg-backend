"""用户信息（基础数据管理 - 用户列表 + 登录）"""
from __future__ import annotations

import json
import secrets
import string
from datetime import datetime

import bcrypt
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app import jt808_user
from app.database import get_db
from app.models import OrgCompany, SysRole, SysUser, UserLoginLog, UserOperationLog
from app.permission_names import permission_ids_to_piped_titles

router = APIRouter(prefix="/api/user", tags=["user"])


def _client_login_ip(request: Request) -> str:
    xff = (request.headers.get("x-forwarded-for") or "").strip()
    if xff:
        return (xff.split(",")[0] or "").strip()[:64] or ""
    xri = (request.headers.get("x-real-ip") or "").strip()
    if xri:
        return xri[:64]
    try:
        host = request.client.host if request.client else ""
    except Exception:
        host = ""
    return (host or "")[:64]


class UserLoginPayload(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=128)


class UserOperationLogIn(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    operation_content: str = Field(..., min_length=1, max_length=2000)


class UserCreatePayload(BaseModel):
    org_id: int = Field(..., ge=1)
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=6, max_length=128)
    allow_pwd_edit: int = Field(default=1)
    role_id: int = Field(..., ge=1)
    user_status: int = Field(default=1)


class UserUpdatePayload(BaseModel):
    user_id: int = Field(..., ge=1)
    org_id: int = Field(..., ge=1)
    username: str = Field(..., min_length=1, max_length=64)
    allow_pwd_edit: int = Field(default=1)
    role_id: int = Field(..., ge=1)
    user_status: int = Field(default=1)


class UserResetPasswordPayload(BaseModel):
    user_id: int = Field(..., ge=1)
    length: int = Field(default=6, ge=6, le=32)


class UserSetPasswordPayload(BaseModel):
    user_id: int = Field(..., ge=1)
    password: str = Field(..., min_length=6, max_length=128)


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return ""
    if getattr(dt, "tzinfo", None) is not None:
        dt = dt.replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _effective_role_code_for_session(role: SysRole | None, username: str) -> str:
    un = (username or "").strip().lower()
    if un == "admin":
        return "admin"
    if role is None:
        return ""
    code_raw = (role.code or "").strip()
    if code_raw.lower() == "admin":
        return "admin"
    name = (role.name or "").strip()
    if getattr(role, "is_global", False) and not code_raw and name in ("系统管理员", "管理员"):
        return "admin"
    return code_raw


def _generate_login_password(length: int = 6) -> str:
    lower = string.ascii_lowercase
    upper = string.ascii_uppercase
    digit = string.digits
    chars = [secrets.choice(lower), secrets.choice(upper), secrets.choice(digit)]
    pool = lower + upper + digit
    while len(chars) < length:
        chars.append(secrets.choice(pool))
    secrets.SystemRandom().shuffle(chars)
    return "".join(chars)


@router.get("/list")
async def user_list(
    keyword: str | None = Query(default=None),
    role_id: int | None = Query(default=None, ge=1),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(SysUser).options(selectinload(SysUser.role), selectinload(SysUser.org))
    kw = (keyword or "").strip()
    if kw:
        like_kw = f"%{kw}%"
        stmt = stmt.join(OrgCompany, SysUser.org_id == OrgCompany.id, isouter=True).where(
            or_(SysUser.username.like(like_kw), OrgCompany.name.like(like_kw))
        )
    if role_id is not None:
        stmt = stmt.where(SysUser.role_id == role_id)
    stmt = stmt.order_by(SysUser.id)
    users = (await db.execute(stmt)).scalars().all()
    out: list[dict] = []
    for u in users:
        org_name = u.org.name if u.org else ""
        role_name = u.role.name if u.role else ""
        role_perm = "—"
        if u.role:
            if (u.role.code or "").lower() == "admin":
                role_perm = "全部模块"
            else:
                role_perm = permission_ids_to_piped_titles(u.role.permissions)
        allow = getattr(u, "allow_pwd_edit", True)
        out.append(
            {
                "id": u.id,
                "org_id": u.org_id,
                "org_name": org_name,
                "username": u.username,
                "role_id": u.role_id,
                "role_name": role_name,
                "role_perm": role_perm,
                "user_status": 1 if u.is_active else 0,
                "allow_pwd_edit": 1 if allow else 0,
                "updated_at": _fmt_dt(u.updated_at) or _fmt_dt(u.created_at),
            }
        )
    return {"ok": True, "list": out, "total": len(out)}


@router.post("/create")
async def user_create(
    payload: UserCreatePayload,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    username = (payload.username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="用户名不能为空")
    exists = await db.scalar(select(SysUser.id).where(SysUser.username == username).limit(1))
    if exists is not None:
        raise HTTPException(status_code=400, detail="用户名已存在，请更换后重试")
    org = await db.scalar(select(OrgCompany).where(OrgCompany.id == payload.org_id).limit(1))
    if org is None:
        raise HTTPException(status_code=400, detail="所属公司不存在，请重新选择")
    role = await db.scalar(select(SysRole).where(SysRole.id == payload.role_id).limit(1))
    if role is None:
        raise HTTPException(status_code=400, detail="角色不存在，请重新选择")
    try:
        pwd_hash = bcrypt.hashpw(payload.password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        user = SysUser(
            username=username,
            password_hash=pwd_hash,
            password_plain=payload.password,
            real_name=username,
            role_id=payload.role_id,
            org_id=payload.org_id,
            allow_pwd_edit=bool(int(payload.allow_pwd_edit)),
            is_active=bool(int(payload.user_status)),
        )
        db.add(user)
        await db.flush()
        await db.refresh(user)
    except IntegrityError:
        raise HTTPException(status_code=400, detail="数据唯一性校验失败（用户名或联合唯一约束冲突）")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"新增用户失败: {e}")

    uid = user.id
    await db.commit()
    background_tasks.add_task(jt808_user.bg_create, uid)
    return {
        "ok": True,
        "message": "保存成功",
        "jt808_sync": "queued",
        "data": {
            "id": user.id,
            "username": user.username,
            "org_id": user.org_id,
            "org_name": org.name,
            "role_id": user.role_id,
            "role_name": role.name,
            "allow_pwd_edit": 1 if user.allow_pwd_edit else 0,
            "user_status": 1 if user.is_active else 0,
            "updated_at": _fmt_dt(user.updated_at) or _fmt_dt(user.created_at),
        },
    }


@router.post("/update")
async def user_update(
    payload: UserUpdatePayload,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    username = (payload.username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="用户名不能为空")
    user = await db.scalar(select(SysUser).where(SysUser.id == payload.user_id).limit(1))
    if user is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    old_username = (user.username or "").strip()
    exists = await db.scalar(
        select(SysUser.id).where(SysUser.username == username, SysUser.id != payload.user_id).limit(1)
    )
    if exists is not None:
        raise HTTPException(status_code=400, detail="用户名已存在，请更换后重试")
    org = await db.scalar(select(OrgCompany).where(OrgCompany.id == payload.org_id).limit(1))
    if org is None:
        raise HTTPException(status_code=400, detail="所属公司不存在，请重新选择")
    role = await db.scalar(select(SysRole).where(SysRole.id == payload.role_id).limit(1))
    if role is None:
        raise HTTPException(status_code=400, detail="角色不存在，请重新选择")
    try:
        user.username = username
        user.real_name = username
        user.org_id = payload.org_id
        user.role_id = payload.role_id
        user.allow_pwd_edit = bool(int(payload.allow_pwd_edit))
        user.is_active = bool(int(payload.user_status))
        await db.flush()
        await db.refresh(user)
    except IntegrityError:
        raise HTTPException(status_code=400, detail="数据唯一性校验失败（用户名或联合唯一约束冲突）")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新用户失败: {e}")

    await db.commit()
    background_tasks.add_task(jt808_user.bg_update, user.id, old_username)
    return {
        "ok": True,
        "message": "更新成功",
        "jt808_sync": "queued",
        "data": {
            "id": user.id,
            "username": user.username,
            "org_id": user.org_id,
            "org_name": org.name,
            "role_id": user.role_id,
            "role_name": role.name,
            "allow_pwd_edit": 1 if user.allow_pwd_edit else 0,
            "user_status": 1 if user.is_active else 0,
            "updated_at": _fmt_dt(user.updated_at) or _fmt_dt(user.created_at),
        },
    }


@router.get("/credential/{user_id}")
async def user_credential(user_id: int, db: AsyncSession = Depends(get_db)):
    user = await db.scalar(select(SysUser).where(SysUser.id == user_id).limit(1))
    if user is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    pwd_plain = (getattr(user, "password_plain", "") or "").strip()
    if not pwd_plain:
        saved_hash = (user.password_hash or "").strip()
        guessed = ""
        if saved_hash and not saved_hash.startswith("$2"):
            guessed = saved_hash
        else:
            for cand in ("123456", "admin123"):
                try:
                    if bcrypt.checkpw(cand.encode("utf-8"), saved_hash.encode("utf-8")):
                        guessed = cand
                        break
                except Exception:
                    pass
        if guessed:
            user.password_plain = guessed
            await db.flush()
            pwd_plain = guessed
    if not pwd_plain:
        raise HTTPException(status_code=400, detail="该用户暂无可复制明文密码，请先重置密码后再复制")
    return {"ok": True, "text": f"{user.username}/{pwd_plain}"}


@router.post("/set-password")
async def user_set_password(
    payload: UserSetPasswordPayload,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    user = await db.scalar(select(SysUser).where(SysUser.id == payload.user_id).limit(1))
    if user is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    new_pwd = (payload.password or "").strip()
    if len(new_pwd) < 6:
        raise HTTPException(status_code=400, detail="密码至少6位")
    try:
        user.password_hash = bcrypt.hashpw(new_pwd.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        user.password_plain = new_pwd
        await db.flush()
        await db.refresh(user)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"修改密码失败: {e}")

    await db.commit()
    background_tasks.add_task(jt808_user.bg_set_password, user.id, new_pwd)
    return {
        "ok": True,
        "message": "密码已更新",
        "jt808_sync": "queued",
        "data": {
            "user_id": user.id,
            "username": user.username,
            "updated_at": _fmt_dt(user.updated_at) or _fmt_dt(user.created_at),
        },
    }


@router.post("/reset-password")
async def user_reset_password(
    payload: UserResetPasswordPayload,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    user = await db.scalar(select(SysUser).where(SysUser.id == payload.user_id).limit(1))
    if user is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    new_pwd = _generate_login_password(payload.length)
    user.password_hash = bcrypt.hashpw(new_pwd.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    user.password_plain = new_pwd
    await db.flush()
    await db.refresh(user)
    await db.commit()
    background_tasks.add_task(jt808_user.bg_set_password, user.id, new_pwd)
    return {
        "ok": True,
        "message": "密码已重置并可复制",
        "jt808_sync": "queued",
        "text": f"{user.username}/{new_pwd}",
        "data": {
            "user_id": user.id,
            "username": user.username,
            "password": new_pwd,
            "updated_at": _fmt_dt(user.updated_at) or _fmt_dt(user.created_at),
        },
    }


@router.post("/operation-log")
async def user_operation_log_append(payload: UserOperationLogIn, db: AsyncSession = Depends(get_db)):
    uname = (payload.username or "").strip()[:64]
    content = (payload.operation_content or "").strip()[:2000]
    if not uname or not content:
        raise HTTPException(status_code=400, detail="username 与 operation_content 不能为空")
    row = UserOperationLog(username=uname, operation_content=content)
    db.add(row)
    await db.flush()
    return {"ok": True, "id": row.id}


@router.post("/login")
async def user_login(payload: UserLoginPayload, request: Request, db: AsyncSession = Depends(get_db)):
    username = (payload.username or "").strip()
    password = payload.password or ""
    if not username or not password:
        raise HTTPException(status_code=400, detail="用户名和密码不能为空")

    result = await db.execute(
        select(SysUser)
        .options(selectinload(SysUser.role), selectinload(SysUser.org))
        .where(SysUser.username == username)
        .limit(1)
    )
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="当前用户已禁用，请联系管理员")

    saved_hash = user.password_hash or ""
    try:
        ok = bcrypt.checkpw(password.encode("utf-8"), saved_hash.encode("utf-8"))
    except Exception:
        ok = False
    if not ok and saved_hash and not saved_hash.startswith("$2"):
        ok = password == saved_hash
    if not ok:
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    role = user.role
    role_code = _effective_role_code_for_session(role, username)
    permission_ids: list[str] = []
    if role and (role.permissions or "").strip():
        try:
            raw = json.loads(role.permissions)
            if isinstance(raw, list):
                permission_ids = [str(x) for x in raw if x is not None]
        except (json.JSONDecodeError, TypeError):
            permission_ids = []

    org_id = user.org_id
    org_name = (user.org.name if user.org else "") or ""
    if org_id is None and role and role.org_id is not None:
        org_id = int(role.org_id)
        ro_co = await db.scalar(select(OrgCompany).where(OrgCompany.id == org_id).limit(1))
        org_name = (ro_co.name if ro_co else "") or org_name
    if org_id is None and role_code.strip().lower() == "admin":
        first_org = await db.scalar(select(OrgCompany).order_by(OrgCompany.id.asc()).limit(1))
        if first_org is not None:
            org_id = int(first_org.id)
            org_name = (first_org.name or "") or org_name

    data = {
        "id": user.id,
        "username": user.username,
        "real_name": user.real_name or user.username,
        "org_id": org_id,
        "org_name": org_name,
        "role_id": user.role_id,
        "role_name": user.role.name if user.role else "",
        "role_code": role_code,
        "permission_ids": permission_ids,
        "allow_pwd_edit": 1 if getattr(user, "allow_pwd_edit", True) else 0,
        "user_status": 1 if user.is_active else 0,
    }

    try:
        db.add(UserLoginLog(username=(user.username or "")[:64], login_ip=_client_login_ip(request)))
        db.add(UserOperationLog(username=(user.username or "")[:64], operation_content="登录"))
        await db.flush()
    except Exception:
        await db.rollback()

    return {"ok": True, "message": "登录成功", "data": data}


@router.delete("/{user_id}")
async def user_delete(
    user_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    user = await db.scalar(select(SysUser).where(SysUser.id == user_id).limit(1))
    if user is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    if (user.username or "").strip().lower() == "admin":
        raise HTTPException(status_code=400, detail="admin 为系统内置账号，不允许删除")
    jt_uid = (getattr(user, "jt808_user_id", None) or "").strip() or None
    uname = (user.username or "").strip()
    await db.delete(user)
    await db.flush()
    await db.commit()
    background_tasks.add_task(jt808_user.bg_delete, user_id, uname, jt_uid)
    return {"ok": True, "message": "删除成功", "id": user_id, "jt808_sync": "queued"}

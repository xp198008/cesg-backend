"""用户信息（基础数据管理 - 用户列表 + 登录）"""
from __future__ import annotations

import json
import re
import secrets
import string
from datetime import date, datetime, time as dt_time

import bcrypt
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, not_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app import jt808_auth, jt808_user
from app.database import get_db
from app.models import OrgCompany, SysRole, SysUser, UserLoginLog, UserOnlineDaily, UserOperationLog
from app.permission_names import permission_ids_to_piped_titles
from app.user_audit import append_operation_log, client_ip, duration_between, duration_seconds_between, format_duration_seconds
from app.user_online_daily import (
    close_open_sessions_for_user,
    record_login_daily,
    resolve_user_org_profile,
    sync_login_session_to_daily,
)
from app.vehicle_alloc_scope import parse_user_id_header, resolve_monitor_scope

router = APIRouter(prefix="/api/user", tags=["user"])


def _client_login_ip(request: Request) -> str:
    return client_ip(request)


def _parse_query_datetime(value: str | None, *, end_of_day: bool = False) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    if len(text) >= 19:
        try:
            return datetime.strptime(text[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    if len(text) >= 10:
        try:
            dt = datetime.strptime(text[:10], "%Y-%m-%d")
            if end_of_day:
                return datetime.combine(dt.date(), dt_time(23, 59, 59))
            return dt
        except ValueError:
            pass
    return None


class UserLoginPayload(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=128)


class UserSessionCheckPayload(BaseModel):
    user_id: int = Field(..., ge=1)
    session_token: str | None = Field(default=None, max_length=128)


class UserOperationLogIn(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    operation_content: str = Field(..., min_length=1, max_length=2000)
    user_id: int | None = Field(default=None, ge=1)
    real_name: str | None = Field(default=None, max_length=64)
    org_id: int | None = Field(default=None, ge=1)
    org_name: str | None = Field(default=None, max_length=128)
    module: str | None = Field(default=None, max_length=64)
    menu: str | None = Field(default=None, max_length=64)
    action: str | None = Field(default=None, max_length=64)
    result: str | None = Field(default="成功", max_length=16)
    vehicle: str | None = Field(default=None, max_length=32)
    plate_color: str | None = Field(default=None, max_length=16)
    device_no: str | None = Field(default=None, max_length=64)


class UserLogoutPayload(BaseModel):
    username: str | None = Field(default=None, max_length=64)
    login_log_id: int | None = Field(default=None, ge=1)
    online_seconds: int | None = Field(default=None, ge=0)


class UserSessionHeartbeatPayload(BaseModel):
    login_log_id: int = Field(..., ge=1)
    online_seconds: int = Field(..., ge=0)
    finalize: bool = Field(default=False)


class UserCreatePayload(BaseModel):
    org_id: int = Field(..., ge=1)
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=6, max_length=128)
    allow_pwd_edit: int = Field(default=1)
    role_id: int = Field(..., ge=1)
    user_status: int = Field(default=1)
    valid_until: date | None = None
    single_login: int = Field(default=0)
    identity: str | None = Field(None, max_length=64)
    phone: str | None = Field(None, max_length=32)


class UserUpdatePayload(BaseModel):
    user_id: int = Field(..., ge=1)
    org_id: int = Field(..., ge=1)
    username: str = Field(..., min_length=1, max_length=64)
    allow_pwd_edit: int = Field(default=1)
    role_id: int = Field(..., ge=1)
    user_status: int = Field(default=1)
    valid_until: date | None = None
    single_login: int = Field(default=0)
    identity: str | None = Field(None, max_length=64)
    phone: str | None = Field(None, max_length=32)


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


def _fmt_date(d: date | None) -> str:
    if d is None:
        return ""
    return d.strftime("%Y-%m-%d")


def _manual_operation_log_clause():
    """仅保留人工操作：排除页面自动跳转、HTTP 自动审计、登录/退出系统写入。"""
    legacy_auto = or_(
        UserOperationLog.action == "页面访问",
        UserOperationLog.operation_content.like("提交数据：%"),
        UserOperationLog.operation_content.like("修改数据：%"),
        UserOperationLog.operation_content.like("删除数据：%"),
        and_(
            UserOperationLog.module == "系统",
            UserOperationLog.menu == "登录",
            UserOperationLog.action.in_(["登录", "退出"]),
        ),
    )
    return or_(
        UserOperationLog.source == "manual",
        and_(UserOperationLog.source.is_(None), not_(legacy_auto)),
    )


def _login_log_duration_seconds(row: UserLoginLog, *, now: datetime | None = None) -> int | None:
    return duration_seconds_between(
        row.login_at,
        row.logout_at,
        online_seconds=row.online_seconds,
        now=now,
    )


def _apply_online_seconds(row: UserLoginLog, online_seconds: int | None) -> None:
    if online_seconds is None or online_seconds < 0:
        return
    current = row.online_seconds
    if current is None or online_seconds > current:
        row.online_seconds = int(online_seconds)


def _norm_optional_text(value: str | None) -> str | None:
    text = (value or "").strip()
    return text or None


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


def _jt808_sync_view(sync_result: dict) -> tuple[str, str | None]:
    """将 sync_set_password 结果转为前端 jt808_sync 字段。"""
    if sync_result.get("skipped"):
        return "skipped", sync_result.get("reason") or "808 同步已关闭"
    if sync_result.get("ok"):
        return "success", sync_result.get("message")
    return "failed", sync_result.get("message") or "808 同步失败"


@router.get("/list")
async def user_list(
    keyword: str | None = Query(default=None),
    role_id: int | None = Query(default=None, ge=1),
    user_status: int | None = Query(default=None),
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
    if user_status is not None:
        stmt = stmt.where(SysUser.is_active == bool(int(user_status)))
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
                "valid_until": _fmt_date(getattr(u, "valid_until", None)),
                "single_login": 1 if getattr(u, "single_login", False) else 0,
                "identity": getattr(u, "identity", None) or "",
                "phone": getattr(u, "phone", None) or "",
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
    identity = _norm_optional_text(payload.identity)
    phone = _norm_optional_text(payload.phone)
    if phone and not re.fullmatch(r"1\d{10}", phone):
        raise HTTPException(status_code=400, detail="手机号格式不正确")
    try:
        pwd_hash = bcrypt.hashpw(payload.password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        user = SysUser(
            username=username,
            password_hash=pwd_hash,
            password_plain=payload.password,
            real_name=username,
            identity=identity,
            phone=phone,
            role_id=payload.role_id,
            org_id=payload.org_id,
            allow_pwd_edit=bool(int(payload.allow_pwd_edit)),
            is_active=bool(int(payload.user_status)),
            valid_until=payload.valid_until,
            single_login=bool(int(payload.single_login)),
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
            "valid_until": _fmt_date(user.valid_until),
            "single_login": 1 if user.single_login else 0,
            "identity": user.identity or "",
            "phone": user.phone or "",
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
    identity = _norm_optional_text(payload.identity)
    phone = _norm_optional_text(payload.phone)
    if phone and not re.fullmatch(r"1\d{10}", phone):
        raise HTTPException(status_code=400, detail="手机号格式不正确")
    try:
        user.username = username
        user.real_name = username
        user.identity = identity
        user.phone = phone
        user.org_id = payload.org_id
        user.role_id = payload.role_id
        user.allow_pwd_edit = bool(int(payload.allow_pwd_edit))
        user.is_active = bool(int(payload.user_status))
        user.valid_until = payload.valid_until
        user.single_login = bool(int(payload.single_login))
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
            "valid_until": _fmt_date(user.valid_until),
            "single_login": 1 if user.single_login else 0,
            "identity": user.identity or "",
            "phone": user.phone or "",
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
    sync_result = await jt808_user.sync_set_password(user.id, new_pwd)
    jt808_sync, jt808_sync_message = _jt808_sync_view(sync_result)
    return {
        "ok": True,
        "message": "密码已更新",
        "jt808_sync": jt808_sync,
        "jt808_sync_message": jt808_sync_message,
        "data": {
            "user_id": user.id,
            "username": user.username,
            "updated_at": _fmt_dt(user.updated_at) or _fmt_dt(user.created_at),
        },
    }


@router.post("/reset-password")
async def user_reset_password(
    payload: UserResetPasswordPayload,
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
    sync_result = await jt808_user.sync_set_password(user.id, new_pwd)
    jt808_sync, jt808_sync_message = _jt808_sync_view(sync_result)
    return {
        "ok": True,
        "message": "密码已重置并可复制",
        "jt808_sync": jt808_sync,
        "jt808_sync_message": jt808_sync_message,
        "text": f"{user.username}/{new_pwd}",
        "data": {
            "user_id": user.id,
            "username": user.username,
            "password": new_pwd,
            "updated_at": _fmt_dt(user.updated_at) or _fmt_dt(user.created_at),
        },
    }


@router.post("/operation-log")
async def user_operation_log_append(payload: UserOperationLogIn, request: Request, db: AsyncSession = Depends(get_db)):
    uname = (payload.username or "").strip()[:64]
    content = (payload.operation_content or "").strip()[:2000]
    if not uname or not content:
        raise HTTPException(status_code=400, detail="username 与 operation_content 不能为空")
    row = await append_operation_log(
        db,
        username=uname,
        operation_content=content,
        user_id=payload.user_id,
        real_name=payload.real_name,
        org_id=payload.org_id,
        org_name=payload.org_name,
        module=payload.module,
        menu=payload.menu,
        action=payload.action,
        operation_ip=_client_login_ip(request),
        result=(payload.result or "成功")[:16],
        vehicle=payload.vehicle,
        plate_color=payload.plate_color,
        device_no=payload.device_no,
        source="manual",
    )
    return {"ok": True, "id": row.id}


@router.post("/logout")
async def user_logout(payload: UserLogoutPayload, request: Request, db: AsyncSession = Depends(get_db)):
    username = (payload.username or "").strip()[:64]
    ip = _client_login_ip(request)
    logout_at = datetime.now()
    login_row: UserLoginLog | None = None
    if payload.login_log_id is not None:
        login_row = await db.scalar(select(UserLoginLog).where(UserLoginLog.id == payload.login_log_id).limit(1))
    if login_row is None and username:
        login_row = await db.scalar(
            select(UserLoginLog)
            .where(UserLoginLog.username == username, UserLoginLog.logout_at.is_(None))
            .order_by(UserLoginLog.login_at.desc())
            .limit(1)
        )
    if login_row is not None and login_row.logout_at is None:
        await sync_login_session_to_daily(db, login_row)
        login_row.logout_at = logout_at
        _apply_online_seconds(login_row, payload.online_seconds)
        username = username or (login_row.username or "")
    elif login_row is not None:
        await sync_login_session_to_daily(db, login_row)
        _apply_online_seconds(login_row, payload.online_seconds)
    await db.flush()
    return {"ok": True, "message": "已退出登录"}


@router.post("/session/heartbeat")
async def user_session_heartbeat(payload: UserSessionHeartbeatPayload, db: AsyncSession = Depends(get_db)):
    login_row = await db.scalar(select(UserLoginLog).where(UserLoginLog.id == payload.login_log_id).limit(1))
    if login_row is None:
        raise HTTPException(status_code=404, detail="登录会话不存在")
    await sync_login_session_to_daily(db, login_row)
    _apply_online_seconds(login_row, payload.online_seconds)
    if payload.finalize and login_row.logout_at is None:
        login_row.logout_at = datetime.now()
    await db.flush()
    return {
        "ok": True,
        "online_seconds": login_row.online_seconds,
        "logout_at": _fmt_dt(login_row.logout_at) or None,
    }


@router.get("/login-logs")
async def user_login_logs(
    username: str | None = Query(default=None),
    start_at: str | None = Query(default=None),
    end_at: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(UserLoginLog)
    count_stmt = select(func.count()).select_from(UserLoginLog)
    uname = (username or "").strip()
    if uname:
        stmt = stmt.where(UserLoginLog.username.like(f"%{uname}%"))
        count_stmt = count_stmt.where(UserLoginLog.username.like(f"%{uname}%"))
    start_dt = _parse_query_datetime(start_at)
    end_dt = _parse_query_datetime(end_at, end_of_day=True)
    if start_dt is not None:
        stmt = stmt.where(UserLoginLog.login_at >= start_dt)
        count_stmt = count_stmt.where(UserLoginLog.login_at >= start_dt)
    if end_dt is not None:
        stmt = stmt.where(UserLoginLog.login_at <= end_dt)
        count_stmt = count_stmt.where(UserLoginLog.login_at <= end_dt)
    total = int((await db.scalar(count_stmt)) or 0)
    rows = (
        await db.execute(
            stmt.order_by(UserLoginLog.login_at.desc()).offset((page - 1) * page_size).limit(page_size)
        )
    ).scalars().all()
    now = datetime.now()
    items = []
    for row in rows:
        company = (row.org_name or "").strip()
        if not company:
            _, _, _, org_name = await resolve_user_org_profile(db, row.username or "")
            company = (org_name or "").strip()
        items.append(
            {
                "id": row.id,
                "user_id": row.user_id,
                "account": row.username,
                "name": row.real_name or row.username,
                "company": company or "--",
                "loginMethod": "web浏览器" if (row.login_method or "web") == "web" else (row.login_method or "--"),
                "loginIp": row.login_ip or "--",
                "startAt": _fmt_dt(row.login_at),
                "endAt": _fmt_dt(row.logout_at) or "--",
                "totalDuration": duration_between(
                    row.login_at, row.logout_at, online_seconds=row.online_seconds, now=now
                ),
                "remark": "--",
            }
        )
    return {"ok": True, "items": items, "total": total, "page": page, "page_size": page_size}


@router.get("/online-duration-stats")
async def user_online_duration_stats(
    username: str | None = Query(default=None),
    start_at: str | None = Query(default=None),
    end_at: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(UserOnlineDaily)
    count_stmt = select(func.count()).select_from(UserOnlineDaily)
    uname = (username or "").strip()
    if uname:
        stmt = stmt.where(UserOnlineDaily.username.like(f"%{uname}%"))
        count_stmt = count_stmt.where(UserOnlineDaily.username.like(f"%{uname}%"))
    start_dt = _parse_query_datetime(start_at)
    end_dt = _parse_query_datetime(end_at, end_of_day=True)
    if start_dt is None and end_dt is None:
        today = date.today()
        start_dt = datetime.combine(today, dt_time.min)
        end_dt = datetime.combine(today, dt_time(23, 59, 59))
    elif start_dt is None:
        start_dt = datetime.combine(end_dt.date(), dt_time.min)
    elif end_dt is None:
        end_dt = datetime.combine(start_dt.date(), dt_time(23, 59, 59))
    stmt = stmt.where(UserOnlineDaily.stat_date >= start_dt.date())
    count_stmt = count_stmt.where(UserOnlineDaily.stat_date >= start_dt.date())
    stmt = stmt.where(UserOnlineDaily.stat_date <= end_dt.date())
    count_stmt = count_stmt.where(UserOnlineDaily.stat_date <= end_dt.date())
    total = int((await db.scalar(count_stmt)) or 0)
    rows = (
        await db.execute(
            stmt.order_by(UserOnlineDaily.stat_date.desc(), UserOnlineDaily.username.asc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).scalars().all()
    items = []
    for row in rows:
        company = (row.org_name or "").strip()
        if not company:
            _, _, _, org_name = await resolve_user_org_profile(db, row.username or "")
            company = (org_name or "").strip()
        items.append(
            {
                "account": row.username or "--",
                "name": row.real_name or row.username or "--",
                "company": company or "--",
                "statDate": row.stat_date.strftime("%Y-%m-%d") if row.stat_date else "--",
                "totalDuration": format_duration_seconds(int(row.online_seconds or 0)),
                "loginCount": int(row.login_count or 0),
            }
        )
    return {"ok": True, "items": items, "total": total, "page": page, "page_size": page_size}


@router.get("/operation-logs")
async def user_operation_logs(
    username: str | None = Query(default=None),
    module: str | None = Query(default=None),
    action: str | None = Query(default=None),
    vehicle: str | None = Query(default=None),
    vehicle_only: int | None = Query(default=None),
    start_at: str | None = Query(default=None),
    end_at: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(UserOperationLog).where(_manual_operation_log_clause())
    count_stmt = select(func.count()).select_from(UserOperationLog).where(_manual_operation_log_clause())
    uname = (username or "").strip()
    if uname:
        stmt = stmt.where(UserOperationLog.username.like(f"%{uname}%"))
        count_stmt = count_stmt.where(UserOperationLog.username.like(f"%{uname}%"))
    mod = (module or "").strip()
    if mod:
        stmt = stmt.where(or_(UserOperationLog.module.like(f"%{mod}%"), UserOperationLog.menu.like(f"%{mod}%")))
        count_stmt = count_stmt.where(or_(UserOperationLog.module.like(f"%{mod}%"), UserOperationLog.menu.like(f"%{mod}%")))
    act = (action or "").strip()
    if act:
        stmt = stmt.where(UserOperationLog.action.like(f"%{act}%"))
        count_stmt = count_stmt.where(UserOperationLog.action.like(f"%{act}%"))
    veh = (vehicle or "").strip()
    if veh:
        stmt = stmt.where(UserOperationLog.vehicle.like(f"%{veh}%"))
        count_stmt = count_stmt.where(UserOperationLog.vehicle.like(f"%{veh}%"))
    if int(vehicle_only or 0) == 1:
        stmt = stmt.where(UserOperationLog.vehicle.isnot(None), UserOperationLog.vehicle != "")
        count_stmt = count_stmt.where(UserOperationLog.vehicle.isnot(None), UserOperationLog.vehicle != "")
    start_dt = _parse_query_datetime(start_at)
    end_dt = _parse_query_datetime(end_at, end_of_day=True)
    if start_dt is not None:
        stmt = stmt.where(UserOperationLog.created_at >= start_dt)
        count_stmt = count_stmt.where(UserOperationLog.created_at >= start_dt)
    if end_dt is not None:
        stmt = stmt.where(UserOperationLog.created_at <= end_dt)
        count_stmt = count_stmt.where(UserOperationLog.created_at <= end_dt)
    total = int((await db.scalar(count_stmt)) or 0)
    rows = (
        await db.execute(
            stmt.order_by(UserOperationLog.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
        )
    ).scalars().all()
    items = [
        {
            "id": row.id,
            "account": row.username,
            "name": row.real_name or row.username,
            "company": row.org_name or "--",
            "module": row.module or "--",
            "menu": row.menu or "--",
            "operationType": row.action or row.module or "--",
            "vehicle": row.vehicle or "--",
            "plateColor": row.plate_color or "--",
            "deviceNo": row.device_no or "--",
            "time": _fmt_dt(row.created_at),
            "operationIp": row.operation_ip or "--",
            "result": row.result or "成功",
            "description": row.operation_content or "--",
        }
        for row in rows
    ]
    return {"ok": True, "items": items, "total": total, "page": page, "page_size": page_size}


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
    if user.valid_until is not None and user.valid_until < date.today():
        raise HTTPException(status_code=403, detail="当前用户已过有效期，请联系管理员")

    saved_hash = user.password_hash or ""
    try:
        ok = bcrypt.checkpw(password.encode("utf-8"), saved_hash.encode("utf-8"))
    except Exception:
        ok = False
    if not ok and saved_hash and not saved_hash.startswith("$2"):
        ok = password == saved_hash
    if not ok:
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    session_token = ""
    if getattr(user, "single_login", False):
        # 开启“单点登录”时，每次成功登录刷新 token，旧浏览器持有的 token 随即失效。
        session_token = secrets.token_urlsafe(32)
        user.login_session_token = session_token
        await db.flush()

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
        "valid_until": _fmt_date(user.valid_until),
        "single_login": 1 if user.single_login else 0,
        "session_token": session_token,
    }

    login_log = UserLoginLog(
        user_id=user.id,
        username=(user.username or "")[:64],
        real_name=(user.real_name or user.username or "")[:64],
        org_id=org_id,
        org_name=(org_name or "")[:128] or None,
        role_id=user.role_id,
        role_name=(user.role.name if user.role else "")[:64] or None,
        login_ip=_client_login_ip(request),
        login_method="web",
    )

    try:
        login_now = datetime.now()
        await close_open_sessions_for_user(db, username, logout_at=login_now)
        db.add(login_log)
        await db.flush()
        await record_login_daily(db, login_log)
        data["login_log_id"] = login_log.id
        data["login_at"] = _fmt_dt(login_log.login_at)
        await db.flush()
    except Exception:
        await db.rollback()
        if session_token:
            fresh_user = await db.scalar(select(SysUser).where(SysUser.id == user.id).limit(1))
            if fresh_user is not None:
                fresh_user.login_session_token = session_token
                await db.commit()
    else:
        await db.commit()

    return {"ok": True, "message": "登录成功", "data": data}


@router.get("/monitor-scope")
async def user_monitor_scope(
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    """实时监控：返回当前用户可见车辆键（车牌/设备号），供 808 树前端过滤。"""
    user_id = parse_user_id_header(x_user_id)
    if user_id is None:
        raise HTTPException(status_code=400, detail="缺少请求头 X-User-Id")
    scope = await resolve_monitor_scope(db, user_id)
    return {"ok": True, "data": scope}


@router.post("/jt808-auth-sync")
async def user_jt808_auth_sync(
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
    x_lingx_token: str | None = Header(None, alias="X-Lingx-Token"),
):
    """登录后调用：用当前用户的 808 token 同步车组授权（1252）。"""
    raw_uid = (x_user_id or "").strip()
    if not raw_uid:
        raise HTTPException(status_code=400, detail="缺少请求头 X-User-Id")
    try:
        user_id = int(raw_uid, 10)
    except ValueError:
        raise HTTPException(status_code=400, detail="X-User-Id 无效") from None
    if user_id < 1:
        raise HTTPException(status_code=400, detail="X-User-Id 无效")

    user = await db.scalar(
        select(SysUser).options(selectinload(SysUser.role)).where(SysUser.id == user_id).limit(1)
    )
    if user is None:
        raise HTTPException(status_code=404, detail="用户不存在")

    role_code = _effective_role_code_for_session(user.role, user.username or "")
    result = await jt808_auth.sync_user_group_auth(
        db,
        user_id,
        (x_lingx_token or "").strip(),
        role_code=role_code,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message") or "808 授权同步失败")
    return {"ok": True, "message": "808 车组授权已同步", "data": result}


@router.post("/session/check")
async def user_session_check(payload: UserSessionCheckPayload, db: AsyncSession = Depends(get_db)):
    user = await db.scalar(select(SysUser).where(SysUser.id == payload.user_id).limit(1))
    if user is None:
        raise HTTPException(status_code=401, detail="当前用户不存在，请重新登录")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="当前用户已禁用，请重新登录")
    if user.valid_until is not None and user.valid_until < date.today():
        raise HTTPException(status_code=403, detail="当前用户已过有效期，请重新登录")
    if getattr(user, "single_login", False):
        server_token = (getattr(user, "login_session_token", None) or "").strip()
        client_token = (payload.session_token or "").strip()
        if not server_token or not client_token or server_token != client_token:
            raise HTTPException(status_code=409, detail="该账号已在其它设备登录，请重新登录")
    return {"ok": True}


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

"""CESG 业务接口登录会话校验。

前端已通过 X-Session-Token（及登录 Cookie）携带会话；本中间件拒绝未带有效会话的请求。
登录 / 短信验证码 / OPTIONS 预检除外。使用纯 ASGI 中间件，避免打断 SSE 流式接口。
"""
from __future__ import annotations

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from app.ttl_cache import ttl_clear, ttl_get, ttl_set

# 轮询接口（alert-cache 每 5s）不能每次都查 SQLite；启动回填占写锁时会卡到 30s+
_SESSION_OK_TTL = 8.0

COOKIE_NAME = "cesg_session"

_PUBLIC_EXACT = {
    "/api/user/login",
    "/api/user/login-by-phone",
    "/api/sms/send-code",
    "/favicon.ico",
    # 运维页先出密码框；门禁接口只认独立 ops 会话，不复用前台登录
    "/obd-status",
    "/api/obd-status/unlock",
    "/api/obd-status/session",
}

_PUBLIC_PREFIXES = (
    # 登录页本地下载（若经 8100 提供）
    "/static/downloads/",
    # 808 /api 校验网关（808 自身用 lingxtoken，不走 CESG 会话）
    "/internal/jt808-gateway",
)

# 仅运维页使用：前台登录不能调用
_OPS_ONLY_PREFIXES = (
    "/api/obd-speed-check",
    "/api/violation-ai-assess",
)


def normalize_api_path(path: str) -> str:
    p = path or "/"
    if p.startswith("/cmapi/"):
        p = "/api/" + p[7:]
    elif p == "/cmapi":
        p = "/api"
    if len(p) > 1 and p.endswith("/"):
        p = p.rstrip("/")
    return p


def is_ops_only_path(path: str) -> bool:
    p = normalize_api_path(path)
    return any(p == prefix or p.startswith(prefix + "/") for prefix in _OPS_ONLY_PREFIXES)


def is_public_path(path: str) -> bool:
    p = normalize_api_path(path)
    if p in _PUBLIC_EXACT:
        return True
    for prefix in _PUBLIC_PREFIXES:
        if p == prefix.rstrip("/") or p.startswith(prefix):
            return True
    return False


def extract_session_token(headers: dict[str, str]) -> str:
    token = (headers.get("x-session-token") or "").strip()
    if token:
        return token
    auth = (headers.get("authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    raw = headers.get("cookie") or ""
    for part in raw.split(";"):
        name, _, val = part.strip().partition("=")
        if name == COOKIE_NAME:
            return val.strip()
    return ""


def unauthorized_response(detail: str = "未登录或会话已失效，请重新登录") -> JSONResponse:
    return JSONResponse(status_code=401, content={"detail": detail})


def forbidden_response(detail: str) -> JSONResponse:
    return JSONResponse(status_code=403, content={"detail": detail})


def kicked_response(detail: str = "该账号已在其它设备登录，请重新登录") -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": detail})


def attach_session_cookie(response, session_token: str | None) -> None:
    token = (session_token or "").strip()
    if not token:
        return
    from app.config import settings

    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        secure=bool(settings.cookie_secure),
        samesite="strict",
        path="/",
    )


def clear_session_cookie(response) -> None:
    from app.config import settings

    response.delete_cookie(
        key=COOKIE_NAME,
        path="/",
        secure=bool(settings.cookie_secure),
        samesite="strict",
    )


def _session_cache_key(token: str) -> str:
    return f"session:tok:{token}"


def invalidate_session_token(token: str | None) -> None:
    raw = (token or "").strip()
    if raw:
        ttl_clear(_session_cache_key(raw))


def invalidate_all_session_tokens() -> None:
    ttl_clear("session:tok:")


def _headers_from_scope(scope: Scope) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, val in scope.get("headers") or []:
        out[key.decode("latin-1").lower()] = val.decode("latin-1")
    return out


async def _resolve_user(token: str, x_user_id: str | None):
    from sqlalchemy import select

    from app.database import AsyncSessionLocal
    from app.models import SysUser
    from app.timeutil import china_today

    cached = ttl_get(_session_cache_key(token), _SESSION_OK_TTL)
    if isinstance(cached, int):
        if x_user_id and str(x_user_id).strip() and str(cached) != str(x_user_id).strip():
            return "forbidden", "登录身份与请求用户不一致"
        return "ok", cached

    async with AsyncSessionLocal() as db:
        user = await db.scalar(
            select(SysUser).where(SysUser.login_session_token == token).limit(1)
        )
        if user is not None:
            uid = int(user.id)
            active = bool(user.is_active)
            valid_until = user.valid_until
            if not active:
                return "forbidden", "当前用户已禁用，请重新登录"
            if valid_until is not None and valid_until < china_today():
                return "forbidden", "当前用户已过有效期，请重新登录"
            if x_user_id and str(x_user_id).strip() and str(uid) != str(x_user_id).strip():
                return "forbidden", "登录身份与请求用户不一致"
            ttl_set(_session_cache_key(token), uid)
            return "ok", uid

        raw_uid = (x_user_id or "").strip()
        if raw_uid.isdigit():
            other = await db.scalar(select(SysUser).where(SysUser.id == int(raw_uid)).limit(1))
            if (
                other is not None
                and getattr(other, "single_login", False)
                and (getattr(other, "login_session_token", None) or "").strip()
                and (getattr(other, "login_session_token", None) or "").strip() != token
            ):
                return "kicked", "该账号已在其它设备登录，请重新登录"
    return "unauthorized", "未登录或会话已失效，请重新登录"


class SessionAuthMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        if (scope.get("method") or "GET") == "OPTIONS":
            await self.app(scope, receive, send)
            return
        path = scope.get("path") or "/"
        if is_public_path(path):
            await self.app(scope, receive, send)
            return

        headers = _headers_from_scope(scope)
        from app.ops_session import extract_ops_token, ops_token_ok

        ops_uid = ops_token_ok(extract_ops_token(headers))
        if ops_uid is not None:
            state = scope.setdefault("state", {})
            state["user_id"] = ops_uid
            state["ops_admin"] = True
            await self.app(scope, receive, send)
            return
        if is_ops_only_path(path):
            await unauthorized_response("请输入 admin 密码")(scope, receive, send)
            return

        token = extract_session_token(headers)
        if not token:
            await unauthorized_response()(scope, receive, send)
            return

        status, payload = await _resolve_user(token, headers.get("x-user-id"))
        if status == "ok":
            state = scope.setdefault("state", {})
            state["user_id"] = payload
            await self.app(scope, receive, send)
            return
        if status == "kicked":
            await kicked_response(str(payload))(scope, receive, send)
            return
        if status == "forbidden":
            await forbidden_response(str(payload))(scope, receive, send)
            return
        await unauthorized_response(str(payload))(scope, receive, send)

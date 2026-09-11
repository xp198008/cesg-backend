"""OBD-STATUS 运维门禁：独立会话，不复用前台 cesg_session。"""
from __future__ import annotations

import secrets
import time

OPS_COOKIE = "cesg_ops_session"
OPS_HEADER = "x-ops-token"
OPS_TTL_SECONDS = 4 * 60 * 60

# token -> (admin_user_id, expiry)
_ops_tokens: dict[str, tuple[int, float]] = {}


def issue_ops_token(admin_user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    _ops_tokens[token] = (int(admin_user_id), time.time() + OPS_TTL_SECONDS)
    return token


def ops_token_ok(token: str | None) -> int | None:
    raw = (token or "").strip()
    if not raw:
        return None
    row = _ops_tokens.get(raw)
    if row is None:
        return None
    uid, exp = row
    if exp < time.time():
        _ops_tokens.pop(raw, None)
        return None
    return uid


def extract_ops_token(headers: dict[str, str]) -> str:
    token = (headers.get(OPS_HEADER) or headers.get("x-ops-token") or "").strip()
    if token:
        return token
    raw = headers.get("cookie") or ""
    for part in raw.split(";"):
        name, _, val = part.strip().partition("=")
        if name == OPS_COOKIE:
            return val.strip()
    return ""


def attach_ops_cookie(response, token: str | None) -> None:
    raw = (token or "").strip()
    if not raw:
        return
    response.set_cookie(
        key=OPS_COOKIE,
        value=raw,
        httponly=True,
        samesite="lax",
        path="/",
        max_age=OPS_TTL_SECONDS,
    )


def clear_ops_cookie(response) -> None:
    response.delete_cookie(key=OPS_COOKIE, path="/")

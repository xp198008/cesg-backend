"""OBD-STATUS 运维门禁：独立会话，不复用前台 cesg_session。"""
from __future__ import annotations

import hashlib
import hmac
import time

OPS_COOKIE = "cesg_ops_session"
OPS_HEADER = "x-ops-token"
OPS_TTL_SECONDS = 4 * 60 * 60


def _ops_secret() -> bytes:
    from app.secret_box import _raw_secret

    return hashlib.sha256(("cesg-ops|" + _raw_secret()).encode("utf-8")).digest()


def issue_ops_token(admin_user_id: int) -> str:
    exp = int(time.time()) + OPS_TTL_SECONDS
    payload = f"{int(admin_user_id)}.{exp}"
    sig = hmac.new(_ops_secret(), payload.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    return f"{payload}.{sig}"


def ops_token_ok(token: str | None) -> int | None:
    raw = (token or "").strip()
    if not raw:
        return None
    parts = raw.split(".")
    if len(parts) != 3:
        return None
    uid_s, exp_s, sig = parts
    if not uid_s.isdigit() or not exp_s.isdigit():
        return None
    payload = f"{uid_s}.{exp_s}"
    expect = hmac.new(_ops_secret(), payload.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    if not hmac.compare_digest(sig, expect):
        return None
    if int(exp_s) < time.time():
        return None
    return int(uid_s)


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
        secure=True,
    )


def clear_ops_cookie(response) -> None:
    response.delete_cookie(key=OPS_COOKIE, path="/", secure=True, samesite="lax")

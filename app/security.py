"""安全整改共用：CORS 白名单、来源校验、严格入参模型、校验错误裁剪。"""
from __future__ import annotations

from urllib.parse import urlparse

from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict

from app.config import settings

INT32_MAX = 2_147_483_647
ONLINE_SECONDS_MAX = 31_536_000
PAGE_MAX = 100_000

_DEFAULT_CORS_ORIGINS = (
    "https://cs.v2xcloud.com",
    "https://113.207.68.96",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


def cors_origin_list() -> list[str]:
    raw = (getattr(settings, "cors_origins", None) or "").strip()
    if raw:
        items = [part.strip().rstrip("/") for part in raw.split(",") if part.strip()]
        if items:
            return items
    return list(_DEFAULT_CORS_ORIGINS)


def _origin_from_url(url: str) -> str:
    text = (url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def is_allowed_origin(origin: str) -> bool:
    value = (origin or "").strip().rstrip("/")
    if not value or value.lower() == "null":
        return False
    allowed = {item.lower() for item in cors_origin_list()}
    return value.lower() in allowed


def is_allowed_request_origin(request: Request) -> bool:
    origin = (request.headers.get("origin") or "").strip()
    if origin:
        return is_allowed_origin(origin)
    referer = (request.headers.get("referer") or "").strip()
    if referer:
        return is_allowed_origin(_origin_from_url(referer))
    return True


def reject_cross_site(request: Request) -> None:
    if not is_allowed_request_origin(request):
        raise HTTPException(status_code=403, detail="拒绝跨站请求")


def sanitize_validation_errors(errors: list[dict]) -> list[dict]:
    out: list[dict] = []
    for err in errors:
        loc = [str(item) for item in (err.get("loc") or ()) if item not in {"body", "query", "path", "header", "cookie"}]
        out.append(
            {
                "loc": loc,
                "msg": "参数不合法",
                "type": str(err.get("type") or "value_error"),
            }
        )
    return out

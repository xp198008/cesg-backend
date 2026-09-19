"""安全整改共用：CORS 白名单、来源校验、严格入参模型、校验错误裁剪。"""
from __future__ import annotations

from urllib.parse import urlparse

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from app.config import settings

INT32_MAX = 2_147_483_647
ONLINE_SECONDS_MAX = 31_536_000
PAGE_MAX = 100_000
OFFSET_MAX = 2_000_000
PAGE_SIZE_MAX = 5_000

_PAGING_LIMITS = {
    "page": (1, PAGE_MAX),
    "page_size": (1, PAGE_SIZE_MAX),
    "offset": (0, OFFSET_MAX),
    "limit": (1, PAGE_SIZE_MAX),
}

_DEFAULT_CORS_ORIGINS = (
    "https://cs.v2xcloud.com",
    "https://113.207.68.96",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
)

# JSON/API 响应用：不执行脚本，供 AppScan 检查 CSP 是否缺失或不安全
CSP_API = (
    "default-src 'none'; script-src 'none'; object-src 'none'; "
    "frame-ancestors 'none'; base-uri 'none'"
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
    """Origin、Referer 任一出现且不在白名单即拒绝（AppScan CSRF 会改 Referer 但保留 Origin）。"""
    origin = (request.headers.get("origin") or "").strip()
    referer = (request.headers.get("referer") or "").strip()
    if origin and not is_allowed_origin(origin):
        return False
    if referer and not is_allowed_origin(_origin_from_url(referer)):
        return False
    return True


def reject_cross_site(request: Request) -> None:
    if not is_allowed_request_origin(request):
        raise HTTPException(status_code=403, detail="拒绝跨站请求")


def reject_oversized_paging(request: Request) -> JSONResponse | None:
    """超大 page/offset 在进 SQL 前直接 400，避免 MySQL OFFSET 500。"""
    for key, raw in request.query_params.multi_items():
        bounds = _PAGING_LIMITS.get((key or "").lower())
        if bounds is None:
            continue
        text = str(raw or "").strip()
        digits = text.lstrip("+-")
        if not digits.isdigit() or len(digits) > 10:
            return JSONResponse(status_code=400, content={"detail": "参数超出允许范围"})
        value = int(text)
        lo, hi = bounds
        if value < lo or value > hi:
            return JSONResponse(status_code=400, content={"detail": "参数超出允许范围"})
    return None


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

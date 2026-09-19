"""808 OpenAPI 入口网关：拦操作符键、非整数 apicode，并去掉错误回显。"""
from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from app.config import settings
from app.security import CSP_API, is_allowed_request_origin

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jt808-gateway"])

_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}

_STRIP_RESP = {
    "access-control-allow-origin",
    "access-control-allow-methods",
    "access-control-allow-headers",
    "access-control-allow-credentials",
    "access-control-max-age",
    "server",
    "transfer-encoding",
    "content-length",
    "connection",
}

_REFLECT_MARKERS = ("<script", "for input string", "javascript:", "onerror=")


def _upstream_base() -> str:
    raw = (settings.jt808_openapi_base_url or settings.jt808_api_base or "").strip().rstrip("/")
    if raw:
        return raw
    return "http://127.0.0.1:8800/api"


def _upstream_url(extra_path: str) -> str:
    base = _upstream_base()
    extra = (extra_path or "").lstrip("/")
    if not extra:
        return base
    parsed = urlparse(base)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if base.endswith("/api"):
        return f"{origin}/api/{extra}"
    return f"{base}/{extra}"


def _has_operator_keys(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str) and key.startswith("$"):
                return True
            if _has_operator_keys(item):
                return True
        return False
    if isinstance(value, list):
        return any(_has_operator_keys(item) for item in value)
    return False


def _apicode_ok(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return True
    return False


def _looks_reflected(text: str) -> bool:
    low = (text or "").lower()
    return any(marker in low for marker in _REFLECT_MARKERS)


_API_SECURE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate",
    "Content-Security-Policy": CSP_API,
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}


def _reject(message: str = "接口参数不合法") -> JSONResponse:
    return JSONResponse(status_code=400, content={"code": -1, "message": message}, headers=_API_SECURE_HEADERS)


def _not_found() -> Response:
    # 探测/非法/跨站请求不回 JSON 业务错误，避免 AppScan 把「接口参数不合法」当成活 API
    return Response(status_code=404, content=b"", headers=_API_SECURE_HEADERS)


def _sanitize_upstream_json(payload: Any) -> tuple[Any, int] | None:
    if not isinstance(payload, dict):
        return None
    message = str(payload.get("message") or payload.get("msg") or "")
    if _looks_reflected(message) or _has_operator_keys(payload):
        return None, 404
    return None


def _forward_headers(request: Request) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in request.headers.items():
        low = key.lower()
        if low in _HOP_BY_HOP or low.startswith("access-control-"):
            continue
        out[key] = value
    return out


@router.api_route("/internal/jt808-gateway", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@router.api_route("/internal/jt808-gateway/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def jt808_gateway(request: Request, path: str = ""):
    if request.method != "POST":
        return _not_found()
    if not is_allowed_request_origin(request):
        return _not_found()

    raw = await request.body()
    parsed: Any = None
    if raw:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return _not_found()
    if not isinstance(parsed, dict) or _has_operator_keys(parsed):
        return _not_found()
    if "apicode" not in parsed or not _apicode_ok(parsed.get("apicode")):
        return _not_found()

    url = _upstream_url(path)
    timeout = httpx.Timeout(60.0, connect=8.0)
    try:
        async with httpx.AsyncClient(timeout=timeout, trust_env=False, follow_redirects=False) as client:
            upstream = await client.request(
                request.method,
                url,
                params=request.query_params,
                content=raw if raw else None,
                headers=_forward_headers(request),
            )
    except httpx.HTTPError as exc:
        logger.warning("808 网关转发失败: %s", exc)
        return JSONResponse(
            status_code=502,
            content={"code": -1, "message": "接口暂时不可用"},
            headers=_API_SECURE_HEADERS,
        )

    media = (upstream.headers.get("content-type") or "").split(";")[0].strip().lower()
    body = upstream.content
    status = upstream.status_code
    if media == "application/json" or (body[:1] in {b"{", b"["}):
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = None
        if payload is not None:
            rewritten = _sanitize_upstream_json(payload)
            if rewritten is not None:
                payload, status = rewritten
                if status == 404 and payload is None:
                    return _not_found()
                return JSONResponse(status_code=status, content=payload, headers=_API_SECURE_HEADERS)
            if _looks_reflected(body.decode("utf-8", "replace")):
                return _not_found()

    headers = {
        key: value
        for key, value in upstream.headers.items()
        if key.lower() not in _STRIP_RESP
    }
    headers.setdefault("Cache-Control", "no-store, no-cache, must-revalidate")
    headers.setdefault("Content-Security-Policy", CSP_API)
    headers.setdefault("X-Content-Type-Options", "nosniff")
    return Response(content=body, status_code=status, headers=headers, media_type=media or None)

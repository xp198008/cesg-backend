"""808 主动安全证据媒体代理（HTTPS 页面同源访问，不依赖 gb35658）。"""



from __future__ import annotations



import httpx

from fastapi import APIRouter, HTTPException

from fastapi.responses import Response



from app.media_url import jt808_media_origin



router = APIRouter(prefix="/api/media", tags=["media"])





@router.get("/adas/{path:path}")

async def proxy_adas_media(path: str) -> Response:

    rel = (path or "").lstrip("/")

    if not rel or ".." in rel.split("/"):

        raise HTTPException(status_code=400, detail="无效路径")



    upstream = f"{jt808_media_origin()}/ADAS_FILE/{rel}"

    try:

        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:

            resp = await client.get(upstream)

    except httpx.HTTPError as exc:

        raise HTTPException(status_code=502, detail=f"808 媒体拉取失败: {exc}") from exc



    if resp.status_code >= 400:

        raise HTTPException(status_code=resp.status_code, detail="媒体不存在")



    content_type = (resp.headers.get("content-type") or "application/octet-stream").split(";")[0].strip()

    headers = {"Cache-Control": "public, max-age=3600"}

    accept_ranges = resp.headers.get("accept-ranges")

    if accept_ranges:

        headers["Accept-Ranges"] = accept_ranges

    content_length = resp.headers.get("content-length")

    if content_length:

        headers["Content-Length"] = content_length



    return Response(content=resp.content, media_type=content_type, headers=headers)


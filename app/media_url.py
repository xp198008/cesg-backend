"""808 主动安全证据媒体 URL 归一化 + 浏览器访问路径。"""



from __future__ import annotations



from typing import Any

from urllib.parse import urlsplit



_LOCAL_808_PREFIXES = (

    "http://113.207.68.96:8800",

    "http://127.0.0.1:8800",

    "http://localhost:8800",

    "https://113.207.68.96:8800",

)





def extract_adas_relative_path(raw: str) -> str:

    """从各类 ADAS 路径/URL 中提取 ADAS_FILE 后的相对路径。"""

    s = str(raw or "").strip()

    if not s:

        return ""

    upper = s.upper()

    marker = "/ADAS_FILE/"

    if marker in upper:

        idx = upper.index(marker)

        return s[idx + len(marker) :].lstrip("/")

    if upper.startswith("ADAS_FILE/"):

        return s.split("/", 1)[-1].lstrip("/")

    return s.lstrip("/")





def adas_browser_url(raw: Any) -> str:

    """浏览器通过 CESG 后端代理访问 808 证据文件（兼容 HTTPS / 旧前端）。"""

    rel = extract_adas_relative_path(str(raw or ""))

    return f"/cmapi/media/adas/{rel}" if rel else ""





def client_media_url(raw: Any) -> str:

    s = str(raw or "").strip()

    if not s:

        return ""

    if s.startswith("/cmapi/media/adas/"):

        return s

    upper = s.upper()

    if upper.startswith("/ADAS_FILE/") or "ADAS_FILE/" in upper:

        return adas_browser_url(s)

    for prefix in _LOCAL_808_PREFIXES:

        if s.startswith(prefix):

            tail = s[len(prefix) :]

            if tail.upper().startswith("/ADAS_FILE/") or "/ADAS_FILE/" in tail.upper():

                return adas_browser_url(tail)

            return tail if tail.startswith("/") else f"/{tail}"

    if s.startswith("http://") and "/ADAS_FILE/" in upper:

        return adas_browser_url(s)

    if s.startswith("https://") and "/ADAS_FILE/" in upper:

        return adas_browser_url(s)

    if s.startswith("/"):

        return s

    if s.startswith("http://") and "gb35658.com" in s.lower():

        return s.replace("http://", "https://", 1)

    return s





def normalize_media_item(item: Any) -> Any:

    if isinstance(item, str):

        return client_media_url(item)

    if not isinstance(item, dict):

        return item

    out = dict(item)

    if out.get("url"):

        out["url"] = client_media_url(out["url"])

    if out.get("wfsl"):

        out["wfsl"] = client_media_url(out["wfsl"])

    if out.get("src"):

        out["src"] = client_media_url(out["src"])

    return out





def normalize_media_list(items: Any) -> list[Any]:

    if not isinstance(items, list):

        return []

    return [normalize_media_item(x) for x in items if x]





def normalize_evidence_payload(evidence: Any) -> dict[str, Any]:

    if not isinstance(evidence, dict):

        return {"images": [], "videos": [], "attachments": []}

    return {

        "images": normalize_media_list(evidence.get("images")),

        "videos": normalize_media_list(evidence.get("videos")),

        "attachments": normalize_media_list(evidence.get("attachments")),

    }





def jt808_media_origin() -> str:

    from app.config import settings



    for base in (settings.jt808_openapi_base_url, settings.jt808_api_base):

        parsed = urlsplit(str(base or "").strip())

        if parsed.scheme and parsed.netloc:

            return f"{parsed.scheme}://{parsed.netloc}"

    return "http://113.207.68.96:8800"


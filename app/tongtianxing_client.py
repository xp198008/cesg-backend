"""通天星 CMS 开放接口（仅车辆树：登录 + queryUserVehicle）。

配置优先读环境变量，其次 backend/config/tongtianxing.local.xml。
"""
from __future__ import annotations

import json
import logging
import threading
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "tongtianxing.local.xml"
_LOCK = threading.Lock()
_SESSION: dict[str, Any] = {
    "base_url": "",
    "account": "",
    "jsession": "",
}


def _txt(root: ET.Element, tag: str) -> str:
    el = root.find(tag)
    if el is None or el.text is None:
        return ""
    return str(el.text).strip()


def _normalize_openapi_base_url(raw: str) -> str:
    s = str(raw or "").strip().rstrip("/")
    if not s:
        return ""
    if s.lower().endswith("/808gps"):
        s = s[: -len("/808gps")].rstrip("/")
    return s


def _load_xml_config() -> dict[str, Any]:
    if not _CONFIG_PATH.is_file():
        return {}
    try:
        root = ET.parse(_CONFIG_PATH).getroot()
    except Exception as e:  # noqa: BLE001
        logger.warning("通天星 XML 解析失败: %s", e)
        return {}
    raw_web = _txt(root, "baseUrl").strip()
    username = _txt(root, "test16Username") or _txt(root, "username")
    password = _txt(root, "test16Password") or _txt(root, "password")
    timeout_ms_raw = _txt(root, "timeoutMs")
    try:
        timeout_ms = int(timeout_ms_raw) if timeout_ms_raw else 60000
    except Exception:
        timeout_ms = 60000
    media_origin = ""
    if raw_web:
        ru = raw_web if "://" in raw_web else f"http://{raw_web}"
        pu = urlparse(ru)
        if pu.scheme and pu.netloc:
            path_part = (pu.path or "").rstrip("/")
            media_origin = f"{pu.scheme}://{pu.netloc}{path_part}" if path_part else f"{pu.scheme}://{pu.netloc}"
    return {
        "base_url": _normalize_openapi_base_url(raw_web),
        "media_origin": media_origin,
        "username": username,
        "password": password,
        "timeout_ms": timeout_ms,
    }


def load_config() -> tuple[dict[str, Any] | None, str]:
    xml = _load_xml_config()
    base_url = _normalize_openapi_base_url(settings.tongtianxing_base_url) or str(xml.get("base_url") or "")
    username = (settings.tongtianxing_username or "").strip() or str(xml.get("username") or "")
    password = settings.tongtianxing_password if settings.tongtianxing_password != "" else str(xml.get("password") or "")
    timeout_ms = int(settings.tongtianxing_timeout_ms or 0) or int(xml.get("timeout_ms") or 60000)
    timeout_ms = max(5000, min(timeout_ms, 300000))
    media_origin = str(xml.get("media_origin") or "")
    if not media_origin and base_url:
        media_origin = f"{base_url}/808gps"
    if not base_url or not username or password == "":
        return None, "未配置通天星账号（TONGTIANXING_BASE_URL / USERNAME / PASSWORD）"
    return {
        "base_url": base_url,
        "media_origin": media_origin,
        "username": username,
        "password": password,
        "timeout_ms": timeout_ms,
    }, ""


def _login_public_hint(url: str) -> str:
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}{p.path}"
    except Exception:  # noqa: BLE001
        return url[:96]


def _http_get_json(url: str, timeout_ms: int) -> dict[str, Any]:
    total = max(1.0, min(float(timeout_ms) / 1000.0, 600.0))
    timeout = httpx.Timeout(total, connect=min(20.0, total * 0.5))
    headers = {"User-Agent": "cesg-ttx/1.0"}
    with httpx.Client(timeout=timeout, verify=True, follow_redirects=True, trust_env=True) as client:
        r = client.get(url, headers=headers)
        r.raise_for_status()
        text = r.content.decode("utf-8-sig", errors="replace").strip()
    if not text:
        raise RuntimeError("通天星接口返回空响应")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise RuntimeError("通天星接口返回格式异常")
    return data


def _login_url_candidates(cfg: dict[str, Any]) -> list[str]:
    q = urlencode({"account": cfg["username"], "password": cfg["password"]})
    suf = f"/StandardApiAction_login.action?{q}"
    seen: set[str] = set()
    out: list[str] = []
    for key in ("base_url", "media_origin"):
        b = (cfg.get(key) or "").strip().rstrip("/")
        if not b:
            continue
        u = f"{b}{suf}"
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _login(cfg: dict[str, Any]) -> tuple[str | None, str]:
    errs: list[str] = []
    for url in _login_url_candidates(cfg):
        try:
            data = _http_get_json(url, cfg["timeout_ms"])
        except Exception as e:  # noqa: BLE001
            errs.append(f"{_login_public_hint(url)} → {e!s}"[:220])
            continue
        if int(data.get("result", -1)) != 0:
            msg = str(data.get("message") or data.get("resultTip") or "登录失败")
            errs.append(f"{_login_public_hint(url)} → 业务失败:{msg[:120]}")
            continue
        jsession = str(data.get("jsession") or data.get("JSESSIONID") or "").strip()
        if not jsession:
            errs.append(f"{_login_public_hint(url)} → 无 jsession")
            continue
        return jsession, ""
    return None, "通天星登录失败：" + ("; ".join(errs[:4]) if errs else "无可用登录地址")


def _ensure_session(cfg: dict[str, Any], force: bool = False) -> tuple[str | None, str]:
    with _LOCK:
        acc = str(cfg.get("username") or "")
        if (
            not force
            and _SESSION.get("base_url") == cfg["base_url"]
            and str(_SESSION.get("account") or "") == acc
            and _SESSION.get("jsession")
        ):
            return str(_SESSION["jsession"]), ""
        jsession, err = _login(cfg)
        if not jsession:
            return None, err
        _SESSION["base_url"] = cfg["base_url"]
        _SESSION["account"] = acc
        _SESSION["jsession"] = jsession
        return jsession, ""


def _is_session_invalid(data: dict[str, Any] | None) -> bool:
    if not isinstance(data, dict):
        return False
    msg = str(data.get("message") or data.get("resultTip") or "").lower()
    return "session does not exist" in msg or int(data.get("result") or 0) == 5


def _fetch_query_user_vehicle(cfg: dict[str, Any], jsession: str) -> tuple[dict[str, Any] | None, str]:
    q = urlencode({"jsession": jsession, "language": "zh"})
    url = f"{cfg['base_url']}/StandardApiAction_queryUserVehicle.action?{q}"
    try:
        data = _http_get_json(url, cfg["timeout_ms"])
    except Exception as e:  # noqa: BLE001
        return None, str(e)
    return data, ""


def _norm(s: Any) -> str:
    if s is None:
        return ""
    return str(s).replace("\u3000", " ").strip()


def extract_plate_and_device(row: dict[str, Any]) -> tuple[str, str]:
    """从 queryUserVehicle 单条 JSON 取车牌与主设备号。"""
    if not isinstance(row, dict):
        return "", ""
    plate = _norm(row.get("nm") or row.get("vehiIdno") or row.get("vehiIDNO") or row.get("plate") or row.get("vid"))
    device = ""
    dl = row.get("dl")
    if isinstance(dl, list) and dl:
        first = dl[0] if isinstance(dl[0], dict) else None
        if first:
            device = _norm(first.get("id") or first.get("devIdno") or first.get("did"))
    if not device:
        for key in ("devIdno", "devIDNO", "deviceId", "deviceNo", "terminalId", "tid", "di", "devNo", "did"):
            device = _norm(row.get(key))
            if device:
                break
    return plate, device


def fetch_user_vehicle_tree() -> dict[str, Any]:
    cfg, err = load_config()
    if cfg is None:
        return {"ok": False, "message": err, "vehicles": []}

    jsession, err = _ensure_session(cfg, force=False)
    if not jsession:
        return {"ok": False, "message": err, "vehicles": []}

    data, q_err = _fetch_query_user_vehicle(cfg, jsession)
    if data is None:
        return {"ok": False, "message": q_err, "vehicles": []}
    if _is_session_invalid(data):
        jsession, err = _ensure_session(cfg, force=True)
        if not jsession:
            return {"ok": False, "message": err, "vehicles": []}
        data, q_err = _fetch_query_user_vehicle(cfg, jsession)
        if data is None:
            return {"ok": False, "message": q_err, "vehicles": []}

    if int(data.get("result", -1)) != 0:
        msg = str(data.get("message") or data.get("resultTip") or f"接口错误 result={data.get('result')}")
        return {"ok": False, "message": msg, "vehicles": []}

    vehicles = data.get("vehicles")
    return {
        "ok": True,
        "vehicles": vehicles if isinstance(vehicles, list) else [],
    }


def list_plate_device_pairs() -> dict[str, Any]:
    """通天星车辆树 → [{plate_no, device_no}]，只保留车牌+设备号都有的记录。"""
    payload = fetch_user_vehicle_tree()
    if not payload.get("ok"):
        return {"ok": False, "message": str(payload.get("message") or "通天星车辆接口不可用"), "items": []}
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in payload.get("vehicles") or []:
        if not isinstance(row, dict):
            continue
        plate, device = extract_plate_and_device(row)
        if not plate or not device:
            continue
        key = f"{plate}|{device}"
        if key in seen:
            continue
        seen.add(key)
        items.append({"plate_no": plate, "device_no": device})
    return {"ok": True, "message": "ok", "items": items, "ttxVehicleCount": len(payload.get("vehicles") or [])}

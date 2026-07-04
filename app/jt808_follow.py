"""JT808 用户关注车辆（OpenAPI 1257）。"""
from __future__ import annotations

import logging
from typing import Any

from app.jt808_vehicle import _encode_password, _post, _terminal_variants

logger = logging.getLogger(__name__)


def expand_terminal_id_variants(device_ids: list[str]) -> list[str]:
    """1257 返回的终端号与库内可能前导 0 不一致，展开为可互匹配的变体集合。"""
    seen: set[str] = set()
    out: list[str] = []
    for did in device_ids:
        for variant in _terminal_variants(did):
            if variant not in seen:
                seen.add(variant)
                out.append(variant)
    return out


def _extract_device_id(item: Any) -> str | None:
    if item is None:
        return None
    if isinstance(item, dict):
        for key in ("deviceId", "device_id", "tid", "id", "car_id"):
            val = item.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
        return None
    text = str(item).strip()
    return text or None


async def fetch_followed_device_ids(username: str, password_plain: str) -> list[str]:
    """以指定 808 账号登录并调用 1257，返回关注车辆的 deviceId 列表。"""
    account = (username or "").strip()
    pwd = (password_plain or "").strip()
    if not account or not pwd:
        return []

    login = await _post(
        {
            "apicode": 8003,
            "account": account,
            "password": _encode_password(pwd, account),
        }
    )
    if login.get("code") != 1 or not login.get("token"):
        msg = login.get("message") or "808 登录失败"
        raise RuntimeError(str(msg))

    token = str(login["token"])
    resp = await _post(
        {
            "apicode": 1257,
            "account": account,
            "lingxtoken": token,
        }
    )
    if resp.get("code") != 1:
        msg = resp.get("message") or "1257 查询关注车辆失败"
        raise RuntimeError(str(msg))

    device_ids: list[str] = []
    seen: set[str] = set()
    for item in resp.get("data") or []:
        did = _extract_device_id(item)
        if did and did not in seen:
            seen.add(did)
            device_ids.append(did)
    logger.info("1257 followed devices account=%s count=%s", account, len(device_ids))
    return device_ids

"""JT808 分组（tgps_group）联动客户端。

业务后端在公司增删改时，best-effort 同步到 JT808 平台的分组树：
- 新增公司 -> 8002 m=add（传父公司的 jt808_group_id 作 fid），再 m=tree 查回新 group_id
- 改名/移动 -> 8002 m=edit（传 id + name + fid）
- 删除公司 -> 8002 m=del（传 id）

所有操作异常/失败都不抛给调用方，仅记 warning；本地公司操作不被阻断。
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

_token: str | None = None
_token_lock = asyncio.Lock()

_AUTH_HINTS = ("登录", "未登录", "登陆", "token", "令牌", "重新登录", "会话")


def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _handler_userid(userid: str) -> str:
    temp = "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM_"
    return "".join(ch for ch in userid if ch in temp)


def _encode_password(pwd: str, userid: str) -> str:
    if len(pwd) == 32:
        return pwd
    return _md5(_md5(pwd) + _md5(_handler_userid(userid)))


async def _post(payload: dict[str, Any]) -> dict[str, Any]:
    timeout = httpx.Timeout(settings.jt808_sync_timeout, connect=min(5.0, settings.jt808_sync_timeout))
    async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
        resp = await client.post(
            settings.jt808_api_base,
            json={"language": "zh-CN", **payload},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()


async def _login() -> str:
    enc = _encode_password(settings.jt808_admin_password, settings.jt808_admin_account)
    r = await _post({"apicode": 8003, "account": settings.jt808_admin_account, "password": enc})
    if r.get("code") != 1 or not r.get("token"):
        raise RuntimeError(f"JT808 登录失败: {r.get('message') or r}")
    return r["token"]


async def _ensure_token(force: bool = False) -> str:
    global _token
    async with _token_lock:
        if force or not _token:
            _token = await _login()
        return _token


async def _call(payload: dict[str, Any]) -> dict[str, Any]:
    token = await _ensure_token()
    r = await _post({**payload, "lingxtoken": token})
    if r.get("code") != 1:
        msg = str(r.get("message") or "")
        if any(h in msg for h in _AUTH_HINTS):
            token = await _ensure_token(force=True)
            r = await _post({**payload, "lingxtoken": token})
    return r


async def _tree(fid: int) -> list[dict[str, Any]]:
    r = await _call({"apicode": 8002, "e": "tgps_group", "m": "tree", "fid": int(fid),
                     "orderField": "orderindex", "orderType": "asc"})
    return r.get("data") or []


async def add_group(name: str, fid: int) -> int | None:
    if not settings.jt808_sync_enabled:
        return None
    try:
        r = await _call({"apicode": 8002, "e": "tgps_group", "m": "add", "name": name, "fid": int(fid),
                         "state": "open", "orderindex": 100, "type": 1, "icon_cls": ""})
        if r.get("code") != 1:
            logger.warning("JT808 新建分组失败 name=%s fid=%s: %s", name, fid, r.get("message") or r)
            return None
        matched = [x.get("id") for x in await _tree(fid) if x.get("name") == name and x.get("id") is not None]
        if not matched:
            logger.warning("JT808 新建分组成功但未查到 id name=%s fid=%s", name, fid)
            return None
        return max(int(i) for i in matched)
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 新建分组异常 name=%s fid=%s: %s", name, fid, e)
        return None


async def edit_group(group_id: int, name: str, fid: int | None) -> bool:
    if not settings.jt808_sync_enabled:
        return False
    if fid is None:
        logger.warning("JT808 改名分组缺少 fid，已跳过 id=%s name=%s", group_id, name)
        return False
    payload: dict[str, Any] = {"apicode": 8002, "e": "tgps_group", "m": "edit",
                               "id": int(group_id), "name": name, "fid": int(fid),
                               "state": "open", "orderindex": 100, "type": 1, "icon_cls": ""}
    try:
        r = await _call(payload)
        if r.get("code") != 1:
            logger.warning("JT808 改名分组失败 id=%s name=%s: %s", group_id, name, r.get("message") or r)
            return False
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 改名分组异常 id=%s name=%s: %s", group_id, name, e)
        return False


async def del_group(group_id: int) -> bool:
    if not settings.jt808_sync_enabled:
        return False
    try:
        r = await _call({"apicode": 8002, "e": "tgps_group", "m": "del", "id": int(group_id)})
        if r.get("code") != 1:
            logger.warning("JT808 删除分组失败 id=%s: %s", group_id, r.get("message") or r)
            return False
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 删除分组异常 id=%s: %s", group_id, e)
        return False


async def _backfill_group_id(company_id: int, group_id: int) -> None:
    from sqlalchemy import update as _update

    from app.database import AsyncSessionLocal
    from app.models import OrgCompany

    async with AsyncSessionLocal() as s:
        await s.execute(_update(OrgCompany).where(OrgCompany.id == company_id).values(jt808_group_id=group_id))
        await s.commit()


async def bg_create(company_id: int, name: str, fid: int) -> None:
    gid = await add_group(name, fid)
    if gid:
        try:
            await _backfill_group_id(company_id, gid)
        except Exception as e:  # noqa: BLE001
            logger.warning("回写 jt808_group_id 失败 company_id=%s gid=%s: %s", company_id, gid, e)


async def bg_edit(group_id: int, name: str, fid: int | None) -> None:
    await edit_group(group_id, name, fid)


async def bg_delete(group_id: int) -> None:
    await del_group(group_id)

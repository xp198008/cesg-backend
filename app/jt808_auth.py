"""JT808 用户车组授权（apicode 1252）同步。

登录后根据 CESG 侧「车辆分配规则 / 所属公司组织树」计算应授权的车组，
与 808 当前 tgps_group_user 对比，调用 1252 add/delete 对齐。
写操作使用前端传入的 lingxtoken（当前登录用户），不再后端 admin 登录。
"""
from __future__ import annotations

import logging
from typing import Any

import httpx
import pymysql
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.models import OrgCompany, SysUser, Vehicle, VehicleAllocRuleUser, VehicleAllocRuleVehicle
from app.org_scope import collect_org_company_subtree_ids

logger = logging.getLogger(__name__)


def _enabled() -> bool:
    return bool(settings.jt808_sync_enabled)


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


def _link_value(cell: Any) -> str | None:
    if cell is None:
        return None
    obj = cell[0] if isinstance(cell, list) and cell else cell
    if isinstance(obj, dict):
        raw = obj.get("value") if obj.get("value") is not None else obj.get("ID")
    else:
        raw = obj
    if raw is None or raw == "":
        return None
    return str(raw).strip()


def _link_int(cell: Any) -> int | None:
    raw = _link_value(cell)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _effective_org_id(user: SysUser, role_code: str) -> int | None:
    if user.org_id is not None:
        return int(user.org_id)
    role = user.role
    if role and role.org_id is not None:
        return int(role.org_id)
    return None


async def _group_ids_for_companies(db: AsyncSession, company_ids: set[int]) -> set[int]:
    if not company_ids:
        return set()
    rows = (
        await db.execute(
            select(OrgCompany.jt808_group_id).where(
                OrgCompany.id.in_(company_ids),
                OrgCompany.jt808_group_id.is_not(None),
            )
        )
    ).all()
    out: set[int] = set()
    for (gid,) in rows:
        if gid is not None:
            out.add(int(gid))
    return out


async def compute_desired_group_ids(
    db: AsyncSession,
    user: SysUser,
    *,
    role_code: str = "",
) -> set[int]:
    """根据 CESG 数据计算用户应在 808 上可见的车组 id 集合。"""
    code = (role_code or "").strip().lower()
    if code == "admin" or (user.username or "").strip().lower() == "admin":
        rows = (await db.execute(select(OrgCompany.jt808_group_id).where(OrgCompany.jt808_group_id.is_not(None)))).all()
        return {int(gid) for (gid,) in rows if gid is not None}

    rule_ids = (
        await db.execute(
            select(VehicleAllocRuleUser.rule_id).where(VehicleAllocRuleUser.user_id == user.id)
        )
    ).scalars().all()

    if rule_ids:
        vehicle_ids = (
            await db.execute(
                select(VehicleAllocRuleVehicle.vehicle_id).where(
                    VehicleAllocRuleVehicle.rule_id.in_(rule_ids)
                )
            )
        ).scalars().all()
        if not vehicle_ids:
            return set()
        company_ids = {
            int(cid)
            for cid in (
                await db.execute(
                    select(Vehicle.company_id).where(
                        Vehicle.id.in_(vehicle_ids),
                        Vehicle.company_id.is_not(None),
                    )
                )
            ).scalars().all()
            if cid is not None
        }
        return await _group_ids_for_companies(db, company_ids)

    org_id = _effective_org_id(user, role_code)
    if org_id is None:
        return set()
    subtree = await collect_org_company_subtree_ids(db, org_id)
    return await _group_ids_for_companies(db, subtree)


async def _lookup_jt808_user_id(account: str, lingxtoken: str) -> str | None:
    account = (account or "").strip()
    if not account:
        return None
    try:
        r = await _post({
            "apicode": 8002,
            "e": "tlingx_user",
            "m": "grid",
            "page": 1,
            "limit": 1,
            "where": f"account='{account}'",
            "lingxtoken": lingxtoken,
        })
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 查询用户失败 account=%s: %s", account, e)
        return None
    rows = r.get("rows") or []
    if not rows:
        return None
    uid = rows[0].get("id")
    return str(uid).strip() if uid else None


def _fetch_current_group_ids_mysql(jt808_user_id: str) -> set[int] | None:
    try:
        conn = pymysql.connect(
            host=settings.jt808_mysql_host,
            port=int(settings.jt808_mysql_port),
            user=settings.jt808_mysql_user,
            password=settings.jt808_mysql_password,
            database=settings.jt808_mysql_database,
            charset="utf8mb4",
            connect_timeout=min(8.0, settings.jt808_sync_timeout),
            read_timeout=15,
            write_timeout=15,
        )
    except Exception:  # noqa: BLE001
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("select group_id from tgps_group_user where user_id=%s", (jt808_user_id,))
            return {int(row[0]) for row in cur.fetchall() if row[0] is not None}
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 读取 tgps_group_user 失败 user=%s: %s", jt808_user_id, e)
        return None
    finally:
        conn.close()


async def _fetch_current_group_ids_api(jt808_user_id: str, lingxtoken: str) -> set[int]:
    out: set[int] = set()
    page = 1
    while page <= 50:
        try:
            r = await _post({
                "apicode": 8002,
                "e": "tgps_group_user",
                "m": "grid",
                "page": page,
                "limit": 200,
                "lingxtoken": lingxtoken,
            })
        except Exception as e:  # noqa: BLE001
            logger.warning("JT808 查询 group_user 失败 page=%s: %s", page, e)
            break
        rows = r.get("rows") or []
        if not rows:
            break
        for row in rows:
            uid = _link_value(row.get("user_id"))
            if uid != jt808_user_id:
                continue
            gid = _link_int(row.get("group_id"))
            if gid is not None:
                out.add(gid)
        if len(rows) < 200:
            break
        page += 1
    return out


async def _fetch_current_group_ids(jt808_user_id: str, lingxtoken: str) -> set[int]:
    mysql_ids = _fetch_current_group_ids_mysql(jt808_user_id)
    if mysql_ids is not None:
        return mysql_ids
    return await _fetch_current_group_ids_api(jt808_user_id, lingxtoken)


async def _call_1252(jt808_user_id: str, group_id: int, op: str, lingxtoken: str) -> bool:
    try:
        r = await _post({
            "apicode": 1252,
            "userId": jt808_user_id,
            "groupId": int(group_id),
            "type": op,
            "lingxtoken": lingxtoken,
        })
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 1252 异常 user=%s group=%s op=%s: %s", jt808_user_id, group_id, op, e)
        return False
    if r.get("code") == 1:
        return True
    msg = str(r.get("message") or "")
    if op == "delete" and any(k in msg for k in ("不存在", "未找到", "无此")):
        return True
    logger.warning("JT808 1252 失败 user=%s group=%s op=%s: %s", jt808_user_id, group_id, op, msg or r)
    return False


async def sync_user_group_auth(
    db: AsyncSession,
    user_id: int,
    lingxtoken: str,
    *,
    role_code: str = "",
) -> dict[str, Any]:
    """将 CESG 用户车组授权同步到 808（1252）。返回摘要供接口响应。"""
    token = (lingxtoken or "").strip()
    if not _enabled():
        return {"ok": True, "skipped": True, "reason": "jt808_sync_disabled"}
    if not token:
        return {"ok": False, "message": "缺少 808 lingxtoken"}

    user = await db.scalar(
        select(SysUser)
        .options(selectinload(SysUser.role), selectinload(SysUser.org))
        .where(SysUser.id == user_id)
        .limit(1)
    )
    if user is None:
        return {"ok": False, "message": "用户不存在"}
    if not user.is_active:
        return {"ok": False, "message": "用户已禁用"}

    jt808_uid = (user.jt808_user_id or "").strip() or None
    if not jt808_uid:
        jt808_uid = await _lookup_jt808_user_id(user.username or "", token)
    if not jt808_uid:
        return {"ok": False, "message": "808 平台未找到对应用户，请先完成用户同步"}

    desired = await compute_desired_group_ids(db, user, role_code=role_code)
    if not desired:
        return {
            "ok": True,
            "skipped": True,
            "reason": "no_group_mapping",
            "message": "未找到可同步的 808 车组（请确认公司已同步 jt808_group_id）",
            "jt808_user_id": jt808_uid,
        }

    current = await _fetch_current_group_ids(jt808_uid, token)
    to_add = desired - current
    to_del = current - desired

    added = 0
    deleted = 0
    for gid in sorted(to_del):
        if await _call_1252(jt808_uid, gid, "delete", token):
            deleted += 1
    for gid in sorted(to_add):
        if await _call_1252(jt808_uid, gid, "add", token):
            added += 1

    logger.info(
        "JT808 1252 授权同步 user=%s jt808_uid=%s desired=%s add=%s del=%s",
        user.username,
        jt808_uid,
        sorted(desired),
        added,
        deleted,
    )
    return {
        "ok": True,
        "jt808_user_id": jt808_uid,
        "desired_groups": sorted(desired),
        "added": added,
        "deleted": deleted,
        "unchanged": len(desired & current),
    }

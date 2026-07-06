"""JT808 用户（tlingx_user）联动客户端。

经 SSH 隧道直连 MySQL jt808 库，在本地用户增删改/改密时 best-effort 同步：
- 新增 -> tlingx_user + tlingx_user_role + tgps_group_user
- 修改 -> 更新账号/姓名/状态/所属分组
- 改密 -> 更新 password（与 lingx Utils.lingxPasswordEncode 一致）
- 删除 -> 清理 tgps_group_user / tlingx_user_role / tlingx_user

失败仅记 warning，不阻断本地操作。改密/改资料若按 id 命中 0 行，自动按 account 兜底重查重绑，
避免「本地存的 jt808_user_id 失效后反复改 808 都改不动」。
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import uuid
from datetime import datetime

from app.timeutil import china_now_naive
from typing import Any

import pymysql

from app.config import settings

logger = logging.getLogger(__name__)

_default_role_org: tuple[str, str] | None = None


def _enabled() -> bool:
    return bool(settings.jt808_sync_enabled)


def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _handler_userid(userid: str) -> str:
    temp = "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM_"
    return "".join(ch for ch in userid if ch in temp)


def lingx_password_encode(plain_password: str, account: str) -> str:
    """与 lingx Utils.lingxPasswordEncode 一致，供 JT808 Api8003 登录校验。"""
    uid = _handler_userid(account)
    pwd_md5 = _md5(plain_password)
    user_id_md5 = _md5(uid)
    return _md5(_md5(pwd_md5 + user_id_md5)) + user_id_md5


def _jt808_time() -> str:
    return china_now_naive().strftime("%Y%m%d%H%M%S")


def _connect() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=settings.jt808_mysql_host,
        port=int(settings.jt808_mysql_port),
        user=settings.jt808_mysql_user,
        password=settings.jt808_mysql_password,
        database=settings.jt808_mysql_database,
        charset="utf8mb4",
        connect_timeout=min(8.0, settings.jt808_sync_timeout),
        autocommit=False,
    )


def _open_connection() -> pymysql.connections.Connection | None:
    """建立 MySQL 连接；隧道/网络不可达时返回 None，避免向上抛出 500。"""
    try:
        return _connect()
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 MySQL 连接失败: %s", e)
        return None


def _get_default_role_org(cur: pymysql.cursors.Cursor) -> tuple[str, str]:
    global _default_role_org
    if _default_role_org:
        return _default_role_org
    cur.execute(
        "SELECT ur.role_id, ur.org_id FROM tlingx_user_role ur "
        "JOIN tlingx_user u ON u.id = ur.user_id "
        "WHERE u.account = %s LIMIT 1",
        (settings.jt808_admin_account,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"JT808 未找到 admin 角色映射: {settings.jt808_admin_account}")
    _default_role_org = (str(row[0]), str(row[1]))
    return _default_role_org


def _find_user_id(cur: pymysql.cursors.Cursor, account: str) -> str | None:
    cur.execute("SELECT id FROM tlingx_user WHERE account = %s LIMIT 1", (account,))
    row = cur.fetchone()
    return str(row[0]) if row else None


def _set_group(cur: pymysql.cursors.Cursor, user_id: str, group_id: int | None) -> None:
    cur.execute("DELETE FROM tgps_group_user WHERE user_id = %s", (user_id,))
    if group_id is not None:
        cur.execute(
            "INSERT INTO tgps_group_user(group_id, user_id) VALUES (%s, %s)",
            (int(group_id), user_id),
        )


def _sync_create(
    account: str,
    plain_password: str,
    display_name: str,
    is_active: bool,
    group_id: int | None,
) -> str | None:
    conn = _open_connection()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            role_id, org_id = _get_default_role_org(cur)
            existing = _find_user_id(cur, account)
            now = _jt808_time()
            encoded = lingx_password_encode(plain_password, account)
            status = 1 if is_active else 0

            if existing:
                cur.execute(
                    "UPDATE tlingx_user SET name=%s, password=%s, status=%s, modify_time=%s WHERE id=%s",
                    (display_name or account, encoded, status, now, existing),
                )
                _set_group(cur, existing, group_id)
                conn.commit()
                logger.info("JT808 用户已存在，已更新绑定 account=%s id=%s", account, existing)
                return existing

            user_id = str(uuid.uuid4())
            cur.execute(
                "INSERT INTO tlingx_user("
                "id,account,name,password,status,tel,email,login_count,"
                "last_login_time,last_login_ip,create_time,modify_time,orderindex,remark"
                ") VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    user_id,
                    account,
                    display_name or account,
                    encoded,
                    status,
                    "",
                    "",
                    0,
                    "",
                    "",
                    now,
                    now,
                    10000,
                    "cesg-sync",
                ),
            )
            cur.execute(
                "INSERT INTO tlingx_user_role(id,user_id,role_id,org_id,type,orderindex) "
                "VALUES(%s,%s,%s,%s,%s,%s)",
                (str(uuid.uuid4()), user_id, role_id, org_id, 1, 100),
            )
            _set_group(cur, user_id, group_id)
            conn.commit()
            logger.info("JT808 新建用户 account=%s id=%s group=%s", account, user_id, group_id)
            return user_id
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        logger.warning("JT808 新建用户失败 account=%s: %s", account, e)
        return None
    finally:
        conn.close()


def _sync_update(
    jt808_user_id: str | None,
    account: str,
    old_account: str | None,
    display_name: str,
    is_active: bool,
    group_id: int | None,
    plain_password: str | None,
) -> str | None:
    conn = _open_connection()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            uid = jt808_user_id or _find_user_id(cur, account) or (
                _find_user_id(cur, old_account) if old_account else None
            )
            if not uid:
                logger.warning("JT808 更新用户未找到 account=%s old=%s", account, old_account)
                return None

            now = _jt808_time()
            status = 1 if is_active else 0
            sets = ["name=%s", "account=%s", "status=%s", "modify_time=%s"]
            params: list[Any] = [display_name or account, account, status, now]

            if plain_password:
                sets.append("password=%s")
                params.append(lingx_password_encode(plain_password, account))

            params.append(uid)
            res = cur.execute(f"UPDATE tlingx_user SET {', '.join(sets)} WHERE id=%s", params)
            # 按 id 命中 0 行：本地存的 jt808_user_id 可能已失效，按 account 兜底重查
            if not res:
                fallback = _find_user_id(cur, account) or (
                    _find_user_id(cur, old_account) if old_account else None
                )
                if fallback and fallback != uid:
                    uid = fallback
                    params[-1] = uid
                    cur.execute(f"UPDATE tlingx_user SET {', '.join(sets)} WHERE id=%s", params)
            _set_group(cur, uid, group_id)
            conn.commit()
            logger.info("JT808 更新用户 id=%s account=%s", uid, account)
            return uid
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        logger.warning("JT808 更新用户失败 account=%s: %s", account, e)
        return None
    finally:
        conn.close()


def _sync_set_password(jt808_user_id: str | None, account: str, plain_password: str) -> bool:
    conn = _open_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            uid = jt808_user_id or _find_user_id(cur, account)
            if not uid:
                logger.warning("JT808 改密未找到用户 account=%s", account)
                return False
            encoded = lingx_password_encode(plain_password, account)
            res = cur.execute(
                "UPDATE tlingx_user SET password=%s, modify_time=%s WHERE id=%s",
                (encoded, _jt808_time(), uid),
            )
            # 按 id 命中 0 行：jt808_user_id 失效，按 account 兜底重查重改，避免「改不动 808」
            if not res:
                fallback = _find_user_id(cur, account)
                if fallback and fallback != uid:
                    uid = fallback
                    res = cur.execute(
                        "UPDATE tlingx_user SET password=%s, modify_time=%s WHERE id=%s",
                        (encoded, _jt808_time(), uid),
                    )
            if not res:
                conn.rollback()
                logger.warning("JT808 改密未命中任何行 account=%s（账号可能未同步到 808）", account)
                return False
            conn.commit()
            logger.info("JT808 改密成功 account=%s id=%s", account, uid)
            return True
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        logger.warning("JT808 改密失败 account=%s: %s", account, e)
        return False
    finally:
        conn.close()


def _sync_delete(jt808_user_id: str | None, account: str) -> bool:
    if (account or "").strip().lower() == settings.jt808_admin_account.lower():
        logger.warning("JT808 跳过删除内置管理员 account=%s", account)
        return False
    conn = _open_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            uid = jt808_user_id or _find_user_id(cur, account)
            if not uid:
                logger.warning("JT808 删除用户未找到 account=%s", account)
                return False
            cur.execute("DELETE FROM tgps_group_user WHERE user_id = %s", (uid,))
            cur.execute("DELETE FROM tlingx_user_role WHERE user_id = %s", (uid,))
            cur.execute("DELETE FROM tlingx_user WHERE id = %s", (uid,))
            conn.commit()
            logger.info("JT808 删除用户 account=%s id=%s", account, uid)
            return True
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        logger.warning("JT808 删除用户失败 account=%s: %s", account, e)
        return False
    finally:
        conn.close()


async def _backfill_jt808_user_id(user_id: int, jt808_uid: str) -> None:
    from sqlalchemy import update as _update

    from app.database import AsyncSessionLocal
    from app.models import SysUser

    async with AsyncSessionLocal() as s:
        await s.execute(
            _update(SysUser).where(SysUser.id == user_id).values(jt808_user_id=jt808_uid)
        )
        await s.commit()


async def _load_user(user_id: int) -> dict[str, Any] | None:
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    from app.database import AsyncSessionLocal
    from app.models import OrgCompany, SysUser

    async with AsyncSessionLocal() as s:
        row = await s.scalar(
            select(SysUser)
            .options(selectinload(SysUser.org))
            .where(SysUser.id == user_id)
            .limit(1)
        )
        if row is None:
            return None
        org = row.org
        gid = None
        if org is not None and org.jt808_group_id:
            gid = int(org.jt808_group_id)
        elif row.org_id:
            co = await s.scalar(select(OrgCompany).where(OrgCompany.id == row.org_id).limit(1))
            if co and co.jt808_group_id:
                gid = int(co.jt808_group_id)
        return {
            "id": row.id,
            "username": (row.username or "").strip(),
            "password_plain": (row.password_plain or "").strip(),
            "jt808_user_id": (row.jt808_user_id or "").strip() or None,
            "is_active": bool(row.is_active),
            "group_id": gid,
        }


# ---------- 后台任务 ----------


async def bg_create(user_id: int) -> None:
    if not _enabled():
        return
    data = await _load_user(user_id)
    if not data or not data["username"]:
        return
    pwd = data["password_plain"]
    if not pwd:
        logger.warning("JT808 新建用户跳过：无 password_plain user_id=%s", user_id)
        return
    jt_uid = await asyncio.to_thread(
        _sync_create,
        data["username"],
        pwd,
        data["username"],
        data["is_active"],
        data["group_id"],
    )
    if jt_uid:
        try:
            await _backfill_jt808_user_id(user_id, jt_uid)
        except Exception as e:  # noqa: BLE001
            logger.warning("回写 jt808_user_id 失败 user_id=%s: %s", user_id, e)


async def bg_update(user_id: int, old_username: str | None = None) -> None:
    if not _enabled():
        return
    data = await _load_user(user_id)
    if not data or not data["username"]:
        return
    old = (old_username or "").strip() or None
    pwd = data["password_plain"] if old and old != data["username"] else None
    jt_uid = await asyncio.to_thread(
        _sync_update,
        data["jt808_user_id"],
        data["username"],
        old,
        data["username"],
        data["is_active"],
        data["group_id"],
        pwd,
    )
    if jt_uid and not data["jt808_user_id"]:
        try:
            await _backfill_jt808_user_id(user_id, jt_uid)
        except Exception as e:  # noqa: BLE001
            logger.warning("回写 jt808_user_id 失败 user_id=%s: %s", user_id, e)


async def _lookup_jt808_id(account: str) -> str | None:
    def _lookup() -> str | None:
        conn = _open_connection()
        if conn is None:
            return None
        try:
            with conn.cursor() as cur:
                return _find_user_id(cur, account)
        finally:
            conn.close()

    return await asyncio.to_thread(_lookup)


async def sync_set_password(user_id: int, plain_password: str) -> dict[str, Any]:
    """同步用户密码到 808 MySQL；808 无账号时补建。返回同步结果供 API 反馈前端。"""
    if not _enabled():
        return {"ok": True, "skipped": True, "reason": "jt808_sync_disabled"}
    try:
        data = await _load_user(user_id)
        if not data or not data["username"]:
            return {"ok": False, "message": "用户数据不完整，无法同步 808"}

        ok = await asyncio.to_thread(
            _sync_set_password,
            data["jt808_user_id"],
            data["username"],
            plain_password,
        )
        created = False
        created_uid: str | None = None
        if not ok:
            created_uid = await asyncio.to_thread(
                _sync_create,
                data["username"],
                plain_password,
                data["username"],
                data["is_active"],
                data["group_id"],
            )
            if created_uid:
                ok = True
                created = True
                logger.info("JT808 改密时补建用户 account=%s id=%s", data["username"], created_uid)

        jt808_uid = data["jt808_user_id"] or created_uid
        if ok and not data["jt808_user_id"]:
            jt808_uid = created_uid or await _lookup_jt808_id(data["username"])
            if jt808_uid:
                try:
                    await _backfill_jt808_user_id(user_id, jt808_uid)
                except Exception as e:  # noqa: BLE001
                    logger.warning("回写 jt808_user_id 失败 user_id=%s: %s", user_id, e)

        if ok:
            return {
                "ok": True,
                "created": created,
                "jt808_user_id": jt808_uid,
                "message": "808 密码已同步" + ("（已补建账号）" if created else ""),
            }
        return {
            "ok": False,
            "message": "808 MySQL 同步失败，请确认 SSH 隧道已建立且 808 数据库可连接",
        }
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 改密同步异常 user_id=%s: %s", user_id, e)
        return {
            "ok": False,
            "message": f"808 MySQL 同步异常：{e}",
        }


async def bg_set_password(user_id: int, plain_password: str) -> None:
    await sync_set_password(user_id, plain_password)


async def bg_delete(user_id: int, username: str, jt808_user_id: str | None) -> None:
    if not _enabled():
        return
    await asyncio.to_thread(_sync_delete, jt808_user_id, username)

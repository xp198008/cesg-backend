"""司机信息同步到 808 平台（直连 MySQL，经 SSH 隧道）。

808 司机主表为 tgps_driver，其主键 id 非自增、idcard 非唯一。
该表当前仅由本系统写入，故直接以本地 driver.id 作为 tgps_driver.id，
通过主键冲突走 ON DUPLICATE KEY UPDATE，实现精确 upsert，无需额外映射列。

设计与车辆同步一致：后台 best-effort，隧道未开/失败仅记日志，不阻断主流程。
"""
from __future__ import annotations

import logging
import time

import pymysql

from app.config import settings

logger = logging.getLogger("jt808_driver")


def _conn():
    return pymysql.connect(
        host=settings.jt808_mysql_host,
        port=settings.jt808_mysql_port,
        user=settings.jt808_mysql_user,
        password=settings.jt808_mysql_password,
        database=settings.jt808_mysql_database,
        charset="utf8mb4",
        connect_timeout=8,
    )


def _now_dt() -> str:
    """tgps_driver.create_time / update_time 为 datetime。"""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _sex_code(gender: str | None):
    """tgps_driver.sex 为 tinyint：男=1，女=2，其它/空=NULL（GB/T 2261.1）。"""
    s = (gender or "").strip()
    if s in ("男", "1"):
        return 1
    if s in ("女", "2"):
        return 2
    return None


_UPSERT_SQL = (
    "insert into tgps_driver(id,name,idcard,phone,sex,birthday,jszh,group_id,"
    "remark,create_time,update_time,driver_state) "
    "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
    "on duplicate key update name=values(name),idcard=values(idcard),phone=values(phone),"
    "sex=values(sex),birthday=values(birthday),jszh=values(jszh),group_id=values(group_id),"
    "remark=values(remark),update_time=values(update_time),driver_state=values(driver_state)"
)


def _sync_upsert(data: dict) -> bool:
    did = data.get("id")
    if not did:
        logger.warning("JT808 司机同步跳过：id 为空 (%s)", data)
        return False
    conn = None
    try:
        conn = _conn()
        with conn.cursor() as cur:
            now = _now_dt()
            cur.execute(_UPSERT_SQL, (
                did,
                data.get("name") or "",
                data.get("id_card") or None,
                data.get("phone") or None,
                _sex_code(data.get("gender")),
                data.get("birth_date") or None,
                data.get("driver_license_no") or None,
                data.get("group_id") or 0,
                data.get("remark") or None,
                now,
                now,
                1,
            ))
        conn.commit()
        logger.info("JT808 司机已同步 id=%s", did)
        return True
    except Exception as e:
        logger.warning("JT808 司机同步失败：%s", e)
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _sync_delete(driver_id: int) -> bool:
    if not driver_id:
        return False
    conn = None
    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute("delete from tgps_driver where id=%s", (driver_id,))
        conn.commit()
        logger.info("JT808 司机已删除 id=%s", driver_id)
        return True
    except Exception as e:
        logger.warning("JT808 司机删除失败：%s", e)
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


async def _load_driver(driver_id: int) -> dict | None:
    """从本地库读取司机 + 机构映射（jt808_group_id），组装同步字段。"""
    from app.database import async_session
    from app.models import Driver, OrgCompany

    async with async_session() as db:
        d = await db.get(Driver, driver_id)
        if d is None:
            return None
        company = await db.get(OrgCompany, d.company_id) if d.company_id else None
        group_id = getattr(company, "jt808_group_id", None) if company is not None else None
        return {
            "id": d.id,
            "name": d.name,
            "id_card": d.id_card,
            "phone": d.phone,
            "gender": d.gender,
            "birth_date": d.birth_date.isoformat() if d.birth_date else None,
            "driver_license_no": d.driver_license_no,
            "remark": d.remark,
            "group_id": group_id,
        }


def bg_upsert(driver_id: int):
    """后台任务：加载本地司机并 upsert 到 808。"""
    import asyncio

    data = asyncio.run(_load_driver(driver_id))
    if not data:
        return
    _sync_upsert(data)


def bg_delete(driver_id: int):
    _sync_delete(driver_id)

"""JT808 车辆（tgps_car + tgps_car_tdh）联动客户端。

经 SSH 隧道直连 MySQL jt808 库，在本地车辆增删改时 best-effort 同步：
- 新增/修改：INSERT ... ON DUPLICATE KEY UPDATE tgps_car，并重建通道表 tgps_car_tdh
- 删除：删除 tgps_car + tgps_car_tdh
- 设备号变更：先删旧 id 对应的平台车辆，再 upsert 新 id

平台主键 id = parseTid(device_no)（去前导 0，与平台 1214 入库口径一致）。
直连而非走 1214 接口：1214 服务端加列后连接池预编译缓存会间歇报 cjh，直连完全可控。
隧道未开 / 同步失败仅记 warning，不抛出，不阻断本地车辆操作。
"""
from __future__ import annotations

import asyncio
import logging
import time

import pymysql

from app.config import settings

logger = logging.getLogger(__name__)

DEFAULT_VERSION = "JT808-2013"


def _enabled() -> bool:
    return bool(settings.jt808_sync_enabled)


def _now() -> str:
    """平台 tgps_car.create_time / modify_time 为 char(14)：yyyyMMddHHmmss。"""
    return time.strftime("%Y%m%d%H%M%S", time.localtime())


def _ensure_cjh_column(cur) -> None:
    cur.execute(
        "select count(*) from information_schema.columns "
        "where table_schema=%s and table_name='tgps_car' and column_name='cjh'",
        (settings.jt808_mysql_database,),
    )
    if cur.fetchone()[0]:
        return
    cur.execute("alter table tgps_car add column cjh varchar(64) default '' comment '车架号' after tel")
    logger.info("JT808 tgps_car 已自动补列 cjh")


def _car_id_by_tid(cur, tid: str) -> int | None:
    cur.execute("select id from tgps_car where tid=%s limit 1", (tid,))
    row = cur.fetchone()
    return int(row[0]) if row else None


def _terminal_variants(terminal_id: str) -> list[str]:
    t = (terminal_id or "").strip()
    if not t:
        return []
    variants = {t, _parse_tid(t)}
    if t.isdigit():
        variants.add(t.zfill(12))
    return [x for x in variants if x]


def _connect() -> pymysql.connections.Connection:
    return pymysql.connect(
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


def _parse_tid(tid: str) -> str:
    """与平台 Utils.parseTid 一致：去掉所有前导 0。"""
    if not tid:
        return tid
    return tid.lstrip("0") or "0"


def _channel_num(n) -> int:
    try:
        n = int(n or 0)
    except (TypeError, ValueError):
        n = 0
    if n <= 0:
        n = 1
    return min(n, 32)


# ---------- 同步原语（阻塞，放入 to_thread 执行） ----------

_UPSERT_SQL = (
    "insert into tgps_car(tid,carno,sim,czxm,tel,cjh,remark,version,channel_num,"
    "create_time,modify_time,group_id) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
    "on duplicate key update carno=values(carno),sim=values(sim),czxm=values(czxm),"
    "tel=values(tel),cjh=values(cjh),remark=values(remark),version=values(version),"
    "channel_num=values(channel_num),modify_time=values(modify_time),group_id=values(group_id)"
)


def _sync_upsert(data: dict) -> bool:
    dev = (data.get("device_no") or "").strip()
    if not dev:
        logger.info("JT808 车辆同步跳过：无主设备号 vehicle_id=%s", data.get("id"))
        return False
    gid = data.get("group_id")
    if not gid:
        logger.warning("JT808 车辆同步跳过：公司无 jt808_group_id vehicle_id=%s", data.get("id"))
        return False
    tid = _parse_tid(dev)
    ch = _channel_num(data.get("channel_count"))
    now = _now()
    try:
        conn = _connect()
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 车辆同步连接失败（隧道未开?）vehicle_id=%s: %s", data.get("id"), e)
        return False
    try:
        with conn.cursor() as cur:
            _ensure_cjh_column(cur)
            cur.execute(_UPSERT_SQL, (
                tid,
                (data.get("plate_no") or "").strip(),
                (data.get("sim_no") or "").strip(),
                (data.get("driver_name") or data.get("contact_name") or "").strip(),
                (data.get("contact_phone") or "").strip(),
                (data.get("vin") or "").strip(),
                (data.get("remark") or "CESG同步").strip(),
                DEFAULT_VERSION, ch, now, now, int(gid),
            ))
            car_id = _car_id_by_tid(cur, tid)
            if car_id is None:
                raise RuntimeError(f"upsert 后未找到 tid={tid}")
            cur.execute("delete from tgps_car_tdh where car_id=%s", (car_id,))
            for t in range(1, ch + 1):
                cur.execute(
                    "insert into tgps_car_tdh(car_id,tdh,name) values(%s,%s,%s)",
                    (car_id, t, f"CH-{t}"),
                )
        conn.commit()
        expected_plate = (data.get("plate_no") or "").strip()
        with conn.cursor() as verify_cur:
            verify_cur.execute("select carno from tgps_car where tid=%s limit 1", (tid,))
            verify_row = verify_cur.fetchone()
        actual_plate = (verify_row[0] or "").strip() if verify_row else ""
        if actual_plate != expected_plate:
            logger.warning(
                "JT808 车辆同步校验失败 vehicle_id=%s tid=%s expected=%s actual=%s",
                data.get("id"),
                tid,
                expected_plate,
                actual_plate,
            )
            return False
        logger.info("JT808 车辆同步成功 vehicle_id=%s id=%s carno=%s", data.get("id"), tid, expected_plate)
        return True
    except Exception as e:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        logger.warning("JT808 车辆同步失败 vehicle_id=%s id=%s: %s", data.get("id"), tid, e)
        return False
    finally:
        conn.close()


def _sync_delete(device_no: str | None, plate_no: str | None = None) -> bool:
    dev = (device_no or "").strip()
    plate = (plate_no or "").strip()
    if not dev and not plate:
        return False
    tids = _terminal_variants(dev)
    try:
        conn = _connect()
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 车辆删除连接失败（隧道未开?）device=%s plate=%s: %s", dev, plate, e)
        return False
    try:
        with conn.cursor() as cur:
            clauses = []
            params = []
            if tids:
                clauses.append("tid in (" + ",".join(["%s"] * len(tids)) + ")")
                params.extend(tids)
            if plate:
                clauses.append("carno=%s")
                params.append(plate)
            cur.execute("select id,tid,carno from tgps_car where " + " or ".join(clauses), tuple(params))
            rows = cur.fetchall()
            if not rows:
                logger.info("JT808 车辆删除跳过：平台无匹配记录 device=%s plate=%s tids=%s", dev, plate, tids)
                return True
            car_ids = [int(row[0]) for row in rows]
            cur.execute("delete from tgps_car_tdh where car_id in (" + ",".join(["%s"] * len(car_ids)) + ")", tuple(car_ids))
            cur.execute("delete from tgps_car where id in (" + ",".join(["%s"] * len(car_ids)) + ")", tuple(car_ids))
        conn.commit()
        logger.info("JT808 车辆删除成功 ids=%s device=%s plate=%s", car_ids, dev, plate)
        return True
    except Exception as e:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        logger.warning("JT808 车辆删除失败 device=%s plate=%s: %s", dev, plate, e)
        return False
    finally:
        conn.close()


# ---------- 读取本地车辆 ----------

async def _load_vehicle(vehicle_id: int) -> dict | None:
    from sqlalchemy import select

    from app.database import AsyncSessionLocal
    from app.models import Driver, OrgCompany, Vehicle, VehicleDevice

    async with AsyncSessionLocal() as s:
        v = await s.scalar(select(Vehicle).where(Vehicle.id == vehicle_id).limit(1))
        if v is None:
            return None
        d = await s.scalar(
            select(VehicleDevice)
            .where(VehicleDevice.vehicle_id == vehicle_id, VehicleDevice.is_main.is_(True))
            .limit(1)
        )
        gid = None
        if v.company_id:
            co = await s.scalar(select(OrgCompany).where(OrgCompany.id == v.company_id).limit(1))
            if co and co.jt808_group_id:
                gid = int(co.jt808_group_id)
        dname = None
        if v.driver_id:
            dname = await s.scalar(select(Driver.name).where(Driver.id == v.driver_id).limit(1))
        return {
            "id": v.id,
            "plate_no": v.plate_no,
            "vin": v.vin,
            "channel_count": v.channel_count,
            "contact_name": v.contact_name,
            "contact_phone": v.contact_phone,
            "remark": v.remark,
            "device_no": (d.device_no if d else None),
            "sim_no": (d.sim_no if d else None),
            "group_id": gid,
            "driver_name": dname,
        }


# ---------- 后台任务 ----------

async def bg_upsert(vehicle_id: int, old_device_no: str | None = None) -> None:
    if not _enabled():
        return
    data = await _load_vehicle(vehicle_id)
    if not data:
        return
    old = (old_device_no or "").strip()
    new = (data.get("device_no") or "").strip()
    if old and old != new:
        # 设备号变更：先删旧 id 对应的平台车辆，避免残留
        await asyncio.to_thread(_sync_delete, old)
    await asyncio.to_thread(_sync_upsert, data)


async def bg_delete(device_no: str | None, plate_no: str | None = None) -> None:
    if not _enabled():
        return
    await asyncio.to_thread(_sync_delete, device_no, plate_no)


async def delete_now(device_no: str | None, plate_no: str | None = None) -> bool | None:
    """立即执行删除并返回结果；None 表示同步开关关闭。"""
    if not _enabled():
        return None
    return await asyncio.to_thread(_sync_delete, device_no, plate_no)


async def upsert_now(vehicle_id: int, old_device_no: str | None = None) -> bool | None:
    """立即执行新增/更新同步并返回结果；None 表示同步开关关闭。"""
    if not _enabled():
        return None
    data = await _load_vehicle(vehicle_id)
    if not data:
        return False
    old = (old_device_no or "").strip()
    new = (data.get("device_no") or "").strip()
    if old and old != new:
        await asyncio.to_thread(_sync_delete, old, data.get("plate_no"))
    return await asyncio.to_thread(_sync_upsert, data)

"""JT808 车辆联动客户端。

本地车辆增删改时 best-effort 同步到 808 平台：
- 新增/修改：OpenAPI **1251**（扩展字段写入 `ext_json`）
- 删除：OpenAPI **1216**（按 deviceId）
- 通道表：1251 成功后若 SSH 隧道可用，补充维护 `tgps_car_tdh`

失败仅记 warning，不抛出，不阻断本地车辆操作。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from decimal import Decimal
from typing import Any

import httpx
import pymysql

from app.config import settings

logger = logging.getLogger(__name__)

DEFAULT_VERSION = "JT808-2013"

_token: str | None = None
_token_lock = asyncio.Lock()

_AUTH_HINTS = ("登录", "未登录", "登陆", "token", "令牌", "重新登录", "会话")


def _enabled() -> bool:
    return bool(settings.jt808_sync_enabled)


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


def _parse_tid(tid: str) -> str:
    """与平台 Utils.parseTid 一致：去掉所有前导 0。"""
    if not tid:
        return tid
    return tid.lstrip("0") or "0"


def _terminal_variants(terminal_id: str) -> list[str]:
    t = (terminal_id or "").strip()
    if not t:
        return []
    variants = {t, _parse_tid(t)}
    if t.isdigit():
        variants.add(t.zfill(12))
    return [x for x in variants if x]


def _channel_num(n) -> int:
    try:
        n = int(n or 0)
    except (TypeError, ValueError):
        n = 0
    if n <= 0:
        n = 1
    return min(n, 32)


def _json_val(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, str)):
        return v
    return str(v)


def _oil_box(data: dict) -> int:
    raw = (data.get("fuel_tank_capacity") or "").strip()
    if not raw:
        return 0
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return 0


def _device_type(data: dict) -> str:
    t = (data.get("terminal_type") or "").strip()
    return t or DEFAULT_VERSION


def _build_ext_json(data: dict) -> str:
    """1251 扩展字段：标准列 + CESG 业务字段（含偏移量）。"""
    ch = _channel_num(data.get("channel_count"))
    ext: dict[str, Any] = {
        "sim": (data.get("sim_no") or "").strip(),
        "czxm": (data.get("driver_name") or data.get("contact_name") or "").strip(),
        "tel": (data.get("contact_phone") or "").strip(),
        "cjh": (data.get("vin") or "").strip(),
        "version": _device_type(data),
        "channel_num": ch,
        "remark": (data.get("remark") or "CESG同步").strip(),
    }
    optional_keys = (
        "plate_color",
        "vehicle_category",
        "vehicle_type",
        "vehicle_type_ii",
        "vehicle_usage",
        "status",
        "brand",
        "model",
        "manufacturer",
        "engine_displacement",
        "fuel_tank_capacity",
        "battery_capacity",
        "range_mileage",
        "battery_no",
        "motor_no",
        "vehicle_grade",
        "owner_name",
        "contact_name",
        "route",
        "agent",
        "mileage_offset",
        "mileage_factor",
        "speed_limit",
        "track_retain_days",
        "icon_id",
        "night_speed_enabled",
        "night_start_time",
        "night_end_time",
        "night_speed_percent",
        "plate_login",
        "is_connect",
    )
    for key in optional_keys:
        val = _json_val(data.get(key))
        if val is None or val == "":
            continue
        ext[key] = val
    for date_key in ("install_date", "service_start_date", "service_end_date", "scrap_date", "inspect_date"):
        val = data.get(date_key)
        if val:
            ext[date_key] = str(val)
    return json.dumps(ext, ensure_ascii=False)


def _device_id_from_cesg(dev: str) -> str:
    """CESG 设备号转 1251 deviceId：数字设备保留 12 位前导 0。"""
    dev = (dev or "").strip()
    if dev.isdigit() and len(dev) <= 12:
        return dev.zfill(12)
    return _parse_tid(dev) if dev else dev


def _lookup_car_row_mysql(dev: str = "", plate: str = "") -> dict | None:
    dev = (dev or "").strip()
    plate = (plate or "").strip()
    conn = _connect_mysql()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            if dev:
                for variant in _terminal_variants(dev):
                    cur.execute(
                        "select id, tid, carno from tgps_car where tid=%s limit 1",
                        (variant,),
                    )
                    row = cur.fetchone()
                    if row:
                        return {"id": row[0], "tid": row[1], "carno": row[2]}
            if plate:
                cur.execute(
                    "select id, tid, carno from tgps_car where carno=%s limit 1",
                    (plate,),
                )
                row = cur.fetchone()
                if row:
                    return {"id": row[0], "tid": row[1], "carno": row[2]}
    except Exception as e:  # noqa: BLE001
        logger.debug("JT808 MySQL 查车失败: %s", e)
    finally:
        conn.close()
    return None


async def _lookup_car_row_via_1211(dev: str, plate: str = "") -> dict | None:
    """用 1211 + deviceId 查车（8002 grid 的 where 在当前平台不生效）。"""
    dev = (dev or "").strip()
    plate = (plate or "").strip()
    if not dev:
        return None
    target_parsed = _parse_tid(dev)
    for variant in _terminal_variants(dev):
        r = await _call({"apicode": 1211, "deviceId": variant, "page": 1, "rows": 10})
        if r.get("code") != 1:
            continue
        for row in r.get("data") or []:
            row_tid = str(row.get("tid") or "").strip()
            if _parse_tid(row_tid) != target_parsed:
                continue
            if plate and str(row.get("carno") or "").strip() != plate:
                continue
            return row
    return None


async def _lookup_car_row(dev: str, plate: str = "") -> dict | None:
    row = await _lookup_car_row_via_1211(dev, plate)
    if row:
        return row
    return await asyncio.to_thread(_lookup_car_row_mysql, dev, plate)


async def _lookup_car_id(dev: str) -> int | None:
    row = await _lookup_car_row(dev)
    if row and row.get("id") is not None:
        return int(row["id"])
    return None


async def _lookup_car_row_by_plate(plate_no: str) -> dict | None:
    plate = (plate_no or "").strip()
    if not plate:
        return None
    row = await asyncio.to_thread(_lookup_car_row_mysql, "", plate)
    return row


async def _resolve_1251_device_id_and_car_id(dev: str, plate: str) -> tuple[str, int | None]:
    """解析 1251 的 deviceId 与 808 id。

    - 808 已有且设备号一致：带 id 更新，deviceId 用 808 返回的 tid
    - 808 没有：不带 id，deviceId 用 CESG 设备号补全 12 位
    """
    dev = (dev or "").strip()
    plate = (plate or "").strip()
    target_parsed = _parse_tid(dev)

    for row in (
        await _lookup_car_row(dev, plate),
        await _lookup_car_row_by_plate(plate) if plate else None,
    ):
        if not row or row.get("id") is None:
            continue
        row_tid = str(row.get("tid") or "").strip()
        if dev and _parse_tid(row_tid) != target_parsed:
            continue
        return row_tid or _device_id_from_cesg(dev), int(row["id"])

    return _device_id_from_cesg(dev), None


async def _lookup_tid_by_plate(plate_no: str) -> str | None:
    row = await _lookup_car_row_by_plate(plate_no)
    if row and row.get("tid"):
        return str(row["tid"]).strip()
    return None


async def _build_1251_request(data: dict, token: str) -> tuple[dict[str, Any] | None, str | None]:
    """组装 1251 完整 HTTP 请求体；失败时返回 (None, 原因)。"""
    dev = (data.get("device_no") or "").strip()
    if not dev:
        return None, "无主设备号"
    gid = data.get("group_id")
    if not gid:
        return None, "所属公司未配置 jt808_group_id"

    plate = (data.get("plate_no") or "").strip()
    device_id, car_id = await _resolve_1251_device_id_and_car_id(dev, plate)

    body: dict[str, Any] = {
        "language": "zh-CN",
        "apicode": 1251,
        "lingxtoken": token,
        "deviceId": device_id,
        "groupId": int(gid),
        "carno": plate,
        "deviceType": _device_type(data),
        "oilBox": _oil_box(data),
        "ext_json": _build_ext_json(data),
    }
    if car_id is not None:
        body["id"] = car_id
    return body, None


async def _sync_upsert(data: dict, trace: dict | None = None) -> bool:
    dev = (data.get("device_no") or "").strip()
    if trace is not None:
        trace["fuel_tank_capacity_raw"] = data.get("fuel_tank_capacity")
        trace["oil_box_parsed"] = _oil_box(data)
        trace["group_id"] = data.get("group_id")
        trace["device_no"] = dev or None
        trace["plate_no"] = data.get("plate_no")
    if not dev:
        logger.info("JT808 车辆同步跳过：无主设备号 vehicle_id=%s", data.get("id"))
        if trace is not None:
            trace["skip_reason"] = "无主设备号"
        return False
    gid = data.get("group_id")
    if not gid:
        logger.warning("JT808 车辆同步跳过：公司无 jt808_group_id vehicle_id=%s", data.get("id"))
        if trace is not None:
            trace["skip_reason"] = "所属公司未配置 jt808_group_id"
        return False

    tid = ""
    try:
        token = await _ensure_token()
        payload, err = await _build_1251_request(data, token)
        if payload is None:
            logger.warning("JT808 1251 车辆同步跳过 vehicle_id=%s: %s", data.get("id"), err)
            if trace is not None:
                trace["skip_reason"] = err
            return False
        tid = payload["deviceId"]
        plate = payload["carno"]
        car_id = payload.get("id")
        # _call 会再次附带 language/lingxtoken，这里去掉避免重复
        call_body = {k: v for k, v in payload.items() if k not in ("language", "lingxtoken")}
        if trace is not None:
            trace["request_apicode"] = 1251
            trace["api_url"] = settings.jt808_api_base
            trace["request_body"] = dict(call_body)
        r = await _call(call_body)
        if trace is not None:
            trace["response"] = r
            trace["response_code"] = r.get("code")
            trace["response_message"] = r.get("message")
    except Exception as e:  # noqa: BLE001
        logger.warning("JT808 1251 车辆同步异常 vehicle_id=%s tid=%s: %s", data.get("id"), tid, e)
        if trace is not None:
            trace["exception"] = str(e)
        return False

    if r.get("code") != 1:
        logger.warning(
            "JT808 1251 车辆同步失败 vehicle_id=%s tid=%s: %s",
            data.get("id"),
            tid,
            r.get("message") or r,
        )
        return False

    if car_id is None:
        data_obj = r.get("data") or {}
        if isinstance(data_obj, dict) and data_obj.get("id") is not None:
            car_id = int(data_obj["id"])
        else:
            car_id = await _lookup_car_id(dev)

    await asyncio.to_thread(_sync_channels_mysql, tid, _channel_num(data.get("channel_count")), car_id)
    logger.info("JT808 1251 车辆同步成功 vehicle_id=%s tid=%s carno=%s", data.get("id"), tid, plate)
    return True


async def _sync_delete(device_no: str | None, plate_no: str | None = None) -> bool:
    dev = (device_no or "").strip()
    plate = (plate_no or "").strip()
    if not dev and not plate:
        return False

    tids = list(_terminal_variants(dev))
    if not tids and plate:
        found = await _lookup_tid_by_plate(plate)
        if found:
            tids = _terminal_variants(found)

    if not tids:
        logger.info("JT808 车辆删除跳过：无可用 deviceId device=%s plate=%s", dev, plate)
        return True

    ok = False
    for tid in tids:
        try:
            r = await _call({"apicode": 1216, "deviceId": tid})
        except Exception as e:  # noqa: BLE001
            logger.warning("JT808 1216 删除异常 device=%s: %s", tid, e)
            continue
        if r.get("code") == 1:
            ok = True
            logger.info("JT808 1216 车辆删除成功 device=%s", tid)
        else:
            msg = str(r.get("message") or "")
            if any(k in msg for k in ("不存在", "未找到", "无此", "not found")):
                ok = True
            else:
                logger.warning("JT808 1216 删除失败 device=%s: %s", tid, msg or r)

    return ok


def _connect_mysql() -> pymysql.connections.Connection | None:
    try:
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
    except Exception as e:  # noqa: BLE001
        logger.debug("JT808 MySQL 不可用（通道同步跳过）: %s", e)
        return None


def _car_id_by_tid_mysql(cur, tid: str) -> int | None:
    cur.execute("select id from tgps_car where tid=%s limit 1", (tid,))
    row = cur.fetchone()
    return int(row[0]) if row else None


def _sync_channels_mysql(tid: str, ch: int, car_id: int | None = None) -> None:
    """1251 不维护通道明细时，经隧道补充 tgps_car_tdh。"""
    conn = _connect_mysql()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cid = car_id or _car_id_by_tid_mysql(cur, tid)
            if cid is None:
                return
            cur.execute("delete from tgps_car_tdh where car_id=%s", (cid,))
            for t in range(1, ch + 1):
                cur.execute(
                    "insert into tgps_car_tdh(car_id,tdh,name) values(%s,%s,%s)",
                    (cid, t, f"CH-{t}"),
                )
        conn.commit()
    except Exception as e:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        logger.warning("JT808 通道表同步失败 tid=%s: %s", tid, e)
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
            "plate_color": v.plate_color,
            "vin": v.vin,
            "channel_count": v.channel_count,
            "contact_name": v.contact_name,
            "contact_phone": v.contact_phone,
            "owner_name": v.owner_name,
            "remark": v.remark,
            "device_no": (d.device_no if d else None),
            "sim_no": (d.sim_no if d else None),
            "terminal_type": (d.terminal_type if d else None),
            "group_id": gid,
            "driver_name": dname,
            "vehicle_category": v.vehicle_category,
            "vehicle_type": v.vehicle_type,
            "vehicle_type_ii": v.vehicle_type_ii,
            "vehicle_usage": v.vehicle_usage,
            "status": v.status,
            "brand": v.brand,
            "model": v.model,
            "manufacturer": v.manufacturer,
            "engine_displacement": v.engine_displacement,
            "fuel_tank_capacity": v.fuel_tank_capacity,
            "battery_capacity": v.battery_capacity,
            "range_mileage": v.range_mileage,
            "battery_no": v.battery_no,
            "motor_no": v.motor_no,
            "vehicle_grade": v.vehicle_grade,
            "route": v.route,
            "agent": v.agent,
            "mileage_offset": v.mileage_offset,
            "mileage_factor": v.mileage_factor,
            "speed_limit": v.speed_limit,
            "track_retain_days": v.track_retain_days,
            "icon_id": v.icon_id,
            "night_speed_enabled": v.night_speed_enabled,
            "night_start_time": v.night_start_time,
            "night_end_time": v.night_end_time,
            "night_speed_percent": v.night_speed_percent,
            "plate_login": v.plate_login,
            "is_connect": v.is_connect,
            "install_date": v.install_date.isoformat() if v.install_date else None,
            "service_start_date": v.service_start_date.isoformat() if v.service_start_date else None,
            "service_end_date": v.service_end_date.isoformat() if v.service_end_date else None,
            "scrap_date": v.scrap_date.isoformat() if v.scrap_date else None,
            "inspect_date": v.inspect_date.isoformat() if v.inspect_date else None,
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
        await _sync_delete(old, data.get("plate_no"))
    await _sync_upsert(data)


async def bg_delete(device_no: str | None, plate_no: str | None = None) -> None:
    if not _enabled():
        return
    await _sync_delete(device_no, plate_no)


async def delete_now(device_no: str | None, plate_no: str | None = None) -> bool | None:
    """立即执行删除并返回结果；None 表示同步开关关闭。"""
    if not _enabled():
        return None
    return await _sync_delete(device_no, plate_no)


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
        await _sync_delete(old, data.get("plate_no"))
    return await _sync_upsert(data)

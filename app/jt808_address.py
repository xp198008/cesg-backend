"""808 平台逆地理配置与地址缓存（对齐 AddressService）。

配置项（MySQL tlingx_config）：
  lingx.jt808.type1   境内解析：gaode / photon
  lingx.jt808.appkey1 境内高德 Web 服务 Key（逆地理专用）
  lingx.jt808.address 是否启用地址解析 true/false

缓存表：tgps_address(lat, lng, address) — 坐标保留 3 位小数。
"""
from __future__ import annotations

import logging
from decimal import Decimal, ROUND_HALF_UP

import pymysql

from app.config import settings

logger = logging.getLogger(__name__)

_CONFIG_CACHE: dict[str, str] = {}
_MYSQL_DOWN_UNTIL: float = 0.0
_MYSQL_DOWN_COOLDOWN_SEC = 60.0


def _mysql_mark_down() -> None:
    import time

    global _MYSQL_DOWN_UNTIL
    _MYSQL_DOWN_UNTIL = time.monotonic() + _MYSQL_DOWN_COOLDOWN_SEC


def _mysql_is_down() -> bool:
    import time

    return time.monotonic() < _MYSQL_DOWN_UNTIL


def _connect_mysql() -> pymysql.connections.Connection | None:
    if _mysql_is_down():
        return None
    try:
        return pymysql.connect(
            host=settings.jt808_mysql_host,
            port=int(settings.jt808_mysql_port),
            user=settings.jt808_mysql_user,
            password=settings.jt808_mysql_password,
            database=settings.jt808_mysql_database,
            charset="utf8mb4",
            connect_timeout=2,
            read_timeout=3,
            write_timeout=3,
        )
    except Exception as exc:  # noqa: BLE001
        _mysql_mark_down()
        logger.debug("808 MySQL 不可用（逆地理配置跳过）: %s", exc)
        return None


def clear_jt808_config_cache(key: str | None = None) -> None:
    """清空 808 配置内存缓存；key 为空时清全部。"""
    if key is None:
        _CONFIG_CACHE.clear()
        return
    _CONFIG_CACHE.pop(key, None)


def get_jt808_config(key: str, default: str = "", *, force_refresh: bool = False) -> str:
    if not force_refresh:
        cached = _CONFIG_CACHE.get(key)
        if cached is not None:
            return cached
    else:
        _CONFIG_CACHE.pop(key, None)
    conn = _connect_mysql()
    if conn is None:
        return default
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select config_value from tlingx_config where config_key=%s limit 1",
                (key,),
            )
            row = cur.fetchone()
            val = str(row[0]).strip() if row and row[0] is not None else default
            _CONFIG_CACHE[key] = val
            return val
    except Exception as exc:  # noqa: BLE001
        logger.debug("读取 808 配置 %s 失败: %s", key, exc)
        return default
    finally:
        conn.close()


def jt808_address_enabled() -> bool:
    return get_jt808_config("lingx.jt808.address", "true").strip().lower() == "true"


def get_jt808_regeo_amap_key(*, force_refresh: bool = False) -> str:
    """808 境内高德 Web 服务 Key（appkey1，逆地理/纠偏等）。"""
    if get_jt808_config("lingx.jt808.type1", "photon", force_refresh=force_refresh).strip().lower() != "gaode":
        return ""
    key = get_jt808_config("lingx.jt808.appkey1", "-", force_refresh=force_refresh).strip()
    if not key or key == "-":
        return ""
    return key


def _coord_key(lat: float, lng: float) -> tuple[str, str]:
    """与 AddressService DecimalFormat('#.000') 一致。"""
    q = Decimal("0.001")
    la = format(Decimal(str(lat)).quantize(q, rounding=ROUND_HALF_UP), "f")
    ln = format(Decimal(str(lng)).quantize(q, rounding=ROUND_HALF_UP), "f")
    return la, ln


def lookup_jt808_address_cache(lat: float, lng: float) -> str | None:
    """查 808 tgps_address 缓存。"""
    la, ln = _coord_key(lat, lng)
    conn = _connect_mysql()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select address from tgps_address where lat=%s and lng=%s limit 1",
                (la, ln),
            )
            row = cur.fetchone()
            if not row or row[0] is None:
                return None
            addr = str(row[0]).strip()
            return addr if addr and addr != "-" else None
    except Exception as exc:  # noqa: BLE001
        logger.debug("读取 tgps_address 失败: %s", exc)
        return None
    finally:
        conn.close()


def save_jt808_address_cache(lat: float, lng: float, address: str) -> None:
    """写入 808 tgps_address，与 AddressService 共用缓存。"""
    addr = (address or "").strip()
    if not addr or addr == "-":
        return
    la, ln = _coord_key(lat, lng)
    conn = _connect_mysql()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select 1 from tgps_address where lat=%s and lng=%s limit 1",
                (la, ln),
            )
            if cur.fetchone():
                return
            from app.timeutil import china_now_naive

            ts = china_now_naive().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(
                "insert into tgps_address(lat,lng,address,ts) values(%s,%s,%s,%s)",
                (la, ln, addr[:512], ts),
            )
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        logger.debug("写入 tgps_address 失败: %s", exc)
    finally:
        conn.close()


def reload_jt808_address_config() -> None:
    _CONFIG_CACHE.clear()

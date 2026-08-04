"""将 CESG 日油耗（obd_fuel_daily）同步到 808 MySQL 镜像表。

本地一车一日 upsert 后默认立即同步：
1. 检查 808 库是否已有镜像表 cesg_obd_fuel_daily
2. 无表 → 建表并把本地全部记录灌入
3. 有表 → 仅 upsert 本条
失败不阻断本地入库（best-effort）。

注意：不写液位、油量事件或轨迹油量表，仅独立事实表。
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from threading import Lock
from typing import Any

import pymysql
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import ObdFuelDaily

logger = logging.getLogger(__name__)

TABLE_NAME = "cesg_obd_fuel_daily"

_SYNC_LOCK = Lock()
_MYSQL_DOWN_UNTIL = 0.0
_MYSQL_DOWN_COOLDOWN_SEC = 60.0

_COLUMNS: list[tuple[str, str]] = [
    ("id", "INT NOT NULL"),
    ("device_no", "VARCHAR(64) NULL"),
    ("plate_no", "VARCHAR(32) NULL"),
    ("vehicle_id", "INT NULL"),
    ("company_id", "INT NULL"),
    ("day", "VARCHAR(8) NOT NULL"),
    ("fuel_l", "DOUBLE NULL"),
    ("drive_km", "DOUBLE NULL"),
    ("start_mileage", "DOUBLE NULL"),
    ("end_mileage", "DOUBLE NULL"),
    ("fuel_per_100km", "DOUBLE NULL"),
    ("source", "VARCHAR(32) NOT NULL DEFAULT 'obd_fdjrlll'"),
    ("report_time", "DATETIME NULL"),
    ("updated_at", "DATETIME NULL"),
    ("created_at", "DATETIME NULL"),
]

_COLUMN_NAMES = [c[0] for c in _COLUMNS]


def _mysql_mark_down() -> None:
    import time

    global _MYSQL_DOWN_UNTIL
    _MYSQL_DOWN_UNTIL = time.monotonic() + _MYSQL_DOWN_COOLDOWN_SEC


def _mysql_is_down() -> bool:
    import time

    return time.monotonic() < _MYSQL_DOWN_UNTIL


def _connect() -> pymysql.connections.Connection | None:
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
            connect_timeout=3,
            read_timeout=30,
            write_timeout=30,
        )
    except Exception as exc:  # noqa: BLE001
        _mysql_mark_down()
        logger.warning("808 MySQL 不可用，跳过日油耗同步: %s", exc)
        return None


def _dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", ""))
    except ValueError:
        return None


def serialize_obd_fuel_daily_row(row: ObdFuelDaily) -> dict[str, Any]:
    return {
        "id": int(row.id) if row.id is not None else None,
        "device_no": (row.device_no or "").strip() or None,
        "plate_no": (row.plate_no or "").strip() or None,
        "vehicle_id": row.vehicle_id,
        "company_id": row.company_id,
        "day": str(row.day or "").strip()[:8],
        "fuel_l": float(row.fuel_l) if row.fuel_l is not None else None,
        "drive_km": float(row.drive_km) if row.drive_km is not None else None,
        "start_mileage": float(row.start_mileage) if row.start_mileage is not None else None,
        "end_mileage": float(row.end_mileage) if row.end_mileage is not None else None,
        "fuel_per_100km": float(row.fuel_per_100km) if row.fuel_per_100km is not None else None,
        "source": (row.source or "obd_fdjrlll")[:32],
        "report_time": _dt(row.report_time),
        "updated_at": _dt(row.updated_at),
        "created_at": _dt(row.created_at),
    }


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = %s
        """,
        (table,),
    )
    row = cur.fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _create_table(cur, table: str) -> None:
    cols_sql = ",\n  ".join(f"`{name}` {ctype}" for name, ctype in _COLUMNS)
    ddl = f"""
CREATE TABLE `{table}` (
  {cols_sql},
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_device_day` (`device_no`, `day`),
  KEY `ix_day` (`day`),
  KEY `ix_plate_no` (`plate_no`),
  KEY `ix_vehicle_id` (`vehicle_id`),
  KEY `ix_company_id` (`company_id`),
  KEY `ix_updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
    cur.execute(ddl)


def _existing_columns(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = %s
        """,
        (table,),
    )
    return {str(r[0]).lower() for r in cur.fetchall()}


def _ensure_columns(cur, table: str) -> None:
    existing = _existing_columns(cur, table)
    for name, ctype in _COLUMNS:
        if name.lower() in existing:
            continue
        cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{name}` {ctype}")
        logger.info("808 日油耗表补列: %s.%s", table, name)


def _row_count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _upsert_rows(cur, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    cols = ", ".join(f"`{c}`" for c in _COLUMN_NAMES)
    placeholders = ", ".join(["%s"] * len(_COLUMN_NAMES))
    updates = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in _COLUMN_NAMES if c != "id")
    sql = (
        f"INSERT INTO `{table}` ({cols}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )
    values = []
    for row in rows:
        if row.get("id") is None:
            continue
        day = str(row.get("day") or "").strip()
        if len(day) != 8:
            continue
        values.append(tuple(row.get(c) for c in _COLUMN_NAMES))
    if not values:
        return 0
    cur.executemany(sql, values)
    return len(values)


def _sync_locked(row_dict: dict[str, Any], all_rows: list[dict[str, Any]] | None) -> str:
    """返回 skipped / created_full / upsert。"""
    table = TABLE_NAME
    conn = _connect()
    if conn is None:
        return "skipped"
    try:
        with _SYNC_LOCK:
            with conn.cursor() as cur:
                existed = _table_exists(cur, table)
                if not existed:
                    _create_table(cur, table)
                    payload = all_rows if all_rows is not None else [row_dict]
                    n = _upsert_rows(cur, table, payload)
                    conn.commit()
                    logger.info("808 日油耗表已创建并全量同步: table=%s rows=%s", table, n)
                    return "created_full"
                _ensure_columns(cur, table)
                count = _row_count(cur, table)
                if count == 0 and all_rows:
                    n = _upsert_rows(cur, table, all_rows)
                    conn.commit()
                    logger.info("808 日油耗表为空，已全量补同步: table=%s rows=%s", table, n)
                    return "created_full"
                n = _upsert_rows(cur, table, [row_dict])
                conn.commit()
                logger.debug(
                    "808 日油耗已同步: table=%s device=%s day=%s n=%s",
                    table,
                    row_dict.get("device_no"),
                    row_dict.get("day"),
                    n,
                )
                return "upsert"
    except Exception as exc:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        _mysql_mark_down()
        logger.warning("同步日油耗到 808 失败: %s", exc)
        return "skipped"
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


def _needs_full_sync_probe() -> bool:
    table = TABLE_NAME
    conn = _connect()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, table):
                return True
            return _row_count(cur, table) == 0
    except Exception:  # noqa: BLE001
        return False
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


async def notify_obd_fuel_daily_upserted(db: AsyncSession, row: ObdFuelDaily) -> None:
    """本地日油耗 upsert 后同步 808（失败不影响主流程）。"""
    if row is None or row.id is None:
        return
    row_dict = serialize_obd_fuel_daily_row(row)
    if not row_dict.get("day") or len(str(row_dict["day"])) != 8:
        return
    all_rows: list[dict[str, Any]] | None = None
    try:
        need_all = await asyncio.to_thread(_needs_full_sync_probe)
        if need_all:
            rows = (await db.execute(select(ObdFuelDaily))).scalars().all()
            all_rows = [
                serialize_obd_fuel_daily_row(r)
                for r in rows
                if r.id is not None and str(r.day or "").strip() and len(str(r.day).strip()) == 8
            ]
        await asyncio.to_thread(_sync_locked, row_dict, all_rows)
    except Exception as exc:  # noqa: BLE001
        logger.warning("日油耗同步 808 异常: %s", exc)


async def notify_obd_fuel_daily_by_keys(
    db: AsyncSession,
    *,
    device_no: str,
    day: str,
) -> None:
    """按设备号+日期查本地行后同步。"""
    device = str(device_no or "").strip()
    d = str(day or "").strip().replace("-", "").replace("/", "")[:8]
    if not device or len(d) != 8:
        return
    row = (
        await db.execute(
            select(ObdFuelDaily).where(
                ObdFuelDaily.device_no == device,
                ObdFuelDaily.day == d,
            ).limit(1)
        )
    ).scalar_one_or_none()
    if row is not None:
        await notify_obd_fuel_daily_upserted(db, row)


def probe_jt808_obd_fuel_table() -> dict[str, Any]:
    target = (
        f"{settings.jt808_mysql_host}:{int(settings.jt808_mysql_port)}/"
        f"{settings.jt808_mysql_database}"
    )
    out: dict[str, Any] = {
        "table": TABLE_NAME,
        "mysql_target": target,
        "mysql_ok": False,
        "table_exists": False,
        "row_count": None,
        "error": None,
    }
    conn = _connect()
    if conn is None:
        out["error"] = "808 MySQL 不可用（连接失败或冷却中）"
        return out
    try:
        with conn.cursor() as cur:
            out["mysql_ok"] = True
            out["table_exists"] = _table_exists(cur, TABLE_NAME)
            if out["table_exists"]:
                out["row_count"] = _row_count(cur, TABLE_NAME)
    except Exception as exc:  # noqa: BLE001
        out["error"] = str(exc)
        _mysql_mark_down()
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
    return out


async def jt808_obd_fuel_sync_status(db: AsyncSession | None = None) -> dict[str, Any]:
    local_count = None
    if db is not None:
        try:
            local_count = int(await db.scalar(select(func.count()).select_from(ObdFuelDaily)) or 0)
        except Exception:  # noqa: BLE001
            local_count = None
    remote = await asyncio.to_thread(probe_jt808_obd_fuel_table)
    return {
        "local_obd_fuel_daily_count": local_count,
        "jt808_mirror": remote,
    }


def _full_sync_locked(all_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """强制建表（如需）并全量 upsert。会清除 MySQL 冷却。"""
    global _MYSQL_DOWN_UNTIL
    _MYSQL_DOWN_UNTIL = 0.0

    out: dict[str, Any] = {
        "ok": False,
        "table": TABLE_NAME,
        "action": None,
        "upserted": 0,
        "local_rows": len(all_rows or []),
        "error": None,
    }
    table = TABLE_NAME
    conn = _connect()
    if conn is None:
        out["error"] = "808 MySQL 不可用"
        return out
    try:
        with _SYNC_LOCK:
            with conn.cursor() as cur:
                existed = _table_exists(cur, table)
                if not existed:
                    _create_table(cur, table)
                    out["action"] = "created_full"
                else:
                    _ensure_columns(cur, table)
                    out["action"] = "full_upsert"
                n = _upsert_rows(cur, table, all_rows or [])
                conn.commit()
                out["upserted"] = n
                out["ok"] = True
                out["row_count"] = _row_count(cur, table)
                logger.info(
                    "808 日油耗强制全量同步: table=%s action=%s upserted=%s total=%s",
                    table,
                    out["action"],
                    n,
                    out["row_count"],
                )
        return out
    except Exception as exc:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        _mysql_mark_down()
        out["error"] = str(exc)
        logger.warning("强制同步日油耗到 808 失败: %s", exc)
        return out
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


async def force_full_sync_obd_fuel_daily(db: AsyncSession) -> dict[str, Any]:
    """把本地全部 obd_fuel_daily 灌入 808（建表 + upsert）。"""
    rows = (await db.execute(select(ObdFuelDaily))).scalars().all()
    all_rows = [
        serialize_obd_fuel_daily_row(r)
        for r in rows
        if r.id is not None and str(r.day or "").strip() and len(str(r.day).strip()) == 8
    ]
    return await asyncio.to_thread(_full_sync_locked, all_rows)

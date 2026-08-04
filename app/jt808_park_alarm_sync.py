"""808 停车超限报警表 cesg_park_alarm：建表 + upsert（best-effort）。"""
from __future__ import annotations

import logging
from datetime import datetime
from threading import Lock
from typing import Any

import pymysql

from app.config import settings

logger = logging.getLogger(__name__)

TABLE_NAME = "cesg_park_alarm"

_SYNC_LOCK = Lock()
_MYSQL_DOWN_UNTIL = 0.0
_MYSQL_DOWN_COOLDOWN_SEC = 60.0

_COLUMNS: list[tuple[str, str]] = [
    ("id", "BIGINT NOT NULL AUTO_INCREMENT"),
    ("plate_no", "VARCHAR(32) NOT NULL"),
    ("device_no", "VARCHAR(64) NULL"),
    ("lng", "DOUBLE NULL"),
    ("lat", "DOUBLE NULL"),
    ("lng_r", "DOUBLE NOT NULL"),
    ("lat_r", "DOUBLE NOT NULL"),
    ("address", "VARCHAR(512) NULL"),
    ("start_time", "DATETIME NULL"),
    ("end_time", "DATETIME NULL"),
    ("duration_min", "INT NOT NULL DEFAULT 0"),
    ("limit_min", "INT NULL"),
    ("day", "VARCHAR(8) NOT NULL"),
    ("category_id", "INT NULL"),
    ("rule_id", "INT NULL"),
    ("rule_name", "VARCHAR(200) NULL"),
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
        logger.warning("808 MySQL 不可用，跳过停车报警同步: %s", exc)
        return None


def round_coord(value: float | None, digits: int = 5) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    s = str(value).strip()
    if not s:
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 14:
        try:
            return datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return None


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


def _existing_columns(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT COLUMN_NAME FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = %s
        """,
        (table,),
    )
    return {str(r[0]).lower() for r in cur.fetchall()}


def _create_table(cur, table: str) -> None:
    cols_sql = ",\n  ".join(f"`{name}` {ctype}" for name, ctype in _COLUMNS)
    ddl = f"""
CREATE TABLE `{table}` (
  {cols_sql},
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_plate_day_coord` (`plate_no`, `day`, `lng_r`, `lat_r`),
  KEY `ix_day` (`day`),
  KEY `ix_device_no` (`device_no`),
  KEY `ix_end_time` (`end_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
    cur.execute(ddl)


def _ensure_columns(cur, table: str) -> None:
    existing = _existing_columns(cur, table)
    for name, ctype in _COLUMNS:
        if name.lower() in existing:
            continue
        cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{name}` {ctype}")
        logger.info("808 停车报警表补列: %s.%s", table, name)


def _ensure_table(cur, table: str) -> None:
    if not _table_exists(cur, table):
        _create_table(cur, table)
        logger.info("808 停车报警表已创建: %s", table)
        return
    _ensure_columns(cur, table)


def ensure_and_insert(row: dict[str, Any]) -> str:
    """
    写入一条停车报警。
    返回：inserted / duplicate / skipped / error
    """
    plate = str(row.get("plate_no") or "").strip()
    day = str(row.get("day") or "").strip()[:8]
    lng_r = round_coord(row.get("lng_r") if row.get("lng_r") is not None else row.get("lng"))
    lat_r = round_coord(row.get("lat_r") if row.get("lat_r") is not None else row.get("lat"))
    if not plate or len(day) != 8 or lng_r is None or lat_r is None:
        return "skipped"

    payload = {
        "plate_no": plate[:32],
        "device_no": (str(row.get("device_no") or "").strip() or None),
        "lng": float(row["lng"]) if row.get("lng") is not None else lng_r,
        "lat": float(row["lat"]) if row.get("lat") is not None else lat_r,
        "lng_r": lng_r,
        "lat_r": lat_r,
        "address": (str(row.get("address") or "").strip() or None),
        "start_time": _dt(row.get("start_time")),
        "end_time": _dt(row.get("end_time")),
        "duration_min": int(row.get("duration_min") or 0),
        "limit_min": int(row["limit_min"]) if row.get("limit_min") is not None else None,
        "day": day,
        "category_id": int(row["category_id"]) if row.get("category_id") is not None else None,
        "rule_id": int(row["rule_id"]) if row.get("rule_id") is not None else None,
        "rule_name": (str(row.get("rule_name") or "").strip() or None),
        "created_at": _dt(row.get("created_at")) or datetime.now().replace(microsecond=0),
    }
    if payload["rule_name"]:
        payload["rule_name"] = payload["rule_name"][:200]

    conn = _connect()
    if conn is None:
        return "skipped"
    try:
        with _SYNC_LOCK:
            with conn.cursor() as cur:
                _ensure_table(cur, TABLE_NAME)
                cols = [c for c in _COLUMN_NAMES if c != "id"]
                placeholders = ", ".join(["%s"] * len(cols))
                col_sql = ", ".join(f"`{c}`" for c in cols)
                sql = (
                    f"INSERT INTO `{TABLE_NAME}` ({col_sql}) VALUES ({placeholders}) "
                    f"ON DUPLICATE KEY UPDATE id=id"
                )
                values = tuple(payload.get(c) for c in cols)
                affected = cur.execute(sql, values)
                conn.commit()
                # MySQL: insert=1, duplicate update with no change=0 or 2 depending on version
                if affected == 1:
                    return "inserted"
                return "duplicate"
    except Exception as exc:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        _mysql_mark_down()
        logger.warning("写入 808 停车报警失败: %s", exc)
        return "error"
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


def ensure_park_alarm_table() -> dict[str, Any]:
    """确保 808 表存在（启动/扫描时调用；不依赖是否已有停车限时或报警数据）。"""
    target = (
        f"{settings.jt808_mysql_host}:{int(settings.jt808_mysql_port)}/"
        f"{settings.jt808_mysql_database}"
    )
    out: dict[str, Any] = {
        "ok": False,
        "table": TABLE_NAME,
        "mysql_target": target,
        "created": False,
        "error": None,
    }
    conn = _connect()
    if conn is None:
        out["error"] = "808 MySQL 不可用（连接失败或冷却中）"
        return out
    try:
        with _SYNC_LOCK:
            with conn.cursor() as cur:
                existed = _table_exists(cur, TABLE_NAME)
                _ensure_table(cur, TABLE_NAME)
                conn.commit()
                out["ok"] = True
                out["created"] = not existed
    except Exception as exc:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        out["error"] = str(exc)
        _mysql_mark_down()
        logger.warning("确保 808 停车报警表失败: %s", exc)
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
    return out


def probe_park_alarm_table() -> dict[str, Any]:
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
                cur.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}`")
                row = cur.fetchone()
                out["row_count"] = int(row[0] or 0) if row else 0
    except Exception as exc:  # noqa: BLE001
        out["error"] = str(exc)
        _mysql_mark_down()
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
    return out

"""将 CESG 安全报警（vehicle_violation）同步到 808 MySQL。

产生一条本地报警后默认立即同步：
1. 检查 808 库是否已有镜像表 cesg_vehicle_violation
2. 无表 → 按 CESG 同结构建表，并把本地全部记录灌入
3. 有表 → 仅 upsert 本条新增记录
失败不阻断本地入库（best-effort）。
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from threading import Lock
from typing import Any

import pymysql
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import OrgCompany, VehicleViolation
from app.violation_alert_cache import push_violation_alert, violation_alert_payload

logger = logging.getLogger(__name__)

# 808 侧对外安全报警镜像表（与 CESG vehicle_violation 同结构）
TABLE_NAME = "cesg_vehicle_violation"

_SYNC_LOCK = Lock()
_MYSQL_DOWN_UNTIL = 0.0
_MYSQL_DOWN_COOLDOWN_SEC = 60.0

# 与 VehicleViolation 对齐的列（顺序固定，供建表/写入共用）
_COLUMNS: list[tuple[str, str]] = [
    ("id", "INT NOT NULL"),
    ("biz_no", "VARCHAR(32) NOT NULL"),
    ("external_alarm_id", "VARCHAR(128) NULL"),
    ("terminal_id", "VARCHAR(32) NOT NULL"),
    ("vehicle_id", "INT NULL"),
    ("plate_no", "VARCHAR(16) NULL"),
    ("company_id", "INT NULL"),
    ("company_name", "VARCHAR(128) NULL"),
    ("violation_type_code", "INT NULL"),
    ("violation_type_name", "VARCHAR(64) NULL"),
    ("risk_level", "VARCHAR(8) NOT NULL DEFAULT 'low'"),
    ("violation_time", "DATETIME NOT NULL"),
    ("lat", "DOUBLE NULL"),
    ("lng", "DOUBLE NULL"),
    ("address", "VARCHAR(512) NULL"),
    ("source", "VARCHAR(32) NOT NULL"),
    ("transparent_type", "INT NULL"),
    ("weather", "VARCHAR(32) NULL"),
    ("private_rule_name", "VARCHAR(200) NULL"),
    ("rule_category_name", "VARCHAR(128) NULL"),
    ("raw_preview", "MEDIUMTEXT NULL"),
    ("stream_snapshot_refs", "MEDIUMTEXT NULL"),
    ("ttx_evidence_refs", "MEDIUMTEXT NULL"),
    ("status", "VARCHAR(16) NOT NULL DEFAULT '待处理'"),
    ("pre_audit_kind", "VARCHAR(16) NULL"),
    ("ticket_appeal_remark", "MEDIUMTEXT NULL"),
    ("ticket_appeal_attachment_refs", "MEDIUMTEXT NULL"),
    ("handler_remark", "MEDIUMTEXT NULL"),
    ("handler_name", "VARCHAR(64) NULL"),
    ("handled_at", "DATETIME NULL"),
    ("auditor_name", "VARCHAR(64) NULL"),
    ("audited_at", "DATETIME NULL"),
    ("audit_reject_remark", "MEDIUMTEXT NULL"),
    ("appeal_reason", "MEDIUMTEXT NULL"),
    ("appeal_submitted_at", "DATETIME NULL"),
    ("appeal_status", "VARCHAR(16) NULL"),
    ("ai_queried", "TINYINT(1) NOT NULL DEFAULT 0"),
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
        logger.warning("808 MySQL 不可用，跳过安全报警同步: %s", exc)
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


def serialize_violation_row(row: VehicleViolation) -> dict[str, Any]:
    return {
        "id": int(row.id) if row.id is not None else None,
        "biz_no": row.biz_no,
        "external_alarm_id": row.external_alarm_id,
        "terminal_id": row.terminal_id or "",
        "vehicle_id": row.vehicle_id,
        "plate_no": row.plate_no or "",
        "company_id": row.company_id,
        "company_name": (getattr(row, "company_name", None) or "").strip() or None,
        "violation_type_code": row.violation_type_code,
        "violation_type_name": row.violation_type_name,
        "risk_level": row.risk_level or "mid",
        "violation_time": _dt(row.violation_time),
        "lat": row.lat,
        "lng": row.lng,
        "address": row.address,
        "source": row.source or "",
        "transparent_type": row.transparent_type,
        "weather": getattr(row, "weather", None),
        "private_rule_name": getattr(row, "private_rule_name", None),
        "rule_category_name": getattr(row, "rule_category_name", None),
        "raw_preview": row.raw_preview,
        "stream_snapshot_refs": row.stream_snapshot_refs,
        "ttx_evidence_refs": row.ttx_evidence_refs,
        "status": row.status or "待处理",
        "pre_audit_kind": row.pre_audit_kind,
        "ticket_appeal_remark": row.ticket_appeal_remark,
        "ticket_appeal_attachment_refs": row.ticket_appeal_attachment_refs,
        "handler_remark": row.handler_remark,
        "handler_name": row.handler_name,
        "handled_at": _dt(row.handled_at),
        "auditor_name": row.auditor_name,
        "audited_at": _dt(row.audited_at),
        "audit_reject_remark": row.audit_reject_remark,
        "appeal_reason": row.appeal_reason,
        "appeal_submitted_at": _dt(row.appeal_submitted_at),
        "appeal_status": row.appeal_status,
        "ai_queried": 1 if bool(getattr(row, "ai_queried", False)) else 0,
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
  UNIQUE KEY `uk_biz_no` (`biz_no`),
  UNIQUE KEY `uk_external_alarm_id` (`external_alarm_id`),
  KEY `ix_violation_time` (`violation_time`),
  KEY `ix_plate_no` (`plate_no`),
  KEY `ix_terminal_id` (`terminal_id`),
  KEY `ix_source` (`source`)
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
        # 已有表缺列时补齐（如 weather / private_rule_name / rule_category_name）
        cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{name}` {ctype}")
        logger.info("808 安全报警表补列: %s.%s", table, name)


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
        values.append(tuple(row.get(c) for c in _COLUMN_NAMES))
    if not values:
        return 0
    cur.executemany(sql, values)
    return len(values)


def _sync_locked(row_dict: dict[str, Any], all_rows: list[dict[str, Any]] | None) -> str:
    """
    返回：
    - skipped：MySQL 不可用
    - created_full：新建表并全量同步
    - upsert：表已存在，写入本条
    """
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
                    logger.info("808 安全报警表已创建并全量同步: table=%s rows=%s", table, n)
                    return "created_full"
                _ensure_columns(cur, table)
                count = _row_count(cur, table)
                if count == 0 and all_rows:
                    n = _upsert_rows(cur, table, all_rows)
                    conn.commit()
                    logger.info("808 安全报警表为空，已全量补同步: table=%s rows=%s", table, n)
                    return "created_full"
                n = _upsert_rows(cur, table, [row_dict])
                conn.commit()
                logger.debug("808 安全报警已同步: table=%s biz_no=%s n=%s", table, row_dict.get("biz_no"), n)
                return "upsert"
    except Exception as exc:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        _mysql_mark_down()
        logger.warning("同步安全报警到 808 失败: %s", exc)
        return "skipped"
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


async def lookup_company_name(
    db: AsyncSession,
    company_id: int | None,
    cache: dict[int, str | None] | None = None,
) -> str | None:
    """按 org_company.id 取公司名称；可选 cache 避免同批次重复查库。"""
    if not company_id:
        return None
    cid = int(company_id)
    if cache is not None and cid in cache:
        return cache[cid]
    name = await db.scalar(select(OrgCompany.name).where(OrgCompany.id == cid).limit(1))
    text = (name or "").strip()[:128] or None
    if cache is not None:
        cache[cid] = text
    return text


async def ensure_violation_company_name(db: AsyncSession, row: VehicleViolation) -> None:
    """若缺 company_name 则按 company_id 从 org_company 回填（写入本地行，供同步）。"""
    if row is None:
        return
    existing = (getattr(row, "company_name", None) or "").strip()
    if existing:
        return
    cid = row.company_id
    if not cid:
        return
    name = await db.scalar(select(OrgCompany.name).where(OrgCompany.id == int(cid)).limit(1))
    text = (name or "").strip()
    if text:
        row.company_name = text[:128]


async def notify_violation_created(db: AsyncSession, row: VehicleViolation) -> None:
    """本地入库后：弹窗缓存 + 同步 808（失败不影响主流程）。"""
    try:
        push_violation_alert(violation_alert_payload(row))
    except Exception as exc:  # noqa: BLE001
        logger.debug("push_violation_alert 失败: %s", exc)

    if row is None or row.id is None:
        return

    try:
        await ensure_violation_company_name(db, row)
        await db.flush()
    except Exception as exc:  # noqa: BLE001
        logger.debug("回填 company_name 失败: %s", exc)

    row_dict = serialize_violation_row(row)
    all_rows: list[dict[str, Any]] | None = None
    try:
        need_all = await asyncio.to_thread(_needs_full_sync_probe)
        if need_all:
            rows = (await db.execute(select(VehicleViolation))).scalars().all()
            # 全量同步前尽量补齐公司名
            missing_ids = {
                int(r.company_id)
                for r in rows
                if r.company_id and not (getattr(r, "company_name", None) or "").strip()
            }
            name_map: dict[int, str] = {}
            if missing_ids:
                for cid, cname in (
                    await db.execute(
                        select(OrgCompany.id, OrgCompany.name).where(OrgCompany.id.in_(missing_ids))
                    )
                ).all():
                    text = (cname or "").strip()
                    if text:
                        name_map[int(cid)] = text[:128]
            all_rows = []
            for r in rows:
                if r.id is None:
                    continue
                if not (getattr(r, "company_name", None) or "").strip() and r.company_id:
                    filled = name_map.get(int(r.company_id))
                    if filled:
                        r.company_name = filled
                all_rows.append(serialize_violation_row(r))
            try:
                await db.flush()
            except Exception:  # noqa: BLE001
                pass
        await asyncio.to_thread(_sync_locked, row_dict, all_rows)
    except Exception as exc:  # noqa: BLE001
        logger.warning("安全报警同步 808 异常: %s", exc)


def _needs_full_sync_probe() -> bool:
    """表不存在或行数为 0 → 需要全量。MySQL 不可用则 False（跳过）。"""
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


def probe_jt808_violation_table() -> dict[str, Any]:
    """供 OBD 状态页查看 808 镜像表是否存在及记录数。"""
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
            exists = _table_exists(cur, TABLE_NAME)
            out["table_exists"] = exists
            if exists:
                out["row_count"] = _row_count(cur, TABLE_NAME)
            else:
                out["row_count"] = 0
                out["error"] = "表尚未创建（下一条安全报警入库时会自动建表并全量同步）"
        return out
    except Exception as exc:  # noqa: BLE001
        out["error"] = str(exc)
        return out
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


async def jt808_violation_sync_status(db: AsyncSession | None = None) -> dict[str, Any]:
    """状态汇总：本地报警条数 + 808 镜像表条数。"""
    local_count = None
    if db is not None:
        try:
            from sqlalchemy import func

            local_count = int(await db.scalar(select(func.count()).select_from(VehicleViolation)) or 0)
        except Exception:  # noqa: BLE001
            local_count = None
    remote = await asyncio.to_thread(probe_jt808_violation_table)
    return {
        "local_vehicle_violation_count": local_count,
        "jt808_mirror": remote,
    }


def _backfill_remote_company_names(
    name_by_company_id: dict[int, str],
    rows_by_id: list[tuple[int, str]],
) -> dict[str, Any]:
    """回填 808 镜像表空的 company_name（不清空、不删行）。

    优先按本地记录 id 更新；再按 company_id 兜底补一轮。
    """
    out: dict[str, Any] = {
        "ok": False,
        "table": TABLE_NAME,
        "updated_by_id": 0,
        "updated_by_company_id": 0,
        "updated_rows": 0,
        "company_ids": len(name_by_company_id or {}),
        "local_named_rows": len(rows_by_id or []),
        "empty_before": None,
        "empty_after": None,
        "error": None,
        "skipped": False,
    }

    global _MYSQL_DOWN_UNTIL
    _MYSQL_DOWN_UNTIL = 0.0

    conn = _connect()
    if conn is None:
        out["error"] = "808 MySQL 不可用（请确认本机可连 jt808 库）"
        return out
    try:
        with _SYNC_LOCK:
            with conn.cursor() as cur:
                if not _table_exists(cur, TABLE_NAME):
                    out["ok"] = True
                    out["skipped"] = True
                    out["error"] = "镜像表尚未创建，跳过回填"
                    return out
                _ensure_columns(cur, TABLE_NAME)

                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM `{TABLE_NAME}`
                    WHERE `company_name` IS NULL OR TRIM(`company_name`) = ''
                    """
                )
                out["empty_before"] = int((cur.fetchone() or [0])[0] or 0)

                updated_by_id = 0
                if rows_by_id:
                    sql = (
                        f"UPDATE `{TABLE_NAME}` SET `company_name`=%s "
                        f"WHERE `id`=%s AND (`company_name` IS NULL OR TRIM(`company_name`)='')"
                    )
                    cur.executemany(sql, [(name, rid) for rid, name in rows_by_id])
                    updated_by_id = int(cur.rowcount or 0)

                updated_by_cid = 0
                for cid, cname in (name_by_company_id or {}).items():
                    cur.execute(
                        f"""
                        UPDATE `{TABLE_NAME}`
                        SET `company_name` = %s
                        WHERE `company_id` = %s
                          AND (`company_name` IS NULL OR TRIM(`company_name`) = '')
                        """,
                        (cname, int(cid)),
                    )
                    updated_by_cid += int(cur.rowcount or 0)

                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM `{TABLE_NAME}`
                    WHERE `company_name` IS NULL OR TRIM(`company_name`) = ''
                    """
                )
                out["empty_after"] = int((cur.fetchone() or [0])[0] or 0)
                conn.commit()
                out["ok"] = True
                out["updated_by_id"] = updated_by_id
                out["updated_by_company_id"] = updated_by_cid
                # MySQL executemany 的 rowcount 在部分版本不准，用前后空值差更直观
                before = out["empty_before"] or 0
                after = out["empty_after"] or 0
                out["updated_rows"] = max(0, before - after)
                return out
    except Exception as exc:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        out["error"] = str(exc)
        logger.warning("回填 808 安全报警 company_name 失败: %s", exc)
        return out
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


async def backfill_violation_company_names(db: AsyncSession) -> dict[str, Any]:
    """补齐本地 + 808 已有报警的公司名称（只填空，不删不截断）。"""
    name_by_id: dict[int, str] = {}
    for cid, cname in (await db.execute(select(OrgCompany.id, OrgCompany.name))).all():
        text = (cname or "").strip()[:128]
        if cid is not None and text:
            name_by_id[int(cid)] = text

    local_updated = 0
    rows = (await db.execute(select(VehicleViolation))).scalars().all()
    for row in rows:
        if (getattr(row, "company_name", None) or "").strip():
            continue
        cid = int(row.company_id) if row.company_id is not None else None
        if cid is None:
            continue
        name = name_by_id.get(cid)
        if not name:
            continue
        row.company_name = name
        local_updated += 1
    if local_updated:
        await db.flush()

    # 带名称的本地行，按 id 回写 808
    named_rows: list[tuple[int, str]] = []
    for row in rows:
        if row.id is None:
            continue
        name = (getattr(row, "company_name", None) or "").strip()
        if not name and row.company_id is not None:
            name = name_by_id.get(int(row.company_id)) or ""
        if name:
            named_rows.append((int(row.id), name[:128]))

    remote = await asyncio.to_thread(_backfill_remote_company_names, name_by_id, named_rows)
    result = {
        "local_updated": local_updated,
        "local_named_rows": len(named_rows),
        "org_company_count": len(name_by_id),
        "remote": remote,
    }
    # 无论成功/失败/0 条，都打一条醒目日志，方便在 backend.log 检索
    logger.info(
        "[company_name回填] 本地补写=%s, 可同步行=%s, 组织数=%s, "
        "808成功=%s, 808补齐=%s(空值 %s→%s), by_id=%s, by_company_id=%s, err=%s",
        local_updated,
        len(named_rows),
        len(name_by_id),
        remote.get("ok"),
        remote.get("updated_rows"),
        remote.get("empty_before"),
        remote.get("empty_after"),
        remote.get("updated_by_id"),
        remote.get("updated_by_company_id"),
        remote.get("error"),
    )
    return result


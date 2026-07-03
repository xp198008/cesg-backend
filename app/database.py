"""数据库连接与会话（异步 SQLAlchemy + SQLite）。"""
from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

logger = logging.getLogger(__name__)

_BACKEND_ROOT = Path(__file__).resolve().parent.parent


def _resolved_database_url() -> str:
    """SQLite 相对路径锚定到 backend 根目录，避免不同 cwd 读到不同库文件。"""
    url = (settings.database_url or "").strip()
    prefix = "sqlite+aiosqlite:///"
    if prefix in url:
        rest = url.split(prefix, 1)[1]
        qmark = rest.find("?")
        path_only = rest[:qmark] if qmark >= 0 else rest
        query = rest[qmark:] if qmark >= 0 else ""
        p = Path(path_only)
        if not p.is_absolute():
            p = (_BACKEND_ROOT / path_only).resolve()
        return f"{prefix}{p.as_posix()}{query}"
    return url


DATABASE_URL = _resolved_database_url()

_engine_kw: dict = {}
if "sqlite" in DATABASE_URL:
    db_path = DATABASE_URL.replace("sqlite+aiosqlite:///", "").split("?", 1)[0]
    try:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:  # noqa: BLE001
        logger.warning("SQLite 数据目录创建失败: %s", e)
    _engine_kw["connect_args"] = {"timeout": 60.0}

engine = create_async_engine(DATABASE_URL, echo=False, **_engine_kw)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


class Base(DeclarativeBase):
    pass


async def init_models() -> None:
    """确保所需表存在（已有库会跳过；空库会建表）。"""
    import app.models  # noqa: F401  注册所有模型

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        if "sqlite" in DATABASE_URL:
            cols = await conn.exec_driver_sql("PRAGMA table_info(sys_user)")
            names = {row[1] for row in cols.fetchall()}
            if names:
                if "valid_until" not in names:
                    await conn.exec_driver_sql("ALTER TABLE sys_user ADD COLUMN valid_until DATE")
                if "single_login" not in names:
                    await conn.exec_driver_sql("ALTER TABLE sys_user ADD COLUMN single_login BOOLEAN DEFAULT 0")
                if "login_session_token" not in names:
                    await conn.exec_driver_sql("ALTER TABLE sys_user ADD COLUMN login_session_token VARCHAR(64)")
                if "identity" not in names:
                    await conn.exec_driver_sql("ALTER TABLE sys_user ADD COLUMN identity VARCHAR(64)")
                if "phone" not in names:
                    await conn.exec_driver_sql("ALTER TABLE sys_user ADD COLUMN phone VARCHAR(32)")
            cols = await conn.exec_driver_sql("PRAGMA table_info(map_rule_category)")
            names = {row[1] for row in cols.fetchall()}
            if "weather_types" not in names:
                await conn.exec_driver_sql("ALTER TABLE map_rule_category ADD COLUMN weather_types JSON")
            if "weather_speed_limits" not in names:
                await conn.exec_driver_sql("ALTER TABLE map_rule_category ADD COLUMN weather_speed_limits JSON")
            cols = await conn.exec_driver_sql("PRAGMA table_info(private_map_rule)")
            names = {row[1] for row in cols.fetchall()}
            if "category_ids" not in names:
                await conn.exec_driver_sql("ALTER TABLE private_map_rule ADD COLUMN category_ids JSON")
            cols = await conn.exec_driver_sql("PRAGMA table_info(vehicle_violation)")
            names = {row[1] for row in cols.fetchall()}
            if names:
                if "external_alarm_id" not in names:
                    await conn.exec_driver_sql("ALTER TABLE vehicle_violation ADD COLUMN external_alarm_id VARCHAR(128)")
                    await conn.exec_driver_sql(
                        "CREATE UNIQUE INDEX IF NOT EXISTS ix_vehicle_violation_external_alarm_id "
                        "ON vehicle_violation(external_alarm_id)"
                    )
                if "ttx_evidence_refs" not in names:
                    await conn.exec_driver_sql("ALTER TABLE vehicle_violation ADD COLUMN ttx_evidence_refs TEXT")
                if "stream_snapshot_refs" not in names:
                    await conn.exec_driver_sql("ALTER TABLE vehicle_violation ADD COLUMN stream_snapshot_refs TEXT")
                if "pre_audit_kind" not in names:
                    await conn.exec_driver_sql("ALTER TABLE vehicle_violation ADD COLUMN pre_audit_kind VARCHAR(16)")
                if "ticket_appeal_attachment_refs" not in names:
                    await conn.exec_driver_sql("ALTER TABLE vehicle_violation ADD COLUMN ticket_appeal_attachment_refs TEXT")
                if "ai_queried" not in names:
                    await conn.exec_driver_sql("ALTER TABLE vehicle_violation ADD COLUMN ai_queried BOOLEAN DEFAULT 0")
            cols = await conn.exec_driver_sql("PRAGMA table_info(vehicle_location)")
            names = {row[1] for row in cols.fetchall()}
            if names and "source" not in names:
                await conn.exec_driver_sql("ALTER TABLE vehicle_location ADD COLUMN source VARCHAR(32) DEFAULT 'jt808_openapi'")
            cols = await conn.exec_driver_sql("PRAGMA table_info(violation_ticket)")
            names = {row[1] for row in cols.fetchall()}
            if names and "created_by_name" not in names:
                await conn.exec_driver_sql("ALTER TABLE violation_ticket ADD COLUMN created_by_name VARCHAR(64)")
            cols = await conn.exec_driver_sql("PRAGMA table_info(vehicle)")
            names = {row[1] for row in cols.fetchall()}
            if names:
                for col_name, col_type in (
                    ("vehicle_category", "VARCHAR(16)"),
                    ("driver_name", "VARCHAR(64)"),
                    ("engine_displacement", "VARCHAR(32)"),
                    ("fuel_tank_capacity", "VARCHAR(32)"),
                    ("battery_capacity", "VARCHAR(32)"),
                    ("range_mileage", "VARCHAR(32)"),
                    ("battery_no", "VARCHAR(64)"),
                    ("motor_no", "VARCHAR(64)"),
                    ("mileage_offset", "NUMERIC(10, 2)"),
                ):
                    if col_name not in names:
                        await conn.exec_driver_sql(f"ALTER TABLE vehicle ADD COLUMN {col_name} {col_type}")
            cols = await conn.exec_driver_sql("PRAGMA table_info(vehicle_device)")
            names = {row[1] for row in cols.fetchall()}
            if names and "channels" not in names:
                await conn.exec_driver_sql("ALTER TABLE vehicle_device ADD COLUMN channels JSON")
            cols = await conn.exec_driver_sql("PRAGMA table_info(driver)")
            names = {row[1] for row in cols.fetchall()}
            if names:
                for col_name, col_type in (
                    ("certificate_code", "VARCHAR(64)"),
                    ("entry_date", "DATE"),
                    ("license_issue_date", "DATE"),
                    ("driver_type", "VARCHAR(16)"),
                    ("license_expiry", "VARCHAR(32)"),
                    ("drive_hours", "INTEGER"),
                    ("drive_mileage", "INTEGER"),
                    ("score", "INTEGER"),
                    ("native_place", "VARCHAR(128)"),
                    ("avatar_url", "VARCHAR(256)"),
                ):
                    if col_name not in names:
                        await conn.exec_driver_sql(f"ALTER TABLE driver ADD COLUMN {col_name} {col_type}")
            cols = await conn.exec_driver_sql("PRAGMA table_info(user_login_log)")
            names = {row[1] for row in cols.fetchall()}
            if names:
                for col_name, col_type in (
                    ("user_id", "INTEGER"),
                    ("real_name", "VARCHAR(64)"),
                    ("org_id", "INTEGER"),
                    ("org_name", "VARCHAR(128)"),
                    ("role_id", "INTEGER"),
                    ("role_name", "VARCHAR(64)"),
                    ("logout_at", "DATETIME"),
                    ("login_method", "VARCHAR(32) DEFAULT 'web'"),
                    ("online_seconds", "INTEGER"),
                    ("last_heartbeat_at", "DATETIME"),
                ):
                    if col_name not in names:
                        await conn.exec_driver_sql(f"ALTER TABLE user_login_log ADD COLUMN {col_name} {col_type}")
            cols = await conn.exec_driver_sql("PRAGMA table_info(user_operation_log)")
            names = {row[1] for row in cols.fetchall()}
            if names:
                for col_name, col_type in (
                    ("user_id", "INTEGER"),
                    ("real_name", "VARCHAR(64)"),
                    ("org_id", "INTEGER"),
                    ("org_name", "VARCHAR(128)"),
                    ("module", "VARCHAR(64)"),
                    ("menu", "VARCHAR(64)"),
                    ("action", "VARCHAR(64)"),
                    ("operation_ip", "VARCHAR(64)"),
                    ("result", "VARCHAR(16) DEFAULT '成功'"),
                    ("vehicle", "VARCHAR(32)"),
                    ("plate_color", "VARCHAR(16)"),
                    ("device_no", "VARCHAR(64)"),
                    ("source", "VARCHAR(16) DEFAULT 'manual'"),
                ):
                    if col_name not in names:
                        await conn.exec_driver_sql(f"ALTER TABLE user_operation_log ADD COLUMN {col_name} {col_type}")


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

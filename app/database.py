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
            cols = await conn.exec_driver_sql("PRAGMA table_info(map_rule_category)")
            names = {row[1] for row in cols.fetchall()}
            if "weather_types" not in names:
                await conn.exec_driver_sql("ALTER TABLE map_rule_category ADD COLUMN weather_types JSON")
            if "weather_speed_limits" not in names:
                await conn.exec_driver_sql("ALTER TABLE map_rule_category ADD COLUMN weather_speed_limits JSON")


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

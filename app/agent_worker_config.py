"""Agent Worker 接口配置：库表维护，不读 .env。"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AgentWorkerConfig
from app.timeutil import china_now_naive

_DEFAULT_BASE_URL = "http://113.207.68.94:5002"
_DEFAULT_API_KEY = "bf601236-0e26-457c-9617-0a75eb3e5396"
_DEFAULT_COMPANY = "三峰城服"

_cache: dict[str, Any] | None = None


def _row_dict(row: AgentWorkerConfig | None) -> dict[str, Any]:
    if row is None:
        return {
            "enabled": False,
            "base_url": "",
            "api_key": "",
            "default_company": _DEFAULT_COMPANY,
            "timeout_seconds": 60,
            "video_timeout_seconds": 600,
            "remark": None,
            "ready": False,
            "ready_reason": "未配置 AI 接口",
            "updated_at": None,
        }
    base = (row.base_url or "").strip().rstrip("/")
    key = (row.api_key or "").strip()
    enabled = bool(row.enabled)
    ready = enabled and bool(base) and bool(key)
    if not enabled:
        reason = "AI 接口未启用"
    elif not base:
        reason = "未填写接口根地址"
    elif not key:
        reason = "未填写 API Key"
    else:
        reason = "就绪"
    return {
        "id": row.id,
        "provider": row.provider,
        "enabled": enabled,
        "base_url": row.base_url,
        "api_key": key,
        "default_company": (row.default_company or "").strip() or _DEFAULT_COMPANY,
        "timeout_seconds": int(row.timeout_seconds or 60),
        "video_timeout_seconds": int(row.video_timeout_seconds or 600),
        "remark": row.remark,
        "ready": ready,
        "ready_reason": reason,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _apply_cache(data: dict[str, Any]) -> dict[str, Any]:
    global _cache
    _cache = data
    return data


def cached_runtime() -> dict[str, Any]:
    return dict(_cache or {})


def cached_default_company() -> str:
    return str((_cache or {}).get("default_company") or _DEFAULT_COMPANY)


def config_out(row: AgentWorkerConfig | None, *, mask_secret: bool = False) -> dict[str, Any]:
    data = _row_dict(row)
    if mask_secret:
        secret = str(data.get("api_key") or "")
        if secret:
            data["api_key"] = (
                secret[:2] + "*" * max(0, len(secret) - 4) + secret[-2:] if len(secret) > 4 else "****"
            )
    return data


async def get_ai_worker_row(db: AsyncSession, provider: str = "agent") -> AgentWorkerConfig | None:
    return await db.scalar(
        select(AgentWorkerConfig).where(AgentWorkerConfig.provider == (provider or "agent")).limit(1)
    )


async def ensure_ai_worker_config(db: AsyncSession, provider: str = "agent") -> AgentWorkerConfig:
    """没有行则写入文档默认值，保证各处调用能读到同一套配置。"""
    row = await get_ai_worker_row(db, provider)
    if row is None:
        row = AgentWorkerConfig(
            provider=provider or "agent",
            enabled=True,
            base_url=_DEFAULT_BASE_URL,
            api_key=_DEFAULT_API_KEY,
            default_company=_DEFAULT_COMPANY,
            timeout_seconds=60,
            video_timeout_seconds=600,
        )
        db.add(row)
        await db.flush()
    row.updated_at = row.updated_at or china_now_naive()
    _apply_cache(_row_dict(row))
    return row


async def refresh_ai_worker_cache(db: AsyncSession | None = None) -> dict[str, Any]:
    if db is not None:
        row = await ensure_ai_worker_config(db)
        return _apply_cache(_row_dict(row))
    from app.database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        row = await ensure_ai_worker_config(session)
        await session.commit()
        return _apply_cache(_row_dict(row))


def _keep_existing_secret(incoming: str | None) -> bool:
    s = (incoming or "").strip()
    return (not s) or ("*" in s)


async def save_ai_worker_config(
    db: AsyncSession,
    *,
    provider: str = "agent",
    enabled: bool = True,
    base_url: str | None = None,
    api_key: str | None = None,
    default_company: str | None = None,
    timeout_seconds: int | None = None,
    video_timeout_seconds: int | None = None,
    remark: str | None = None,
) -> AgentWorkerConfig:
    row = await ensure_ai_worker_config(db, provider)
    row.enabled = bool(enabled)
    row.base_url = (base_url or "").strip().rstrip("/") or None
    if not _keep_existing_secret(api_key):
        row.api_key = (api_key or "").strip()
    row.default_company = (default_company or "").strip() or _DEFAULT_COMPANY
    if timeout_seconds is not None:
        row.timeout_seconds = max(5, min(600, int(timeout_seconds)))
    if video_timeout_seconds is not None:
        row.video_timeout_seconds = max(30, min(3600, int(video_timeout_seconds)))
    row.remark = (remark or "").strip() or None
    row.updated_at = china_now_naive()
    await db.flush()
    _apply_cache(_row_dict(row))
    return row

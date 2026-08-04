"""JT808 OpenAPI 服务账号凭据：密码以 CESG 库 sys_user.password_plain 为准，不读 .env 密码。"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal
from app.models import SysUser


def service_openapi_username() -> str:
    """后台任务（报警同步、车辆/分组同步、OBD 定位等）使用的 808 登录账号。"""
    return (settings.jt808_openapi_account or settings.jt808_admin_account or "admin").strip()


def is_service_openapi_user(username: str | None) -> bool:
    return (username or "").strip() == service_openapi_username()


async def load_service_password_plain(db: AsyncSession | None = None) -> str:
    """读取服务账号明文密码；改密/登录后写入库，所有 808 HTTP 接口据此登 808。"""
    account = service_openapi_username()
    if not account:
        raise RuntimeError("未配置 JT808 服务账号")

    async def _read(session: AsyncSession) -> str:
        user = await session.scalar(
            select(SysUser).where(SysUser.username == account, SysUser.is_active.is_(True)).limit(1)
        )
        if user is None:
            raise RuntimeError(f"CESG 未找到 808 服务账号「{account}」")
        pwd = (getattr(user, "password_plain", None) or "").strip()
        if not pwd:
            raise RuntimeError(
                f"账号「{account}」未存储明文密码，请在用户管理中修改密码或重新登录后再试"
            )
        return pwd

    if db is not None:
        return await _read(db)
    async with AsyncSessionLocal() as session:
        return await _read(session)


def invalidate_all_service_jt808_tokens() -> None:
    """服务账号改密/登录后清空各模块缓存 token，下一轮请求用库中新密码。"""
    from app.jt808_openapi_client import jt808_openapi_client

    jt808_openapi_client.invalidate_token()
    from app.jt808_group import invalidate_token as invalidate_group_token
    from app.jt808_vehicle import invalidate_token as invalidate_vehicle_token

    invalidate_vehicle_token()
    invalidate_group_token()


def invalidate_openapi_token_if_service_user(username: str | None) -> None:
    if is_service_openapi_user(username):
        invalidate_all_service_jt808_tokens()

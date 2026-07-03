"""CESG 业务后端入口（独立 FastAPI 服务，默认端口 8100）。

只负责"与设备无关的业务功能"：用户 / 角色 / 机构 / 车辆 / 司机，
并在增删改时 best-effort 同步基础档案到 808 平台。
设备 / 视频 / 实时 / 历史回放 / 808 控制由 808 平台负责，本服务不涉及。
"""
import asyncio
import logging
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv

    _env = Path(__file__).resolve().parent / ".env"
    if _env.exists():
        load_dotenv(_env)
except ImportError:
    pass

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import HTTPException
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.database import init_models
from app.jt808_alarm_sync import (
    cleanup_jt808_violations_unknown_type,
    cleanup_jt808_violations_without_evidence,
    cleanup_jt808_violations_without_vehicle,
    jt808_alarm_scheduler,
)
from app.obd_speed_monitor import obd_speed_scheduler
from app.routers import (
    api_ai,
    api_alarm_type,
    api_dashboard,
    api_device_fault,
    api_driver,
    api_fault_type,
    api_jt808_alarm_sync,
    api_manual_fault,
    api_map_rules,
    api_obd_speed,
    api_org,
    api_permission_menu,
    api_repair,
    api_role,
    api_shortcut,
    api_user,
    api_vehicle,
    api_vehicle_alloc,
    api_vehicle_type,
    api_violation,
    api_violation_ticket,
    api_violation_type,
    api_weather,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="CESG 业务后端", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_vehicle_type_icon_media_dir = Path(__file__).resolve().parent / "data" / "vehicle_type_icons"
_vehicle_type_icon_media_dir.mkdir(parents=True, exist_ok=True)


@app.get("/media/vehicle-type-icons/{filename}")
async def vehicle_type_icon_file(filename: str):
    suffix = Path(filename).suffix.lower()
    if Path(filename).name != filename or suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
        raise HTTPException(status_code=404, detail="图片不存在")
    target = _vehicle_type_icon_media_dir / filename
    if not target.exists():
        raise HTTPException(status_code=404, detail="图片不存在")
    return FileResponse(target)


_driver_avatar_media_dir = Path(__file__).resolve().parent / "data" / "driver_avatars"
_driver_avatar_media_dir.mkdir(parents=True, exist_ok=True)


@app.get("/media/driver-avatars/{filename}")
async def driver_avatar_file(filename: str):
    suffix = Path(filename).suffix.lower()
    if Path(filename).name != filename or suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
        raise HTTPException(status_code=404, detail="图片不存在")
    target = _driver_avatar_media_dir / filename
    if not target.exists():
        raise HTTPException(status_code=404, detail="图片不存在")
    return FileResponse(target)


app.include_router(api_user.router)
app.include_router(api_role.router)
app.include_router(api_org.router)
app.include_router(api_vehicle.router)
app.include_router(api_vehicle_type.router)
app.include_router(api_driver.router)
app.include_router(api_alarm_type.router)
app.include_router(api_fault_type.router)
app.include_router(api_jt808_alarm_sync.router)
app.include_router(api_map_rules.router)
app.include_router(api_obd_speed.router)
app.include_router(api_permission_menu.router)
app.include_router(api_vehicle_alloc.router)
app.include_router(api_violation.router)
app.include_router(api_violation_ticket.router)
app.include_router(api_violation_type.router)
app.include_router(api_manual_fault.router)
app.include_router(api_device_fault.router)
app.include_router(api_repair.router)
app.include_router(api_shortcut.router)
app.include_router(api_dashboard.router)
app.include_router(api_weather.router)
app.include_router(api_ai.router)

_ticket_appeal_media_dir = Path(__file__).resolve().parent / "data" / "ticket_appeal_attachments"
_ticket_appeal_media_dir.mkdir(parents=True, exist_ok=True)
app.mount(
    "/media/ticket-appeal-attachments",
    StaticFiles(directory=str(_ticket_appeal_media_dir)),
    name="ticket-appeal-attachments",
)

app.mount(
    "/media/vehicle-type-icons",
    StaticFiles(directory=str(_vehicle_type_icon_media_dir)),
    name="vehicle-type-icons",
)


async def _ensure_default_map_config() -> None:
    """库中无地图配置时补一条高德默认记录，避免地图接口管理页空白。"""
    from sqlalchemy import select

    from app.database import AsyncSessionLocal
    from app.models import MapApiConfig

    async with AsyncSessionLocal() as s:
        row = await s.scalar(select(MapApiConfig).where(MapApiConfig.provider == "amap").limit(1))
        if row:
            return
        s.add(
            MapApiConfig(
                provider="amap",
                default_zoom=12,
                default_center_lng=106.55156,
                default_center_lat=29.56301,
                remark="系统默认",
            )
        )
        await s.commit()


async def _ensure_default_admin() -> None:
    """库中无任何用户时补一条默认 admin（用户名 admin / 密码 123456）。"""
    import bcrypt
    from sqlalchemy import func, select

    from app.database import AsyncSessionLocal
    from app.models import OrgCompany, SysRole, SysUser

    async with AsyncSessionLocal() as s:
        n = await s.scalar(select(func.count()).select_from(SysUser))
        if n and n > 0:
            return
        company = await s.scalar(select(OrgCompany).order_by(OrgCompany.id).limit(1))
        role = await s.scalar(select(SysRole).order_by(SysRole.id).limit(1))
        if not company:
            company = OrgCompany(name="环卫集团", short_name="环卫集团")
            s.add(company)
            await s.flush()
            company.org_code = f"{company.id:04d}"
        if not role:
            role = SysRole(name="系统管理员", code="admin", remark="全部模块", is_global=True, permissions="[]")
            s.add(role)
            await s.flush()
        s.add(
            SysUser(
                username="admin",
                password_hash=bcrypt.hashpw(b"123456", bcrypt.gensalt()).decode("utf-8"),
                password_plain="123456",
                real_name="管理员",
                role_id=role.id,
                org_id=company.id,
                allow_pwd_edit=True,
                is_active=True,
            )
        )
        await s.commit()


@app.on_event("startup")
async def _startup() -> None:
    await init_models()
    from app.database import AsyncSessionLocal
    from app.user_online_daily import backfill_login_log_org_names, rebuild_daily_from_login_logs

    async with AsyncSessionLocal() as s:
        filled = await backfill_login_log_org_names(s)
        if filled:
            logger.info("已补全 %s 条登录明细的所属公司", filled)
        rebuilt = await rebuild_daily_from_login_logs(s)
        await s.commit()
        if rebuilt:
            logger.info("已重建 %s 条登录会话的用户按日在线记录", rebuilt)
    await cleanup_jt808_violations_without_evidence()
    await cleanup_jt808_violations_without_vehicle()
    deleted_unknown = await cleanup_jt808_violations_unknown_type()
    if deleted_unknown:
        logger.info("启动时已清理未知报警类型记录 %s 条", deleted_unknown)
    await _ensure_default_map_config()
    await _ensure_default_admin()
    await api_vehicle_type.ensure_default_vehicle_types()
    jt808_alarm_scheduler.start()
    obd_speed_scheduler.start()
    logger.info("CESG 业务后端已就绪：http://127.0.0.1:%s", settings.app_port)


@app.on_event("shutdown")
async def _shutdown() -> None:
    await jt808_alarm_scheduler.stop()
    await obd_speed_scheduler.stop()


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


@app.get("/")
async def root():
    return {"service": "CESG 业务后端", "ok": True}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.app_port,
        reload=False,
        reload_excludes=["**/data/**", "**/__pycache__/**", "**/*.pyc"],
        log_level="info",
    )

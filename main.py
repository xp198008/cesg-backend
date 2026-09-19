"""CESG 业务后端入口（独立 FastAPI 服务，默认端口 8100）。

只负责"与设备无关的业务功能"：用户 / 角色 / 机构 / 车辆 / 司机，
并在增删改时 best-effort 同步基础档案到 808 平台。
设备 / 视频 / 实时 / 历史回放 / 808 控制由 808 平台负责，本服务不涉及。
"""
import asyncio
import logging
import os
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

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from app.session_auth import SessionAuthMiddleware
from app.security import CSP_API, cors_origin_list, reject_oversized_paging, sanitize_validation_errors
from fastapi import HTTPException
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import DataError

from app.config import settings
from app.database import init_models
from app.scheduler_lock import should_run_schedulers
from app.jt808_alarm_sync import (
    cleanup_jt808_violations_unknown_type,
    cleanup_jt808_violations_without_vehicle,
    jt808_alarm_scheduler,
)
from app.obd_speed_monitor import obd_speed_scheduler
from app.shared_scheduler_flag import obd_speed_flag
from app.park_alarm_scheduler import park_alarm_scheduler
from app.redis_queue_consumer import redis_queue_scheduler
from app.vehicle_jt808_sync import vehicle_jt808_sync_scheduler
from app.org_jt808_sync import org_jt808_sync_scheduler
from app.violation_ai_assessment_scheduler import violation_ai_assessment_scheduler
from app.routers import (
    api_ai,
    api_alarm_type,
    api_dashboard,
    api_device_fault,
    api_driver,
    api_fault_type,
    api_jt808_alarm_sync,
    api_knowledge,
    api_manual_fault,
    api_map_grasp,
    api_map_rules,
    api_media,
    api_obd_fuel,
    api_obd_mileage,
    api_obd_speed,
    api_obd_anomaly,
    api_park_alarm_report,
    api_org,
    api_permission_menu,
    api_repair,
    api_risk_profile,
    api_road_type,
    api_role,
    api_route_plan,
    api_shortcut,
    api_sms,
    api_user,
    api_vehicle,
    api_vehicle_alloc,
    api_vehicle_type,
    api_vehicle_violation,
    api_video_preview,
    api_violation,
    api_violation_ai_assess,
    api_violation_ticket,
    api_violation_type,
    api_weather,
    api_jt808_gateway,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

_enable_docs = (os.getenv("CESG_ENABLE_DOCS") or "").strip() in {"1", "true", "yes"}
app = FastAPI(
    title="CESG 业务后端",
    version="1.0.0",
    docs_url="/docs" if _enable_docs else None,
    redoc_url="/redoc" if _enable_docs else None,
    openapi_url="/openapi.json" if _enable_docs else None,
    redirect_slashes=False,
)

# 先加会话校验（内侧），再加 CORS（外侧），这样 401 也能带跨域头；OPTIONS 由 CORS 直接放行。
app.add_middleware(SessionAuthMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origin_list(),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _no_store_api(request: Request, call_next):
    oversized = reject_oversized_paging(request)
    if oversized is not None:
        return oversized
    response = await call_next(request)
    path = request.url.path or ""
    if path.startswith("/api") or path.startswith("/internal") or path.startswith("/cmapi"):
        response.headers.setdefault("Cache-Control", "no-store, no-cache, must-revalidate")
        response.headers.setdefault("Pragma", "no-cache")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Content-Security-Policy", CSP_API)
    if "server" in response.headers:
        del response.headers["server"]
    return response


@app.exception_handler(RequestValidationError)
async def _validation_error_handler(_request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={"detail": sanitize_validation_errors(exc.errors())})


@app.exception_handler(DataError)
async def _data_error_handler(_request: Request, _exc: DataError):
    return JSONResponse(status_code=400, content={"detail": "参数超出允许范围"})


@app.exception_handler(OverflowError)
async def _overflow_handler(_request: Request, _exc: OverflowError):
    return JSONResponse(status_code=400, content={"detail": "参数超出允许范围"})


@app.exception_handler(Exception)
async def _unhandled_error_handler(_request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        raise exc
    logger.exception("未处理异常")
    return JSONResponse(status_code=500, content={"detail": "服务暂时不可用"})

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

_violation_snapshot_media_dir = Path(__file__).resolve().parent / "data" / "violation_snapshots"
_violation_snapshot_media_dir.mkdir(parents=True, exist_ok=True)


@app.get("/media/driver-avatars/{filename}")
async def driver_avatar_file(filename: str):
    suffix = Path(filename).suffix.lower()
    if Path(filename).name != filename or suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
        raise HTTPException(status_code=404, detail="图片不存在")
    target = _driver_avatar_media_dir / filename
    if not target.exists():
        raise HTTPException(status_code=404, detail="图片不存在")
    return FileResponse(target)


app.include_router(api_jt808_gateway.router)
app.include_router(api_user.router)
app.include_router(api_role.router)
app.include_router(api_org.router)
app.include_router(api_vehicle.router)
app.include_router(api_vehicle_type.router)
app.include_router(api_driver.router)
app.include_router(api_alarm_type.router)
app.include_router(api_fault_type.router)
app.include_router(api_road_type.router)
app.include_router(api_jt808_alarm_sync.router)
app.include_router(api_map_rules.router)
app.include_router(api_map_grasp.router)
app.include_router(api_sms.router)
app.include_router(api_obd_fuel.router)
app.include_router(api_obd_mileage.router)
app.include_router(api_obd_speed.router)
app.include_router(api_obd_anomaly.router)
app.include_router(api_park_alarm_report.router)
app.include_router(api_violation_ai_assess.router)
app.include_router(api_permission_menu.router)
app.include_router(api_vehicle_alloc.router)
app.include_router(api_violation.router)
app.include_router(api_vehicle_violation.router)
app.include_router(api_media.router)
app.include_router(api_violation_ticket.router)
app.include_router(api_violation_type.router)
app.include_router(api_manual_fault.router)
app.include_router(api_device_fault.router)
app.include_router(api_repair.router)
app.include_router(api_route_plan.router)
app.include_router(api_shortcut.router)
app.include_router(api_knowledge.router)
app.include_router(api_dashboard.router)
app.include_router(api_weather.router)
app.include_router(api_ai.router)
app.include_router(api_risk_profile.router)
app.include_router(api_video_preview.router)

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

app.mount(
    "/media/violation-snapshots",
    StaticFiles(directory=str(_violation_snapshot_media_dir)),
    name="violation-snapshots",
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
    """库中无任何用户时补一条默认 admin，口令随机生成，不写死弱口令。"""
    import bcrypt
    from sqlalchemy import func, select

    from app.database import AsyncSessionLocal
    from app.models import OrgCompany, SysRole, SysUser
    from app.password_policy import generate_login_password, require_strong_password
    from app.secret_box import encrypt_secret

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
        bootstrap_pwd = require_strong_password(generate_login_password(12))
        s.add(
            SysUser(
                username="admin",
                password_hash=bcrypt.hashpw(bootstrap_pwd.encode("utf-8"), bcrypt.gensalt()).decode("utf-8"),
                password_plain=encrypt_secret(bootstrap_pwd),
                real_name="管理员",
                role_id=role.id,
                org_id=company.id,
                allow_pwd_edit=True,
                is_active=True,
            )
        )
        await s.commit()


async def _background_address_backfill() -> None:
    """启动后连续多批补地址；错开高峰，避免和登录/列表抢写锁。"""
    await asyncio.sleep(60)
    from app.violation_address_backfill import (
        backfill_vehicle_location_addresses,
        backfill_violation_addresses,
    )

    try:
        for round_no in range(1, 26):
            v = await backfill_violation_addresses(limit=40)
            l = await backfill_vehicle_location_addresses(limit=20)
            if v == 0 and l == 0:
                logger.info("报警地址启动回填已完成（第 %s 轮无待补记录）", round_no)
                break
            logger.info("报警地址启动回填第 %s 轮：违章 %s 条，位置 %s 条", round_no, v, l)
            await asyncio.sleep(2.0)
    except Exception as exc:  # noqa: BLE001
        logger.warning("报警地址启动回填失败: %s", exc)


async def _background_startup_backfill() -> None:
    """启动只做轻量补齐。全量重建日汇总会长时间占 SQLite 写锁，导致登录和基础数据列表卡住。"""
    await asyncio.sleep(3)
    from app.database import AsyncSessionLocal
    from app.user_online_daily import backfill_login_log_org_names, finalize_stale_open_sessions
    from app.obd_speed_monitor import backfill_abnormal_obd_speed_false_alarms
    from app.violation_ai_assessment import backfill_insufficient_evidence_false_alarms
    from app.violation_risk_backfill import backfill_violation_risk_levels
    from app.routers.api_driver import backfill_driver_company_from_vehicles

    try:
        async with AsyncSessionLocal() as s:
            filled = await backfill_login_log_org_names(s)
            if filled:
                logger.info("已补全 %s 条登录明细的所属公司", filled)
            driver_filled = await backfill_driver_company_from_vehicles(s)
            if driver_filled:
                logger.info("已补全 %s 条司机的所属公司", driver_filled)
            stale = await finalize_stale_open_sessions(s)
            await backfill_violation_risk_levels(s)
            await s.commit()
            if stale:
                logger.info("启动已补全 %s 条过期未退出会话", stale)
    except Exception as exc:  # noqa: BLE001
        logger.warning("启动后台回填失败: %s", exc)

    await asyncio.sleep(15)
    try:
        total = 0
        before_id = None
        for _ in range(80):
            async with AsyncSessionLocal() as s:
                n, before_id, scanned = await backfill_insufficient_evidence_false_alarms(
                    s, limit=200, before_id=before_id
                )
                await s.commit()
            total += n
            if scanned <= 0:
                break
            await asyncio.sleep(1.0)
        if total:
            logger.info("启动回填：证据不足已按误报处理 %s 条", total)
    except Exception as exc:  # noqa: BLE001
        logger.warning("启动回填证据不足误报失败: %s", exc)

    try:
        total = 0
        before_id = None
        for _ in range(80):
            async with AsyncSessionLocal() as s:
                n, before_id, scanned = await backfill_abnormal_obd_speed_false_alarms(
                    s, limit=200, before_id=before_id
                )
                await s.commit()
            total += n
            if scanned <= 0:
                break
            await asyncio.sleep(1.0)
        if total:
            logger.info("启动回填：OBD 时速异常已按误报处理 %s 条", total)
    except Exception as exc:  # noqa: BLE001
        logger.warning("启动回填 OBD 时速异常误报失败: %s", exc)

    try:
        from datetime import datetime, timedelta

        from app.obd_mileage_daily import backfill_obd_mileage_range
        from app.timeutil import china_now_naive

        now = china_now_naive()
        start = (now - timedelta(days=14)).strftime("%Y%m%d")
        end = now.strftime("%Y%m%d")
        async with AsyncSessionLocal() as s:
            result = await backfill_obd_mileage_range(s, start, end)
        logger.info("启动回填 OBD 日里程 %s~%s wrote=%s", start, end, (result or {}).get("wrote"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("启动回填 OBD 日里程失败: %s", exc)


@app.on_event("startup")
async def _startup() -> None:
    await init_models()
    if not should_run_schedulers():
        try:
            from app.database import AsyncSessionLocal
            from app.agent_worker_config import ensure_ai_worker_config

            async with AsyncSessionLocal() as s:
                ai_row = await ensure_ai_worker_config(s)
                await s.commit()
            logger.info(
                "CESG HTTP worker 已就绪：http://127.0.0.1:%s（不跑后台调度，AI enabled=%s）",
                settings.app_port,
                bool(ai_row.enabled),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("HTTP worker 加载 AI 配置失败: %s", exc)
            logger.info("CESG HTTP worker 已就绪：http://127.0.0.1:%s（不跑后台调度）", settings.app_port)
        return
    try:
        from app.secret_box import migrate_legacy_plaintext_passwords

        n = await migrate_legacy_plaintext_passwords()
        if n:
            logger.info("已将 %s 条历史明文代登口令加密入库", n)
    except Exception as exc:  # noqa: BLE001
        logger.warning("历史明文口令加密迁移失败: %s", exc)
    from app.database import AsyncSessionLocal
    from app.agent_worker_config import ensure_ai_worker_config

    async with AsyncSessionLocal() as s:
        ai_row = await ensure_ai_worker_config(s)
        logger.info(
            "AI 接口配置已加载：enabled=%s base_url=%s",
            bool(ai_row.enabled),
            (ai_row.base_url or "").strip() or "—",
        )
    asyncio.create_task(_background_startup_backfill())
    asyncio.create_task(_background_address_backfill())
    # 不再启动时删除「无图片/视频证据」的 JT808 报警：证据常晚于报警到达，删掉会导致
    # 安全监控/安全管理只剩 OBD 等无需证据的来源。保留无车辆关联与未知类型清理。
    await cleanup_jt808_violations_without_vehicle()
    deleted_unknown = await cleanup_jt808_violations_unknown_type()
    if deleted_unknown:
        logger.info("启动时已清理未知报警类型记录 %s 条", deleted_unknown)
    await _ensure_default_map_config()
    await _ensure_default_admin()
    try:
        from app.amap_web_service_key import sync_web_service_key_from_jt808
        from app.database import AsyncSessionLocal

        async with AsyncSessionLocal() as s:
            # 仅库为空时从 808 补全，不覆盖地图接口管理页已保存的 Key
            key = await sync_web_service_key_from_jt808(
                s, force_refresh=False, only_if_empty=True,
            )
            await s.commit()
            if key:
                logger.info("Web 服务 Key：已确保 map_api_config.web_service_key 可用（来源 808/库）")
            else:
                logger.info("Web 服务 Key：库为空且 808 appkey1 未同步到，纠偏/逆地理将在调用时再尝试")
    except Exception as exc:  # noqa: BLE001
        logger.warning("启动同步 Web 服务 Key 失败: %s", exc)
    await api_vehicle_type.ensure_default_vehicle_types()
    await api_road_type.ensure_default_road_types()
    try:
        from app.permission_bootstrap import grant_road_type_permission

        await grant_road_type_permission()
    except Exception as exc:  # noqa: BLE001
        logger.warning("补发道路类型维护权限失败: %s", exc)
    jt808_alarm_scheduler.start()
    try:
        if obd_speed_flag.get_desired(default=True):
            obd_speed_scheduler.start()
        else:
            logger.info("OBD 时速调度未启动（运维页已停止）")
        obd_speed_scheduler.start_watch()
    except Exception as exc:  # noqa: BLE001
        logger.warning("OBD 时速调度未启用: %s", exc)
    try:
        from app.address_backfill_scheduler import address_backfill_scheduler

        address_backfill_scheduler.start()
    except Exception as exc:  # noqa: BLE001
        logger.warning("地址定时回填未启用: %s", exc)
    redis_queue_scheduler.start()
    try:
        park_alarm_scheduler.start()
    except Exception as exc:  # noqa: BLE001
        logger.warning("停车超限报警调度未启用: %s", exc)
    try:
        vehicle_jt808_sync_scheduler.start()
    except Exception as exc:  # noqa: BLE001
        logger.warning("车辆 808 同步调度未启用: %s", exc)
    try:
        org_jt808_sync_scheduler.start()
    except Exception as exc:  # noqa: BLE001
        logger.warning("组织 808 同步调度未启用: %s", exc)
    try:
        from app.ai_assess_control import get_desired

        want = get_desired(default=settings.violation_ai_assess_auto_enabled)
        if want:
            violation_ai_assessment_scheduler.start()
        else:
            logger.info("安全报警自动 AI 评估未启动（运维页已停止或 auto_enabled=0）")
        violation_ai_assessment_scheduler.start_watch()
    except Exception as exc:  # noqa: BLE001
        logger.warning("安全报警自动 AI 评估未启用: %s", exc)
    try:
        from app.amap_web_service_key import get_stored_web_service_key
        from app.database import AsyncSessionLocal
        from app.jt808_address import get_jt808_config, get_jt808_regeo_amap_key

        async with AsyncSessionLocal() as s:
            stored = await get_stored_web_service_key(s)
        jt808_key = await asyncio.to_thread(get_jt808_regeo_amap_key)
        if stored:
            logger.info("逆地理/纠偏 Key：使用 CESG 库 web_service_key")
        elif jt808_key:
            logger.info(
                "逆地理/纠偏 Key：库为空，808 appkey1 可用（type1=%s）",
                await asyncio.to_thread(get_jt808_config, "lingx.jt808.type1", "gaode"),
            )
        else:
            logger.info("逆地理/纠偏 Key：库与 808 appkey1 均未就绪")
    except Exception as exc:  # noqa: BLE001
        logger.warning("读取逆地理/纠偏 Key 状态失败: %s", exc)
    logger.info("CESG 业务后端已就绪：http://127.0.0.1:%s", settings.app_port)


@app.on_event("shutdown")
async def _shutdown() -> None:
    await jt808_alarm_scheduler.stop()
    await obd_speed_scheduler.stop_local()
    try:
        await violation_ai_assessment_scheduler.stop_local()
    except Exception:
        pass
    try:
        from app.address_backfill_scheduler import address_backfill_scheduler

        await address_backfill_scheduler.stop()
    except Exception:
        pass
    await redis_queue_scheduler.stop()
    try:
        await park_alarm_scheduler.stop()
    except Exception:
        pass
    try:
        await vehicle_jt808_sync_scheduler.stop()
    except Exception:
        pass
    try:
        await org_jt808_sync_scheduler.stop()
    except Exception:
        pass
    try:
        from app.database import DATABASE_URL, engine

        if "sqlite" in (DATABASE_URL or ""):
            try:
                async with engine.begin() as conn:
                    await conn.exec_driver_sql("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
        await engine.dispose()
    except Exception:
        pass


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


@app.get("/")
async def root():
    return {"service": "CESG 业务后端", "ok": True}


if __name__ == "__main__":
    import uvicorn

    workers = int(os.getenv("CESG_WEB_WORKERS") or ("1" if os.name == "nt" else "3"))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.app_port,
        reload=False,
        workers=max(1, workers),
        reload_excludes=["**/data/**", "**/__pycache__/**", "**/*.pyc"],
        log_level="info",
        server_header=False,
    )

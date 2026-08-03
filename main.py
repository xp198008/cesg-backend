"""CESG 涓氬姟鍚庣鍏ュ彛锛堢嫭绔?FastAPI 鏈嶅姟锛岄粯璁ょ鍙?8100锛夈€?
鍙礋璐?涓庤澶囨棤鍏崇殑涓氬姟鍔熻兘"锛氱敤鎴?/ 瑙掕壊 / 鏈烘瀯 / 杞﹁締 / 鍙告満锛?骞跺湪澧炲垹鏀规椂 best-effort 鍚屾鍩虹妗ｆ鍒?808 骞冲彴銆?璁惧 / 瑙嗛 / 瀹炴椂 / 鍘嗗彶鍥炴斁 / 808 鎺у埗鐢?808 骞冲彴璐熻矗锛屾湰鏈嶅姟涓嶆秹鍙娿€?"""
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
    cleanup_jt808_violations_without_vehicle,
    jt808_alarm_scheduler,
)
from app.obd_speed_monitor import obd_speed_scheduler
from app.redis_queue_consumer import redis_queue_scheduler
from app.routers import (
    api_ai,
    api_alarm_filter_rule,
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
    api_obd_speed,
    api_org,
    api_permission_menu,
    api_repair,
    api_risk_profile,
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

app = FastAPI(title="CESG 涓氬姟鍚庣", version="1.0.0")

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
        raise HTTPException(status_code=404, detail="鍥剧墖涓嶅瓨鍦?)
    target = _vehicle_type_icon_media_dir / filename
    if not target.exists():
        raise HTTPException(status_code=404, detail="鍥剧墖涓嶅瓨鍦?)
    return FileResponse(target)


_driver_avatar_media_dir = Path(__file__).resolve().parent / "data" / "driver_avatars"
_driver_avatar_media_dir.mkdir(parents=True, exist_ok=True)

_violation_snapshot_media_dir = Path(__file__).resolve().parent / "data" / "violation_snapshots"
_violation_snapshot_media_dir.mkdir(parents=True, exist_ok=True)


@app.get("/media/driver-avatars/{filename}")
async def driver_avatar_file(filename: str):
    suffix = Path(filename).suffix.lower()
    if Path(filename).name != filename or suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
        raise HTTPException(status_code=404, detail="鍥剧墖涓嶅瓨鍦?)
    target = _driver_avatar_media_dir / filename
    if not target.exists():
        raise HTTPException(status_code=404, detail="鍥剧墖涓嶅瓨鍦?)
    return FileResponse(target)


app.include_router(api_user.router)
app.include_router(api_role.router)
app.include_router(api_org.router)
app.include_router(api_vehicle.router)
app.include_router(api_vehicle_type.router)
app.include_router(api_driver.router)
app.include_router(api_alarm_type.router)
app.include_router(api_alarm_filter_rule.router)
app.include_router(api_fault_type.router)
app.include_router(api_jt808_alarm_sync.router)
app.include_router(api_map_rules.router)
app.include_router(api_map_grasp.router)
app.include_router(api_obd_speed.router)
app.include_router(api_permission_menu.router)
app.include_router(api_vehicle_alloc.router)
app.include_router(api_violation.router)
app.include_router(api_media.router)
app.include_router(api_violation_ticket.router)
app.include_router(api_violation_type.router)
app.include_router(api_manual_fault.router)
app.include_router(api_device_fault.router)
app.include_router(api_repair.router)
app.include_router(api_shortcut.router)
app.include_router(api_knowledge.router)
app.include_router(api_dashboard.router)
app.include_router(api_weather.router)
app.include_router(api_ai.router)
app.include_router(api_risk_profile.router)

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
    """搴撲腑鏃犲湴鍥鹃厤缃椂琛ヤ竴鏉￠珮寰烽粯璁よ褰曪紝閬垮厤鍦板浘鎺ュ彛绠＄悊椤电┖鐧姐€?""
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
                remark="绯荤粺榛樿",
            )
        )
        await s.commit()


async def _ensure_default_admin() -> None:
    """搴撲腑鏃犱换浣曠敤鎴锋椂琛ヤ竴鏉￠粯璁?admin锛堢敤鎴峰悕 admin / 瀵嗙爜 123456锛夈€?""
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
            company = OrgCompany(name="鐜崼闆嗗洟", short_name="鐜崼闆嗗洟")
            s.add(company)
            await s.flush()
            company.org_code = f"{company.id:04d}"
        if not role:
            role = SysRole(name="绯荤粺绠＄悊鍛?, code="admin", remark="鍏ㄩ儴妯″潡", is_global=True, permissions="[]")
            s.add(role)
            await s.flush()
        s.add(
            SysUser(
                username="admin",
                password_hash=bcrypt.hashpw(b"123456", bcrypt.gensalt()).decode("utf-8"),
                password_plain="123456",
                real_name="绠＄悊鍛?,
                role_id=role.id,
                org_id=company.id,
                allow_pwd_edit=True,
                is_active=True,
            )
        )
        await s.commit()


async def _background_address_backfill() -> None:
    """鍚姩鍚庤繛缁鎵硅ˉ鍦板潃锛屽敖蹇竻绌哄巻鍙茬┖鍦板潃銆?""
    await asyncio.sleep(5)
    from app.database import AsyncSessionLocal
    from app.violation_address_backfill import (
        backfill_vehicle_location_addresses,
        backfill_violation_addresses,
    )

    try:
        for round_no in range(1, 26):
            async with AsyncSessionLocal() as s:
                v = await backfill_violation_addresses(s, limit=40)
                l = await backfill_vehicle_location_addresses(s, limit=20)
                await s.commit()
            if v == 0 and l == 0:
                logger.info("鎶ヨ鍦板潃鍚姩鍥炲～宸插畬鎴愶紙绗?%s 杞棤寰呰ˉ璁板綍锛?, round_no)
                break
            logger.info("鎶ヨ鍦板潃鍚姩鍥炲～绗?%s 杞細杩濈珷 %s 鏉★紝浣嶇疆 %s 鏉?, round_no, v, l)
            await asyncio.sleep(0.5)
    except Exception as exc:  # noqa: BLE001
        logger.warning("鎶ヨ鍦板潃鍚姩鍥炲～澶辫触: %s", exc)


@app.on_event("startup")
async def _startup() -> None:
    await init_models()
    from app.database import AsyncSessionLocal
    from app.user_online_daily import backfill_login_log_org_names, rebuild_daily_from_login_logs

    async with AsyncSessionLocal() as s:
        filled = await backfill_login_log_org_names(s)
        if filled:
            logger.info("宸茶ˉ鍏?%s 鏉＄櫥褰曟槑缁嗙殑鎵€灞炲叕鍙?, filled)
        rebuilt = await rebuild_daily_from_login_logs(s)
        from app.violation_risk_backfill import backfill_violation_risk_levels

        risk_updated = await backfill_violation_risk_levels(s)
        await s.commit()
        if rebuilt:
            logger.info("宸查噸寤?%s 鏉＄櫥褰曚細璇濈殑鐢ㄦ埛鎸夋棩鍦ㄧ嚎璁板綍", rebuilt)
    asyncio.create_task(_background_address_backfill())
    # 涓嶅啀鍚姩鏃跺垹闄ゃ€屾棤鍥剧墖/瑙嗛璇佹嵁銆嶇殑 JT808 鎶ヨ锛氳瘉鎹父鏅氫簬鎶ヨ鍒拌揪锛屽垹鎺変細瀵艰嚧
    # 瀹夊叏鐩戞帶/瀹夊叏绠＄悊鍙墿 OBD 绛夋棤闇€璇佹嵁鐨勬潵婧愩€備繚鐣欐棤杞﹁締鍏宠仈涓庢湭鐭ョ被鍨嬫竻鐞嗐€?    await cleanup_jt808_violations_without_vehicle()
    deleted_unknown = await cleanup_jt808_violations_unknown_type()
    if deleted_unknown:
        logger.info("鍚姩鏃跺凡娓呯悊鏈煡鎶ヨ绫诲瀷璁板綍 %s 鏉?, deleted_unknown)
    await _ensure_default_map_config()
    await _ensure_default_admin()
    try:
        from app.amap_web_service_key import sync_web_service_key_from_jt808
        from app.database import AsyncSessionLocal

        async with AsyncSessionLocal() as s:
            key = await sync_web_service_key_from_jt808(s, force_refresh=False)
            await s.commit()
            if key:
                logger.info("Web 鏈嶅姟 Key锛氬凡纭繚 map_api_config.web_service_key 鍙敤锛堟潵婧?808/搴擄級")
            else:
                logger.info("Web 鏈嶅姟 Key锛氬簱涓虹┖涓?808 appkey1 鏈悓姝ュ埌锛岀籂鍋?閫嗗湴鐞嗗皢鍦ㄨ皟鐢ㄦ椂鍐嶅皾璇?)
    except Exception as exc:  # noqa: BLE001
        logger.warning("鍚姩鍚屾 Web 鏈嶅姟 Key 澶辫触: %s", exc)
    from app.permission_bootstrap import ensure_alarm_filter_rule_permission

    await ensure_alarm_filter_rule_permission()
    await api_vehicle_type.ensure_default_vehicle_types()
    jt808_alarm_scheduler.start()
    obd_speed_scheduler.start()
    try:
        from app.address_backfill_scheduler import address_backfill_scheduler

        address_backfill_scheduler.start()
    except Exception as exc:  # noqa: BLE001
        logger.warning("鍦板潃瀹氭椂鍥炲～鏈惎鐢? %s", exc)
    redis_queue_scheduler.start()
    try:
        from app.amap_web_service_key import get_stored_web_service_key
        from app.database import AsyncSessionLocal
        from app.jt808_address import get_jt808_config, get_jt808_regeo_amap_key

        async with AsyncSessionLocal() as s:
            stored = await get_stored_web_service_key(s)
        jt808_key = await asyncio.to_thread(get_jt808_regeo_amap_key)
        if stored:
            logger.info("閫嗗湴鐞?绾犲亸 Key锛氫娇鐢?CESG 搴?web_service_key")
        elif jt808_key:
            logger.info(
                "閫嗗湴鐞?绾犲亸 Key锛氬簱涓虹┖锛?08 appkey1 鍙敤锛坱ype1=%s锛?,
                await asyncio.to_thread(get_jt808_config, "lingx.jt808.type1", "gaode"),
            )
        else:
            logger.info("閫嗗湴鐞?绾犲亸 Key锛氬簱涓?808 appkey1 鍧囨湭灏辩华")
    except Exception as exc:  # noqa: BLE001
        logger.warning("璇诲彇閫嗗湴鐞?绾犲亸 Key 鐘舵€佸け璐? %s", exc)
    logger.info("CESG 涓氬姟鍚庣宸插氨缁細http://127.0.0.1:%s", settings.app_port)


@app.on_event("shutdown")
async def _shutdown() -> None:
    await jt808_alarm_scheduler.stop()
    await obd_speed_scheduler.stop()
    try:
        from app.address_backfill_scheduler import address_backfill_scheduler

        await address_backfill_scheduler.stop()
    except Exception:
        pass
    await redis_queue_scheduler.stop()


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


@app.get("/")
async def root():
    return {"service": "CESG 涓氬姟鍚庣", "ok": True}


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

"""新 JT808 OpenAPI 主动安全报警同步调度。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from app.timeutil import china_now_naive
from typing import Any
from app.media_url import extract_adas_relative_path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal
from app.alarm_type_gate import evaluate_alarm_type_ingest, log_alarm_type_gate
from app.jt808_openapi_client import Jt808OpenApiError, jt808_openapi_client
from app.jt808_openapi_credentials import service_openapi_username
from app.jt808_violation_sync import lookup_company_name, notify_violation_created
from app.models import Jt808AlarmSyncState, Vehicle, VehicleDevice, VehicleLocation, VehicleViolation, ViolationTicket
from app.plate_util import norm_plate
from app.amap_regeo import resolve_address_wgs84
from app.violation_filters import is_unknown_violation_type_name

logger = logging.getLogger(__name__)

_SOURCE_ADAS = "jt808_adas"
_SOURCE_DSM = "jt808_dsm"  # 历史库内来源标记；同步已不再调 1209
_SOURCE_LOCATION = "jt808_location"
# 808 平台约定：主动安全仅调 apicode 1208（已含原 DSM），不再调 1209。
_SYNC_ALARM_SOURCES = (_SOURCE_ADAS,)

_ADAS_ALARM_NAMES = {
    1: "前向碰撞报警",
    2: "车道偏离报警",
    3: "车距过近报警",
    4: "行人碰撞报警",
    5: "频繁变道报警",
    6: "道路标识超限报警",
    7: "障碍物报警",
    16: "道路标志识别事件",
    17: "主动抓拍事件",
    18: "前方拥堵报警",
}

_DSM_ALARM_NAMES = {
    1: "疲劳驾驶报警",
    2: "接打电话报警",
    3: "抽烟报警",
    4: "分神驾驶报警",
    5: "驾驶员异常报警",
    6: "双手脱离方向盘报警",
    7: "驾驶员行为监测功能失效报警",
    15: "未系安全带报警",
    16: "自动抓拍事件",
    17: "驾驶员变更事件",
    18: "驾驶员身份识别事件",
    21: "遮挡摄像头失效报警",
    22: "喝水报警",
}

# BSD 盲区监测（808 字典 BSD_BJLX）——不分级
_BSD_ALARM_NAMES = {
    1: "后方接近报警",
    2: "左侧后方接近报警",
    3: "右侧后方接近报警",
    81: "后方接近预警",
    82: "左侧后方接近预警",
    83: "右侧后方接近预警",
    97: "后方接近提示事件",
    98: "左侧后方提示事件",
    99: "右侧后方提示事件",
}

_BSD_TYPE_NAMES = frozenset(_BSD_ALARM_NAMES.values())
_LEVEL_SUFFIX = {1: "一级", 2: "二级", 3: "三级"}
_LEVEL_STRIP_RE = re.compile(r"(一级|二级|三级|1级|2级|3级)$")
_ALARM_LEVEL_LABELS = ("一级", "二级", "三级")


def _strip_alarm_level_suffix(name: str) -> str:
    return _LEVEL_STRIP_RE.sub("", (name or "").strip()).strip()


def _is_bsd_type_name(name: str) -> bool:
    base = _strip_alarm_level_suffix(name)
    return base in _BSD_TYPE_NAMES


def _type_supports_levels(base_name: str) -> bool:
    """ADAS/DSM 的「报警」分一/二/三级；BSD 与「事件/预警」不分级（与 808 弹窗配置一致）。"""
    base = _strip_alarm_level_suffix(base_name)
    if not base:
        return False
    if base in _BSD_TYPE_NAMES:
        return False
    if base.endswith("事件") or base.endswith("预警"):
        return False
    return base.endswith("报警")


def jt808_alarm_type_base_catalog() -> list[str]:
    """808 主动安全报警基础类型名（ADAS/DSM/BSD 去重，不含级别后缀）。"""
    seen: set[str] = set()
    names: list[str] = []
    for mapping in (_ADAS_ALARM_NAMES, _DSM_ALARM_NAMES, _BSD_ALARM_NAMES):
        for name in mapping.values():
            text = str(name or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            names.append(text)
    return names


def jt808_alarm_type_catalog() -> list[str]:
    """
    报警类型字典灌库目录（按 808 真实规则）：
    - ADAS/DSM 报警：每种拆成一级/二级/三级
    - BSD、事件、预警：只保留基础名，不带级别后缀
    - OBD 超速：单独一条（不分级），供超速监测入库闸门使用
    """
    names: list[str] = []
    for base in jt808_alarm_type_base_catalog():
        if _type_supports_levels(base):
            for suffix in _ALARM_LEVEL_LABELS:
                names.append(f"{base}{suffix}")
        else:
            names.append(base)
    if "OBD超速" not in names:
        names.append("OBD超速")
    return names


@dataclass
class SyncResult:
    source: str
    total: int = 0
    inserted: int = 0
    skipped_no_evidence: int = 0
    skipped_no_vehicle: int = 0
    skipped_unknown_type: int = 0
    skipped_filtered: int = 0  # 停用或不在报警类型字典
    skipped_interval: int = 0  # 最小间隔内同车同类型
    updated_positions: int = 0
    error: str | None = None


def _now() -> datetime:
    return china_now_naive()


def _fmt_api_time(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def _parse_api_time(raw: Any) -> datetime | None:
    s = str(raw or "").strip()
    if not s:
        return None
    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _as_int(raw: Any) -> int | None:
    try:
        if raw is None or raw == "":
            return None
        return int(raw)
    except (TypeError, ValueError):
        return None


def _as_float(raw: Any) -> float | None:
    try:
        if raw is None or raw == "":
            return None
        return float(raw)
    except (TypeError, ValueError):
        return None


# 接打电话 + 报警包 GPS 车速不超过该值（含）→ 停车场景，自动误报
_PHONE_CALL_STOPPED_MAX_KMH = 5.0
_PHONE_CALL_STOPPED_HANDLER = "系统自动判定"
_PHONE_CALL_NAME_HINTS = ("接打电话", "打电话报警", "使用手机")


def _is_phone_call_alarm(type_name: str, item: dict[str, Any] | None = None) -> bool:
    # 不看 bjlx：1208 里 ADAS/DSM 编号重叠（2=车道偏离 / 接打电话），以类型名为准
    texts = [_strip_alarm_level_suffix(type_name or "")]
    if item:
        texts.append(_strip_alarm_level_suffix(str(item.get("name") or "")))
    for text in texts:
        if any(hint in text for hint in _PHONE_CALL_NAME_HINTS):
            return True
        if text.startswith("打电话"):
            return True
    return False


def _alarm_packet_speed_kmh(item: dict[str, Any] | None) -> float | None:
    if not item:
        return None
    for key in ("speed", "velocity", "gps_speed", "gpsspeed", "sudu"):
        value = _as_float(item.get(key))
        if value is not None:
            return value
    return None


def _phone_call_stopped_false_alarm(type_name: str, item: dict[str, Any] | None) -> tuple[float, str] | None:
    """停车接打电话：返回 (车速, 备注)；不命中则 None。"""
    if not _is_phone_call_alarm(type_name, item):
        return None
    speed = _alarm_packet_speed_kmh(item)
    if speed is None or speed > _PHONE_CALL_STOPPED_MAX_KMH:
        return None
    speed_txt = str(int(speed)) if float(speed).is_integer() else str(round(speed, 1))
    remark = (
        f"接打电话时车辆处于停止（报警包车速 {speed_txt} km/h ≤ "
        f"{int(_PHONE_CALL_STOPPED_MAX_KMH)}），系统自动判定为误报"
    )
    return speed, remark


def _stable_biz_no(source: str, external_id: str, violation_time: datetime) -> str:
    digest = hashlib.md5(f"{source}:{external_id}".encode("utf-8")).hexdigest()[:8].upper()  # noqa: S324
    return f"WZ{violation_time.strftime('%Y%m%d%H%M%S')}{digest}"


def _external_alarm_id(source: str, item: dict[str, Any]) -> str:
    raw_id = str(item.get("id") or "").strip()
    if raw_id:
        return f"jt808:{source}:{raw_id}"
    parts = [
        str(item.get("car_id") or item.get("deviceId") or "").strip(),
        str(item.get("bjid") or "").strip(),
        str(item.get("bjlx") or "").strip(),
        str(item.get("gpstime") or item.get("ts") or "").strip(),
    ]
    digest = hashlib.md5(":".join(parts).encode("utf-8")).hexdigest()  # noqa: S324
    return f"jt808:{source}:{digest}"


def _resolve_base_alarm_name(source: str, item: dict[str, Any]) -> str:
    direct = str(item.get("name") or "").strip()
    code = _as_int(item.get("bjlx") if item.get("bjlx") is not None else item.get("bjid"))
    if direct:
        return _strip_alarm_level_suffix(direct) or direct
    if source == _SOURCE_ADAS:
        base = (
            _ADAS_ALARM_NAMES.get(code or -1)
            or _DSM_ALARM_NAMES.get(code or -1)
            or _BSD_ALARM_NAMES.get(code or -1)
        )
    else:
        base = _DSM_ALARM_NAMES.get(code or -1)
    if base:
        return base
    prefix = "主动安全报警"
    return f"{prefix}{code}" if code is not None else prefix


def _alarm_type_name(source: str, item: dict[str, Any]) -> str:
    """按 808 真实规则生成类型名，必须能与 alarm_type_dict 对上。"""
    base = _resolve_base_alarm_name(source, item)
    if not _type_supports_levels(base):
        return base
    level = _as_int(item.get("bjjb"))
    if level not in _LEVEL_SUFFIX:
        level = 1
    return f"{base}{_LEVEL_SUFFIX[level]}"


def _is_unknown_alarm_item(source: str, item: dict[str, Any]) -> bool:
    raw_name = str(item.get("name") or "").strip()
    if is_unknown_violation_type_name(raw_name):
        return True
    return is_unknown_violation_type_name(_alarm_type_name(source, item))


def _split_media_files(files: Any) -> dict[str, Any]:
    images: list[dict[str, Any]] = []
    videos: list[dict[str, Any]] = []
    attachments: list[dict[str, Any]] = []
    raw_files = files if isinstance(files, list) else []
    for idx, item in enumerate(raw_files):
        if not isinstance(item, dict):
            continue
        url = _jt808_media_url(item.get("path") or item.get("url"))
        if not url:
            continue
        name = str(item.get("name") or f"证据{idx + 1}").strip()
        ext = name.lower().rsplit(".", 1)[-1] if "." in name else ""
        media_type = _as_int(item.get("type"))
        row = {"url": url, "label": name, "name": name, "length": item.get("length")}
        if media_type == 2 or ext in {"mp4", "flv", "avi", "mov", "mkv"}:
            videos.append({**row, "wfsl": url})
        elif media_type in (0, 1) or ext in {"jpg", "jpeg", "png", "gif", "webp", "bmp"}:
            images.append(row)
        else:
            # type=3 的 .bin 是主动安全附件原始数据，不是浏览器可直接展示的图片。
            attachments.append(row)
    return {"images": images, "videos": videos, "attachments": attachments, "raw_files": raw_files}


def _has_image_or_video_evidence(media: Any) -> bool:
    """JT808 主动安全记录必须有可展示的图片或视频证据才进入业务库。"""
    if isinstance(media, str):
        try:
            media = json.loads(media)
        except json.JSONDecodeError:
            return False
    if not isinstance(media, dict):
        return False
    images = media.get("images")
    videos = media.get("videos")
    return bool((isinstance(images, list) and images) or (isinstance(videos, list) and videos))


def _jt808_media_url(raw: Any) -> str:
    url = str(raw or "").strip()
    if not url:
        return ""
    if re.match(r"^https?://", url, flags=re.I):
        # 存库时用相对路径，HTTPS 页面不再触发混合内容
        for prefix in (
            "http://113.207.68.96:8800",
            "http://127.0.0.1:8800",
            "https://113.207.68.96:8800",
        ):
            if url.startswith(prefix):
                path = url[len(prefix) :]
                return path if path.startswith("/") else f"/{path}"
        return url
    if url.startswith("/ADAS_FILE/"):
        return url
    return url


def _normalize_terminal_id(raw: Any) -> str:
    tid = str(raw or "").strip()
    if not tid:
        return ""
    if tid.isdigit():
        core = tid.lstrip("0") or "0"
        if len(core) <= 12:
            return core.zfill(12)
        return tid[-12:]
    return tid


def _looks_like_terminal_id(raw: Any) -> bool:
    tid = str(raw or "").strip()
    if not tid:
        return False
    if not tid.isdigit():
        return True
    return len(tid) >= 10


def _terminal_from_alarm_payload(item: dict[str, Any]) -> str:
    """优先从报警 payload 本身提取真实设备号，不把短 car_id 当终端号。"""
    for key in ("deviceId", "device_id", "tid", "terminal_id", "terminalId"):
        tid = _normalize_terminal_id(item.get(key))
        if tid:
            return tid
    files = item.get("files") if isinstance(item.get("files"), list) else []
    for f in files:
        if not isinstance(f, dict):
            continue
        path = str(f.get("path") or "")
        m = re.search(r"/ADAS_?FILE/\d{4}/\d{2}/\d{2}/([^/]+)/", path, flags=re.I)
        if m:
            tid = _normalize_terminal_id(m.group(1))
            if tid:
                return tid
    car_id = item.get("car_id")
    return _normalize_terminal_id(car_id) if _looks_like_terminal_id(car_id) else ""


async def _platform_car_row(car_id: Any, cache: dict[str, dict[str, str]]) -> dict[str, str]:
    cid = str(car_id or "").strip()
    if not cid:
        return {}
    if cid in cache:
        return cache[cid]
    cache[cid] = {}
    try:
        data = await jt808_openapi_client.list_vehicles(device_id=cid, page=1, rows=1)
    except Exception as exc:  # noqa: BLE001
        logger.warning("JT808 车辆ID反查失败 car_id=%s: %s", cid, exc)
        return cache[cid]
    rows = data.get("data") if isinstance(data.get("data"), list) else []
    if not rows or not isinstance(rows[0], dict):
        return cache[cid]
    row = rows[0]
    tid = _normalize_terminal_id(row.get("tid") or row.get("deviceId") or row.get("id"))
    carno = str(row.get("carno") or row.get("plate") or "").strip()
    cache[cid] = {"tid": tid if tid and tid != cid else "", "carno": carno}
    return cache[cid]


async def _terminal_by_platform_car_id(car_id: Any, cache: dict[str, dict[str, str]]) -> str:
    return (await _platform_car_row(car_id, cache)).get("tid") or ""


async def _plate_by_platform_car_id(car_id: Any, cache: dict[str, dict[str, str]]) -> str:
    return (await _platform_car_row(car_id, cache)).get("carno") or ""


async def _terminal_from_alarm_item(item: dict[str, Any], car_id_cache: dict[str, dict[str, str]]) -> str:
    terminal_id = _terminal_from_alarm_payload(item)
    if terminal_id:
        return terminal_id
    return await _terminal_by_platform_car_id(item.get("car_id"), car_id_cache)


def _terminal_variants(terminal_id: str) -> set[str]:
    tid = (terminal_id or "").strip()
    if not tid:
        return set()
    variants = {tid}
    if tid.isdigit():
        variants.add(tid.zfill(12))
        stripped = tid.lstrip("0") or "0"
        variants.add(stripped)
    return variants


async def _vehicle_by_terminal(db: AsyncSession, terminal_id: str) -> Vehicle | None:
    variants = _terminal_variants(terminal_id)
    if not variants:
        return None
    variant_list = list(variants)
    stmt = (
        select(Vehicle)
        .join(VehicleDevice, VehicleDevice.vehicle_id == Vehicle.id)
        .where(
            (VehicleDevice.device_no.in_(variant_list))
            | (VehicleDevice.device_sn.in_(variant_list))
            | (VehicleDevice.sim_no.in_(variant_list))
            | (VehicleDevice.actual_sim.in_(variant_list))
        )
        .limit(1)
    )
    vehicle = await db.scalar(stmt)
    if vehicle is not None:
        return vehicle
    loc = await db.scalar(
        select(VehicleLocation).where(VehicleLocation.terminal_id.in_(variant_list)).limit(1)
    )
    if loc is None:
        return None
    return await db.scalar(select(Vehicle).where(Vehicle.id == loc.vehicle_id).limit(1))


async def _vehicle_by_plate(db: AsyncSession, raw_plate: Any) -> Vehicle | None:
    plate = norm_plate(str(raw_plate or ""))
    if not plate:
        return None
    vehicle = await db.scalar(select(Vehicle).where(Vehicle.plate_no == plate).limit(1))
    if vehicle is not None:
        return vehicle
    return await db.scalar(select(Vehicle).where(func.upper(Vehicle.plate_no) == plate.upper()).limit(1))


async def _resolve_vehicle_for_alarm(
    db: AsyncSession,
    terminal_id: str,
    raw_plate: Any = None,
) -> Vehicle | None:
    vehicle = await _vehicle_by_terminal(db, terminal_id)
    if vehicle is not None:
        return vehicle
    return await _vehicle_by_plate(db, raw_plate)


async def _upsert_state(
    db: AsyncSession,
    source: str,
    start_at: datetime | None,
    end_at: datetime | None,
    result: SyncResult,
) -> None:
    row = await db.scalar(select(Jt808AlarmSyncState).where(Jt808AlarmSyncState.source == source).limit(1))
    if row is None:
        row = Jt808AlarmSyncState(source=source)
        db.add(row)
    row.last_window_start_at = start_at
    row.last_window_end_at = end_at
    row.last_success_at = _now() if result.error is None else row.last_success_at
    row.last_error = result.error
    row.last_total = result.total
    row.last_inserted = result.inserted


async def _last_window_start(db: AsyncSession, source: str) -> datetime:
    row = await db.scalar(select(Jt808AlarmSyncState).where(Jt808AlarmSyncState.source == source).limit(1))
    fallback = _now() - timedelta(minutes=max(1, int(settings.jt808_alarm_sync_lookback_minutes)))
    if not row or not row.last_window_end_at:
        return fallback
    return row.last_window_end_at - timedelta(seconds=30)


async def _sync_alarm_source(db: AsyncSession, source: str, start_at: datetime, end_at: datetime) -> SyncResult:
    result = SyncResult(source=source)
    page_size = max(1, int(settings.jt808_alarm_sync_page_size))
    max_pages = max(1, int(settings.jt808_alarm_sync_max_pages))
    if source != _SOURCE_ADAS:
        result.error = f"unsupported sync source: {source}"
        return result
    list_func = jt808_openapi_client.list_adas_alarms
    terminals: set[str] = set()
    car_id_cache: dict[str, dict[str, str]] = {}
    company_name_cache: dict[int, str | None] = {}
    try:
        for page in range(1, max_pages + 1):
            data = await list_func(_fmt_api_time(start_at), _fmt_api_time(end_at), page=page, rows=page_size)
            items = data.get("data") if isinstance(data.get("data"), list) else []
            result.total = max(result.total, int(data.get("total") or len(items) or 0))
            if not items:
                break
            for item in items:
                if not isinstance(item, dict):
                    continue
                ext_id = _external_alarm_id(source, item)
                exists = await db.scalar(select(VehicleViolation.id).where(VehicleViolation.external_alarm_id == ext_id).limit(1))
                if exists:
                    continue
                terminal_id = await _terminal_from_alarm_item(item, car_id_cache)
                plate = str(item.get("carno") or item.get("plate") or "").strip()
                if not plate and item.get("car_id"):
                    plate = await _plate_by_platform_car_id(item.get("car_id"), car_id_cache)
                vehicle = await _resolve_vehicle_for_alarm(db, terminal_id, plate)
                if vehicle is None:
                    result.skipped_no_vehicle += 1
                    continue
                if _is_unknown_alarm_item(source, item):
                    result.skipped_unknown_type += 1
                    continue
                alarm_time = _parse_api_time(item.get("gpstime") or item.get("ts")) or end_at
                type_name = _alarm_type_name(source, item)
                gate = await evaluate_alarm_type_ingest(
                    db,
                    type_name=type_name,
                    vehicle_id=vehicle.id,
                    alarm_time=alarm_time,
                )
                if not gate.get("allow"):
                    reason = str(gate.get("reason") or "filtered")
                    log_alarm_type_gate(
                        source=source,
                        external_id=ext_id,
                        alarm_type_name=type_name,
                        reason=reason,
                        plate=plate,
                        interval_minutes=getattr(gate.get("alarm_type"), "min_interval_minutes", None),
                    )
                    if reason == "interval":
                        result.skipped_interval += 1
                    else:
                        result.skipped_filtered += 1
                    continue
                # 808 主动安全：无图片/视频证据一律不入库（OBD 超速走独立通道，不受此限制）。
                media = _split_media_files(item.get("files"))
                if not _has_image_or_video_evidence(media):
                    result.skipped_no_evidence += 1
                    continue
                lat = _as_float(item.get("lat"))
                lng = _as_float(item.get("lng"))
                address = await resolve_address_wgs84(
                    db, lat, lng, existing=str(item.get("address") or "")
                )
                company_name = await lookup_company_name(db, vehicle.company_id, company_name_cache)
                auto_false = _phone_call_stopped_false_alarm(type_name, item)
                row = VehicleViolation(
                    biz_no=_stable_biz_no(source, ext_id, alarm_time),
                    external_alarm_id=ext_id,
                    terminal_id=terminal_id,
                    vehicle_id=vehicle.id,
                    plate_no=vehicle.plate_no[:16],
                    company_id=vehicle.company_id,
                    company_name=company_name,
                    violation_type_code=_as_int(item.get("bjlx") if item.get("bjlx") is not None else item.get("bjid")),
                    violation_type_name=type_name,
                    risk_level=gate.get("risk_level") or "mid",
                    violation_time=alarm_time,
                    lat=lat,
                    lng=lng,
                    address=address,
                    source=source,
                    transparent_type=_as_int(item.get("bjid")),
                    raw_preview=json.dumps(item, ensure_ascii=False)[:4000],
                    ttx_evidence_refs=json.dumps(media, ensure_ascii=False),
                    status="误报" if auto_false else "待处理",
                    pre_audit_kind="false_alarm" if auto_false else None,
                    handler_name=_PHONE_CALL_STOPPED_HANDLER if auto_false else None,
                    handler_remark=auto_false[1] if auto_false else None,
                    handled_at=_now() if auto_false else None,
                )
                db.add(row)
                await db.flush()
                if auto_false:
                    logger.info(
                        "接打电话停车自动误报: plate=%s speed=%s type=%s ext_id=%s",
                        vehicle.plate_no,
                        auto_false[0],
                        type_name,
                        ext_id,
                    )
                await notify_violation_created(db, row)
                result.inserted += 1
                if terminal_id:
                    terminals.add(terminal_id)
            if len(items) < page_size:
                break
        await _sync_positions(db, list(terminals), result)
    except Exception as exc:  # noqa: BLE001
        result.error = str(exc)
        logger.warning("JT808 %s 主动安全同步失败: %s", source, exc)
    await _upsert_state(db, source, start_at, end_at if result.error is None else None, result)
    return result


async def _sync_positions(db: AsyncSession, terminals: list[str], result: SyncResult) -> None:
    if not terminals:
        return
    for i in range(0, len(terminals), 50):
        data = await jt808_openapi_client.list_positions(terminals[i : i + 50])
        rows = data.get("data") if isinstance(data.get("data"), list) else []
        for item in rows:
            if not isinstance(item, dict):
                continue
            terminal_id = str(item.get("tid") or item.get("car_id") or "").strip()
            vehicle = await _vehicle_by_terminal(db, terminal_id)
            if vehicle is None:
                continue
            loc = await db.scalar(select(VehicleLocation).where(VehicleLocation.vehicle_id == vehicle.id).limit(1))
            if loc is None:
                loc = VehicleLocation(vehicle_id=vehicle.id, plate_no=vehicle.plate_no)
                db.add(loc)
            loc.plate_no = vehicle.plate_no
            loc.company_id = vehicle.company_id
            loc.terminal_id = terminal_id
            lat = _as_float(item.get("lat"))
            lng = _as_float(item.get("lng"))
            loc.lat = lat
            loc.lng = lng
            loc.speed = _as_float(item.get("speed"))
            loc.pos_time = _parse_api_time(item.get("gpstime") or item.get("systime"))
            loc.current_position = await resolve_address_wgs84(
                db, lat, lng, existing=str(item.get("address") or "")
            )
            loc.is_online = bool(_as_int(item.get("online")) == 1)
            loc.source = "jt808_openapi"
            result.updated_positions += 1
    await _upsert_state(db, _SOURCE_LOCATION, None, _now(), SyncResult(_SOURCE_LOCATION, updated_positions=result.updated_positions))


async def _delete_violations_with_tickets(db: AsyncSession, rows: list[VehicleViolation]) -> int:
    if not rows:
        return 0
    biz_nos = [row.biz_no for row in rows if row.biz_no]
    if biz_nos:
        tickets = (
            await db.execute(select(ViolationTicket).where(ViolationTicket.biz_no.in_(biz_nos)))
        ).scalars().all()
        for ticket in tickets:
            await db.delete(ticket)
    for row in rows:
        await db.delete(row)
    return len(rows)


async def cleanup_jt808_violations_without_vehicle() -> int:
    """删除 JT808 同步但无法关联 CESG 车辆的报警记录。"""
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(VehicleViolation).where(
                    VehicleViolation.source.ilike("jt808%"),
                    VehicleViolation.vehicle_id.is_(None),
                )
            )
        ).scalars().all()
        deleted = await _delete_violations_with_tickets(db, list(rows))
        if deleted:
            await db.commit()
            logger.info("已清理无法关联车辆的 JT808 报警记录 %s 条", deleted)
        return deleted


async def cleanup_jt808_violations_without_evidence() -> int:
    """删除历史 JT808 来源但没有图片/视频证据的报警记录。"""
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(VehicleViolation).where(VehicleViolation.source.ilike("jt808%"))
            )
        ).scalars().all()
        to_delete = [row for row in rows if not _has_image_or_video_evidence(row.ttx_evidence_refs)]
        deleted = await _delete_violations_with_tickets(db, to_delete)
        if deleted:
            await db.commit()
            logger.info("已清理无图片/视频证据的 JT808 报警记录 %s 条", deleted)
        return deleted


async def cleanup_jt808_violations_unknown_type() -> int:
    """删除 JT808 同步的未知报警类型记录（如「未知报警类型」）。"""
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(VehicleViolation).where(VehicleViolation.source.ilike("jt808%"))
            )
        ).scalars().all()
        to_delete = [row for row in rows if is_unknown_violation_type_name(row.violation_type_name)]
        deleted = await _delete_violations_with_tickets(db, to_delete)
        if deleted:
            await db.commit()
            logger.info("已清理未知报警类型的 JT808 报警记录 %s 条", deleted)
        return deleted


class Jt808AlarmScheduler:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_results: list[dict[str, Any]] = []
        self._last_error: str | None = None

    @property
    def running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    def status(self) -> dict[str, Any]:
        return {
            "enabled": bool(settings.jt808_alarm_sync_enabled),
            "configured": jt808_openapi_client.configured(),
            "base_url": (settings.jt808_openapi_base_url or "").strip(),
            "auth_mode": jt808_openapi_client.auth_mode(),
            "credential_password_source": "database",
            "service_account": service_openapi_username(),
            "running": self.running,
            "interval_seconds": settings.jt808_alarm_sync_interval_seconds,
            "lookback_minutes": settings.jt808_alarm_sync_lookback_minutes,
            "last_results": self._last_results,
            "last_error": self._last_error,
        }

    def start(self) -> None:
        if not settings.jt808_alarm_sync_enabled:
            logger.info("JT808 主动安全同步未启用")
            return
        if not jt808_openapi_client.configured():
            logger.warning("JT808 主动安全同步已启用，但 OpenAPI 账号配置不完整")
            return
        if self.running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="jt808-alarm-sync")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_once(self) -> list[SyncResult]:
        if not jt808_openapi_client.configured():
            raise Jt808OpenApiError("JT808 OpenAPI 配置不完整")
        async with AsyncSessionLocal() as db:
            end_at = _now()
            results: list[SyncResult] = []
            for source in _SYNC_ALARM_SOURCES:
                start_at = await _last_window_start(db, source)
                results.append(await _sync_alarm_source(db, source, start_at, end_at))
            await db.commit()
        self._last_results = [r.__dict__ for r in results]
        self._last_error = next((r.error for r in results if r.error), None)
        return results

    async def run_backfill(self, lookback_minutes: int = 120, reset_state: bool = False) -> list[SyncResult]:
        if not jt808_openapi_client.configured():
            raise Jt808OpenApiError("JT808 OpenAPI 配置不完整")
        async with AsyncSessionLocal() as db:
            if reset_state:
                for source in _SYNC_ALARM_SOURCES:
                    row = await db.scalar(select(Jt808AlarmSyncState).where(Jt808AlarmSyncState.source == source).limit(1))
                    if row is not None:
                        await db.delete(row)
                await db.flush()
            end_at = _now()
            start_at = end_at - timedelta(minutes=max(1, int(lookback_minutes)))
            results: list[SyncResult] = []
            for source in _SYNC_ALARM_SOURCES:
                results.append(await _sync_alarm_source(db, source, start_at, end_at))
            await db.commit()
        self._last_results = [r.__dict__ for r in results]
        self._last_error = next((r.error for r in results if r.error), None)
        return results

    async def _loop(self) -> None:
        logger.info("JT808 主动安全同步调度已启动")
        while self._running:
            try:
                await self.run_once()
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("JT808 主动安全同步调度执行失败: %s", exc)
            await asyncio.sleep(max(10, int(settings.jt808_alarm_sync_interval_seconds)))


jt808_alarm_scheduler = Jt808AlarmScheduler()


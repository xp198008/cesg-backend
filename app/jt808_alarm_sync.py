"""新 JT808 OpenAPI 主动安全报警同步调度。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal
from app.jt808_openapi_client import Jt808OpenApiError, jt808_openapi_client
from app.models import Jt808AlarmSyncState, Vehicle, VehicleDevice, VehicleLocation, VehicleViolation, ViolationTicket

logger = logging.getLogger(__name__)

_SOURCE_ADAS = "jt808_adas"
_SOURCE_DSM = "jt808_dsm"
_SOURCE_LOCATION = "jt808_location"

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


@dataclass
class SyncResult:
    source: str
    total: int = 0
    inserted: int = 0
    skipped_no_evidence: int = 0
    updated_positions: int = 0
    error: str | None = None


def _now() -> datetime:
    return datetime.now()


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


def _alarm_type_name(source: str, item: dict[str, Any]) -> str:
    code = _as_int(item.get("bjlx") if item.get("bjlx") is not None else item.get("bjid"))
    names = _ADAS_ALARM_NAMES if source == _SOURCE_ADAS else _DSM_ALARM_NAMES
    base = names.get(code or -1)
    if not base:
        prefix = "ADAS主动安全报警" if source == _SOURCE_ADAS else "DSM主动安全报警"
        base = f"{prefix}{code}" if code is not None else prefix
    level = _as_int(item.get("bjjb"))
    if level in (1, 2) and base.endswith("报警"):
        return f"{base}{level}级"
    return base


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
        return url
    if url.startswith("/ADAS_FILE/"):
        base = (settings.jt808_openapi_base_url or "").strip()
        parsed = urlsplit(base)
        origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else "https://www.gb35658.com"
        return origin + url
    return url


def _normalize_terminal_id(raw: Any) -> str:
    tid = str(raw or "").strip()
    if tid.isdigit() and 10 <= len(tid) < 12:
        return tid.zfill(12)
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
    for key in ("deviceId", "device_id", "tid", "terminal_id", "terminalId"):
        tid = _normalize_terminal_id(item.get(key))
        if tid:
            return tid
    car_id = item.get("car_id")
    return _normalize_terminal_id(car_id) if _looks_like_terminal_id(car_id) else ""


async def _terminal_by_platform_car_id(car_id: Any, cache: dict[str, str]) -> str:
    cid = str(car_id or "").strip()
    if not cid:
        return ""
    if cid in cache:
        return cache[cid]
    cache[cid] = ""
    try:
        data = await jt808_openapi_client.list_vehicles(device_id=cid, page=1, rows=1)
    except Exception as exc:  # noqa: BLE001
        logger.warning("JT808 车辆ID反查终端号失败 car_id=%s: %s", cid, exc)
        return ""
    rows = data.get("data") if isinstance(data.get("data"), list) else []
    if not rows or not isinstance(rows[0], dict):
        return ""
    tid = _normalize_terminal_id(rows[0].get("tid") or rows[0].get("deviceId") or rows[0].get("id"))
    if tid and tid != cid:
        cache[cid] = tid
    return cache[cid]


async def _terminal_from_alarm_item(item: dict[str, Any], car_id_cache: dict[str, str]) -> str:
    terminal_id = _terminal_from_alarm_payload(item)
    if terminal_id:
        return terminal_id
    return await _terminal_by_platform_car_id(item.get("car_id"), car_id_cache)


async def _vehicle_by_terminal(db: AsyncSession, terminal_id: str) -> Vehicle | None:
    tid = (terminal_id or "").strip()
    if not tid:
        return None
    variants = {tid}
    if tid.isdigit():
        variants.add(tid.zfill(12))
        variants.add(tid.lstrip("0") or "0")
    stmt = (
        select(Vehicle)
        .join(VehicleDevice, VehicleDevice.vehicle_id == Vehicle.id)
        .where(
            (VehicleDevice.device_no.in_(list(variants)))
            | (VehicleDevice.device_sn.in_(list(variants)))
            | (VehicleDevice.sim_no.in_(list(variants)))
        )
        .limit(1)
    )
    return await db.scalar(stmt)


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
    list_func = jt808_openapi_client.list_adas_alarms if source == _SOURCE_ADAS else jt808_openapi_client.list_dsm_alarms
    terminals: set[str] = set()
    car_id_cache: dict[str, str] = {}
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
                vehicle = await _vehicle_by_terminal(db, terminal_id)
                alarm_time = _parse_api_time(item.get("gpstime") or item.get("ts")) or end_at
                media = _split_media_files(item.get("files"))
                if not _has_image_or_video_evidence(media):
                    result.skipped_no_evidence += 1
                    continue
                row = VehicleViolation(
                    biz_no=_stable_biz_no(source, ext_id, alarm_time),
                    external_alarm_id=ext_id,
                    terminal_id=terminal_id,
                    vehicle_id=vehicle.id if vehicle else None,
                    plate_no=(vehicle.plate_no if vehicle else str(item.get("carno") or ""))[:16],
                    company_id=vehicle.company_id if vehicle else None,
                    violation_type_code=_as_int(item.get("bjlx") if item.get("bjlx") is not None else item.get("bjid")),
                    violation_type_name=_alarm_type_name(source, item),
                    violation_time=alarm_time,
                    lat=_as_float(item.get("lat")),
                    lng=_as_float(item.get("lng")),
                    address=str(item.get("address") or ""),
                    source=source,
                    transparent_type=_as_int(item.get("bjid")),
                    raw_preview=json.dumps(item, ensure_ascii=False)[:4000],
                    ttx_evidence_refs=json.dumps(media, ensure_ascii=False),
                    status="待处理",
                )
                db.add(row)
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
            loc.lat = _as_float(item.get("lat"))
            loc.lng = _as_float(item.get("lng"))
            loc.speed = _as_float(item.get("speed"))
            loc.pos_time = _parse_api_time(item.get("gpstime") or item.get("systime"))
            loc.current_position = str(item.get("address") or "")
            loc.is_online = bool(_as_int(item.get("online")) == 1)
            loc.source = "jt808_openapi"
            result.updated_positions += 1
    await _upsert_state(db, _SOURCE_LOCATION, None, _now(), SyncResult(_SOURCE_LOCATION, updated_positions=result.updated_positions))


async def cleanup_jt808_violations_without_evidence() -> int:
    """删除历史 JT808 来源但没有图片/视频证据的报警记录。"""
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(VehicleViolation).where(VehicleViolation.source.ilike("jt808%"))
            )
        ).scalars().all()
        to_delete = [row for row in rows if not _has_image_or_video_evidence(row.ttx_evidence_refs)]
        if not to_delete:
            return 0
        biz_nos = [row.biz_no for row in to_delete if row.biz_no]
        if biz_nos:
            tickets = (
                await db.execute(select(ViolationTicket).where(ViolationTicket.biz_no.in_(biz_nos)))
            ).scalars().all()
            for ticket in tickets:
                await db.delete(ticket)
        for row in to_delete:
            await db.delete(row)
        await db.commit()
        logger.info("已清理无图片/视频证据的 JT808 报警记录 %s 条", len(to_delete))
        return len(to_delete)


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
            for source in (_SOURCE_ADAS, _SOURCE_DSM):
                start_at = await _last_window_start(db, source)
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


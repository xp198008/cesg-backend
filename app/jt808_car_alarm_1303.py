"""地图超速 → JT808 apicode 1303（tgps_car_alarm）会话同步。

持续超速处理（对齐 808 规则引擎起止模型）：
1. **开始**：首次判定超速 → 打开内存会话，记录起点
2. **持续**：后续轮询仍超速 → 只刷新终点（不重复写 1303）
3. **结束**：降速/驶出限速区/OBD 消失 → 按起终点算 time/bjlc，一次写入 1303
4. **过长**：单次会话超过上限则强制落库并开启新会话（仍在超速时）

本地 vehicle_violation 入库节奏（报警类型间隔）与 1303 会话独立。
1303 HTTP 在后台推送，不阻塞多车超速判定主循环。
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Any

from app.config import settings
from app.jt808_openapi_client import Jt808OpenApiError, jt808_openapi_client

logger = logging.getLogger(__name__)

CAR_ALARM_NAME_SPEEDING = "超速报警"
_bg_tasks: set[asyncio.Task] = set()


def _ts_char14(dt: datetime | None) -> str:
    if dt is None:
        return ""
    try:
        return dt.strftime("%Y%m%d%H%M%S")
    except (ValueError, OSError):
        return ""


def _round_or_none(value: float | int | None, ndigits: int) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), ndigits)
    except (TypeError, ValueError):
        return None


@dataclass
class OverspeedTick:
    """本轮仍处于超速状态的采样点。"""

    vehicle_id: int
    device_id: str
    speed_kmh: float
    lat: float | None
    lng: float | None
    mileage_km: float | None
    at: datetime
    limit_kmh: float | None = None
    rule_id: int | None = None
    rule_name: str | None = None
    kind: str | None = None
    address: str | None = None
    plate_no: str | None = None
    company_id: int | None = None
    weather: str | None = None
    obd_snapshot: dict[str, Any] | None = None
    obd_type: str | None = None
    # 规则类别即时通知（开会话时下发 808 文字/语音）
    category_id: int | None = None
    instant_notify_enabled: bool = False
    broadcast_content: str | None = None


@dataclass
class OverspeedSession:
    vehicle_id: int
    device_id: str
    plate_no: str | None
    start_at: datetime
    start_speed: float
    start_lat: float | None
    start_lng: float | None
    start_mileage: float | None
    end_at: datetime
    end_speed: float
    end_lat: float | None
    end_lng: float | None
    end_mileage: float | None
    limit_kmh: float | None = None
    rule_id: int | None = None
    rule_name: str | None = None
    kind: str | None = None
    address: str | None = None
    company_id: int | None = None
    weather: str | None = None
    # 报警开始时刻 OBD 快照（供处理页「OBD详情」展示）
    obd_snapshot: dict[str, Any] | None = None
    obd_type: str | None = None
    peak_speed: float = 0.0
    tick_count: int = 1
    category_id: int | None = None
    instant_notify_enabled: bool = False
    broadcast_content: str | None = None

    def touch(self, tick: OverspeedTick) -> None:
        self.device_id = tick.device_id or self.device_id
        self.plate_no = tick.plate_no or self.plate_no
        self.end_at = tick.at
        self.end_speed = float(tick.speed_kmh)
        self.end_lat = tick.lat
        self.end_lng = tick.lng
        if tick.mileage_km is not None:
            self.end_mileage = float(tick.mileage_km)
        self.limit_kmh = tick.limit_kmh if tick.limit_kmh is not None else self.limit_kmh
        self.rule_id = tick.rule_id if tick.rule_id is not None else self.rule_id
        self.rule_name = tick.rule_name or self.rule_name
        self.kind = tick.kind or self.kind
        self.address = tick.address or self.address
        if tick.company_id is not None:
            self.company_id = tick.company_id
        self.weather = tick.weather or self.weather
        # 开始快照只在开会话时写入；过程中不覆盖，保证与 violation_time(开始)一致
        if self.obd_snapshot is None and tick.obd_snapshot:
            self.obd_snapshot = tick.obd_snapshot
            self.obd_type = tick.obd_type
        if tick.category_id is not None:
            self.category_id = tick.category_id
        if tick.instant_notify_enabled:
            self.instant_notify_enabled = True
            if tick.broadcast_content:
                self.broadcast_content = tick.broadcast_content
        self.peak_speed = max(self.peak_speed, float(tick.speed_kmh))
        self.tick_count += 1

    def external_alarm_id(self) -> str:
        """会话唯一键：本地违章与 1303 共用，保证 1:1。"""
        return f"obd_sess:{self.vehicle_id}:{_ts_char14(self.start_at) or '0'}"

    def duration_sec(self) -> int:
        delta = (self.end_at - self.start_at).total_seconds()
        return max(0, int(delta))

    def bjlc_km(self) -> float:
        if self.start_mileage is not None and self.end_mileage is not None:
            diff = float(self.end_mileage) - float(self.start_mileage)
            if diff >= 0:
                return round(diff, 3)
        # 里程缺失时按平均时速粗算
        sec = self.duration_sec()
        if sec <= 0:
            return 0.0
        avg = (float(self.start_speed) + float(self.end_speed)) / 2.0
        return round(max(0.0, avg * sec / 3600.0), 3)


_sessions: dict[int, OverspeedSession] = {}
_lock = Lock()


def open_session_count() -> int:
    with _lock:
        return len(_sessions)


def session_status_snapshot() -> list[dict[str, Any]]:
    with _lock:
        items = list(_sessions.values())
    return [
        {
            "vehicle_id": s.vehicle_id,
            "device_id": s.device_id,
            "plate_no": s.plate_no,
            "start_at": s.start_at.isoformat(sep=" ", timespec="seconds"),
            "end_at": s.end_at.isoformat(sep=" ", timespec="seconds"),
            "duration_sec": s.duration_sec(),
            "tick_count": s.tick_count,
            "peak_speed": s.peak_speed,
            "limit_kmh": s.limit_kmh,
            "rule_name": s.rule_name,
        }
        for s in items
    ]


def _new_session(tick: OverspeedTick) -> OverspeedSession:
    speed = float(tick.speed_kmh)
    mileage = float(tick.mileage_km) if tick.mileage_km is not None else None
    return OverspeedSession(
        vehicle_id=int(tick.vehicle_id),
        device_id=str(tick.device_id or "").strip(),
        plate_no=tick.plate_no,
        start_at=tick.at,
        start_speed=speed,
        start_lat=tick.lat,
        start_lng=tick.lng,
        start_mileage=mileage,
        end_at=tick.at,
        end_speed=speed,
        end_lat=tick.lat,
        end_lng=tick.lng,
        end_mileage=mileage,
        limit_kmh=tick.limit_kmh,
        rule_id=tick.rule_id,
        rule_name=tick.rule_name,
        kind=tick.kind,
        address=tick.address,
        company_id=tick.company_id,
        weather=tick.weather,
        obd_snapshot=tick.obd_snapshot,
        obd_type=tick.obd_type,
        peak_speed=speed,
        tick_count=1,
        category_id=tick.category_id,
        instant_notify_enabled=bool(tick.instant_notify_enabled),
        broadcast_content=(tick.broadcast_content or "").strip() or None,
    )


def build_session_alarm_payload(session: OverspeedSession) -> dict[str, Any]:
    gpstime = _ts_char14(session.start_at)
    end_gpstime = _ts_char14(session.end_at) or gpstime
    time_sec = session.duration_sec()
    bjlc = session.bjlc_km()
    remark_parts = ["地图超速"]
    if session.kind:
        remark_parts.append(str(session.kind))
    if session.limit_kmh is not None:
        remark_parts.append(f"限速{session.limit_kmh:g}km/h")
    if session.rule_name:
        remark_parts.append(str(session.rule_name).strip())
    remark_parts.append(f"持续{time_sec}s")
    remark_parts.append(f"峰值{session.peak_speed:g}km/h")
    if session.address:
        remark_parts.append(str(session.address).strip()[:80])
    remark = " ".join(p for p in remark_parts if p)[:255]

    return {
        "device_id": str(session.device_id or "").strip(),
        "name": CAR_ALARM_NAME_SPEEDING,
        "gpstime": gpstime,
        "speed": _round_or_none(session.start_speed, 2),
        "lat": session.start_lat,
        "lng": session.start_lng,
        "mileage": _round_or_none(session.start_mileage, 3),
        "time_sec": time_sec,
        "bjlc": bjlc,
        "end_lat": session.end_lat,
        "end_lng": session.end_lng,
        "end_speed": _round_or_none(session.end_speed, 2),
        "end_gpstime": end_gpstime,
        "end_mileage": _round_or_none(session.end_mileage, 3),
        "remark": remark,
    }


async def _push_payload(payload: dict[str, Any]) -> bool:
    if not getattr(settings, "obd_speed_push_1303_enabled", True):
        return False
    if not jt808_openapi_client.configured():
        logger.debug("1303 跳过：JT808 OpenAPI 未配置")
        return False
    device_id = payload.get("device_id") or ""
    gpstime = payload.get("gpstime") or ""
    if not device_id or not gpstime:
        logger.warning("1303 跳过：缺少 deviceId/gpstime device=%s gpstime=%s", device_id, gpstime)
        return False
    try:
        await jt808_openapi_client.create_car_alarm(**payload)
        logger.info(
            "1303 超速报警已写入 device=%s start=%s end=%s time=%ss bjlc=%s",
            device_id,
            gpstime,
            payload.get("end_gpstime"),
            payload.get("time_sec"),
            payload.get("bjlc"),
        )
        return True
    except Jt808OpenApiError as exc:
        logger.warning("1303 超速报警同步失败 device=%s: %s", device_id, exc)
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("1303 超速报警同步异常 device=%s: %s", device_id, exc)
        return False


async def _flush_session(session: OverspeedSession, *, reason: str) -> bool:
    payload = build_session_alarm_payload(session)
    ok = await _push_payload(payload)
    logger.info(
        "1303 会话结束 reason=%s plate=%s device=%s ticks=%s duration=%ss ok=%s",
        reason,
        session.plate_no,
        session.device_id,
        session.tick_count,
        session.duration_sec(),
        ok,
    )
    return ok


def schedule_flush_1303(session: OverspeedSession, *, reason: str) -> bool:
    """后台推送 1303，避免拖住多车超速判定主循环。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning(
            "1303 无事件循环，跳过推送 device=%s reason=%s",
            session.device_id,
            reason,
        )
        return False

    async def _runner() -> None:
        try:
            await _flush_session(session, reason=reason)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "1303 后台推送异常 device=%s reason=%s: %s",
                session.device_id,
                reason,
                exc,
            )

    task = loop.create_task(_runner(), name=f"obd-1303-{session.device_id}-{reason}")
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return True


def advance_overspeed_sessions(
    active_ticks: dict[int, OverspeedTick],
) -> tuple[dict[str, int], list[tuple[OverspeedSession, str]], list[OverspeedSession]]:
    """刷新内存会话，返回本轮需要落库的会话（结束/拆段）与新开会话。

    调用方应对每个会话同时写本地 OBD超速 + 后台 1303，保证 1:1。
    just_opened 仅含「从无到有」的新会话（不含时长拆段），供即时通知使用。
    """
    max_sec = max(60, int(getattr(settings, "obd_speed_session_max_seconds", 900) or 900))
    stats = {"opened": 0, "updated": 0, "closed": 0, "split": 0}
    to_flush: list[tuple[OverspeedSession, str]] = []
    just_opened: list[OverspeedSession] = []

    for vehicle_id, tick in active_ticks.items():
        with _lock:
            session = _sessions.get(vehicle_id)
            if session is None:
                new_session = _new_session(tick)
                _sessions[vehicle_id] = new_session
                stats["opened"] += 1
                just_opened.append(new_session)
                continue
            session.touch(tick)
            stats["updated"] += 1
            if session.duration_sec() >= max_sec:
                old = session
                del _sessions[vehicle_id]
                _sessions[vehicle_id] = _new_session(tick)
                stats["split"] += 1
                stats["opened"] += 1
                to_flush.append((old, "max_duration"))

    with _lock:
        ended_ids = [vid for vid in list(_sessions.keys()) if vid not in active_ticks]
        for vid in ended_ids:
            to_flush.append((_sessions.pop(vid), "overspeed_ended"))
            stats["closed"] += 1

    return stats, to_flush, just_opened


def take_all_overspeed_sessions(*, reason: str = "shutdown") -> list[tuple[OverspeedSession, str]]:
    """取出全部未结束会话（调度停止时由调用方做 1:1 落库）。"""
    with _lock:
        pending = list(_sessions.values())
        _sessions.clear()
    return [(session, reason) for session in pending]


# 兼容旧名
async def sync_overspeed_sessions(active_ticks: dict[int, OverspeedTick]) -> dict[str, int]:
    stats, flushes, _opened = advance_overspeed_sessions(active_ticks)
    pushed = 0
    for session, reason in flushes:
        if schedule_flush_1303(session, reason=reason):
            pushed += 1
    return {**stats, "pushed": pushed}


async def flush_all_overspeed_sessions(*, reason: str = "shutdown") -> int:
    """仅推送 1303（无本地入库）；正常路径请用 take_all + 会话级 1:1 落库。"""
    pending = take_all_overspeed_sessions(reason=reason)
    if not pending:
        return 0
    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                *[_flush_session(session, reason=reason) for session, _reason in pending],
                return_exceptions=True,
            ),
            timeout=8.0,
        )
    except asyncio.TimeoutError:
        logger.warning("1303 停服落库超时，已丢弃等待中的 %s 条会话推送", len(pending))
        return 0
    return sum(1 for item in results if item is True)


# 兼容旧调用名：单点立即推送（无会话时仍可用）
async def push_obd_speed_car_alarm_1303(
    *,
    device_id: str,
    speed_kmh: float,
    lat: float | None,
    lng: float | None,
    mileage_km: float | None,
    alarm_at: datetime | None,
    limit_kmh: float | None = None,
    rule_name: str | None = None,
    kind: str | None = None,
    address: str | None = None,
    end_speed_kmh: float | None = None,
    end_lat: float | None = None,
    end_lng: float | None = None,
    end_mileage_km: float | None = None,
    end_at: datetime | None = None,
    time_sec: int | None = None,
    bjlc: float | None = None,
) -> bool:
    start_at = alarm_at
    finish_at = end_at or alarm_at
    session = OverspeedSession(
        vehicle_id=0,
        device_id=str(device_id or "").strip(),
        plate_no=None,
        start_at=start_at or datetime.now(),
        start_speed=float(speed_kmh),
        start_lat=lat,
        start_lng=lng,
        start_mileage=float(mileage_km) if mileage_km is not None else None,
        end_at=finish_at or start_at or datetime.now(),
        end_speed=float(end_speed_kmh if end_speed_kmh is not None else speed_kmh),
        end_lat=end_lat if end_lat is not None else lat,
        end_lng=end_lng if end_lng is not None else lng,
        end_mileage=(
            float(end_mileage_km)
            if end_mileage_km is not None
            else (float(mileage_km) if mileage_km is not None else None)
        ),
        limit_kmh=limit_kmh,
        rule_name=rule_name,
        kind=kind,
        address=address,
        peak_speed=float(speed_kmh),
        tick_count=1,
    )
    payload = build_session_alarm_payload(session)
    if time_sec is not None:
        payload["time_sec"] = int(time_sec)
    if bjlc is not None:
        payload["bjlc"] = float(bjlc)
    return await _push_payload(payload)

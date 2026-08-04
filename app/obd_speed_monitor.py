"""OBD 时速监测：定时读 Redis 车辆 OBD 数据，按私有地图规则判定超速违章。

数据链路：
1. 车辆 OBD 上报 → Redis Key ``{设备号}_OBD``（JSON：时速/总里程/时间戳）
2. 定时器 SCAN 读取全部 OBD Key，只处理时速 > 阈值（默认 10 km/h）的车辆
3. 设备号 → CESG 车辆（复用 JT808 同步的设备号变体匹配）
4. 车辆坐标：与实时监控页同源——JT808 OpenAPI 1201 定位接口（WGS84），
   失败时兜底 vehicle_location 快照
4b. WGS84→GCJ02 后调用高德轨迹纠偏 API，将漂移点吸附到道路再判定
5. 规则匹配：车辆 → 规则类别(assigned_vehicle_ids) → 私有规则(category_ids)，
   坐标转 GCJ02 后做几何命中（围栏=点在形内；限速折线=点距折线 <= 缓冲带）
5b. 按车辆坐标查询实况天气，套用类别 weather_speed_limits 调整生效限速
6. 优先级仲裁（用户约定，四档线性，仅重叠时生效）：
   继承集团范围 < 纯私有范围 < 继承集团折线 < 纯私有公司折线
   圆/矩形/多边形同属「范围」档；同级多条命中时随机取一条
7. 超过生效限速 → 维护超速会话（持续只更新终点）；
   会话结束或满 15 分钟拆段时 **同时** 写本地 OBD超速 + 后台 1303（1:1）
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from collections import deque

from app.alarm_type_gate import (
    evaluate_alarm_type_ingest,
    load_alarm_type_by_name,
    log_alarm_type_gate,
    risk_level_from_alarm_type,
)
from app.amap_grasp_road import GraspTrailPoint, grasp_road_with_keys
from app.amap_regeo import resolve_address_wgs84
from app.config import settings
from app.database import AsyncSessionLocal
from app.geo_utils import geometry_hit, wgs84_to_gcj02
from app.header_weather import _fetch_weather_by_coords
from app.jt808_alarm_sync import _vehicle_by_terminal
from app.jt808_car_alarm_1303 import (
    OverspeedSession,
    OverspeedTick,
    advance_overspeed_sessions,
    open_session_count,
    schedule_flush_1303,
    session_status_snapshot,
    take_all_overspeed_sessions,
)
from app.jt808_openapi_client import Jt808OpenApiError, jt808_openapi_client
from app.map_rule_weather import WEATHER_TYPE_OPTIONS, effective_limit_kmh, weather_text_to_type_code
from app.jt808_violation_sync import lookup_company_name, notify_violation_created
from app.models import (
    AlarmTypeDict,
    MapRuleCategory,
    ObdEnergySnapshot,
    PrivateMapRule,
    PrivateMapRuleWeather,
    Vehicle,
    VehicleLocation,
    VehicleViolation,
)
from app.timeutil import china_now_naive
from app.violation_risk import derive_risk_level

logger = logging.getLogger(__name__)

SOURCE_OBD_SPEED = "obd_speed"
OBD_VIOLATION_TYPE_NAME = "OBD超速"
# 改名前旧类型名，间隔闸门一并计入，避免 5 分钟内再出一条
OBD_VIOLATION_TYPE_ALIASES = ("OBD超速", "OBD限速路段超速", "OBD区域超速")
_WEATHER_CODE_LABEL = {str(x["code"]): str(x["label"]) for x in WEATHER_TYPE_OPTIONS}

# 即时通知冷却：同车两次语音下发至少间隔（秒），避免临界限速抖动刷屏
_NOTIFY_COOLDOWN_SEC = 300
_notify_cooldown: dict[int, float] = {}
_notify_cooldown_lock = Lock()
_notify_bg_tasks: set[asyncio.Task] = set()

# 接近限速预警冷却（与超速即时通知分开计数）
_WARN_COOLDOWN_SEC = 300
_warn_cooldown: dict[int, float] = {}
_warn_cooldown_lock = Lock()
_warn_bg_tasks: set[asyncio.Task] = set()

DEFAULT_WARN_CONTENT = "车路协同数字平台提醒您，您已达到限速值的百分之SS，请注意文明驾驶！"


def _weather_label(code: str | None) -> str:
    c = (code or "").strip().lower() or "sunny"
    return _WEATHER_CODE_LABEL.get(c, c)


# ---------------------------------------------------------------------------
# OBD JSON 解析（字段名做多别名兼容）
# ---------------------------------------------------------------------------

_SPEED_KEYS = ("speed", "velocity", "vehicle_speed", "vehicleSpeed", "sudu", "时速", "车速")
_MILEAGE_KEYS = ("mileage", "total_mileage", "totalMileage", "odometer", "licheng", "总里程", "里程")
_TS_KEYS = ("ts", "timestamp", "time", "gpstime", "report_time", "reportTime", "时间戳", "时间")


def _pick(data: dict, keys: tuple[str, ...]) -> Any:
    for k in keys:
        if k in data and data[k] is not None and data[k] != "":
            return data[k]
    return None


def _parse_ts(raw: Any) -> datetime | None:
    """兼容 epoch 秒/毫秒 与常见字符串格式。"""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        v = float(raw)
        if v > 1e12:  # 毫秒
            v /= 1000.0
        if v > 1e9:  # 合理的 epoch 秒
            try:
                return datetime.fromtimestamp(v)
            except (OSError, OverflowError, ValueError):
                return None
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.isdigit():
        return _parse_ts(int(s))
    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


@dataclass
class ObdReading:
    device_no: str
    speed_kmh: float
    mileage_km: float | None
    report_at: datetime | None
    raw: str


# 与实时监控油车/电车 OBD 列对齐的快照字段（写入 raw_preview.obd_snapshot）
_OBD_YC_SNAPSHOT_KEYS = (
    "speed",
    "dqyl",
    "fdjjscnj",
    "mcnj",
    "fdjzs",
    "fdjrlll",
    "scrnox1",
    "scrnox2",
    "fyjyl",
    "jql",
    "scrwd1",
    "scrwd2",
    "dpfyc",
    "fdjncywd",
    "yxyw",
    "zlc",
    "bclc",
    "ts",
    "carno",
    "car_id",
)
_OBD_DC_SNAPSHOT_KEYS = (
    "speed",
    "clzt",
    "cdzt",
    "zlc",
    "zdy",
    "zdl",
    "soc",
    "dw",
    "jydz",
    "cddjzt",
    "cddjzs",
    "cddjzj",
    "cddjwd",
    "jsdbbcxfd",
    "zddbcxfd",
    "fxpdqzxjd",
    "ts",
    "carno",
    "car_id",
)
_OBD_YC_HINT_KEYS = ("fdjzs", "fdjrlll", "yxyw", "scrnox1", "dqyl", "dpfyc")
_OBD_DC_HINT_KEYS = ("soc", "zdy", "zdl", "cddjzs", "cddjwd", "clzt")


def _json_dict(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _dict_get_ci(data: dict[str, Any], key: str) -> Any:
    if key in data and data[key] is not None and data[key] != "":
        return data[key]
    low = key.lower()
    for k, v in data.items():
        if str(k).lower() == low and v is not None and v != "":
            return v
    return None


def _infer_obd_type(data: dict[str, Any], energy_type: str | None = None) -> str:
    et = (energy_type or "").strip().lower()
    if et in ("oil", "yc"):
        return "yc"
    if et in ("ev", "dc", "electric"):
        return "dc"
    yc_hits = sum(1 for k in _OBD_YC_HINT_KEYS if _dict_get_ci(data, k) is not None)
    dc_hits = sum(1 for k in _OBD_DC_HINT_KEYS if _dict_get_ci(data, k) is not None)
    if dc_hits > yc_hits:
        return "dc"
    return "yc"


def _ts_char14(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    try:
        return dt.strftime("%Y%m%d%H%M%S")
    except (ValueError, OSError):
        return None


def _pick_snapshot_fields(data: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in keys:
        val = _dict_get_ci(data, key)
        if val is not None and val != "":
            out[key] = val
    return out


def build_obd_snapshot(
    *,
    reading: ObdReading,
    plate_no: str | None,
    energy_raw: str | None = None,
    energy_type: str | None = None,
) -> tuple[dict[str, Any], str]:
    """组装与实时监控字段对齐的 OBD 快照，供违章 raw_preview 落库。"""
    merged: dict[str, Any] = {}
    merged.update(_json_dict(energy_raw))
    merged.update(_json_dict(reading.raw))
    # 触发报警的时速/里程以监测读数为准
    merged["speed"] = reading.speed_kmh
    if reading.mileage_km is not None:
        merged.setdefault("zlc", reading.mileage_km)
        merged.setdefault("bclc", reading.mileage_km)
    ts = _ts_char14(reading.report_at) or _dict_get_ci(merged, "ts")
    if ts is not None:
        merged["ts"] = ts
    if plate_no:
        merged["carno"] = str(plate_no).strip()
    obd_type = _infer_obd_type(merged, energy_type)
    keys = _OBD_DC_SNAPSHOT_KEYS if obd_type == "dc" else _OBD_YC_SNAPSHOT_KEYS
    snapshot = _pick_snapshot_fields(merged, keys)
    if "speed" not in snapshot:
        snapshot["speed"] = reading.speed_kmh
    if ts and "ts" not in snapshot:
        snapshot["ts"] = ts
    if plate_no and "carno" not in snapshot:
        snapshot["carno"] = str(plate_no).strip()
    return snapshot, obd_type


async def _latest_energy_raw(
    db: AsyncSession, device_no: str
) -> tuple[str | None, str | None]:
    """取该设备当日最新油/电 OBD 原始报文（QUEUE 消费落库），优先油车。"""
    day = china_now_naive().strftime("%Y%m%d")
    for etype, mapped in (("oil", "yc"), ("ev", "dc")):
        row = (
            await db.execute(
                select(ObdEnergySnapshot).where(
                    ObdEnergySnapshot.device_no == str(device_no),
                    ObdEnergySnapshot.day == day,
                    ObdEnergySnapshot.energy_type == etype,
                )
            )
        ).scalar_one_or_none()
        if row and (row.raw or "").strip():
            return str(row.raw), mapped
    return None, None


def parse_obd_payload(device_no: str, payload: str) -> ObdReading | None:
    try:
        data = json.loads(payload)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    speed = _pick(data, _SPEED_KEYS)
    try:
        speed_kmh = float(speed)
    except (TypeError, ValueError):
        return None
    mileage = _pick(data, _MILEAGE_KEYS)
    try:
        mileage_km = float(mileage) if mileage is not None else None
    except (TypeError, ValueError):
        mileage_km = None
    return ObdReading(
        device_no=device_no,
        speed_kmh=speed_kmh,
        mileage_km=mileage_km,
        report_at=_parse_ts(_pick(data, _TS_KEYS)),
        raw=payload[:2000],
    )


# ---------------------------------------------------------------------------
# 规则仲裁
# ---------------------------------------------------------------------------

@dataclass
class RuleHit:
    rule: PrivateMapRule
    category: MapRuleCategory
    limit_kmh: int
    weather_rule_row: PrivateMapRuleWeather | None = None

    @property
    def is_self_drawn(self) -> bool:
        return self.rule.ref_public_rule_id is None

    @property
    def is_speed_rule(self) -> bool:
        return (self.rule.rule_type_code or "").strip().lower() == "speed_rule"

    def limit_at_weather(self, weather_type_code: str) -> int:
        return effective_limit_kmh(
            self.rule,
            self.category,
            weather_type_code,
            weather_rule_row=self.weather_rule_row,
        )


_weather_loc_cache: dict[str, tuple[str, float]] = {}
_weather_loc_lock = Lock()
_WEATHER_LOC_TTL = 15 * 60


async def _weather_code_at(lat: float, lng: float) -> str:
    """按车辆坐标取实况天气编码，同网格 15 分钟缓存。"""
    key = f"{round(lat, 2)}:{round(lng, 2)}"
    now = time.time()
    with _weather_loc_lock:
        hit = _weather_loc_cache.get(key)
        if hit and now - hit[1] < _WEATHER_LOC_TTL:
            return hit[0]
    data = await _fetch_weather_by_coords(lat, lng) or {}
    code = weather_text_to_type_code(str(data.get("weather") or ""))
    with _weather_loc_lock:
        if len(_weather_loc_cache) > 2000:
            _weather_loc_cache.clear()
        _weather_loc_cache[key] = (code, now)
    return code


def rule_priority_rank(h: RuleHit) -> int:
    """四档线性优先级（数值越小越优先，仅重叠仲裁时用）。

    0 纯私有折线 > 1 继承集团折线 > 2 纯私有范围 > 3 继承集团范围

    「范围」含圆 / 矩形 / 多边形，三者同档同等处理（非折线即范围）。
    """
    if h.is_self_drawn:
        return 0 if h.is_speed_rule else 2
    return 1 if h.is_speed_rule else 3


def arbitrate(hits: list[RuleHit]) -> RuleHit | None:
    """重叠命中时挑一条生效规则：

    1. 四档线性：纯私有折线 > 继承集团折线 > 纯私有范围 > 继承集团范围
       （圆/矩形/多边形同属范围档）
    2. 同档多条命中时随机取一条
    """
    if not hits:
        return None
    best_rank = min(rule_priority_rank(h) for h in hits)
    top = [h for h in hits if rule_priority_rank(h) == best_rank]
    return random.choice(top)


# ---------------------------------------------------------------------------
# 同步执行体
# ---------------------------------------------------------------------------

@dataclass
class ObdSyncResult:
    scanned_keys: int = 0
    parsed: int = 0
    skipped_low_speed: int = 0
    skipped_stale: int = 0
    skipped_no_vehicle: int = 0
    skipped_no_position: int = 0
    skipped_no_rule: int = 0
    checked: int = 0
    grasp_road_corrected: int = 0
    grasp_road_fallback: int = 0
    violations_inserted: int = 0
    overspeed_active: int = 0
    session_opened: int = 0
    session_updated: int = 0
    session_closed: int = 0
    session_split: int = 0
    alarm_1303_pushed: int = 0
    warn_scheduled: int = 0
    open_sessions: int = 0
    error: str | None = None
    detail: list[dict[str, Any]] = field(default_factory=list)


def _external_id(vehicle_id: int, rule_id: int, bucket: str) -> str:
    return f"obd_speed:{vehicle_id}:{rule_id}:{bucket}"


def _cooldown_bucket(at: datetime, interval_minutes: int = 5) -> str:
    """按间隔分钟分桶：同一车辆+规则在该桶内只入库一条（默认 5 分钟，与类型表间隔对齐）。"""
    step = max(1, int(interval_minutes or 5))
    total_min = at.hour * 60 + at.minute
    bucket_idx = total_min // step
    return f"{at.strftime('%Y%m%d')}{bucket_idx:04d}"


async def _ensure_obd_alarm_type(db: AsyncSession) -> AlarmTypeDict:
    """字典缺 OBD超速 时自动补一条（启用/中/15分钟），避免闸门把超速全部拒掉。"""
    row = await load_alarm_type_by_name(db, OBD_VIOLATION_TYPE_NAME)
    if row is not None:
        return row
    stamp = china_now_naive().strftime("%Y%m%d%H%M%S")
    row = AlarmTypeDict(
        type_code=f"AT{stamp}OBD1",
        type_name=OBD_VIOLATION_TYPE_NAME,
        description="OBD 时速监测超速（限速路段/区域）",
        alarm_level="中级",
        safety_level="中",
        min_interval_minutes=15,
        status="启用",
        data_source="obd_speed",
        ttx_atp_code=None,
    )
    db.add(row)
    await db.flush()
    logger.info("已自动补齐报警类型: %s interval=%s", OBD_VIOLATION_TYPE_NAME, 15)
    return row


def _stable_biz_no(external_id: str, violation_time: datetime) -> str:
    digest = hashlib.md5(external_id.encode("utf-8")).hexdigest()[:8].upper()  # noqa: S324
    return f"WZ{violation_time.strftime('%Y%m%d%H%M%S')}{digest}"


async def _scan_obd_keys(redis) -> dict[str, str]:
    """SCAN 全部 *_OBD Key，返回 {device_no: payload}。"""
    out: dict[str, str] = {}
    pattern = settings.obd_redis_key_pattern
    async for key in redis.scan_iter(match=pattern, count=500):
        name = key if isinstance(key, str) else key.decode("utf-8", "ignore")
        if not name.endswith("_OBD"):
            continue
        device_no = name[: -len("_OBD")].strip()
        if not device_no:
            continue
        try:
            payload = await redis.get(name)
        except Exception:  # noqa: BLE001
            continue
        if payload is None:
            continue
        out[device_no] = payload if isinstance(payload, str) else payload.decode("utf-8", "ignore")
    return out


def _new_redis():
    """构造 Redis 客户端：连接失败不做客户端级重试（定时器每轮本身就是重试）。"""
    from redis import asyncio as aioredis
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    return aioredis.Redis(
        host=settings.obd_redis_host,
        port=settings.obd_redis_port,
        password=settings.obd_redis_password or None,
        db=settings.obd_redis_db,
        socket_timeout=8,
        socket_connect_timeout=5,
        decode_responses=True,
        retry=Retry(NoBackoff(), 0),
    )


async def ping_redis() -> dict[str, Any]:
    """主动连一次 Redis：PING + 扫描 OBD Key + 抓取样例数据，用于状态页诊断。"""
    import time as _time

    info: dict[str, Any] = {
        "target": f"{settings.obd_redis_host}:{settings.obd_redis_port}/{settings.obd_redis_db}",
        "connected": False,
        "ping_ms": None,
        "obd_key_count": 0,
        "sample_keys": [],
        "sample_payload": None,
        "sample_parsed": None,
        "error": None,
    }
    redis = _new_redis()
    try:
        t0 = _time.perf_counter()
        await redis.ping()
        info["ping_ms"] = round((_time.perf_counter() - t0) * 1000, 1)
        info["connected"] = True

        keys: list[str] = []
        async for key in redis.scan_iter(match=settings.obd_redis_key_pattern, count=500):
            name = key if isinstance(key, str) else key.decode("utf-8", "ignore")
            if name.endswith("_OBD"):
                keys.append(name)
            if len(keys) >= 500:
                break
        info["obd_key_count"] = len(keys)
        info["sample_keys"] = keys[:10]
        if keys:
            payload = await redis.get(keys[0])
            if payload is not None:
                text = payload if isinstance(payload, str) else payload.decode("utf-8", "ignore")
                info["sample_payload"] = text[:500]
                reading = parse_obd_payload(keys[0][: -len("_OBD")], text)
                if reading is not None:
                    info["sample_parsed"] = {
                        "device_no": reading.device_no,
                        "speed_kmh": reading.speed_kmh,
                        "mileage_km": reading.mileage_km,
                        "report_at": reading.report_at.isoformat(sep=" ", timespec="seconds")
                        if reading.report_at
                        else None,
                    }
    except Exception as exc:  # noqa: BLE001
        info["error"] = str(exc)
    finally:
        try:
            await redis.aclose()
        except Exception:  # noqa: BLE001
            pass
    return info


async def _fetch_positions(device_nos: list[str]) -> dict[str, dict[str, Any]]:
    """经 JT808 OpenAPI 1201 批量取车辆坐标（与实时监控页数据同源）。

    返回 {device_no: {lng, lat, gpstime}}，坐标为 WGS84。
    """
    result: dict[str, dict[str, Any]] = {}
    if not device_nos or not jt808_openapi_client.configured():
        return result
    for i in range(0, len(device_nos), 50):
        chunk = device_nos[i : i + 50]
        try:
            data = await jt808_openapi_client.list_positions(chunk)
        except Exception as exc:  # noqa: BLE001
            logger.warning("OBD 监测取车辆坐标失败: %s", exc)
            continue
        rows = data.get("data") if isinstance(data.get("data"), list) else []
        for item in rows:
            if not isinstance(item, dict):
                continue
            tid = str(item.get("tid") or item.get("car_id") or "").strip()
            if tid:
                result[tid] = item
    return result


def _position_from_item(item: dict[str, Any]) -> tuple[float, float] | None:
    try:
        lng = float(item.get("lng"))
        lat = float(item.get("lat"))
    except (TypeError, ValueError):
        return None
    if not lng or not lat:
        return None
    return lng, lat


def _direction_from_item(item: dict[str, Any] | None) -> float | None:
    if not item:
        return None
    try:
        direction = float(item.get("direction") or item.get("dir") or 0)
    except (TypeError, ValueError):
        return None
    if 0 < direction < 360:
        return direction
    return None


def _pos_time_from_item(item: dict[str, Any] | None, fallback: datetime | None, now: datetime) -> datetime:
    if item:
        parsed = _parse_ts(item.get("gpstime") or item.get("systime") or item.get("ts"))
        if parsed is not None:
            return parsed
    return fallback or now


@dataclass
class _TrailPoint:
    lng_gcj: float
    lat_gcj: float
    speed_kmh: float
    angle: float | None
    at: datetime


_device_trails: dict[str, deque[_TrailPoint]] = {}
_trail_lock = Lock()
_TRAIL_MAX_POINTS = 12
_TRAIL_MAX_AGE = timedelta(minutes=10)


def _append_device_trail(device_no: str, point: _TrailPoint) -> list[GraspTrailPoint]:
    with _trail_lock:
        trail = _device_trails.setdefault(device_no, deque(maxlen=_TRAIL_MAX_POINTS))
        if trail:
            last = trail[-1]
            if (
                abs(last.lng_gcj - point.lng_gcj) < 1e-6
                and abs(last.lat_gcj - point.lat_gcj) < 1e-6
                and abs((last.at - point.at).total_seconds()) < 3
            ):
                trail[-1] = point
            else:
                trail.append(point)
        else:
            trail.append(point)
        cutoff = point.at - _TRAIL_MAX_AGE
        while trail and trail[0].at < cutoff:
            trail.popleft()
        return [
            GraspTrailPoint(p.lng_gcj, p.lat_gcj, p.speed_kmh, p.angle, p.at)
            for p in trail
        ]


async def _load_rule_index(db: AsyncSession) -> dict[int, list[RuleHit]]:
    """构建 vehicle_id → 候选规则列表（含类别，未做几何判定；限速在判定时按天气解析）。"""
    categories = (await db.execute(select(MapRuleCategory))).scalars().all()
    rules = (await db.execute(select(PrivateMapRule))).scalars().all()
    weather_rows = (await db.execute(select(PrivateMapRuleWeather))).scalars().all()
    weather_by_id = {int(wr.id): wr for wr in weather_rows}

    cat_by_id: dict[int, MapRuleCategory] = {c.id: c for c in categories}
    vehicle_cats: dict[int, set[int]] = {}
    for cat in categories:
        ids = cat.assigned_vehicle_ids if isinstance(cat.assigned_vehicle_ids, list) else []
        for vid in ids:
            try:
                vehicle_cats.setdefault(int(vid), set()).add(cat.id)
            except (TypeError, ValueError):
                continue

    index: dict[int, list[RuleHit]] = {}
    for rule in rules:
        rule_cat_ids = rule.category_ids if isinstance(rule.category_ids, list) else []
        rule_cat_set = set()
        for cid in rule_cat_ids:
            try:
                rule_cat_set.add(int(cid))
            except (TypeError, ValueError):
                continue
        if not rule_cat_set:
            continue
        for vid, cats in vehicle_cats.items():
            matched = cats & rule_cat_set
            if not matched:
                continue
            cat = cat_by_id.get(next(iter(matched)))
            if cat is None:
                continue
            weather_rule_row = weather_by_id.get(int(cat.weather_rule_id)) if cat.weather_rule_id else None
            limit = effective_limit_kmh(rule, cat, "sunny", weather_rule_row=weather_rule_row)
            if limit <= 0:
                continue
            index.setdefault(vid, []).append(
                RuleHit(rule=rule, category=cat, limit_kmh=limit, weather_rule_row=weather_rule_row)
            )
    return index


async def run_obd_speed_check_once() -> ObdSyncResult:
    """完整执行一轮：读 Redis → 关联车辆 → 取坐标 → 规则判定 → 违章入库。"""
    result = ObdSyncResult()
    redis = _new_redis()
    try:
        payloads = await _scan_obd_keys(redis)
    except Exception as exc:  # noqa: BLE001
        result.error = f"Redis 读取失败: {exc}"
        await redis.aclose()
        return result
    await redis.aclose()

    result.scanned_keys = len(payloads)
    now = china_now_naive()
    min_speed = float(settings.obd_min_speed_kmh)
    stale_after = timedelta(seconds=max(30, int(settings.obd_stale_seconds)))

    readings: list[ObdReading] = []
    for device_no, payload in payloads.items():
        reading = parse_obd_payload(device_no, payload)
        if reading is None:
            continue
        result.parsed += 1
        # 用户约定：时速 <= 10 km/h 不处理
        if reading.speed_kmh <= min_speed:
            result.skipped_low_speed += 1
            continue
        if reading.report_at is not None and now - reading.report_at > stale_after:
            result.skipped_stale += 1
            continue
        readings.append(reading)

    overspeed_ticks: dict[int, OverspeedTick] = {}
    if not readings:
        await _apply_session_pair_flush(result, overspeed_ticks)
        return result

    async with AsyncSessionLocal() as db:
        # 设备号 → 车辆
        vehicle_by_device: dict[str, Vehicle] = {}
        for reading in readings:
            vehicle = await _vehicle_by_terminal(db, reading.device_no)
            if vehicle is None:
                result.skipped_no_vehicle += 1
                continue
            vehicle_by_device[reading.device_no] = vehicle

        if not vehicle_by_device:
            await _apply_session_pair_flush(result, overspeed_ticks)
            return result

        # 车辆坐标：OpenAPI 1201 优先，vehicle_location 快照兜底
        positions = await _fetch_positions(list(vehicle_by_device.keys()))
        rule_index = await _load_rule_index(db)
        await _ensure_obd_alarm_type(db)
        # 本轮仍超速 → 只刷新会话；本地违章与 1303 在会话结束/拆段时 1:1 落库

        for reading in readings:
            vehicle = vehicle_by_device.get(reading.device_no)
            if vehicle is None:
                continue
            candidates = rule_index.get(vehicle.id) or []
            if not candidates:
                result.skipped_no_rule += 1
                continue

            pos_item = positions.get(reading.device_no)
            lng_lat = _position_from_item(pos_item) if pos_item else None
            pos_time: datetime | None = None
            address = ""
            if lng_lat is None:
                loc = await db.scalar(
                    select(VehicleLocation).where(VehicleLocation.vehicle_id == vehicle.id).limit(1)
                )
                if loc is not None and loc.lng and loc.lat:
                    lng_lat = (float(loc.lng), float(loc.lat))
                    pos_time = loc.pos_time
                    address = loc.current_position or ""
            else:
                address = str(pos_item.get("address") or "")
                pos_time = _pos_time_from_item(pos_item, reading.report_at, now)
            if lng_lat is None:
                result.skipped_no_position += 1
                continue
            # 坐标过旧同样跳过，避免用停车前的位置误判
            if pos_time is not None and now - pos_time > stale_after:
                result.skipped_no_position += 1
                continue

            if not (address or "").strip():
                address = await resolve_address_wgs84(db, lng_lat[1], lng_lat[0])

            lng_gcj, lat_gcj = wgs84_to_gcj02(lng_lat[0], lng_lat[1])
            grasp_applied = False
            direction = _direction_from_item(pos_item)
            pos_at = _pos_time_from_item(pos_item, reading.report_at, now)
            grasp_trail = _append_device_trail(
                reading.device_no,
                _TrailPoint(lng_gcj, lat_gcj, reading.speed_kmh, direction, pos_at),
            )
            grasp_result = await grasp_road_with_keys(db, grasp_trail)
            if grasp_result.lng is not None and grasp_result.lat is not None:
                lng_gcj, lat_gcj = float(grasp_result.lng), float(grasp_result.lat)
                grasp_applied = True
                result.grasp_road_corrected += 1
            else:
                result.grasp_road_fallback += 1
            weather_code = await _weather_code_at(lng_lat[1], lng_lat[0])
            buffer_m = float(settings.obd_polyline_buffer_m)
            hits: list[RuleHit] = []
            for h in candidates:
                if not geometry_hit(lng_gcj, lat_gcj, h.rule.draw_shape_type, h.rule.geometry_json, buffer_m):
                    continue
                limit = h.limit_at_weather(weather_code)
                if limit <= 0:
                    continue
                hits.append(
                    RuleHit(
                        rule=h.rule,
                        category=h.category,
                        limit_kmh=limit,
                        weather_rule_row=h.weather_rule_row,
                    )
                )
            result.checked += 1
            winner = arbitrate(hits)
            if winner is None:
                continue

            # 未超速但达到预警百分比：提前下发预警（不写违章）
            if reading.speed_kmh <= float(winner.limit_kmh):
                if schedule_speed_warn(
                    vehicle_id=int(vehicle.id),
                    device_id=reading.device_no,
                    plate_no=vehicle.plate_no or "",
                    speed_kmh=float(reading.speed_kmh),
                    limit_kmh=float(winner.limit_kmh),
                    category=winner.category,
                ):
                    result.warn_scheduled += 1
                continue

            violation_time = reading.report_at or now
            kind = "限速路段超速" if winner.is_speed_rule else "区域超速"
            weather_text = _weather_label(weather_code)
            energy_raw, energy_mapped = await _latest_energy_raw(db, reading.device_no)
            obd_snapshot, obd_type = build_obd_snapshot(
                reading=reading,
                plate_no=vehicle.plate_no,
                energy_raw=energy_raw,
                energy_type=energy_mapped,
            )
            cat = winner.category
            notify_on = bool(getattr(cat, "instant_notify_enabled", False))
            broadcast = (getattr(cat, "broadcast_content", None) or "").strip() or None
            overspeed_ticks[int(vehicle.id)] = OverspeedTick(
                vehicle_id=int(vehicle.id),
                device_id=reading.device_no,
                speed_kmh=reading.speed_kmh,
                lat=lng_lat[1],
                lng=lng_lat[0],
                mileage_km=reading.mileage_km,
                at=violation_time,
                limit_kmh=winner.limit_kmh,
                rule_id=int(winner.rule.id) if winner.rule.id is not None else None,
                rule_name=winner.rule.rule_name,
                kind=kind,
                address=address,
                plate_no=vehicle.plate_no,
                company_id=int(vehicle.company_id) if vehicle.company_id is not None else None,
                weather=weather_text,
                obd_snapshot=obd_snapshot,
                obd_type=obd_type,
                category_id=int(cat.id) if getattr(cat, "id", None) is not None else None,
                instant_notify_enabled=notify_on and bool(broadcast),
                # 模板原文落会话（含 XX），下发时再替换为生效限速
                broadcast_content=broadcast if notify_on else None,
            )
        await db.commit()

    await _apply_session_pair_flush(result, overspeed_ticks)
    return result


async def _persist_session_pair(session: OverspeedSession, *, reason: str) -> dict[str, bool]:
    """会话落库：本地 OBD超速 + 后台 1303（同 external_id，1:1）。

    遵守基础报警类型：停用/缺失/最小间隔内 → 本地与 1303 都不写。
    本地已存在则只补推 1303。
    """
    out = {"local": False, "alarm_1303": False}
    ext_id = session.external_alarm_id()
    async with AsyncSessionLocal() as db:
        # 与改会话前一致：同车同类型必须满足报警类型最小间隔（如 15 分钟）
        gate = await evaluate_alarm_type_ingest(
            db,
            type_name=OBD_VIOLATION_TYPE_NAME,
            vehicle_id=session.vehicle_id,
            alarm_time=session.start_at,
            interval_type_names=list(OBD_VIOLATION_TYPE_ALIASES),
        )
        if not gate.get("allow"):
            log_alarm_type_gate(
                source=SOURCE_OBD_SPEED,
                external_id=ext_id,
                alarm_type_name=OBD_VIOLATION_TYPE_NAME,
                reason=str(gate.get("reason") or "blocked"),
                plate=(session.plate_no or ""),
                interval_minutes=getattr(gate.get("alarm_type"), "min_interval_minutes", None),
            )
            return out

        alarm_row = gate.get("alarm_type") or await load_alarm_type_by_name(db, OBD_VIOLATION_TYPE_NAME)

        exists = await db.scalar(
            select(VehicleViolation.id).where(VehicleViolation.external_alarm_id == ext_id).limit(1)
        )
        if exists is None:
            risk = str(
                gate.get("risk_level")
                or risk_level_from_alarm_type(alarm_row, OBD_VIOLATION_TYPE_NAME)
                or derive_risk_level(OBD_VIOLATION_TYPE_NAME)
            )
            company_name = await lookup_company_name(db, session.company_id)
            row = VehicleViolation(
                biz_no=_stable_biz_no(ext_id, session.start_at),
                external_alarm_id=ext_id,
                terminal_id=(session.device_id or "")[:32],
                vehicle_id=session.vehicle_id,
                plate_no=(session.plate_no or "")[:16],
                company_id=session.company_id,
                company_name=company_name,
                violation_type_code=None,
                violation_type_name=OBD_VIOLATION_TYPE_NAME,
                risk_level=str(risk),
                violation_time=session.start_at,
                lat=session.start_lat,
                lng=session.start_lng,
                address=(session.address or "")[:512],
                source=SOURCE_OBD_SPEED,
                weather=(session.weather or "")[:32] or None,
                private_rule_name=(session.rule_name or "").strip()[:200] or None,
                rule_category_name=None,
                raw_preview=json.dumps(
                    {
                        "session_reason": reason,
                        "limit_kmh": session.limit_kmh,
                        "obd_speed_kmh": session.start_speed,
                        "start_speed_kmh": session.start_speed,
                        "end_speed_kmh": session.end_speed,
                        "peak_speed_kmh": session.peak_speed,
                        "duration_sec": session.duration_sec(),
                        "bjlc_km": session.bjlc_km(),
                        "start_mileage_km": session.start_mileage,
                        "end_mileage_km": session.end_mileage,
                        "mileage_km": session.start_mileage,
                        "end_lat": session.end_lat,
                        "end_lng": session.end_lng,
                        "end_at": session.end_at.isoformat(sep=" ", timespec="seconds"),
                        "rule_id": session.rule_id,
                        "rule_name": session.rule_name,
                        "kind": session.kind,
                        "tick_count": session.tick_count,
                        "weather": session.weather,
                        # 处理页 OBD 详情：优先读入库时的 obd_snapshot
                        "obd_type": session.obd_type or "yc",
                        "obd_snapshot": session.obd_snapshot or {
                            "speed": session.start_speed,
                            "carno": session.plate_no,
                            "ts": session.start_at.strftime("%Y%m%d%H%M%S"),
                        },
                    },
                    ensure_ascii=False,
                ),
                status="待处理",
            )
            db.add(row)
            await db.flush()
            await notify_violation_created(db, row)
            await db.commit()
            out["local"] = True
        else:
            await db.rollback()

    if schedule_flush_1303(session, reason=reason):
        out["alarm_1303"] = True
    return out


def _render_broadcast_for_limit(template: str, limit_kmh: float | int | None) -> str:
    """将播报模板中的 XX 替换为生效限速值后再下发。"""
    text = (template or "").strip()
    if not text:
        return ""
    limit_str = ""
    if limit_kmh is not None:
        try:
            n = float(limit_kmh)
            if n > 0:
                limit_str = str(int(round(n))) if abs(n - round(n)) < 1e-6 else f"{n:g}"
        except (TypeError, ValueError):
            limit_str = ""
    if limit_str:
        for token in ("XX", "xx", "ＸＸ"):
            text = text.replace(token, limit_str)
    return text


async def _send_instant_notify(session: OverspeedSession) -> bool:
    """规则类别开启即时通知时，对车辆下发 808 文字（默认语音播报）。"""
    if not session.instant_notify_enabled:
        return False
    content = _render_broadcast_for_limit(session.broadcast_content or "", session.limit_kmh)
    device_id = (session.device_id or "").strip()
    if not content or not device_id:
        return False
    if not jt808_openapi_client.configured():
        logger.debug("即时通知跳过：JT808 OpenAPI 未配置 device=%s", device_id)
        return False

    now = time.time()
    with _notify_cooldown_lock:
        last = _notify_cooldown.get(int(session.vehicle_id))
        if last is not None and now - last < _NOTIFY_COOLDOWN_SEC:
            logger.debug(
                "即时通知冷却中 plate=%s remain=%.0fs",
                session.plate_no,
                _NOTIFY_COOLDOWN_SEC - (now - last),
            )
            return False
        _notify_cooldown[int(session.vehicle_id)] = now
        if len(_notify_cooldown) > 5000:
            cutoff = now - _NOTIFY_COOLDOWN_SEC
            stale = [k for k, t in _notify_cooldown.items() if t < cutoff]
            for k in stale:
                _notify_cooldown.pop(k, None)

    try:
        # 规则即时通知：默认直接语音播报（voice=1）
        await jt808_openapi_client.send_text_message(
            device_id=device_id,
            content=content,
            voice=True,
            display=False,
            urgent=False,
            smart=False,
            send_type="instant",
        )
        logger.info(
            "规则即时通知已下发 plate=%s device=%s category=%s limit=%s content=%s",
            session.plate_no,
            device_id,
            session.category_id,
            session.limit_kmh,
            content[:40],
        )
        return True
    except Jt808OpenApiError as exc:
        logger.warning(
            "规则即时通知失败 plate=%s device=%s: %s",
            session.plate_no,
            device_id,
            exc,
        )
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "规则即时通知异常 plate=%s device=%s: %s",
            session.plate_no,
            device_id,
            exc,
        )
        return False


def schedule_instant_notify(session: OverspeedSession) -> bool:
    """后台下发即时通知，不阻塞超速主循环。"""
    if not session.instant_notify_enabled or not (session.broadcast_content or "").strip():
        return False
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning("即时通知无事件循环，跳过 plate=%s", session.plate_no)
        return False

    async def _runner() -> None:
        try:
            await _send_instant_notify(session)
        except Exception as exc:  # noqa: BLE001
            logger.warning("即时通知后台任务异常 plate=%s: %s", session.plate_no, exc)

    task = loop.create_task(_runner(), name=f"obd-notify-{session.device_id}")
    _notify_bg_tasks.add(task)
    task.add_done_callback(_notify_bg_tasks.discard)
    return True


def _render_warn_content(template: str, percent: int) -> str:
    """将预警模板中的 SS 替换为设定百分比。"""
    text = (template or "").strip() or DEFAULT_WARN_CONTENT
    pct = str(int(percent))
    for token in ("SS", "ss", "ＳＳ"):
        text = text.replace(token, pct)
    return text


def _category_warn_threshold(category: MapRuleCategory | None, limit_kmh: float) -> tuple[bool, int, float, str]:
    """返回 (是否启用, 百分比, 阈值车速, 文案模板)。"""
    if category is None or limit_kmh <= 0:
        return False, 0, 0.0, ""
    if not bool(getattr(category, "warn_enabled", False)):
        return False, 0, 0.0, ""
    try:
        pct = int(getattr(category, "warn_percent", None) or 0)
    except (TypeError, ValueError):
        return False, 0, 0.0, ""
    if pct < 1 or pct > 100:
        return False, 0, 0.0, ""
    threshold = float(limit_kmh) * pct / 100.0
    content = (getattr(category, "warn_content", None) or "").strip() or DEFAULT_WARN_CONTENT
    return True, pct, threshold, content


async def _send_speed_warn(
    *,
    vehicle_id: int,
    device_id: str,
    plate_no: str,
    percent: int,
    content_template: str,
    limit_kmh: float,
    speed_kmh: float,
) -> bool:
    content = _render_warn_content(content_template, percent)
    device = (device_id or "").strip()
    if not content or not device:
        return False
    if not jt808_openapi_client.configured():
        logger.debug("限速预警跳过：JT808 OpenAPI 未配置 device=%s", device)
        return False

    now = time.time()
    with _warn_cooldown_lock:
        last = _warn_cooldown.get(int(vehicle_id))
        if last is not None and now - last < _WARN_COOLDOWN_SEC:
            logger.debug(
                "限速预警冷却中 plate=%s remain=%.0fs",
                plate_no,
                _WARN_COOLDOWN_SEC - (now - last),
            )
            return False
        _warn_cooldown[int(vehicle_id)] = now
        if len(_warn_cooldown) > 5000:
            cutoff = now - _WARN_COOLDOWN_SEC
            stale = [k for k, t in _warn_cooldown.items() if t < cutoff]
            for k in stale:
                _warn_cooldown.pop(k, None)

    try:
        await jt808_openapi_client.send_text_message(
            device_id=device,
            content=content,
            voice=True,
            display=False,
            urgent=False,
            smart=False,
            send_type="instant",
        )
        logger.info(
            "限速预警已下发 plate=%s device=%s speed=%s limit=%s percent=%s content=%s",
            plate_no,
            device,
            speed_kmh,
            limit_kmh,
            percent,
            content[:40],
        )
        return True
    except Jt808OpenApiError as exc:
        logger.warning("限速预警失败 plate=%s device=%s: %s", plate_no, device, exc)
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("限速预警异常 plate=%s device=%s: %s", plate_no, device, exc)
        return False


def schedule_speed_warn(
    *,
    vehicle_id: int,
    device_id: str,
    plate_no: str,
    speed_kmh: float,
    limit_kmh: float,
    category: MapRuleCategory | None,
) -> bool:
    """规则范围内且车速达到限速百分比时，后台下发预警语音。"""
    enabled, pct, threshold, template = _category_warn_threshold(category, limit_kmh)
    if not enabled:
        return False
    if float(speed_kmh) < threshold:
        return False
    # 已超速走即时通知，不在此重复预警
    if float(speed_kmh) > float(limit_kmh):
        return False
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning("限速预警无事件循环，跳过 plate=%s", plate_no)
        return False

    async def _runner() -> None:
        try:
            await _send_speed_warn(
                vehicle_id=vehicle_id,
                device_id=device_id,
                plate_no=plate_no,
                percent=pct,
                content_template=template,
                limit_kmh=limit_kmh,
                speed_kmh=speed_kmh,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("限速预警后台任务异常 plate=%s: %s", plate_no, exc)

    task = loop.create_task(_runner(), name=f"obd-warn-{device_id}")
    _warn_bg_tasks.add(task)
    task.add_done_callback(_warn_bg_tasks.discard)
    return True


async def _apply_session_pair_flush(
    result: ObdSyncResult,
    overspeed_ticks: dict[int, OverspeedTick],
) -> None:
    """会话结束/拆段：本地违章与 1303 成对落库；新开会话触发即时通知。"""
    result.overspeed_active = len(overspeed_ticks)
    try:
        session_stats, flushes, just_opened = advance_overspeed_sessions(overspeed_ticks)
        result.session_opened = int(session_stats.get("opened") or 0)
        result.session_updated = int(session_stats.get("updated") or 0)
        result.session_closed = int(session_stats.get("closed") or 0)
        result.session_split = int(session_stats.get("split") or 0)
        for opened in just_opened:
            schedule_instant_notify(opened)
        pushed = 0
        for session, reason in flushes:
            pair = await _persist_session_pair(session, reason=reason)
            if pair.get("local"):
                result.violations_inserted += 1
                result.detail.append(
                    {
                        "plate_no": session.plate_no,
                        "device_no": session.device_id,
                        "speed": session.peak_speed,
                        "limit": session.limit_kmh,
                        "rule": session.rule_name,
                        "duration_sec": session.duration_sec(),
                        "session_reason": reason,
                    }
                )
            if pair.get("alarm_1303"):
                pushed += 1
        result.alarm_1303_pushed = pushed
    except Exception as exc:  # noqa: BLE001
        logger.warning("超速会话成对落库失败: %s", exc)
    result.open_sessions = open_session_count()


def _vehicle_category_sets(categories: list[MapRuleCategory]) -> dict[int, set[int]]:
    vehicle_cats: dict[int, set[int]] = {}
    for cat in categories:
        ids = cat.assigned_vehicle_ids if isinstance(cat.assigned_vehicle_ids, list) else []
        for vid in ids:
            try:
                vehicle_cats.setdefault(int(vid), set()).add(int(cat.id))
            except (TypeError, ValueError):
                continue
    return vehicle_cats


def _rule_category_id_set(rule: PrivateMapRule) -> set[int]:
    out: set[int] = set()
    for cid in rule.category_ids if isinstance(rule.category_ids, list) else []:
        try:
            out.add(int(cid))
        except (TypeError, ValueError):
            continue
    return out


def _matched_category_for_vehicle(
    *,
    vehicle_id: int,
    rule: PrivateMapRule,
    vehicle_cats: dict[int, set[int]],
    cat_by_id: dict[int, MapRuleCategory],
) -> MapRuleCategory | None:
    matched = vehicle_cats.get(int(vehicle_id), set()) & _rule_category_id_set(rule)
    if not matched:
        return None
    return cat_by_id.get(next(iter(matched)))


async def backfill_obd_speed_violation_limits(db: AsyncSession) -> dict[str, Any]:
    """按当前限速规则重算 obd_speed 违章 raw_preview.limit_kmh，并清零围栏规则遗留限速。"""
    categories = (await db.execute(select(MapRuleCategory))).scalars().all()
    rules = (await db.execute(select(PrivateMapRule))).scalars().all()
    weather_rows = (await db.execute(select(PrivateMapRuleWeather))).scalars().all()
    weather_by_id = {int(wr.id): wr for wr in weather_rows}
    cat_by_id: dict[int, MapRuleCategory] = {int(c.id): c for c in categories}
    rule_by_id: dict[int, PrivateMapRule] = {int(r.id): r for r in rules}
    vehicle_cats = _vehicle_category_sets(categories)

    rules_cleared = 0
    for rule in rules:
        if (rule.rule_type_code or "").strip().lower() == "speed_rule":
            continue
        if not _rule_category_id_set(rule):
            continue
        if int(rule.speed_limit_kmh or 0) > 0:
            rule.speed_limit_kmh = 0
            rules_cleared += 1

    rows = (
        await db.execute(select(VehicleViolation).where(VehicleViolation.source == SOURCE_OBD_SPEED))
    ).scalars().all()
    updated = 0
    skipped = 0
    for row in rows:
        try:
            preview = json.loads(row.raw_preview or "{}")
        except json.JSONDecodeError:
            skipped += 1
            continue
        if not isinstance(preview, dict):
            skipped += 1
            continue
        try:
            rule_id = int(preview.get("rule_id"))
        except (TypeError, ValueError):
            skipped += 1
            continue
        rule = rule_by_id.get(rule_id)
        if rule is None or not row.vehicle_id:
            skipped += 1
            continue
        cat = _matched_category_for_vehicle(
            vehicle_id=int(row.vehicle_id),
            rule=rule,
            vehicle_cats=vehicle_cats,
            cat_by_id=cat_by_id,
        )
        if cat is None:
            skipped += 1
            continue
        weather_code = str(preview.get("weather_code") or "sunny").strip().lower() or "sunny"
        weather_rule_row = weather_by_id.get(int(cat.weather_rule_id)) if cat.weather_rule_id else None
        new_limit = effective_limit_kmh(rule, cat, weather_code, weather_rule_row=weather_rule_row)
        if int(preview.get("limit_kmh") or 0) == new_limit:
            continue
        preview["limit_kmh"] = new_limit
        row.raw_preview = json.dumps(preview, ensure_ascii=False)[:4000]
        updated += 1

    await db.commit()
    return {
        "total": len(rows),
        "updated": updated,
        "skipped": skipped,
        "fence_rules_cleared": rules_cleared,
    }


# ---------------------------------------------------------------------------
# 调度器
# ---------------------------------------------------------------------------

class ObdSpeedScheduler:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_result: dict[str, Any] | None = None
        self._last_error: str | None = None
        self._last_run_at: datetime | None = None

    @property
    def running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    def status(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "interval_seconds": settings.obd_speed_check_interval_seconds,
            "redis": f"{settings.obd_redis_host}:{settings.obd_redis_port}/{settings.obd_redis_db}",
            "min_speed_kmh": settings.obd_min_speed_kmh,
            "push_1303_enabled": bool(getattr(settings, "obd_speed_push_1303_enabled", True)),
            "session_max_seconds": int(getattr(settings, "obd_speed_session_max_seconds", 900) or 900),
            "open_overspeed_sessions": open_session_count(),
            "open_session_preview": session_status_snapshot()[:10],
            "last_run_at": self._last_run_at.isoformat(sep=" ", timespec="seconds") if self._last_run_at else None,
            "last_result": self._last_result,
            "last_error": self._last_error,
        }

    def start(self, **_kwargs) -> None:
        """启动调度循环（服务启动时默认自动运行）。"""
        if self.running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="obd-speed-check")

    async def stop(self, **_kwargs) -> None:
        """停止当前会话的调度循环（服务重启后会再次自动启动）。"""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        try:
            pending = take_all_overspeed_sessions(reason="scheduler_stop")
            local_n = 0
            alarm_n = 0
            for session, reason in pending:
                pair = await _persist_session_pair(session, reason=reason)
                if pair.get("local"):
                    local_n += 1
                if pair.get("alarm_1303"):
                    alarm_n += 1
            if pending:
                logger.info(
                    "OBD 调度停止：未结束会话成对落库 local=%s 1303=%s total=%s",
                    local_n,
                    alarm_n,
                    len(pending),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("OBD 调度停止时落库超速会话失败: %s", exc)

    async def run_once(self) -> ObdSyncResult:
        result = await run_obd_speed_check_once()
        self._last_run_at = china_now_naive()
        self._last_result = {k: v for k, v in result.__dict__.items() if k != "detail"} | {
            "detail": result.detail[:20]
        }
        self._last_error = result.error
        if result.violations_inserted:
            logger.info(
                "OBD 时速监测：本轮新增违章 %s 条（扫描 %s Key，有效读数 %s）",
                result.violations_inserted,
                result.scanned_keys,
                result.parsed,
            )
        return result

    async def _loop(self) -> None:
        logger.info("OBD 时速违章监测调度已启动")
        while self._running:
            try:
                await self.run_once()
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("OBD 时速违章监测执行失败: %s", exc)
            await asyncio.sleep(max(10, int(settings.obd_speed_check_interval_seconds)))


obd_speed_scheduler = ObdSpeedScheduler()

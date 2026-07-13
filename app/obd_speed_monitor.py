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
7. 超过生效限速 → 写 vehicle_violation（source=obd_speed），
   按 车辆+规则+时间桶 幂等去重，避免持续超速每轮重复入库
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from collections import deque

from app.amap_grasp_road import GraspTrailPoint, grasp_road_with_keys
from app.amap_regeo import resolve_address_wgs84
from app.config import settings
from app.database import AsyncSessionLocal
from app.geo_utils import geometry_hit, wgs84_to_gcj02
from app.header_weather import _fetch_weather_by_coords
from app.jt808_alarm_sync import _vehicle_by_terminal
from app.jt808_openapi_client import jt808_openapi_client
from app.map_rule_weather import effective_limit_kmh, weather_text_to_type_code
from app.models import (
    MapRuleCategory,
    PrivateMapRule,
    PrivateMapRuleWeather,
    Vehicle,
    VehicleLocation,
    VehicleViolation,
)
from app.timeutil import china_now_naive
from app.violation_alert_cache import push_violation_alert, violation_alert_payload
from app.violation_risk import derive_risk_level

logger = logging.getLogger(__name__)

SOURCE_OBD_SPEED = "obd_speed"


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
    """
    if h.is_self_drawn:
        return 0 if h.is_speed_rule else 2
    return 1 if h.is_speed_rule else 3


def arbitrate(hits: list[RuleHit]) -> RuleHit | None:
    """重叠命中时挑一条生效规则：

    1. 四档线性：纯私有折线 > 继承集团折线 > 纯私有范围 > 继承集团范围
    2. 同档多条命中时取限速最严（数值最小）的一条
    """
    if not hits:
        return None
    return min(
        hits,
        key=lambda h: (rule_priority_rank(h), h.limit_kmh),
    )


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
    error: str | None = None
    detail: list[dict[str, Any]] = field(default_factory=list)


def _external_id(vehicle_id: int, rule_id: int, bucket: str) -> str:
    return f"obd_speed:{vehicle_id}:{rule_id}:{bucket}"


def _cooldown_bucket(at: datetime) -> str:
    """5 分钟桶：同一车辆+规则在该桶内只入库一条。"""
    return f"{at.strftime('%Y%m%d%H')}{at.minute // 5}"


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

    if not readings:
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
            return result

        # 车辆坐标：OpenAPI 1201 优先，vehicle_location 快照兜底
        positions = await _fetch_positions(list(vehicle_by_device.keys()))
        rule_index = await _load_rule_index(db)
        cooldown_bucket = _cooldown_bucket(now)

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
            if winner is None or reading.speed_kmh <= winner.limit_kmh:
                continue

            ext_id = _external_id(vehicle.id, winner.rule.id, cooldown_bucket)
            exists = await db.scalar(
                select(VehicleViolation.id).where(VehicleViolation.external_alarm_id == ext_id).limit(1)
            )
            if exists:
                continue
            violation_time = reading.report_at or now
            kind = "限速路段超速" if winner.is_speed_rule else "区域超速"
            row = VehicleViolation(
                biz_no=_stable_biz_no(ext_id, violation_time),
                external_alarm_id=ext_id,
                terminal_id=reading.device_no[:32],
                vehicle_id=vehicle.id,
                plate_no=(vehicle.plate_no or "")[:16],
                company_id=vehicle.company_id,
                violation_type_code=None,
                violation_type_name=f"OBD{kind}",
                risk_level=derive_risk_level(f"OBD{kind}"),
                violation_time=violation_time,
                lat=lng_lat[1],
                lng=lng_lat[0],
                address=address[:512],
                source=SOURCE_OBD_SPEED,
                raw_preview=json.dumps(
                    {
                        "rule_id": winner.rule.id,
                        "rule_code": winner.rule.rule_code,
                        "rule_name": winner.rule.rule_name,
                        "rule_type_code": winner.rule.rule_type_code,
                        "is_self_drawn": winner.is_self_drawn,
                        "limit_kmh": winner.limit_kmh,
                        "weather_code": weather_code,
                        "grasp_road_applied": grasp_applied,
                        "match_lng_gcj": round(lng_gcj, 6),
                        "match_lat_gcj": round(lat_gcj, 6),
                        "obd_speed_kmh": reading.speed_kmh,
                        "mileage_km": reading.mileage_km,
                        "obd_raw": reading.raw[:800],
                    },
                    ensure_ascii=False,
                )[:4000],
                status="待处理",
            )
            db.add(row)
            await db.flush()
            push_violation_alert(violation_alert_payload(row))
            result.violations_inserted += 1
            result.detail.append(
                {
                    "plate_no": vehicle.plate_no,
                    "device_no": reading.device_no,
                    "speed": reading.speed_kmh,
                    "limit": winner.limit_kmh,
                    "rule": winner.rule.rule_name,
                }
            )
        await db.commit()
    return result


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

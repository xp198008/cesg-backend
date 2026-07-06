"""OBD 时速监测：定时读 Redis 车辆 OBD 数据，按私有地图规则判定超速违章。

数据链路：
1. 车辆 OBD 上报 → Redis Key ``{设备号}_OBD``（JSON：时速/总里程/时间戳）
2. 定时器 SCAN 读取全部 OBD Key，只处理时速 > 阈值（默认 10 km/h）的车辆
3. 设备号 → CESG 车辆（复用 JT808 同步的设备号变体匹配）
4. 车辆坐标：与实时监控页同源——JT808 OpenAPI 1201 定位接口（WGS84），
   失败时兜底 vehicle_location 快照
5. 规则匹配：车辆 → 规则类别(assigned_vehicle_ids) → 私有规则(category_ids)，
   坐标转 GCJ02 后做几何命中（围栏=点在形内；限速折线=点距折线 <= 缓冲带）
6. 优先级仲裁（用户约定）：
   公司自绘(ref_public_rule_id 为空) > 继承集团公共规则；
   同级别内 限速规则(speed_rule/折线) > 范围规则(fence)
7. 超过生效限速 → 写 vehicle_violation（source=obd_speed），
   按 车辆+规则+时间桶 幂等去重，避免持续超速每轮重复入库
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from app.timeutil import china_now_naive
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal
from app.geo_utils import geometry_hit, wgs84_to_gcj02
from app.jt808_alarm_sync import _vehicle_by_terminal
from app.jt808_openapi_client import jt808_openapi_client
from app.models import (
    MapRuleCategory,
    PrivateMapRule,
    Vehicle,
    VehicleLocation,
    VehicleViolation,
)

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

    @property
    def is_self_drawn(self) -> bool:
        return self.rule.ref_public_rule_id is None

    @property
    def is_speed_rule(self) -> bool:
        return (self.rule.rule_type_code or "").strip().lower() == "speed_rule"


def effective_limit_kmh(rule: PrivateMapRule, category: MapRuleCategory) -> int:
    """规则自身限速优先；为 0 时回落到类别限速。"""
    limit = int(rule.speed_limit_kmh or 0)
    if limit > 0:
        return limit
    return int(category.speed_limit_kmh or 0)


def arbitrate(hits: list[RuleHit]) -> RuleHit | None:
    """按约定优先级挑一条生效规则：

    1. 公司自绘 > 继承公共（ref_public_rule_id 是否为空）
    2. 同级别内：限速规则(speed_rule) > 范围规则(fence)
    3. 同优先级多条命中时取限速最严（数值最小）的一条
    """
    if not hits:
        return None
    return min(
        hits,
        key=lambda h: (
            0 if h.is_self_drawn else 1,
            0 if h.is_speed_rule else 1,
            h.limit_kmh,
        ),
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
    violations_inserted: int = 0
    error: str | None = None
    detail: list[dict[str, Any]] = field(default_factory=list)


def _external_id(vehicle_id: int, rule_id: int, bucket: str) -> str:
    return f"obd_speed:{vehicle_id}:{rule_id}:{bucket}"


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


async def _load_rule_index(db: AsyncSession) -> dict[int, list[RuleHit]]:
    """构建 vehicle_id → 候选规则列表（含类别，限速已解析，未做几何判定）。"""
    categories = (await db.execute(select(MapRuleCategory))).scalars().all()
    rules = (await db.execute(select(PrivateMapRule))).scalars().all()

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
            limit = effective_limit_kmh(rule, cat)
            if limit <= 0:
                continue
            index.setdefault(vid, []).append(RuleHit(rule=rule, category=cat, limit_kmh=limit))
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
        bucket = now.strftime("%Y%m%d%H%M")[: -1]  # 10 分钟桶，同一持续超速不重复入库
        cooldown_bucket = bucket

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
            if lng_lat is None:
                result.skipped_no_position += 1
                continue
            # 坐标过旧同样跳过，避免用停车前的位置误判
            if pos_time is not None and now - pos_time > stale_after:
                result.skipped_no_position += 1
                continue

            lng_gcj, lat_gcj = wgs84_to_gcj02(lng_lat[0], lng_lat[1])
            buffer_m = float(settings.obd_polyline_buffer_m)
            hits = [
                h
                for h in candidates
                if geometry_hit(lng_gcj, lat_gcj, h.rule.draw_shape_type, h.rule.geometry_json, buffer_m)
            ]
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
                        "obd_speed_kmh": reading.speed_kmh,
                        "mileage_km": reading.mileage_km,
                        "obd_raw": reading.raw[:800],
                    },
                    ensure_ascii=False,
                )[:4000],
                status="待处理",
            )
            db.add(row)
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


# ---------------------------------------------------------------------------
# 开关持久化：页面手动启停写入该文件，重启后优先于 .env 生效
# ---------------------------------------------------------------------------

_STATE_FILE = Path(__file__).resolve().parent / "data" / "obd_speed_check.json"


def _load_persisted_enabled() -> bool | None:
    """读取页面保存的开关；文件不存在或格式错误返回 None（回落 .env）。"""
    try:
        data = json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    value = data.get("enabled") if isinstance(data, dict) else None
    return value if isinstance(value, bool) else None


def _persist_enabled(value: bool) -> None:
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(
            json.dumps(
                {"enabled": value, "updated_at": china_now_naive().isoformat(sep=" ", timespec="seconds")},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except OSError as exc:
        logger.warning("OBD 调度开关持久化失败: %s", exc)


# ---------------------------------------------------------------------------
# 调度器（模式与 Jt808AlarmScheduler 一致）
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
        persisted = _load_persisted_enabled()
        auto_start = persisted if persisted is not None else bool(settings.obd_speed_check_enabled)
        return {
            "enabled": auto_start,
            "config_source": "页面配置" if persisted is not None else ".env",
            "running": self.running,
            "interval_seconds": settings.obd_speed_check_interval_seconds,
            "redis": f"{settings.obd_redis_host}:{settings.obd_redis_port}/{settings.obd_redis_db}",
            "min_speed_kmh": settings.obd_min_speed_kmh,
            "last_run_at": self._last_run_at.isoformat(sep=" ", timespec="seconds") if self._last_run_at else None,
            "last_result": self._last_result,
            "last_error": self._last_error,
        }

    def start(self, *, force: bool = False, persist: bool = False) -> None:
        """启动调度循环。

        - force=True：状态页手动启动，忽略开关配置直接启动。
        - persist=True：把"启用"写入配置文件，重启后自动恢复运行。
        - 默认（开机调用）：页面保存的开关优先，其次 .env 的 OBD_SPEED_CHECK_ENABLED。
        """
        if persist:
            _persist_enabled(True)
        if not force:
            persisted = _load_persisted_enabled()
            enabled = persisted if persisted is not None else bool(settings.obd_speed_check_enabled)
            if not enabled:
                logger.info("OBD 时速违章监测未启用")
                return
        if self.running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="obd-speed-check")

    async def stop(self, *, persist: bool = False) -> None:
        if persist:
            _persist_enabled(False)
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

"""智慧看板 Redis 队列消费器：LPOP 三个队列并落库供看板展示。

数据链路：
1. 车辆故障码 / OBD 数据 → Redis 队列（QUEUE_GZM / QUEUE_OBD_YC / QUEUE_OBD_DC）
2. 定时器 LPOP 三个队列，每轮最多取 redis_queue_batch_size 条
3. 故障码 → vehicle_fault_live（设备号 → 车辆匹配，复用 _terminal_variants）
4. OBD 能耗 → obd_energy_snapshot（同设备同日 upsert，避免表膨胀）
5. 智慧看板 board-stats 接口读取两张表汇总展示

字段名未完全确定，解析层沿用 obd_speed_monitor 的多别名兼容模式（_pick），
部署后可用 /api/dashboard/redis-peek 抓样例回填 _XXX_KEYS 别名表。
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal
from app.jt808_alarm_sync import _vehicle_by_terminal
from app.jt808_vehicle import _terminal_variants
from app.models import ObdEnergySnapshot, VehicleFaultLive

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 字段别名表（部署后用 /api/dashboard/redis-peek 抓样例回填）
# ---------------------------------------------------------------------------

_DEVICE_KEYS = (
    "device_no", "deviceId", "deviceNo", "terminal_id", "terminalId",
    "terminelNo", "terminal", "tid", "设备号", "终端号",
)
_PLATE_KEYS = (
    "plate_no", "plateNo", "plate", "cph", "CPH", "车牌", "车牌号",
)
_FAULT_CODE_KEYS = (
    "fault_code", "faultCode", "code", "gzm", "GZM", "faultCode4",
    "alarmCode", "alarm_code", "故障码",
)
_FAULT_LEVEL_KEYS = (
    "level", "fault_level", "faultLevel", "grade", "等级", "级别",
)
_FUEL_KEYS = (
    "oil", "fuel", "fuel_consumption", "fuelConsumption", "油耗", "耗油",
    "ryl", "RYL", "oil_consumption",
    # 注意：QUEUE_OBD_YC 里没有「今日耗油量(L)」字段！
    # jql=进气量(mg/st)、fyjyl=反应剂余量(%)、fdjrlll=燃料流量(L/h)、bclc=本次里程(km)
    # 不可当作油耗累加，日油耗仍走 808 接口 1253/1169。
)
_POWER_KEYS = (
    "power", "electric", "elec", "kwh", "KWH", "电量", "耗电",
    "dl", "DL", "electricity", "energy",
    # 电车 QUEUE_OBD_DC：soc/zdl 等，待有数据后补充
    "soc", "zdl",
)
_OBD_FLOW_KEYS = ("fdjrlll", "FDJRLLL")  # 发动机燃料流量 L/h，用于积分估算日油耗
_MILEAGE_KEYS = (
    # OBD 油车：bclc=本次里程(km)，用于看板「今日行驶」；zlc=总里程不可多车相加
    "bclc",
    "mileage", "lc", "LC", "licheng", "totalMileage", "total_mileage",
    "odometer", "里程", "总里程",
    "zlc",
)
_TS_KEYS = (
    "ts", "timestamp", "time", "gpstime", "gpsTime", "report_time",
    "reportTime", "时间戳", "时间",
)


def _pick(data: dict, keys: tuple[str, ...]) -> Any:
    for k in keys:
        if k in data and data[k] is not None and data[k] != "":
            return data[k]
    return None


def _parse_ts(raw: Any) -> datetime | None:
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


def _to_float(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v != v or v in (float("inf"), float("-inf")):  # NaN / inf
        return None
    return v


def _normalize_fault_level(raw: Any) -> str | None:
    """把各种等级表达归一化到 高/中/低；无法识别时原样返回字符串。"""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    low = s.lower()
    if low in ("高", "high", "1", "一级", "严重", "critical"):
        return "高"
    if low in ("中", "mid", "middle", "2", "二级", "一般", "normal"):
        return "中"
    if low in ("低", "low", "3", "三级", "轻微", "minor"):
        return "低"
    return s[:16]


# ---------------------------------------------------------------------------
# Redis 客户端（复用 obd_redis_* 连接参数）
# ---------------------------------------------------------------------------

def _new_redis():
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


# ---------------------------------------------------------------------------
# 落库
# ---------------------------------------------------------------------------

async def _resolve_vehicle(db: AsyncSession, device_no: str):
    """设备号 → (vehicle_id, plate_no, company_id)；找不到返回 (None, None, None)。"""
    if not device_no:
        return None, None, None
    try:
        vehicle = await _vehicle_by_terminal(db, device_no)
    except Exception as exc:  # noqa: BLE001
        logger.debug("vehicle_by_terminal failed for %s: %s", device_no, exc)
        return None, None, None
    if vehicle is None:
        return None, None, None
    return vehicle.id, vehicle.plate_no, getattr(vehicle, "company_id", None)


async def _handle_fault(db: AsyncSession, data: dict, raw_text: str) -> None:
    device_no = _pick(data, _DEVICE_KEYS)
    plate_raw = _pick(data, _PLATE_KEYS)
    report_time = _parse_ts(_pick(data, _TS_KEYS)) or datetime.now()
    fault_code = _pick(data, _FAULT_CODE_KEYS)
    fault_level = _normalize_fault_level(_pick(data, _FAULT_LEVEL_KEYS))

    vehicle_id, plate_from_vehicle, company_id = await _resolve_vehicle(db, str(device_no or ""))
    plate_no = str(plate_raw or plate_from_vehicle or "") or None

    row = VehicleFaultLive(
        device_no=str(device_no) if device_no is not None else None,
        plate_no=plate_no,
        vehicle_id=vehicle_id,
        company_id=company_id,
        fault_code=str(fault_code) if fault_code is not None else None,
        fault_level=fault_level,
        report_time=report_time,
        handled=False,
        raw=raw_text[:2000],
    )
    db.add(row)
    await db.flush()


def _flow_from_raw(raw: str | None) -> float | None:
    if not raw:
        return None
    try:
        return _to_float(json.loads(raw).get("fdjrlll"))
    except (json.JSONDecodeError, TypeError, AttributeError):
        return None


def _accumulate_oil_fuel(
    existing: ObdEnergySnapshot | None,
    new_flow: float | None,
    report_time: datetime,
) -> float | None:
    """用 fdjrlll(L/h) 对相邻两条 OBD 采样做梯形积分，估算当日累计油耗(L)。"""
    if new_flow is None:
        return float(existing.fuel) if existing and existing.fuel is not None else None
    acc = float(existing.fuel or 0) if existing else 0.0
    if existing and existing.report_time and report_time > existing.report_time:
        old_flow = _flow_from_raw(existing.raw)
        dt_h = (report_time - existing.report_time).total_seconds() / 3600
        dt_h = min(max(dt_h, 0.0), 2.0)  # 间隔过大视为离线，避免一次跳变
        if dt_h > 0:
            avg = ((old_flow if old_flow is not None else new_flow) + new_flow) / 2
            acc += avg * dt_h
    return round(acc, 3)


async def _handle_obd(db: AsyncSession, data: dict, raw_text: str, energy_type: str) -> None:
    device_no = _pick(data, _DEVICE_KEYS)
    report_time = _parse_ts(_pick(data, _TS_KEYS)) or datetime.now()
    day = report_time.strftime("%Y%m%d")

    existing = None
    if device_no is not None:
        existing = (
            await db.execute(
                select(ObdEnergySnapshot).where(
                    ObdEnergySnapshot.device_no == str(device_no),
                    ObdEnergySnapshot.day == day,
                    ObdEnergySnapshot.energy_type == energy_type,
                )
            )
        ).scalar_one_or_none()

    if energy_type == "oil":
        explicit = _to_float(_pick(data, _FUEL_KEYS))
        flow = _to_float(_pick(data, _OBD_FLOW_KEYS))
        fuel = explicit if explicit is not None else _accumulate_oil_fuel(existing, flow, report_time)
    else:
        fuel = _to_float(_pick(data, _POWER_KEYS))
    mileage = _to_float(_pick(data, _MILEAGE_KEYS))

    # 同设备同日同类型 upsert（SQLite ON CONFLICT）
    values = {
        "device_no": str(device_no) if device_no is not None else None,
        "energy_type": energy_type,
        "fuel": fuel,
        "mileage": mileage,
        "report_time": report_time,
        "day": day,
        "raw": raw_text[:2000],
        "created_at": datetime.utcnow(),
    }
    stmt = sqlite_insert(ObdEnergySnapshot).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["device_no", "day", "energy_type"],
        set_={
            "fuel": stmt.excluded.fuel,
            "mileage": stmt.excluded.mileage,
            "report_time": stmt.excluded.report_time,
            "raw": stmt.excluded.raw,
            "created_at": stmt.excluded.created_at,
        },
    )
    await db.execute(stmt)
    await db.flush()


async def _purge_old_faults(db: AsyncSession) -> int:
    cutoff = datetime.utcnow() - timedelta(hours=int(settings.redis_queue_fault_ttl_hours))
    result = await db.execute(
        delete(VehicleFaultLive).where(VehicleFaultLive.created_at < cutoff)
    )
    deleted = result.rowcount or 0
    if deleted:
        await db.commit()
    return deleted


# ---------------------------------------------------------------------------
# 单轮消费
# ---------------------------------------------------------------------------

async def consume_once() -> dict[str, Any]:
    """LPOP 三个队列各最多 redis_queue_batch_size 条，落库后返回统计。"""
    stats = {"gzm": 0, "obd_yc": 0, "obd_dc": 0, "errors": 0, "purged": 0, "error": None}
    redis = _new_redis()
    try:
        await redis.ping()
    except Exception as exc:  # noqa: BLE001
        stats["error"] = f"redis connect failed: {exc}"
        logger.warning("Redis 队列消费器连接失败: %s", exc)
        try:
            await redis.aclose()
        except Exception:  # noqa: BLE001
            pass
        return stats

    queues = (
        (settings.redis_queue_gzm, "gzm", None),
        (settings.redis_queue_obd_yc, "obd_yc", "oil"),
        (settings.redis_queue_obd_dc, "obd_dc", "ev"),
    )
    batch = max(1, int(settings.redis_queue_batch_size))

    async with AsyncSessionLocal() as db:
        for qname, stat_key, etype in queues:
            count = 0
            for _ in range(batch):
                try:
                    raw = await redis.lpop(qname)
                except Exception as exc:  # noqa: BLE001
                    stats["errors"] += 1
                    logger.warning("LPOP %s 失败: %s", qname, exc)
                    break
                if raw is None:
                    break
                text = raw if isinstance(raw, str) else raw.decode("utf-8", "ignore")
                try:
                    data = json.loads(text)
                except (TypeError, ValueError):
                    stats["errors"] += 1
                    continue
                if not isinstance(data, dict):
                    stats["errors"] += 1
                    continue
                try:
                    if etype is None:
                        await _handle_fault(db, data, text)
                    else:
                        await _handle_obd(db, data, text, etype)
                    count += 1
                except Exception as exc:  # noqa: BLE001
                    stats["errors"] += 1
                    logger.warning("处理 %s 队列消息失败: %s", qname, exc)
            stats[stat_key] = count
        try:
            await db.commit()
        except Exception as exc:  # noqa: BLE001
            stats["error"] = f"commit failed: {exc}"
            logger.warning("Redis 队列消费器提交失败: %s", exc)
            await db.rollback()
        else:
            stats["purged"] = await _purge_old_faults(db)

    try:
        await redis.aclose()
    except Exception:  # noqa: BLE001
        pass
    return stats


# ---------------------------------------------------------------------------
# 调度器（模式与 ObdSpeedScheduler 一致）
# ---------------------------------------------------------------------------

class RedisQueueScheduler:
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
            "enabled": bool(settings.redis_queue_enabled),
            "running": self.running,
            "interval_seconds": int(settings.redis_queue_interval_seconds),
            "batch_size": int(settings.redis_queue_batch_size),
            "fault_ttl_hours": int(settings.redis_queue_fault_ttl_hours),
            "redis": f"{settings.obd_redis_host}:{settings.obd_redis_port}/{settings.obd_redis_db}",
            "queues": {
                "gzm": settings.redis_queue_gzm,
                "obd_yc": settings.redis_queue_obd_yc,
                "obd_dc": settings.redis_queue_obd_dc,
            },
            "last_run_at": self._last_run_at.isoformat(sep=" ", timespec="seconds") if self._last_run_at else None,
            "last_result": self._last_result,
            "last_error": self._last_error,
        }

    def start(self, *, force: bool = False) -> None:
        if not force and not bool(settings.redis_queue_enabled):
            logger.info("Redis 队列消费器未启用（redis_queue_enabled=False）")
            return
        if self.running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="redis-queue-consumer")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_once(self) -> dict[str, Any]:
        result = await consume_once()
        self._last_run_at = datetime.now()
        self._last_result = result
        self._last_error = result.get("error")
        if any((result.get("gzm", 0), result.get("obd_yc", 0), result.get("obd_dc", 0))):
            logger.info(
                "Redis 队列消费：故障 %s / 油车OBD %s / 电车OBD %s，清理过期 %s",
                result.get("gzm", 0),
                result.get("obd_yc", 0),
                result.get("obd_dc", 0),
                result.get("purged", 0),
            )
        return result

    async def _loop(self) -> None:
        logger.info("Redis 队列消费调度已启动")
        while self._running:
            try:
                await self.run_once()
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("Redis 队列消费执行失败: %s", exc)
            await asyncio.sleep(max(3, int(settings.redis_queue_interval_seconds)))


redis_queue_scheduler = RedisQueueScheduler()


# ---------------------------------------------------------------------------
# 只读 peek（供 /api/dashboard/redis-peek 调试接口与字段校准）
# ---------------------------------------------------------------------------

_PEEK_ALLOWED = {
    settings.redis_queue_gzm,
    settings.redis_queue_obd_yc,
    settings.redis_queue_obd_dc,
}


async def peek_queue(key: str, count: int = 3) -> dict[str, Any]:
    """LRANGE 0 count-1，**不移除数据**，用于部署后看真实字段。"""
    info: dict[str, Any] = {"key": key, "count": count, "samples": [], "error": None}
    if key not in _PEEK_ALLOWED:
        info["error"] = "key not allowed"
        return info
    count = max(1, min(int(count or 3), 20))
    redis = _new_redis()
    try:
        await redis.ping()
        try:
            llen = await redis.llen(key)
        except Exception as exc:  # noqa: BLE001
            info["error"] = f"llen failed: {exc}"
            return info
        info["llen"] = llen
        try:
            items = await redis.lrange(key, 0, count - 1)
        except Exception as exc:  # noqa: BLE001
            info["error"] = f"lrange failed: {exc}"
            return info
        for item in items:
            text = item if isinstance(item, str) else item.decode("utf-8", "ignore")
            entry: dict[str, Any] = {"raw": text[:1000]}
            try:
                entry["parsed"] = json.loads(text)
            except (TypeError, ValueError):
                entry["parsed"] = None
            info["samples"].append(entry)
    except Exception as exc:  # noqa: BLE001
        info["error"] = f"redis connect failed: {exc}"
    finally:
        try:
            await redis.aclose()
        except Exception:  # noqa: BLE001
            pass
    return info

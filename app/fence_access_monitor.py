"""禁入 / 禁出围栏报警。

地图规则的道路类型为「禁入范围」或「禁出范围」，并且车辆通过规则类别挂到该规则上时：
- 禁入范围：车从范围外进入范围内 → 禁入报警
- 禁出范围：车从范围内驶出范围外 → 禁出报警

坐标与电子围栏一致（定位 WGS84，围栏几何 GCJ02）。首次见到某车与某范围的组合只记状态，不补历史报警。
报警无现场照片，处理页按 OBD 超速的数据面板展示。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.alarm_type_gate import (
    evaluate_alarm_type_ingest,
    load_alarm_type_by_name,
    log_alarm_type_gate,
    risk_level_from_alarm_type,
)
from app.database import AsyncSessionLocal
from app.geo_utils import geometry_hit, wgs84_to_gcj02
from app.jt808_violation_sync import lookup_company_name, notify_violation_created
from app.models import AlarmTypeDict, FenceAccessState, MapRuleCategory, PrivateMapRule, Vehicle, VehicleLocation, VehicleViolation
from app.timeutil import china_now_naive

logger = logging.getLogger(__name__)

SOURCE_FENCE_ACCESS = "fence_access"
ROAD_TYPE_ENTER = "禁入范围"
ROAD_TYPE_EXIT = "禁出范围"
ALARM_ENTER = "禁入报警"
ALARM_EXIT = "禁出报警"
_FENCE_SHAPES = frozenset({"circle", "rectangle", "polygon"})
_STALE_SECONDS = 180
_INTERVAL_MINUTES = 15


@dataclass
class FenceAccessRunResult:
    rules: int = 0
    vehicles: int = 0
    checked: int = 0
    entered: int = 0
    exited: int = 0
    inserted: int = 0
    baselined: int = 0
    skipped_stale: int = 0
    blocked: int = 0
    errors: int = 0
    error: str | None = None
    detail: list[dict[str, Any]] = field(default_factory=list)


def _stable_biz_no(external_id: str, violation_time: datetime) -> str:
    digest = hashlib.md5(external_id.encode("utf-8")).hexdigest()[:8].upper()  # noqa: S324
    return f"WZ{violation_time.strftime('%Y%m%d%H%M%S')}{digest}"


def _as_int_set(raw: Any) -> set[int]:
    out: set[int] = set()
    if not isinstance(raw, list):
        return out
    for item in raw:
        try:
            out.add(int(item))
        except (TypeError, ValueError):
            continue
    return out


async def _ensure_alarm_type(db: AsyncSession, type_name: str, description: str, code_suffix: str) -> AlarmTypeDict:
    row = await load_alarm_type_by_name(db, type_name)
    if row is not None:
        return row
    stamp = china_now_naive().strftime("%Y%m%d%H%M%S")
    row = AlarmTypeDict(
        type_code=f"AT{stamp}{code_suffix}",
        type_name=type_name,
        description=description,
        alarm_level="中级",
        safety_level="中",
        min_interval_minutes=_INTERVAL_MINUTES,
        status="启用",
        data_source=SOURCE_FENCE_ACCESS,
        ttx_atp_code=None,
    )
    db.add(row)
    await db.flush()
    logger.info("已自动补齐报警类型: %s", type_name)
    return row


def _bindings(rules: list[PrivateMapRule], categories: list[MapRuleCategory]) -> dict[tuple[int, int], tuple[PrivateMapRule, str, MapRuleCategory]]:
    """(vehicle_id, rule_id) → (规则, enter|exit, 类别)。"""
    cat_by_id = {int(cat.id): cat for cat in categories if getattr(cat, "id", None) is not None}
    out: dict[tuple[int, int], tuple[PrivateMapRule, str, MapRuleCategory]] = {}
    for rule in rules:
        road = str(getattr(rule, "road_type_name", "") or "").strip()
        if road == ROAD_TYPE_ENTER:
            kind = "enter"
        elif road == ROAD_TYPE_EXIT:
            kind = "exit"
        else:
            continue
        shape = str(getattr(rule, "draw_shape_type", "") or "").strip().lower()
        if shape not in _FENCE_SHAPES:
            continue
        matched_cats = [cat_by_id[cid] for cid in _as_int_set(rule.category_ids) if cid in cat_by_id]
        if not matched_cats:
            continue
        for cat in matched_cats:
            for vid in _as_int_set(cat.assigned_vehicle_ids):
                key = (vid, int(rule.id))
                out.setdefault(key, (rule, kind, cat))
    return out


async def run_fence_access_once() -> FenceAccessRunResult:
    result = FenceAccessRunResult()
    now = china_now_naive()
    stale_before = now - timedelta(seconds=_STALE_SECONDS)
    try:
        async with AsyncSessionLocal() as db:
            await _ensure_alarm_type(db, ALARM_ENTER, "车辆进入禁入范围", "FE1")
            await _ensure_alarm_type(db, ALARM_EXIT, "车辆驶出禁出范围", "FX1")
            rules = (await db.execute(select(PrivateMapRule))).scalars().all()
            categories = (await db.execute(select(MapRuleCategory))).scalars().all()
            links = _bindings(list(rules), list(categories))
            result.rules = len({rule_id for _, rule_id in links})
            vehicle_ids = {vid for vid, _ in links}
            result.vehicles = len(vehicle_ids)
            if not links:
                await db.commit()
                return result

            locations = (
                await db.execute(select(VehicleLocation).where(VehicleLocation.vehicle_id.in_(list(vehicle_ids))))
            ).scalars().all()
            loc_by_vehicle = {int(loc.vehicle_id): loc for loc in locations if loc.vehicle_id is not None}
            states = (
                await db.execute(
                    select(FenceAccessState).where(FenceAccessState.vehicle_id.in_(list(vehicle_ids)))
                )
            ).scalars().all()
            state_by_key = {(int(row.vehicle_id), int(row.rule_id)): row for row in states}
            vehicles = (
                await db.execute(select(Vehicle).where(Vehicle.id.in_(list(vehicle_ids))))
            ).scalars().all()
            vehicle_by_id = {int(row.id): row for row in vehicles}

            for (vid, rule_id), (rule, kind, cat) in links.items():
                loc = loc_by_vehicle.get(vid)
                if loc is None or loc.lng is None or loc.lat is None:
                    result.skipped_stale += 1
                    continue
                pos_time = loc.pos_time
                if pos_time is not None and pos_time < stale_before:
                    result.skipped_stale += 1
                    continue
                try:
                    lng_gcj, lat_gcj = wgs84_to_gcj02(float(loc.lng), float(loc.lat))
                except (TypeError, ValueError):
                    result.skipped_stale += 1
                    continue
                geom = rule.geometry_json if isinstance(rule.geometry_json, dict) else {}
                inside = geometry_hit(lng_gcj, lat_gcj, str(rule.draw_shape_type or ""), geom, 0.0)
                result.checked += 1
                state = state_by_key.get((vid, rule_id))
                if state is None:
                    state = FenceAccessState(vehicle_id=vid, rule_id=rule_id, inside=inside, updated_at=now)
                    db.add(state)
                    state_by_key[(vid, rule_id)] = state
                    result.baselined += 1
                    continue
                was_inside = bool(state.inside)
                if was_inside == inside:
                    continue
                state.inside = inside
                state.updated_at = now
                entered = kind == "enter" and inside and not was_inside
                exited = kind == "exit" and was_inside and not inside
                if entered:
                    result.entered += 1
                elif exited:
                    result.exited += 1
                else:
                    continue
                alarm_name = ALARM_ENTER if entered else ALARM_EXIT
                alarm_time = pos_time or now
                vehicle = vehicle_by_id.get(vid)
                gate = await evaluate_alarm_type_ingest(
                    db,
                    type_name=alarm_name,
                    vehicle_id=vid,
                    alarm_time=alarm_time,
                )
                if not gate.get("allow"):
                    result.blocked += 1
                    log_alarm_type_gate(
                        source=SOURCE_FENCE_ACCESS,
                        external_id=f"fence:{kind}:{vid}:{rule_id}",
                        alarm_type_name=alarm_name,
                        reason=str(gate.get("reason") or "blocked"),
                        plate=str(getattr(vehicle, "plate_no", "") or loc.plate_no or ""),
                        interval_minutes=getattr(gate.get("alarm_type"), "min_interval_minutes", None),
                    )
                    continue
                ext_id = f"fence:{kind}:{vid}:{rule_id}:{alarm_time.strftime('%Y%m%d%H%M%S')}"
                exists = await db.scalar(
                    select(VehicleViolation.id).where(VehicleViolation.external_alarm_id == ext_id).limit(1)
                )
                if exists is not None:
                    continue
                alarm_row = gate.get("alarm_type")
                company_id = getattr(vehicle, "company_id", None) or loc.company_id
                company_name = await lookup_company_name(db, company_id)
                speed = loc.speed
                row = VehicleViolation(
                    biz_no=_stable_biz_no(ext_id, alarm_time),
                    external_alarm_id=ext_id[:128],
                    terminal_id=str(loc.terminal_id or "")[:32] or "0",
                    vehicle_id=vid,
                    plate_no=str(getattr(vehicle, "plate_no", "") or loc.plate_no or "")[:16],
                    company_id=company_id,
                    company_name=company_name,
                    violation_type_code=None,
                    violation_type_name=alarm_name,
                    risk_level=str(risk_level_from_alarm_type(alarm_row, alarm_name)),
                    violation_time=alarm_time,
                    lat=lat_gcj,
                    lng=lng_gcj,
                    address=(loc.current_position or "")[:512] or None,
                    source=SOURCE_FENCE_ACCESS,
                    private_rule_name=str(rule.rule_name or "")[:200] or None,
                    rule_category_name=str(getattr(cat, "type_name", "") or "")[:128] or None,
                    raw_preview=json.dumps(
                        {
                            "kind": kind,
                            "road_type_name": str(rule.road_type_name or ""),
                            "rule_id": int(rule.id),
                            "rule_name": str(rule.rule_name or ""),
                            "rule_category_name": str(getattr(cat, "type_name", "") or ""),
                            "action": "进入禁入范围" if entered else "驶出禁出范围",
                            "speed_kmh": speed,
                            "address": loc.current_position or "",
                            "lng": lng_gcj,
                            "lat": lat_gcj,
                        },
                        ensure_ascii=False,
                    ),
                    status="待处理",
                )
                db.add(row)
                await db.flush()
                await notify_violation_created(db, row)
                result.inserted += 1
                if len(result.detail) < 20:
                    result.detail.append(
                        {
                            "plate": row.plate_no,
                            "alarm": alarm_name,
                            "rule": row.private_rule_name,
                        }
                    )
            await db.commit()
    except Exception as exc:  # noqa: BLE001
        result.errors += 1
        result.error = str(exc)
        logger.warning("禁入/禁出围栏扫描失败: %s", exc)
    return result


class FenceAccessScheduler:
    def __init__(self) -> None:
        self._running = False
        self._task: asyncio.Task | None = None
        self._last_run_at: datetime | None = None
        self._last_error: str | None = None
        self._last_result: dict[str, Any] = {}

    @property
    def running(self) -> bool:
        return self._running

    def status(self) -> dict[str, Any]:
        return {
            "running": self._running,
            "last_run_at": self._last_run_at.isoformat(sep=" ", timespec="seconds") if self._last_run_at else None,
            "last_error": self._last_error,
            "last_result": self._last_result,
        }

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="fence-access-scheduler")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_once(self) -> dict[str, Any]:
        result = await run_fence_access_once()
        self._last_run_at = china_now_naive()
        payload = {
            "rules": result.rules,
            "vehicles": result.vehicles,
            "checked": result.checked,
            "entered": result.entered,
            "exited": result.exited,
            "inserted": result.inserted,
            "baselined": result.baselined,
            "skipped_stale": result.skipped_stale,
            "blocked": result.blocked,
            "errors": result.errors,
            "error": result.error,
            "detail": result.detail,
        }
        self._last_result = payload
        self._last_error = result.error
        if result.inserted or result.error:
            logger.info(
                "禁入/禁出扫描：规则%s 车辆%s 判定%s 进入%s 驶出%s 入库%s 建档%s",
                result.rules,
                result.vehicles,
                result.checked,
                result.entered,
                result.exited,
                result.inserted,
                result.baselined,
            )
        return payload

    async def _loop(self) -> None:
        logger.info("禁入/禁出围栏报警调度已启动")
        while self._running:
            try:
                await self.run_once()
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("禁入/禁出围栏报警执行失败: %s", exc)
            await asyncio.sleep(30)


fence_access_scheduler = FenceAccessScheduler()

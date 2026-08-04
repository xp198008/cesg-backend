"""快捷桌面看板指标（仅 CESG 业务库，808 平台数据由前端用登录 token 调用）。"""
from __future__ import annotations

from datetime import datetime, timedelta

from app.timeutil import china_now_naive

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    AlarmTypeDict,
    Driver,
    ManualFaultReport,
    ObdEnergySnapshot,
    OrgCompany,
    Vehicle,
    VehicleFaultLive,
    VehicleLocation,
    VehicleViolation,
)
from app.org_scope import collect_org_company_subtree_ids, require_x_org_id_header, wants_org_tree_scope
from app.alarm_type_gate import load_disabled_alarm_type_names
from app.jt808_alarm_sync import _strip_alarm_level_suffix
from app.violation_filters import violation_list_visibility

# docs/ico：1=高危红 / 2=中危黄 / 3=低危绿，对应 alarm_type_dict.safety_level
_SAFETY_TO_ICON_LEVEL = {"高": "1", "中": "2", "低": "3"}
_SAFETY_RANK = {"高": 3, "中": 2, "低": 1}


def _normalize_safety_level(raw: str | None) -> str:
    s = str(raw or "").strip()
    if s in ("高", "高级", "高危", "high"):
        return "高"
    if s in ("低", "低级", "低危", "low"):
        return "低"
    return "中"


def _board_display_type_name(raw_name: str) -> str:
    """剥一级/二级/三级，并去掉尾部「报警/预警」，便于匹配 docs/ico 文件名。"""
    base = _strip_alarm_level_suffix(str(raw_name or "").strip())
    for suffix in ("报警", "预警"):
        if base.endswith(suffix) and len(base) > len(suffix):
            base = base[: -len(suffix)]
    return base.strip() or str(raw_name or "").strip() or "未知类型"


def _pick_higher_safety(a: str, b: str) -> str:
    return a if _SAFETY_RANK.get(a, 0) >= _SAFETY_RANK.get(b, 0) else b


async def _load_enabled_alarm_type_rows(db: AsyncSession) -> list[tuple[str, str]]:
    """启用中的报警类型：(type_name, safety_level 高/中/低)。"""
    rows = (
        await db.execute(
            select(AlarmTypeDict.type_name, AlarmTypeDict.safety_level).where(
                or_(AlarmTypeDict.status.is_(None), AlarmTypeDict.status != "停用")
            )
        )
    ).all()
    out: list[tuple[str, str]] = []
    for type_name, safety_level in rows:
        name = str(type_name or "").strip()
        if not name:
            continue
        out.append((name, _normalize_safety_level(safety_level)))
    return out


def _build_safety_lookup(rows: list[tuple[str, str]]) -> dict[str, str]:
    """精确名 / 剥级别名 / 展示名 → 高/中/低。"""
    lookup: dict[str, str] = {}
    for name, level in rows:
        for key in (name, _strip_alarm_level_suffix(name), _board_display_type_name(name)):
            key = str(key or "").strip()
            if not key:
                continue
            prev = lookup.get(key)
            lookup[key] = level if prev is None else _pick_higher_safety(prev, level)
    return lookup


def _resolve_type_safety(raw_name: str, safety_lookup: dict[str, str]) -> str:
    name = str(raw_name or "").strip()
    if not name:
        return "中"
    for key in (name, _strip_alarm_level_suffix(name), _board_display_type_name(name)):
        if key in safety_lookup:
            return safety_lookup[key]
    return "中"


def _ensure_warning_bucket(buckets: dict[str, dict], display: str, level: str) -> dict:
    bucket = buckets.get(display)
    if bucket is None:
        bucket = {
            "name": display,
            "count": 0,
            "handled": 0,
            "safety_level": level,
            "icon_level": _SAFETY_TO_ICON_LEVEL.get(level, "2"),
        }
        buckets[display] = bucket
        return bucket
    bucket["safety_level"] = _pick_higher_safety(bucket["safety_level"], level)
    bucket["icon_level"] = _SAFETY_TO_ICON_LEVEL.get(bucket["safety_level"], "2")
    return bucket


def _today_iso_range() -> tuple[str, str]:
    now = china_now_naive()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


async def _scoped_company_ids(db: AsyncSession, x_org_id: str | None) -> set[int] | None:
    if not wants_org_tree_scope(False, x_org_id):
        return None
    root = require_x_org_id_header(x_org_id)
    co = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == root).limit(1))
    if co is None:
        return set()
    return await collect_org_company_subtree_ids(db, root)


def _violation_scope_clause(scoped_company_ids: set[int] | None):
    if scoped_company_ids is None:
        return None
    return or_(
        VehicleViolation.company_id.in_(scoped_company_ids),
        VehicleViolation.company_id.is_(None),
    )


async def build_home_stats(db: AsyncSession, x_org_id: str | None) -> dict:
    scoped_company_ids = await _scoped_company_ids(db, x_org_id)
    scope = _violation_scope_clause(scoped_company_ids)
    filter_rules = await load_disabled_alarm_type_names(db)
    visibility = violation_list_visibility(filter_rules)

    pending_q = select(func.count()).select_from(VehicleViolation).where(
        visibility,
        or_(
            VehicleViolation.status == "待处理",
            and_(VehicleViolation.status == "待审核", VehicleViolation.pre_audit_kind == "preprocess"),
        ),
    )
    if scope is not None:
        pending_q = pending_q.where(scope)

    start_iso, end_iso = _today_iso_range()
    completed_q = select(func.count()).select_from(VehicleViolation).where(
        visibility,
        VehicleViolation.status == "已处理",
    )
    try:
        completed_q = completed_q.where(
            VehicleViolation.handled_at >= datetime.fromisoformat(start_iso),
            VehicleViolation.handled_at <= datetime.fromisoformat(end_iso),
        )
    except ValueError:
        completed_q = completed_q.where(VehicleViolation.id == -1)
    if scope is not None:
        completed_q = completed_q.where(scope)

    pending_tasks = int((await db.scalar(pending_q)) or 0)
    today_completed = int((await db.scalar(completed_q)) or 0)

    return {
        "ok": True,
        "pending_tasks": pending_tasks,
        "today_completed": today_completed,
    }


# ---------------------------------------------------------------------------
# 智慧看板（/main/board）聚合指标
# ---------------------------------------------------------------------------

_FAULT_LEVEL_MAP = {"高": "一级故障", "中": "二级故障", "低": "三级故障"}
_HANDLED_VIOLATION_STATUSES = ("已处理", "误报")


def _fmt_dt(value, fmt: str = "%H:%M:%S") -> str:
    if value is None:
        return "—"
    if isinstance(value, str):
        return value[11:19] or value
    try:
        return value.strftime(fmt)
    except Exception:  # noqa: BLE001
        return str(value)


def _day_start(now: datetime) -> datetime:
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


async def _board_vehicles(db: AsyncSession, scoped_company_ids: set[int] | None) -> dict:
    total_q = select(func.count()).select_from(Vehicle)
    online_q = select(func.count()).select_from(VehicleLocation).where(VehicleLocation.is_online.is_(True))
    if scoped_company_ids is not None:
        total_q = total_q.where(Vehicle.company_id.in_(scoped_company_ids))
        online_q = online_q.where(
            or_(
                VehicleLocation.company_id.in_(scoped_company_ids),
                VehicleLocation.company_id.is_(None),
            )
        )
    total = int((await db.scalar(total_q)) or 0)
    online = int((await db.scalar(online_q)) or 0)
    return {"total": total, "online": online}


async def _board_warnings(db: AsyncSession, scope, now: datetime, filter_rules) -> dict:
    day_start = _day_start(now)
    visibility = violation_list_visibility(filter_rules)

    def scoped(q):
        q = q.where(visibility)
        if scope is not None:
            q = q.where(scope)
        return q

    today_total = int(
        (await db.scalar(scoped(
            select(func.count()).select_from(VehicleViolation).where(VehicleViolation.violation_time >= day_start)
        ))) or 0
    )
    today_handled = int(
        (await db.scalar(scoped(
            select(func.count()).select_from(VehicleViolation).where(
                VehicleViolation.violation_time >= day_start,
                VehicleViolation.status.in_(_HANDLED_VIOLATION_STATUSES),
            )
        ))) or 0
    )

    # 顶部 today_total/today_handled 仍是「今日」；
    # 类型条与下方明细按近 7 天；按报警类型字典全量展示，附带 safety_level / icon_level
    type_since = now - timedelta(days=7)
    type_range = "7d"
    alarm_rows = await _load_enabled_alarm_type_rows(db)
    safety_lookup = _build_safety_lookup(alarm_rows)

    buckets: dict[str, dict] = {}
    for type_name, level in alarm_rows:
        display = _board_display_type_name(type_name)
        if display:
            _ensure_warning_bucket(buckets, display, level)

    type_rows = (
        await db.execute(
            scoped(
                select(
                    VehicleViolation.violation_type_name,
                    func.count().label("cnt"),
                    func.sum(
                        case((VehicleViolation.status.in_(_HANDLED_VIOLATION_STATUSES), 1), else_=0)
                    ).label("handled"),
                )
                .where(VehicleViolation.violation_time >= type_since)
                .group_by(VehicleViolation.violation_type_name)
            )
        )
    ).all()

    for r in type_rows:
        raw_name = str(r[0] or "")
        cnt = int(r[1] or 0)
        handled = int(r[2] or 0)
        display = _board_display_type_name(raw_name)
        if not display:
            continue
        level = _resolve_type_safety(raw_name, safety_lookup)
        bucket = _ensure_warning_bucket(buckets, display, level)
        bucket["count"] += cnt
        bucket["handled"] += handled

    types = sorted(
        buckets.values(),
        key=lambda x: (-int(x.get("count") or 0), str(x.get("name") or "")),
    )

    recent_rows = (
        await db.execute(
            scoped(
                select(
                    VehicleViolation.id,
                    VehicleViolation.biz_no,
                    VehicleViolation.violation_time,
                    VehicleViolation.plate_no,
                    VehicleViolation.violation_type_name,
                    VehicleViolation.status,
                )
                .where(VehicleViolation.violation_time >= type_since)
                .order_by(VehicleViolation.violation_time.desc())
                .limit(20)
            )
        )
    ).all()
    recent = [
        {
            "id": r[0],
            "biz_no": r[1] or "",
            "time": _fmt_dt(r[2]),
            "plate_no": r[3] or "—",
            "type_name": r[4] or "未知类型",
            "status": r[5] or "—",
        }
        for r in recent_rows
    ]

    return {
        "today_total": today_total,
        "today_handled": today_handled,
        "types": types,
        "types_range": type_range,
        "recent": recent,
    }


async def _board_faults(db: AsyncSession, scoped_company_ids: set[int] | None) -> dict:
    def scoped(q):
        if scoped_company_ids is not None:
            q = q.where(
                or_(
                    ManualFaultReport.company_id.in_(scoped_company_ids),
                    ManualFaultReport.company_id.is_(None),
                )
            )
        return q

    level_rows = (
        await db.execute(
            scoped(
                select(
                    ManualFaultReport.fault_level,
                    func.count().label("cnt"),
                    func.sum(
                        case(
                            (
                                ManualFaultReport.handle_status.notin_(
                                    ("未处理", "待处理", "待预审", "待审核")
                                ),
                                1,
                            ),
                            else_=0,
                        )
                    ).label("handled"),
                ).group_by(ManualFaultReport.fault_level)
            )
        )
    ).all()
    by_raw = {str(r[0] or "中"): (int(r[1] or 0), int(r[2] or 0)) for r in level_rows}
    levels = []
    for raw, label in _FAULT_LEVEL_MAP.items():
        cnt, handled = by_raw.get(raw, (0, 0))
        levels.append({"level": label, "count": cnt, "handled": handled})

    total = sum(item["count"] for item in levels)
    handled_total = sum(item["handled"] for item in levels)

    recent_rows = (
        await db.execute(
            scoped(
                select(
                    ManualFaultReport.id,
                    ManualFaultReport.biz_no,
                    ManualFaultReport.discovery_time,
                    ManualFaultReport.plate_no,
                    ManualFaultReport.fault_level,
                    ManualFaultReport.handle_status,
                ).order_by(ManualFaultReport.discovery_time.desc()).limit(20)
            )
        )
    ).all()
    recent = [
        {
            "id": r[0],
            "biz_no": r[1] or "",
            "source": "manual",
            "time": _fmt_dt(r[2]),
            "plate_no": r[3] or "—",
            "level": _FAULT_LEVEL_MAP.get(str(r[4] or "中"), "二级故障"),
            "status": "待处理" if (r[5] or "未处理") == "未处理" else str(r[5]),
        }
        for r in recent_rows
    ]

    # 合并 Redis QUEUE_GZM 实时故障（vehicle_fault_live）
    live_levels, live_total, live_recent = await _board_faults_live(db, scoped_company_ids)
    for level_item in levels:
        level_item["count"] += live_levels.get(level_item["level"], 0)
    total += live_total
    # 实时故障按时间倒序合并到 recent 头部，整体截断到 20 条
    recent = live_recent + recent
    if len(recent) > 20:
        recent = recent[:20]

    return {"total": total, "handled": handled_total, "levels": levels, "recent": recent}


async def _board_faults_live(
    db: AsyncSession, scoped_company_ids: set[int] | None
) -> tuple[dict[str, int], int, list[dict]]:
    """从 vehicle_fault_live 取实时故障：返回 (按一级/二级/三级映射后的计数, 总数, 近期列表)。

    live 表 fault_level 已归一化为 高/中/低；映射到 _FAULT_LEVEL_MAP 的标签。
    """
    def scoped(q):
        if scoped_company_ids is not None:
            q = q.where(
                or_(
                    VehicleFaultLive.company_id.in_(scoped_company_ids),
                    VehicleFaultLive.company_id.is_(None),
                )
            )
        return q

    try:
        level_rows = (
            await db.execute(
                scoped(
                    select(
                        VehicleFaultLive.fault_level,
                        func.count().label("cnt"),
                    ).where(VehicleFaultLive.fault_level.is_not(None)).group_by(VehicleFaultLive.fault_level)
                )
            )
        ).all()
    except Exception:  # noqa: BLE001
        level_rows = []
    live_levels: dict[str, int] = {}
    for r in level_rows:
        label = _FAULT_LEVEL_MAP.get(str(r[0] or "中"), "二级故障")
        live_levels[label] = live_levels.get(label, 0) + int(r[1] or 0)
    live_total = sum(live_levels.values())

    try:
        recent_rows = (
            await db.execute(
                scoped(
                    select(
                        VehicleFaultLive.id,
                        VehicleFaultLive.device_no,
                        VehicleFaultLive.report_time,
                        VehicleFaultLive.plate_no,
                        VehicleFaultLive.fault_level,
                        VehicleFaultLive.fault_code,
                        VehicleFaultLive.handled,
                    ).order_by(VehicleFaultLive.report_time.desc()).limit(20)
                )
            )
        ).all()
    except Exception:  # noqa: BLE001
        recent_rows = []
    live_recent = [
        {
            "id": r[0],
            "biz_no": f"SYS{r[0]:08d}" if r[0] else "",
            "source": "live",
            "device_no": r[1] or "",
            "time": _fmt_dt(r[2]),
            "plate_no": r[3] or "—",
            "level": _FAULT_LEVEL_MAP.get(str(r[4] or "中"), "二级故障"),
            "fault_code": r[5] or "",
            "status": "已处理" if r[6] else "待处理",
        }
        for r in recent_rows
    ]
    return live_levels, live_total, live_recent


async def _board_energy(db: AsyncSession, scoped_company_ids: set[int] | None) -> dict:
    """油/电耗统计（OBD 队列落库部分）。

    oil.fuel：OBD fdjrlll(L/h) 积分估算的当日累计油耗；前端优先用 808 1253/1169。
    oil.mileage：各车最新 bclc（本次点火行程）之和，仅供参考，不可作百公里油耗分母；
    百公里油耗由前端用 808 日里程(1121) 计算。
    """
    today = china_now_naive().strftime("%Y%m%d")
    days_7: list[str] = []
    for i in range(6, -1, -1):
        d = china_now_naive() - timedelta(days=i)
        days_7.append(d.strftime("%Y%m%d"))

    async def _agg_one(etype: str) -> dict:
        # 今日：取 today 的快照，按"最新读数"求和（每车当日只留一条 upsert）
        try:
            today_rows = (
                await db.execute(
                    select(ObdEnergySnapshot.fuel, ObdEnergySnapshot.mileage).where(
                        ObdEnergySnapshot.energy_type == etype,
                        ObdEnergySnapshot.day == today,
                    )
                )
            ).all()
        except Exception:  # noqa: BLE001
            today_rows = []
        today_fuel = sum(float(r[0] or 0) for r in today_rows)
        # bclc 为本次点火行程，多车相加不等于今日行驶里程；不据此算 per100
        today_mileage = sum(float(r[1] or 0) for r in today_rows)

        # 近 7 日走势：每日 sum(fuel)
        daily = []
        for d in days_7:
            try:
                row = (
                    await db.execute(
                        select(func.sum(ObdEnergySnapshot.fuel)).where(
                            ObdEnergySnapshot.energy_type == etype,
                            ObdEnergySnapshot.day == d,
                        )
                    )
                ).scalar()
            except Exception:  # noqa: BLE001
                row = None
            label = f"{int(d[4:6])}/{int(d[6:8])}"
            daily.append({"label": label, "fuel": round(float(row or 0), 1)})
        return {
            "today": round(today_fuel, 1) if today_fuel else 0,
            "mileage": round(today_mileage, 1) if today_mileage else 0,
            "per100": None,
            "daily": daily,
        }

    oil = await _agg_one("oil")
    ev = await _agg_one("ev")
    return {"oil": oil, "ev": ev}


async def _board_drivers(db: AsyncSession, scoped_company_ids: set[int] | None) -> dict:
    def scoped(q):
        if scoped_company_ids is not None:
            q = q.where(
                or_(
                    Driver.company_id.in_(scoped_company_ids),
                    Driver.company_id.is_(None),
                )
            )
        return q

    total = int((await db.scalar(scoped(select(func.count()).select_from(Driver)))) or 0)
    scored = int(
        (await db.scalar(scoped(select(func.count()).select_from(Driver).where(Driver.score.isnot(None))))) or 0
    )
    qualified = int(
        (await db.scalar(scoped(select(func.count()).select_from(Driver).where(Driver.score >= 60)))) or 0
    )

    async def rank(order_clause):
        rows = (
            await db.execute(
                scoped(
                    select(Driver.name, OrgCompany.short_name, OrgCompany.name, Driver.score)
                    .join(OrgCompany, OrgCompany.id == Driver.company_id, isouter=True)
                    .where(Driver.score.isnot(None))
                    .order_by(order_clause)
                    .limit(10)
                )
            )
        ).all()
        return [
            {"name": r[0] or "—", "group": r[1] or r[2] or "—", "score": int(r[3] or 0)}
            for r in rows
        ]

    best = await rank(Driver.score.desc())
    worst = await rank(Driver.score.asc())

    qualify_rate = round(qualified * 100 / scored, 1) if scored else None
    return {
        "total": total,
        "scored": scored,
        "qualified": qualified,
        "qualify_rate": qualify_rate,
        "best": best,
        "worst": worst,
    }


async def build_board_stats(db: AsyncSession, x_org_id: str | None) -> dict:
    """智慧看板聚合指标：车辆、AI 预警、故障、司机画像（808 在线/里程由前端调平台接口）。"""
    scoped_company_ids = await _scoped_company_ids(db, x_org_id)
    scope = _violation_scope_clause(scoped_company_ids)
    now = china_now_naive()
    filter_rules = await load_disabled_alarm_type_names(db)

    vehicles = await _board_vehicles(db, scoped_company_ids)
    warnings = await _board_warnings(db, scope, now, filter_rules)
    faults = await _board_faults(db, scoped_company_ids)
    drivers = await _board_drivers(db, scoped_company_ids)
    energy = await _board_energy(db, scoped_company_ids)

    return {
        "ok": True,
        "vehicles": vehicles,
        "warnings": warnings,
        "faults": faults,
        "drivers": drivers,
        "energy": energy,
    }

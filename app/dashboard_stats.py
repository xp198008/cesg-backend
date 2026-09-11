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
# 看板「已处理」只计正式办结；误报不展示、不参与分类与今日汇总
_HANDLED_VIOLATION_STATUSES = ("已处理",)
_FALSE_ALARM_STATUS = "误报"


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
        q = q.where(visibility, VehicleViolation.status != _FALSE_ALARM_STATUS)
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

    # 下方明细：当天待处理（不再截最近几小时 / 近7天最新20条）
    pending_status = or_(
        VehicleViolation.status == "待处理",
        and_(
            VehicleViolation.status == "待审核",
            VehicleViolation.pre_audit_kind == "preprocess",
        ),
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
                .where(
                    VehicleViolation.violation_time >= day_start,
                    pending_status,
                )
                .order_by(VehicleViolation.violation_time.desc())
                .limit(300)
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
        "recent_range": "today_pending",
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
    oil.mileage：OBD 当日累计行驶（bclc 按点火段累加）。现网 1253 未实现、1121 常只有少数车上数，
    前端在 808 油耗缺失时用这组油/里程算百公里，禁止 OBD 全队油 ÷ 残缺的 1121 里程。
    """
    today = china_now_naive().strftime("%Y%m%d")
    days_7: list[str] = []
    for i in range(6, -1, -1):
        d = china_now_naive() - timedelta(days=i)
        days_7.append(d.strftime("%Y%m%d"))

    def _roll_day(rows: list, etype: str) -> dict:
        """按车汇总一日油/电：总量、配对百公里、能耗异常台数。"""
        fuel_sum = 0.0
        mile_sum = 0.0
        paired_fuel = 0.0
        paired_mile = 0.0
        abnormal = 0
        lo, hi = (10.0, 50.0) if etype == "oil" else (8.0, 80.0)
        for fuel, mileage in rows:
            f = float(fuel or 0)
            m = float(mileage or 0)
            if m > 250:
                m = 250.0
            if f > 0:
                fuel_sum += f
            if m > 0:
                mile_sum += m
            # 里程过短时百公里会飞，不进分子也不判异常
            if f > 0 and m >= 20:
                paired_fuel += f
                paired_mile += m
                p100 = f / m * 100.0
                if p100 < lo or p100 > hi:
                    abnormal += 1
        return {
            "fuel": round(fuel_sum, 1) if fuel_sum else 0,
            "mileage": round(mile_sum, 1) if mile_sum else 0,
            "per100": round(paired_fuel / paired_mile * 100.0, 1) if paired_mile > 0 else None,
            "abnormalVehicles": abnormal,
        }

    async def _agg_one(etype: str) -> dict:
        try:
            week_rows = (
                await db.execute(
                    select(
                        ObdEnergySnapshot.day,
                        ObdEnergySnapshot.fuel,
                        ObdEnergySnapshot.mileage,
                    ).where(
                        ObdEnergySnapshot.energy_type == etype,
                        ObdEnergySnapshot.day.in_(days_7),
                    )
                )
            ).all()
        except Exception:  # noqa: BLE001
            week_rows = []

        by_day: dict[str, list] = {d: [] for d in days_7}
        for day, fuel, mileage in week_rows:
            key = str(day or "")
            if key in by_day:
                by_day[key].append((fuel, mileage))

        daily = []
        for d in days_7:
            stats = _roll_day(by_day.get(d) or [], etype)
            daily.append({
                "label": f"{int(d[4:6])}/{int(d[6:8])}",
                "fuel": stats["fuel"],
                "mileage": stats["mileage"],
                "per100": stats["per100"],
                "abnormalVehicles": stats["abnormalVehicles"],
            })
        today_stats = _roll_day(by_day.get(today) or [], etype)
        return {
            "today": today_stats["fuel"],
            "mileage": today_stats["mileage"],
            "per100": today_stats["per100"],
            "abnormalVehicles": today_stats["abnormalVehicles"],
            "daily": daily,
        }

    oil = await _agg_one("oil")
    ev = await _agg_one("ev")
    return {"oil": oil, "ev": ev}


# 近 7 日安全分：满分 100，每条有效违章扣 1 分。driver.score 是手工字段，现网全空，不能当来源。
# 不按安全等级加权：现网报警类型几乎全是「高」，加权后评分榜会塌成全 0。
_DRIVER_QUALIFY_SCORE = 60
# 最好/最差只是排序，不要再截成 10 人：现网司机少，违章最多的人会被「最好」榜裁掉。
_DRIVER_RANK_LIMIT = 100


async def _board_drivers(
    db: AsyncSession,
    scoped_company_ids: set[int] | None,
    *,
    now: datetime,
    filter_rules,
) -> dict:
    """合格司机 / 合格率 / 评分榜：用近 7 日违章（不含误报）给每位司机算安全分。"""

    def scoped_driver(q):
        if scoped_company_ids is not None:
            q = q.where(
                or_(
                    Driver.company_id.in_(scoped_company_ids),
                    Driver.company_id.is_(None),
                )
            )
        return q

    driver_rows = (
        await db.execute(
            scoped_driver(
                select(Driver.id, Driver.name, OrgCompany.short_name, OrgCompany.name).join(
                    OrgCompany, OrgCompany.id == Driver.company_id, isouter=True
                )
            )
        )
    ).all()
    drivers = [
        {
            "id": int(r[0]),
            "name": (r[1] or "").strip() or "—",
            "group": (r[2] or r[3] or "").strip() or "—",
            "deduct": 0,
            "alarms": 0,
        }
        for r in driver_rows
    ]
    by_id = {d["id"]: d for d in drivers}

    veh_rows = (
        await db.execute(select(Vehicle.id, Vehicle.driver_id).where(Vehicle.driver_id.isnot(None)))
    ).all()
    vehicle_to_driver = {
        int(vid): int(did)
        for vid, did in veh_rows
        if vid and did and int(did) in by_id
    }

    if vehicle_to_driver:
        since = now - timedelta(days=7)
        visibility = violation_list_visibility(filter_rules)
        q = (
            select(VehicleViolation.vehicle_id, func.count().label("cnt"))
            .where(
                visibility,
                VehicleViolation.status != _FALSE_ALARM_STATUS,
                VehicleViolation.violation_time >= since,
                VehicleViolation.vehicle_id.in_(list(vehicle_to_driver)),
            )
            .group_by(VehicleViolation.vehicle_id)
        )
        if scoped_company_ids is not None:
            q = q.where(
                or_(
                    VehicleViolation.company_id.in_(scoped_company_ids),
                    VehicleViolation.company_id.is_(None),
                )
            )
        type_rows = (await db.execute(q)).all()
        for vid, cnt in type_rows:
            bucket = by_id.get(vehicle_to_driver.get(int(vid or 0)))
            if not bucket:
                continue
            n = int(cnt or 0)
            bucket["alarms"] += n

    for d in drivers:
        d["score"] = max(0, 100 - int(d["alarms"]))

    total = len(drivers)
    scored = total
    qualified = sum(1 for d in drivers if d["score"] >= _DRIVER_QUALIFY_SCORE)
    qualify_rate = round(qualified * 100 / scored, 1) if scored else None

    def as_row(item: dict) -> dict:
        return {"name": item["name"], "group": item["group"], "score": item["score"]}

    best = [
        as_row(d)
        for d in sorted(drivers, key=lambda x: (-x["score"], x["alarms"], x["name"]))[:_DRIVER_RANK_LIMIT]
    ]
    worst = [
        as_row(d)
        for d in sorted(drivers, key=lambda x: (x["score"], -x["alarms"], x["name"]))[:_DRIVER_RANK_LIMIT]
    ]
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
    drivers = await _board_drivers(db, scoped_company_ids, now=now, filter_rules=filter_rules)
    energy = await _board_energy(db, scoped_company_ids)

    return {
        "ok": True,
        "vehicles": vehicles,
        "warnings": warnings,
        "faults": faults,
        "drivers": drivers,
        "energy": energy,
    }

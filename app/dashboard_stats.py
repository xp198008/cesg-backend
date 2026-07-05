"""快捷桌面看板指标（仅 CESG 业务库，808 平台数据由前端用登录 token 调用）。"""
from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
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
from app.violation_filters import violation_list_visibility


def _today_iso_range() -> tuple[str, str]:
    now = datetime.now()
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

    pending_q = select(func.count()).select_from(VehicleViolation).where(
        violation_list_visibility(),
        or_(
            VehicleViolation.status == "待处理",
            and_(VehicleViolation.status == "待审核", VehicleViolation.pre_audit_kind == "preprocess"),
        ),
    )
    if scope is not None:
        pending_q = pending_q.where(scope)

    start_iso, end_iso = _today_iso_range()
    completed_q = select(func.count()).select_from(VehicleViolation).where(
        violation_list_visibility(),
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


async def _board_warnings(db: AsyncSession, scope, now: datetime) -> dict:
    day_start = _day_start(now)

    def scoped(q):
        q = q.where(violation_list_visibility())
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

    # 分类统计：今日无数据时回退近 7 天，保证看板不空
    type_since = day_start
    type_range = "today"
    if today_total == 0:
        type_since = now - timedelta(days=7)
        type_range = "7d"

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
                .order_by(func.count().desc())
                .limit(4)
            )
        )
    ).all()
    types = [
        {"name": (r[0] or "未知类型"), "count": int(r[1] or 0), "handled": int(r[2] or 0)}
        for r in type_rows
    ]

    recent_rows = (
        await db.execute(
            scoped(
                select(
                    VehicleViolation.violation_time,
                    VehicleViolation.plate_no,
                    VehicleViolation.violation_type_name,
                    VehicleViolation.status,
                ).order_by(VehicleViolation.violation_time.desc()).limit(20)
            )
        )
    ).all()
    recent = [
        {
            "time": _fmt_dt(r[0]),
            "plate_no": r[1] or "—",
            "type_name": r[2] or "未知类型",
            "status": r[3] or "—",
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
                    func.sum(case((ManualFaultReport.handle_status != "未处理", 1), else_=0)).label("handled"),
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
            "time": _fmt_dt(r[0]),
            "plate_no": r[1] or "—",
            "level": _FAULT_LEVEL_MAP.get(str(r[2] or "中"), "二级故障"),
            "status": "待处理" if (r[3] or "未处理") == "未处理" else str(r[3]),
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
                        VehicleFaultLive.report_time,
                        VehicleFaultLive.plate_no,
                        VehicleFaultLive.fault_level,
                        VehicleFaultLive.handled,
                    ).order_by(VehicleFaultLive.report_time.desc()).limit(20)
                )
            )
        ).all()
    except Exception:  # noqa: BLE001
        recent_rows = []
    live_recent = [
        {
            "time": _fmt_dt(r[0]),
            "plate_no": r[1] or "—",
            "level": _FAULT_LEVEL_MAP.get(str(r[2] or "中"), "二级故障"),
            "status": "已处理" if r[3] else "待处理",
        }
        for r in recent_rows
    ]
    return live_levels, live_total, live_recent


async def _board_energy(db: AsyncSession, scoped_company_ids: set[int] | None) -> dict:
    """油/电耗统计。

    oil.mileage 为各车 bclc（本次里程）之和。
    oil.fuel 为 OBD fdjrlll(L/h) 积分估算的当日累计油耗；808 油箱液位(1169)有数据时前端优先展示 808。
    """
    today = datetime.now().strftime("%Y%m%d")
    days_7: list[str] = []
    for i in range(6, -1, -1):
        d = datetime.now() - timedelta(days=i)
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
        today_mileage = sum(float(r[1] or 0) for r in today_rows)
        per100 = None
        if today_mileage > 0 and today_fuel > 0:
            per100 = round((today_fuel / today_mileage) * 100, 1)

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
            "per100": per100,
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
    now = datetime.now()

    vehicles = await _board_vehicles(db, scoped_company_ids)
    warnings = await _board_warnings(db, scope, now)
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

"""车辆分配规则：决定登录用户在 CESG 侧可见的车辆范围。

规则：
- admin：不限制；
- 用户未绑定任何车辆分配规则：可见所属组织树范围内全部车辆；
- 用户已绑定一条或多条规则：仅可见这些规则管控车辆的并集。
  808 监控树在 UI 层按 resolve_monitor_scope 过滤（808 车组权限仍为 company 级）。
"""
from __future__ import annotations

import logging

import pymysql
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.models import SysUser, Vehicle, VehicleAllocRuleUser, VehicleAllocRuleVehicle, VehicleDevice

logger = logging.getLogger(__name__)


def parse_user_id_header(x_user_id: str | None) -> int | None:
    raw = (x_user_id or "").strip()
    if not raw:
        return None
    try:
        uid = int(raw)
    except ValueError:
        return None
    return uid if uid >= 1 else None


def _is_admin_user(user: SysUser) -> bool:
    if (user.username or "").strip().lower() == "admin":
        return True
    role = user.role
    code = (role.code or "").strip().lower() if role else ""
    return code == "admin"


def _effective_user_org_id(user: SysUser) -> int | None:
    if user.org_id is not None:
        return int(user.org_id)
    role = user.role
    if role and role.org_id is not None:
        return int(role.org_id)
    return None


async def _resolve_scope_vehicle_ids(db: AsyncSession, user: SysUser) -> tuple[set[int], bool]:
    """计算监控范围车辆 id。

    返回 (vehicle_ids, unrestricted)：
    - unrestricted=True：admin 全量，scoped 应为 false，但仍返回完整车辆列表供同步使用；
    - unrestricted=False：按分配规则或所属公司子树限制。
    """
    if _is_admin_user(user):
        rows = (await db.execute(select(Vehicle.id))).scalars().all()
        return {int(vid) for vid in rows}, True

    rule_ids = list(
        (
            await db.execute(
                select(VehicleAllocRuleUser.rule_id).where(VehicleAllocRuleUser.user_id == user.id)
            )
        ).scalars().all()
    )
    if rule_ids:
        vehicle_ids = (
            await db.execute(
                select(VehicleAllocRuleVehicle.vehicle_id).where(
                    VehicleAllocRuleVehicle.rule_id.in_(rule_ids)
                )
            )
        ).scalars().all()
        return {int(vid) for vid in vehicle_ids}, False

    org_id = _effective_user_org_id(user)
    if org_id is None:
        return set(), False

    from app.org_scope import collect_org_company_subtree_ids

    subtree = await collect_org_company_subtree_ids(db, org_id)
    rows = (
        await db.execute(select(Vehicle.id).where(Vehicle.company_id.in_(subtree)))
    ).scalars().all()
    return {int(vid) for vid in rows}, False


async def _build_monitor_scope_payload(
    db: AsyncSession,
    vehicle_ids: set[int],
    *,
    unrestricted: bool,
) -> dict[str, object]:
    if not vehicle_ids:
        return {
            "scoped": not unrestricted,
            "plates": [],
            "device_nos": [],
            "car_ids": [],
        }

    plate_rows = (
        await db.execute(select(Vehicle.plate_no).where(Vehicle.id.in_(vehicle_ids)))
    ).scalars().all()
    plates = sorted({(p or "").strip() for p in plate_rows if (p or "").strip()})

    device_nos: set[str] = set()
    device_rows = (
        await db.execute(
            select(VehicleDevice.device_no).where(
                VehicleDevice.vehicle_id.in_(vehicle_ids),
                VehicleDevice.is_main.is_(True),
            )
        )
    ).scalars().all()
    for dev in device_rows:
        device_nos.update(_normalize_device_no(str(dev) if dev is not None else ""))
    device_list = sorted(device_nos)
    car_ids = _lookup_jt808_car_ids(plates, device_list)
    return {
        "scoped": not unrestricted,
        "plates": plates,
        "device_nos": device_list,
        "car_ids": car_ids,
    }


async def resolve_allowed_vehicle_ids(
    db: AsyncSession,
    user_id: int | None,
) -> set[int] | None:
    """返回 None 表示不做车辆 id 限制；返回 set 表示仅可见集合内车辆（可为空）。

    与 monitor-scope / 业务说明对齐：
    - admin：None（不限制）
    - 未绑定分配规则：所属组织树内车辆
    - 已绑定规则：规则管控车辆并集
    """
    if user_id is None:
        return None

    user = await db.scalar(
        select(SysUser)
        .options(selectinload(SysUser.role))
        .where(SysUser.id == user_id)
        .limit(1)
    )
    if user is None or not user.is_active:
        return set()

    # admin 不做车辆限制：避免先全表拉 Vehicle.id 再丢弃
    if _is_admin_user(user):
        return None

    vehicle_ids, unrestricted = await _resolve_scope_vehicle_ids(db, user)
    if unrestricted:
        return None
    return vehicle_ids


async def user_has_vehicle_alloc_rules(db: AsyncSession, user_id: int | None) -> bool:
    """用户是否绑定了车辆分配规则（有规则才需要额外按车过滤）。"""
    if user_id is None:
        return False
    rid = await db.scalar(
        select(VehicleAllocRuleUser.rule_id).where(VehicleAllocRuleUser.user_id == int(user_id)).limit(1)
    )
    return rid is not None


async def resolve_allowed_plate_nos(
    db: AsyncSession,
    user_id: int | None,
) -> set[str] | None:
    """车牌可见范围；None=不限制，空 set=无可见车辆。"""
    allowed_ids = await resolve_allowed_vehicle_ids(db, user_id)
    if allowed_ids is None:
        return None
    if not allowed_ids:
        return set()
    rows = (
        await db.execute(select(Vehicle.plate_no).where(Vehicle.id.in_(allowed_ids)))
    ).scalars().all()
    return {(p or "").strip() for p in rows if (p or "").strip()}


def apply_vehicle_id_scope(query, allowed_ids: set[int] | None):
    """在 SQLAlchemy Vehicle 查询上附加 id 范围限制。"""
    if allowed_ids is None:
        return query
    if not allowed_ids:
        return query.where(Vehicle.id < 0)
    return query.where(Vehicle.id.in_(allowed_ids))


def _normalize_device_no(raw: str | None) -> set[str]:
    """设备号及其去前导零变体，便于与 808 tgps_car.tid 对齐。"""
    s = (raw or "").strip()
    if not s:
        return set()
    out = {s}
    if s.isdigit():
        stripped = s.lstrip("0") or "0"
        out.add(stripped)
        out.add(stripped.zfill(12))
    return out


def _lookup_jt808_plate_car_id_map(plates: list[str]) -> dict[str, int]:
    """车牌 → 808 tgps_car.id（成对返回，避免与 car_ids 列表错位）。"""
    cleaned = [str(p).strip() for p in plates if str(p).strip()]
    if not cleaned:
        return {}
    try:
        conn = pymysql.connect(
            host=settings.jt808_mysql_host,
            port=int(settings.jt808_mysql_port),
            user=settings.jt808_mysql_user,
            password=settings.jt808_mysql_password,
            database=settings.jt808_mysql_database,
            charset="utf8mb4",
            connect_timeout=min(8.0, settings.jt808_sync_timeout),
            read_timeout=10,
            write_timeout=10,
        )
    except Exception as e:  # noqa: BLE001
        logger.debug("监控范围 808 车牌映射查询跳过: %s", e)
        return {}
    try:
        result: dict[str, int] = {}
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(cleaned))
            cur.execute(
                f"select id, carno from tgps_car where carno in ({placeholders})",
                cleaned,
            )
            for cid, carno in cur.fetchall():
                plate = str(carno or "").strip()
                if cid is None or not plate:
                    continue
                result[plate] = int(cid)
        return result
    except Exception as e:  # noqa: BLE001
        logger.debug("监控范围 808 车牌映射查询失败: %s", e)
        return {}
    finally:
        conn.close()


def _lookup_jt808_car_id_plate_map(car_ids: list[int]) -> dict[int, str]:
    """808 car_id → 车牌（仅作风险计数对齐键，不含公司/司机）。"""
    ids = sorted({int(x) for x in car_ids if x})
    if not ids:
        return {}
    try:
        conn = pymysql.connect(
            host=settings.jt808_mysql_host,
            port=int(settings.jt808_mysql_port),
            user=settings.jt808_mysql_user,
            password=settings.jt808_mysql_password,
            database=settings.jt808_mysql_database,
            charset="utf8mb4",
            connect_timeout=min(8.0, settings.jt808_sync_timeout),
            read_timeout=10,
            write_timeout=10,
        )
    except Exception as e:  # noqa: BLE001
        logger.debug("监控范围 808 car_id 反查跳过: %s", e)
        return {}
    try:
        result: dict[int, str] = {}
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(
                f"select id, carno from tgps_car where id in ({placeholders})",
                ids,
            )
            for cid, carno in cur.fetchall():
                plate = str(carno or "").strip()
                if cid is None or not plate:
                    continue
                result[int(cid)] = plate
        return result
    except Exception as e:  # noqa: BLE001
        logger.debug("监控范围 808 car_id 反查失败: %s", e)
        return {}
    finally:
        conn.close()


def _lookup_jt808_car_ids(plates: list[str], device_nos: list[str]) -> list[str]:
    """可选：从 808 MySQL 解析 tgps_car.id，供监控树按 value/deviceId 匹配。"""
    if not plates and not device_nos:
        return []
    ids: set[str] = set()
    plate_map = _lookup_jt808_plate_car_id_map(plates)
    for cid in plate_map.values():
        ids.add(str(int(cid)))
    if not device_nos:
        return sorted(ids)
    try:
        conn = pymysql.connect(
            host=settings.jt808_mysql_host,
            port=int(settings.jt808_mysql_port),
            user=settings.jt808_mysql_user,
            password=settings.jt808_mysql_password,
            database=settings.jt808_mysql_database,
            charset="utf8mb4",
            connect_timeout=min(8.0, settings.jt808_sync_timeout),
            read_timeout=10,
            write_timeout=10,
        )
    except Exception as e:  # noqa: BLE001
        logger.debug("监控范围 808 车辆 id 查询跳过: %s", e)
        return sorted(ids)
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(device_nos))
            cur.execute(
                f"select id from tgps_car where tid in ({placeholders})",
                device_nos,
            )
            for (cid,) in cur.fetchall():
                if cid is not None:
                    ids.add(str(int(cid)))
        return sorted(ids)
    except Exception as e:  # noqa: BLE001
        logger.debug("监控范围 808 车辆 id 查询失败: %s", e)
        return sorted(ids)
    finally:
        conn.close()


async def resolve_monitor_scope(
    db: AsyncSession,
    user_id: int | None,
) -> dict[str, object]:
    """返回实时监控树过滤信息：scoped=True 时前端仅展示允许车辆。

    无论是否 admin，均返回 plates/device_nos/car_ids 实际列表，避免调用方把空数组误判为「无车」。
    admin 返回 scoped=false 且全量车辆；未绑分配规则的用户返回所属公司及下级车辆。
    """
    if user_id is None:
        return {"scoped": True, "plates": [], "device_nos": [], "car_ids": []}

    user = await db.scalar(
        select(SysUser)
        .options(selectinload(SysUser.role))
        .where(SysUser.id == user_id)
        .limit(1)
    )
    if user is None or not user.is_active:
        return {"scoped": True, "plates": [], "device_nos": [], "car_ids": []}

    vehicle_ids, unrestricted = await _resolve_scope_vehicle_ids(db, user)
    return await _build_monitor_scope_payload(db, vehicle_ids, unrestricted=unrestricted)

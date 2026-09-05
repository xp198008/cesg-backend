"""车辆信息变更写入用户操作日志。"""
from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal

from fastapi import Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Fleet, OrgCompany, SysUser, Vehicle, VehicleDevice
from app.user_audit import append_operation_log, client_ip
from app.vehicle_alloc_scope import parse_user_id_header

logger = logging.getLogger(__name__)

_FIELD_LABELS: list[tuple[str, str]] = [
    ("plate_no", "车牌号"),
    ("plate_color", "车牌颜色"),
    ("vehicle_category", "车辆类别"),
    ("vehicle_type", "车辆类型"),
    ("vehicle_type_ii", "车辆类型II"),
    ("color", "车身颜色"),
    ("vin", "VIN"),
    ("driving_license_no", "行驶证号"),
    ("engine_no", "发动机号"),
    ("product_model_code", "产品型号代码"),
    ("frame_no", "车架号"),
    ("vehicle_type_code", "车辆类型编码"),
    ("vehicle_length", "车长"),
    ("vehicle_width", "车宽"),
    ("vehicle_height", "车高"),
    ("loaded_weight", "核载质量"),
    ("vehicle_payload", "核定载质量"),
    ("curb_weight", "整备质量"),
    ("short_name", "车辆简称"),
    ("company_name", "所属公司"),
    ("fleet_name", "车队"),
    ("driver_name", "司机"),
    ("owner_name", "车主"),
    ("contact_name", "联系人"),
    ("contact_phone", "联系电话"),
    ("legal_contact_phone", "法人电话"),
    ("legal_address", "法人地址"),
    ("route", "线路"),
    ("agent", "经销商"),
    ("install_date", "安装日期"),
    ("service_start_date", "服务开始日"),
    ("service_end_date", "服务到期日"),
    ("status", "使用状态"),
    ("channel_count", "通道数"),
    ("engine_displacement", "发动机排量"),
    ("fuel_tank_capacity", "油箱容积"),
    ("fuel_tank", "油箱"),
    ("battery_capacity", "电池容量"),
    ("range_mileage", "续航里程"),
    ("battery_no", "电池编号"),
    ("motor_no", "电机编号"),
    ("manufacturer", "制造商"),
    ("brand", "品牌"),
    ("model", "型号"),
    ("vehicle_grade", "车辆等级"),
    ("vehicle_usage", "车辆用途"),
    ("speed_limit", "限速"),
    ("track_retain_days", "轨迹保留天数"),
    ("mileage_factor", "里程系数"),
    ("mileage_offset", "里程偏移"),
    ("scrap_date", "报废日期"),
    ("inspect_date", "年检日期"),
    ("plate_login", "车牌登录"),
    ("night_speed_enabled", "夜间限速"),
    ("night_start_time", "夜间开始"),
    ("night_end_time", "夜间结束"),
    ("night_speed_percent", "夜间限速百分比"),
    ("remark", "备注"),
    ("device_no", "设备号"),
    ("device_sn", "设备序列号"),
    ("terminal_type", "设备类型"),
    ("sim_no", "SIM卡号"),
    ("actual_sim", "实际SIM"),
    ("product_model", "设备型号"),
]


def _disp(value) -> str:
    if value is None or value == "":
        return "空"
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, Decimal):
        text = format(value, "f").rstrip("0").rstrip(".")
        return text or "0"
    if isinstance(value, float):
        text = f"{value:.4f}".rstrip("0").rstrip(".")
        return text or "0"
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value) if value else "空"
    return str(value).strip() or "空"


async def snapshot_vehicle(db: AsyncSession, vehicle: Vehicle) -> dict[str, str]:
    company_name = ""
    if vehicle.company_id:
        company_name = await db.scalar(
            select(OrgCompany.name).where(OrgCompany.id == vehicle.company_id).limit(1)
        )
    fleet_name = ""
    if vehicle.fleet_id:
        fleet_name = await db.scalar(select(Fleet.name).where(Fleet.id == vehicle.fleet_id).limit(1))
    device = await db.scalar(
        select(VehicleDevice)
        .where(VehicleDevice.vehicle_id == vehicle.id, VehicleDevice.is_main.is_(True))
        .limit(1)
    )
    data = {
        "plate_no": vehicle.plate_no,
        "plate_color": vehicle.plate_color,
        "vehicle_category": vehicle.vehicle_category,
        "vehicle_type": vehicle.vehicle_type,
        "vehicle_type_ii": vehicle.vehicle_type_ii,
        "color": vehicle.color,
        "vin": vehicle.vin,
        "driving_license_no": vehicle.driving_license_no,
        "engine_no": vehicle.engine_no,
        "product_model_code": vehicle.product_model_code,
        "frame_no": vehicle.frame_no,
        "vehicle_type_code": vehicle.vehicle_type_code,
        "vehicle_length": vehicle.vehicle_length,
        "vehicle_width": vehicle.vehicle_width,
        "vehicle_height": vehicle.vehicle_height,
        "loaded_weight": vehicle.loaded_weight,
        "vehicle_payload": vehicle.vehicle_payload,
        "curb_weight": vehicle.curb_weight,
        "short_name": vehicle.short_name,
        "company_name": company_name,
        "fleet_name": fleet_name,
        "driver_name": vehicle.driver_name,
        "owner_name": vehicle.owner_name,
        "contact_name": vehicle.contact_name,
        "contact_phone": vehicle.contact_phone,
        "legal_contact_phone": vehicle.legal_contact_phone,
        "legal_address": vehicle.legal_address,
        "route": vehicle.route,
        "agent": vehicle.agent,
        "install_date": vehicle.install_date,
        "service_start_date": vehicle.service_start_date,
        "service_end_date": vehicle.service_end_date,
        "status": vehicle.status,
        "channel_count": vehicle.channel_count,
        "engine_displacement": vehicle.engine_displacement,
        "fuel_tank_capacity": vehicle.fuel_tank_capacity,
        "fuel_tank": vehicle.fuel_tank,
        "battery_capacity": vehicle.battery_capacity,
        "range_mileage": vehicle.range_mileage,
        "battery_no": vehicle.battery_no,
        "motor_no": vehicle.motor_no,
        "manufacturer": vehicle.manufacturer,
        "brand": vehicle.brand,
        "model": vehicle.model,
        "vehicle_grade": vehicle.vehicle_grade,
        "vehicle_usage": vehicle.vehicle_usage,
        "speed_limit": vehicle.speed_limit,
        "track_retain_days": vehicle.track_retain_days,
        "mileage_factor": vehicle.mileage_factor,
        "mileage_offset": vehicle.mileage_offset,
        "scrap_date": vehicle.scrap_date,
        "inspect_date": vehicle.inspect_date,
        "plate_login": bool(vehicle.plate_login),
        "night_speed_enabled": bool(vehicle.night_speed_enabled),
        "night_start_time": vehicle.night_start_time,
        "night_end_time": vehicle.night_end_time,
        "night_speed_percent": vehicle.night_speed_percent,
        "remark": vehicle.remark,
        "device_no": device.device_no if device else None,
        "device_sn": device.device_sn if device else None,
        "terminal_type": device.terminal_type if device else None,
        "sim_no": device.sim_no if device else None,
        "actual_sim": device.actual_sim if device else None,
        "product_model": device.product_model if device else None,
    }
    return {key: _disp(value) for key, value in data.items()}


def diff_vehicle_snapshot(old: dict[str, str], new: dict[str, str]) -> list[str]:
    parts: list[str] = []
    for key, label in _FIELD_LABELS:
        before = old.get(key, "空")
        after = new.get(key, "空")
        if before != after:
            parts.append(f"{label}「{before}」→「{after}」")
    return parts


def format_change_content(plate: str, changes: list[str]) -> str:
    head = f"修改车辆信息：{plate or '--'}"
    if not changes:
        return head
    text = f"{head}；" + "；".join(changes)
    if len(text) > 1900:
        return text[:1900] + f"…（共{len(changes)}项）"
    return text


async def resolve_actor(db: AsyncSession, x_user_id: str | None) -> SysUser | None:
    uid = parse_user_id_header(x_user_id)
    if uid is None:
        return None
    return await db.scalar(select(SysUser).where(SysUser.id == uid).limit(1))


async def write_vehicle_operation_log(
    db: AsyncSession,
    *,
    request: Request | None,
    x_user_id: str | None,
    action: str,
    content: str,
    plate: str | None = None,
    plate_color: str | None = None,
    device_no: str | None = None,
) -> None:
    try:
        user = await resolve_actor(db, x_user_id)
        org_name = None
        if user is not None and user.org_id:
            org_name = await db.scalar(
                select(OrgCompany.name).where(OrgCompany.id == user.org_id).limit(1)
            )
        await append_operation_log(
            db,
            username=(user.username if user else "")[:64] or "未知用户",
            operation_content=content[:2000],
            user_id=user.id if user else None,
            real_name=(user.real_name if user else None),
            org_id=user.org_id if user else None,
            org_name=org_name,
            module="基础数据管理",
            menu="车辆信息",
            action=action,
            operation_ip=client_ip(request) if request is not None else None,
            result="成功",
            vehicle=(plate or "")[:32] or None,
            plate_color=(plate_color or "")[:16] or None,
            device_no=(device_no or "")[:64] or None,
            source="manual",
        )
    except Exception:  # noqa: BLE001
        logger.exception("写入车辆操作日志失败")

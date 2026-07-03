"""报警记录列表/统计的公共过滤条件。"""
from __future__ import annotations

from typing import Any

from sqlalchemy import and_, not_, or_

from app.models import VehicleViolation

_UNKNOWN_VIOLATION_TYPE_MARKERS = (
    "未知报警类型",
    "系统将未知报警类型",
)


def is_unknown_violation_type_name(name: Any) -> bool:
    """808 等平台无法识别类型码时返回的占位名称，业务侧不入库、不展示。"""
    text = str(name or "").strip()
    if not text:
        return False
    for marker in _UNKNOWN_VIOLATION_TYPE_MARKERS:
        if marker in text:
            return True
    return "未知" in text and "报警类型" in text


def violation_type_is_known_clause():
    """SQLAlchemy：排除未知报警类型名称。"""
    unknown = or_(
        *[
            VehicleViolation.violation_type_name.ilike(f"%{marker}%")
            for marker in _UNKNOWN_VIOLATION_TYPE_MARKERS
        ],
        and_(
            VehicleViolation.violation_type_name.ilike("%未知%"),
            VehicleViolation.violation_type_name.ilike("%报警类型%"),
        ),
    )
    return or_(
        VehicleViolation.violation_type_name.is_(None),
        VehicleViolation.violation_type_name == "",
        not_(unknown),
    )


def violation_list_visibility():
    """不展示 JT808 同步且未关联 CESG 车辆（vehicle_id 为空）的记录；不展示未知报警类型。"""
    return and_(
        violation_type_is_known_clause(),
        or_(
            ~VehicleViolation.source.ilike("jt808%"),
            VehicleViolation.vehicle_id.isnot(None),
        ),
    )

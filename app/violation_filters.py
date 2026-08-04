"""报警记录列表/统计的公共过滤条件。"""
from __future__ import annotations

import json
from typing import Any

from sqlalchemy import and_, not_, or_

from app.alarm_type_gate import build_disabled_alarm_type_exclusion_clause
from app.models import VehicleViolation

# 与 obd_speed_monitor.SOURCE_OBD_SPEED 一致：无图片/视频也可展示
OBD_SPEED_SOURCE = "obd_speed"

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


def has_displayable_media_evidence(
    ttx_evidence_refs: Any = None,
    stream_snapshot_refs: Any = None,
) -> bool:
    """是否存在可展示的图片或视频（ttx_evidence_refs 或 stream_snapshot_refs）。"""
    media = ttx_evidence_refs
    if isinstance(media, str):
        try:
            media = json.loads(media)
        except json.JSONDecodeError:
            media = None
    if isinstance(media, dict):
        images = media.get("images")
        videos = media.get("videos")
        if (isinstance(images, list) and len(images) > 0) or (
            isinstance(videos, list) and len(videos) > 0
        ):
            return True

    snaps = stream_snapshot_refs
    if isinstance(snaps, str):
        try:
            snaps = json.loads(snaps)
        except json.JSONDecodeError:
            snaps = None
    if isinstance(snaps, list) and len(snaps) > 0:
        return True
    return False


def is_obd_speed_source(source: Any) -> bool:
    return str(source or "").strip().lower() == OBD_SPEED_SOURCE


def is_manual_source(source: Any) -> bool:
    return str(source or "").strip().lower() == "manual"


def violation_row_is_page_visible(row: Any) -> bool:
    """页面可见性：OBD 超速 / 人工录入始终可见；其它来源须有图片/视频证据。"""
    if row is None:
        return False
    if is_obd_speed_source(getattr(row, "source", None)):
        return True
    if is_manual_source(getattr(row, "source", None)):
        return True
    return has_displayable_media_evidence(
        getattr(row, "ttx_evidence_refs", None),
        getattr(row, "stream_snapshot_refs", None),
    )


def _ttx_or_stream_media_clause():
    """SQL：ttx_evidence_refs 含非空 images/videos，或 stream_snapshot_refs 抓拍数组非空。"""
    has_ttx_media = or_(
        VehicleViolation.ttx_evidence_refs.like('%"images":[{%'),
        VehicleViolation.ttx_evidence_refs.like('%"images": [{%'),
        VehicleViolation.ttx_evidence_refs.like('%"videos":[{%'),
        VehicleViolation.ttx_evidence_refs.like('%"videos": [{%'),
    )
    has_stream = and_(
        VehicleViolation.stream_snapshot_refs.isnot(None),
        VehicleViolation.stream_snapshot_refs != "",
        VehicleViolation.stream_snapshot_refs != "[]",
        or_(
            VehicleViolation.stream_snapshot_refs.like("[{%"),
            VehicleViolation.stream_snapshot_refs.like('["%'),
        ),
    )
    return or_(has_ttx_media, has_stream)


def violation_has_media_evidence_clause():
    """SQL：OBD 超速 / 人工录入始终可见；其它来源须证据 JSON 含图片/视频或抓拍。"""
    return or_(
        VehicleViolation.source == OBD_SPEED_SOURCE,
        VehicleViolation.source.ilike(OBD_SPEED_SOURCE),
        VehicleViolation.source == "manual",
        VehicleViolation.source.ilike("manual"),
        _ttx_or_stream_media_clause(),
    )


def violation_non_obd_has_media_clause():
    """SQL：非 OBD 且必须有图片/视频证据（供自动 AI 评估候选）。"""
    return and_(
        or_(
            VehicleViolation.source.is_(None),
            VehicleViolation.source == "",
            not_(VehicleViolation.source.ilike(OBD_SPEED_SOURCE)),
        ),
        _ttx_or_stream_media_clause(),
    )


def violation_list_visibility(disabled_alarm_type_names: list[str] | None = None):
    """列表/统计可见条件：

    - 不展示 JT808 同步且未关联 CESG 车辆的记录
    - 不展示未知类型；停用类型历史记录软隐藏
    - 除 OBD 超速、人工录入外，无图片/视频证据的记录不展示
    """
    return and_(
        violation_type_is_known_clause(),
        or_(
            ~VehicleViolation.source.ilike("jt808%"),
            VehicleViolation.vehicle_id.isnot(None),
        ),
        violation_has_media_evidence_clause(),
        build_disabled_alarm_type_exclusion_clause(disabled_alarm_type_names),
    )

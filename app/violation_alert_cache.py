"""新增安全报警的内存缓存。

任何来源（JT808 同步 / OBD 超速 / 人工录入）向 vehicle_violation 插入记录后，
调用 push_violation_alert 把关键字段写进缓存；前端安全监控弹窗定时器
轮询 GET /api/violation/alert-cache 读取增量，弹出报警框并刷新列表。
"""
from __future__ import annotations

from collections import deque
from datetime import datetime
from threading import Lock
from typing import Any

from app.violation_risk import derive_risk_level

_MAX_ALERTS = 200

_alerts: deque[dict[str, Any]] = deque(maxlen=_MAX_ALERTS)
_seq = 0
_lock = Lock()


def _dt_text(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value or "")


def violation_alert_payload(row) -> dict[str, Any]:
    """从 VehicleViolation 行提取弹窗/列表需要的关键字段（须在 flush 后调用，保证有 id）。"""
    return {
        "id": row.id,
        "biz_no": row.biz_no,
        "plate_no": row.plate_no or "",
        "violation_type_name": row.violation_type_name or "",
        "risk_level": row.risk_level or derive_risk_level(row.violation_type_name),
        "violation_time": _dt_text(row.violation_time),
        "address": row.address or "",
        "lat": row.lat,
        "lng": row.lng,
        "vehicle_id": row.vehicle_id,
        "terminal_id": row.terminal_id or "",
        "company_id": row.company_id,
        "source": row.source or "",
        "status": row.status or "待处理",
    }


def push_violation_alert(payload: dict[str, Any]) -> None:
    global _seq
    with _lock:
        _seq += 1
        _alerts.append({"seq": _seq, **payload})


def get_alerts_after(after_seq: int) -> tuple[list[dict[str, Any]], int]:
    """返回 (seq > after_seq 的缓存条目, 当前最大 seq)。after_seq < 0 表示只取水位。"""
    with _lock:
        if after_seq < 0:
            return [], _seq
        return [a for a in _alerts if a["seq"] > after_seq], _seq

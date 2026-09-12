"""新增安全报警的共享缓存。

任何来源向 vehicle_violation 插入「待处理」后，调用 push_violation_alert。
前端轮询 GET /api/violation/alert-cache 读增量，弹窗并播提示音。

这里只清缓存，不动业务库：
- 已经播报过的条目从缓存删掉，避免水位回放再推一遍
- 已经处理过（不再是待处理）的从缓存删掉
- 与报警类型 15 分钟防重对齐：同车同类型 15 分钟内不重复进缓存；
  超时仍未播报的直接丢掉，不再弹
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

from app.timeutil import china_now_naive
from app.violation_risk import RISK_MID

_MAX_ALERTS = 200
# 与报警类型字典默认 min_interval_minutes=15 对齐
_TYPE_INTERVAL_SEC = 15 * 60
_lock = Lock()


def _cache_path() -> Path:
    raw = (os.getenv("CESG_SCHEDULER_LOCK") or "").strip()
    if raw:
        return Path(raw).parent / "alert_cache.json"
    return Path("/tmp/cesg-alert-cache.json")


def _dt_text(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value or "")


def _parse_dt(value) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    text = str(value or "").strip().replace("T", " ")
    if not text:
        return None
    for fmt, size in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d %H:%M", 16)):
        try:
            return datetime.strptime(text[:size], fmt)
        except ValueError:
            continue
    return None


def _type_key(payload: dict[str, Any]) -> str:
    vid = payload.get("vehicle_id")
    plate = str(payload.get("plate_no") or "").strip()
    tname = str(payload.get("violation_type_name") or "").strip()
    owner = f"v:{vid}" if vid not in (None, "") else f"p:{plate}"
    return f"{owner}|{tname}"


def _alert_id(payload: dict[str, Any]) -> str:
    raw = payload.get("id")
    if raw is None or str(raw).strip() == "":
        return ""
    return str(raw).strip()


def _is_pending(payload: dict[str, Any]) -> bool:
    return (payload.get("status") or "待处理").strip() in ("", "待处理")


def _age_sec(payload: dict[str, Any], now: datetime) -> float | None:
    dt = _parse_dt(payload.get("violation_time")) or _parse_dt(payload.get("pushed_at"))
    if dt is None:
        return None
    return (now - dt).total_seconds()


def _event_time(payload: dict[str, Any], fallback: datetime | None = None) -> datetime | None:
    return (
        _parse_dt(payload.get("violation_time"))
        or _parse_dt(payload.get("pushed_at"))
        or _parse_dt(payload.get("at"))
        or fallback
    )


def _within_interval(left: dict[str, Any], right: dict[str, Any], now: datetime) -> bool:
    if _type_key(left) != _type_key(right):
        return False
    t1 = _event_time(left, now)
    t2 = _event_time(right, now)
    if t1 is None or t2 is None:
        return False
    return abs((t1 - t2).total_seconds()) <= _TYPE_INTERVAL_SEC


def violation_alert_payload(row) -> dict[str, Any]:
    """从 VehicleViolation 行提取弹窗/列表需要的关键字段（须在 flush 后调用，保证有 id）。"""
    from app.jt808_alarm_sync import _strip_alarm_level_suffix

    risk = (getattr(row, "risk_level", None) or "").strip() or RISK_MID
    type_name_raw = (row.violation_type_name or "").strip()
    type_name = _strip_alarm_level_suffix(type_name_raw) or type_name_raw
    return {
        "id": row.id,
        "biz_no": row.biz_no,
        "plate_no": row.plate_no or "",
        "violation_type_name": type_name,
        "risk_level": risk,
        "violation_time": _dt_text(row.violation_time),
        "address": row.address or "",
        "lat": row.lat,
        "lng": row.lng,
        "vehicle_id": row.vehicle_id,
        "terminal_id": row.terminal_id or "",
        "company_id": row.company_id,
        "company_name": (getattr(row, "company_name", None) or "").strip(),
        "group_name": (getattr(row, "company_name", None) or "").strip(),
        "source": row.source or "",
        "status": row.status or "待处理",
        "weather": getattr(row, "weather", None) or "",
        "private_rule_name": getattr(row, "private_rule_name", None) or "",
        "rule_category_name": getattr(row, "rule_category_name", None) or "",
    }


def _flock(fh, exclusive: bool) -> None:
    if os.name == "nt":
        return
    import fcntl

    fcntl.flock(fh.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)


def _funlock(fh) -> None:
    if os.name == "nt":
        return
    import fcntl

    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _read_unlocked(path: Path) -> tuple[int, list[dict[str, Any]], list[dict[str, Any]]]:
    if not path.exists():
        return 0, [], []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0, [], []
    if not isinstance(data, dict):
        return 0, [], []
    try:
        seq = int(data.get("seq") or 0)
    except (TypeError, ValueError):
        seq = 0
    alerts = data.get("alerts") or []
    broadcasted = data.get("broadcasted") or []
    if not isinstance(alerts, list):
        alerts = []
    if not isinstance(broadcasted, list):
        broadcasted = []
    return (
        seq,
        [a for a in alerts if isinstance(a, dict)],
        [b for b in broadcasted if isinstance(b, dict)],
    )


def _write_unlocked(
    path: Path,
    seq: int,
    alerts: list[dict[str, Any]],
    broadcasted: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(
            {
                "seq": seq,
                "alerts": alerts[-_MAX_ALERTS:],
                "broadcasted": broadcasted[-_MAX_ALERTS:],
            },
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )
    tmp.replace(path)


def _prune(
    alerts: list[dict[str, Any]],
    broadcasted: list[dict[str, Any]],
    now: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    kept_bc: list[dict[str, Any]] = []
    for item in broadcasted:
        at = _parse_dt(item.get("at")) or _parse_dt(item.get("violation_time"))
        if at is None or (now - at).total_seconds() <= _TYPE_INTERVAL_SEC:
            kept_bc.append(item)

    kept: list[dict[str, Any]] = []
    seen_ids: set[str] = {str(x.get("id") or "") for x in kept_bc if x.get("id") not in (None, "")}
    for alert in alerts:
        aid = _alert_id(alert)
        if aid and aid in seen_ids:
            continue
        if not _is_pending(alert):
            continue
        age = _age_sec(alert, now)
        if age is not None and age > _TYPE_INTERVAL_SEC:
            continue
        if any(_within_interval(alert, prev, now) for prev in kept_bc):
            continue
        if any(_within_interval(alert, prev, now) for prev in kept):
            continue
        kept.append(alert)
        if aid:
            seen_ids.add(aid)
    return kept, kept_bc


def _recent_same_type(
    payload: dict[str, Any],
    alerts: list[dict[str, Any]],
    broadcasted: list[dict[str, Any]],
    now: datetime,
) -> bool:
    return any(_within_interval(payload, item, now) for item in (*alerts, *broadcasted))


def _mutate(writer) -> Any:
    path = _cache_path()
    lock_path = path.with_suffix(".json.lock")
    with _lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, "a+", encoding="utf-8") as fh:
            _flock(fh, True)
            try:
                seq, alerts, broadcasted = _read_unlocked(path)
                now = china_now_naive()
                alerts, broadcasted = _prune(alerts, broadcasted, now)
                seq, alerts, broadcasted, result = writer(seq, alerts, broadcasted, now)
                _write_unlocked(path, seq, alerts, broadcasted)
                return result
            finally:
                _funlock(fh)


def push_violation_alert(payload: dict[str, Any]) -> None:
    """待处理才进缓存；已处理、超过 15 分钟、同类型防重命中的直接丢弃。"""
    if not _is_pending(payload):
        return

    def _write(seq, alerts, broadcasted, now):
        if _recent_same_type(payload, alerts, broadcasted, now):
            return seq, alerts, broadcasted, None
        age = _age_sec(payload, now)
        if age is not None and age > _TYPE_INTERVAL_SEC:
            return seq, alerts, broadcasted, None
        aid = _alert_id(payload)
        if aid and (
            any(_alert_id(a) == aid for a in alerts)
            or any(str(b.get("id") or "") == aid for b in broadcasted)
        ):
            return seq, alerts, broadcasted, None
        seq += 1
        alerts.append({"seq": seq, "pushed_at": _dt_text(now), **payload})
        return seq, alerts, broadcasted, None

    _mutate(_write)


def get_alerts_after(after_seq: int) -> tuple[list[dict[str, Any]], int]:
    """返回 (seq > after_seq 的未播报条目, 当前最大 seq)。after_seq < 0 只取水位。

    读取时顺带清掉已处理、已过 15 分钟防重窗口、已播报的缓存，不碰业务库。
    """
    def _read(seq, alerts, broadcasted, _now):
        if after_seq < 0:
            return seq, alerts, broadcasted, ([], seq)
        outgoing = [a for a in alerts if int(a.get("seq") or 0) > after_seq]
        return seq, alerts, broadcasted, (outgoing, seq)

    return _mutate(_read)


def acknowledge_alerts(ids: list[Any]) -> None:
    """前端已取走并播报：从缓存删除，15 分钟内同车同类型不再进缓存。"""
    wanted = {str(i).strip() for i in ids if i is not None and str(i).strip() != ""}
    if not wanted:
        return

    def _write(seq, alerts, broadcasted, now):
        remain: list[dict[str, Any]] = []
        for alert in alerts:
            aid = _alert_id(alert)
            if aid and aid in wanted:
                broadcasted.append(
                    {
                        "id": aid,
                        "key": _type_key(alert),
                        "at": _dt_text(now),
                        "vehicle_id": alert.get("vehicle_id"),
                        "plate_no": alert.get("plate_no"),
                        "violation_type_name": alert.get("violation_type_name"),
                        "violation_time": alert.get("violation_time"),
                    }
                )
                continue
            remain.append(alert)
        return seq, remain, broadcasted, None

    _mutate(_write)


def discard_violation_alerts(ids: list[Any]) -> None:
    """入库后已处理（误报/已处理等）：只清缓存，不改业务库。"""
    wanted = {str(i).strip() for i in ids if i is not None and str(i).strip() != ""}
    if not wanted:
        return

    def _write(seq, alerts, broadcasted, now):
        remain = [a for a in alerts if _alert_id(a) not in wanted]
        for aid in wanted:
            broadcasted.append({"id": aid, "at": _dt_text(now)})
        return seq, remain, broadcasted, None

    _mutate(_write)

"""业务时间：统一用东八区（中国标准时间）。"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

_CHINA_STD = timezone(timedelta(hours=8))


def china_now_naive() -> datetime:
    """当前中国时间，naive datetime（与 SQLite 存 DATETIME 的常见用法一致）。"""
    return datetime.now(_CHINA_STD).replace(tzinfo=None)


def china_today() -> date:
    return datetime.now(_CHINA_STD).date()

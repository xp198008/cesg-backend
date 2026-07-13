"""主动安全报警入库过滤规则匹配。"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import and_, false, func, not_, or_, select, true
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AlarmFilterRule, VehicleViolation

logger = logging.getLogger(__name__)

# JT808 主动安全常见报警类型（ADAS / DSM / BSD，与平台 1208 及库内 violation_type_name 对齐）。
KNOWN_ALARM_TYPE_NAMES: tuple[str, ...] = (
    "前向碰撞报警",
    "车道偏离报警",
    "车距过近报警",
    "行人碰撞报警",
    "频繁变道报警",
    "道路标识超限报警",
    "障碍物报警",
    "道路标志识别事件",
    "主动抓拍事件",
    "前方拥堵报警",
    "疲劳驾驶报警",
    "接打电话报警",
    "抽烟报警",
    "分神驾驶报警",
    "驾驶员异常报警",
    "双手脱离方向盘报警",
    "驾驶员行为监测功能失效报警",
    "未系安全带报警",
    "驾驶员变更事件",
    "驾驶员身份识别事件",
    "遮挡摄像头失效报警",
    "喝水报警",
    # BSD 盲区监测（后方 / 左后方 / 右后方）
    "后方接近报警",
    "左侧后方接近报警",
    "右侧后方接近报警",
    "后方接近预警",
    "左侧后方接近预警",
    "右侧后方接近预警",
    "后方接近提示事件",
    "左侧后方提示事件",
    "右侧后方提示事件",
)

ALLOWED_ALARM_LEVELS: tuple[str, ...] = ("1", "2")


def format_alarm_level(level: str | None) -> str:
    raw = (level or "").strip()
    if raw == "1":
        return "一级"
    if raw == "2":
        return "二级"
    return "不限"


def _actual_contains_base(actual: str, base: str) -> bool:
    text = (actual or "").strip()
    name = (base or "").strip()
    if not text or not name:
        return False
    if text == name or text.startswith(name):
        return True
    core = name.replace("报警", "").strip()
    return bool(core) and core in text


def _level_in_type_name(type_name: str, level_filter: str) -> bool:
    text = (type_name or "").strip()
    if level_filter == "1":
        return "1级" in text or "一级" in text
    if level_filter == "2":
        return "2级" in text or "二级" in text
    return True


def matches_alarm_filter(alarm_type_name: str, alarm_level: int | None, rule: AlarmFilterRule) -> bool:
    """判断报警是否命中过滤规则。"""
    if not rule.enabled:
        return False
    base = (rule.alarm_type_name or "").strip()
    if not base:
        return False
    actual = (alarm_type_name or "").strip()
    if not _actual_contains_base(actual, base):
        return False
    level_filter = (rule.alarm_level or "").strip()
    if level_filter in ALLOWED_ALARM_LEVELS:
        if _level_in_type_name(actual, level_filter):
            return True
        if base.endswith("报警"):
            cn = "一级" if level_filter == "1" else "二级"
            return actual in (f"{base}{level_filter}级", f"{base}{cn}")
        return actual == base
    return True


async def load_enabled_rules(db: AsyncSession) -> list[AlarmFilterRule]:
    rows = (
        await db.execute(
            select(AlarmFilterRule)
            .where(or_(AlarmFilterRule.enabled.is_(True), AlarmFilterRule.enabled == 1))
            .order_by(AlarmFilterRule.id.asc())
        )
    ).scalars().all()
    return list(rows)


def _type_name_contains_base_clause(col, base: str):
    core = base.replace("报警", "").strip()
    clauses = [col.ilike(f"%{base}%")]
    if core and core != base:
        clauses.append(col.ilike(f"%{core}%"))
    return or_(*clauses)


def _level_match_clause(col, level_filter: str):
    if level_filter == "1":
        return or_(col.ilike("%1级%"), col.ilike("%一级%"))
    if level_filter == "2":
        return or_(col.ilike("%2级%"), col.ilike("%二级%"))
    return true()


def _rule_type_name_match_clause(rule: AlarmFilterRule):
    """单条规则：violation_type_name 命中过滤。"""
    base = (rule.alarm_type_name or "").strip()
    if not base:
        return false()
    col = func.trim(VehicleViolation.violation_type_name)
    level_filter = (rule.alarm_level or "").strip()
    type_hit = _type_name_contains_base_clause(col, base)
    if level_filter in ALLOWED_ALARM_LEVELS:
        return and_(type_hit, _level_match_clause(col, level_filter))
    return type_hit


def build_alarm_filter_exclusion_clause(rules: list[AlarmFilterRule] | None):
    """列表/统计查询：排除命中启用规则的已有报警记录（软隐藏，不删库）。"""
    enabled = [r for r in (rules or []) if r.enabled]
    if not enabled:
        return true()
    hit_any_rule = or_(*[_rule_type_name_match_clause(r) for r in enabled])
    return or_(
        VehicleViolation.violation_type_name.is_(None),
        VehicleViolation.violation_type_name == "",
        not_(hit_any_rule),
    )


async def find_matching_rule(
    db: AsyncSession,
    alarm_type_name: str,
    alarm_level: int | None,
) -> AlarmFilterRule | None:
    for rule in await load_enabled_rules(db):
        if matches_alarm_filter(alarm_type_name, alarm_level, rule):
            return rule
    return None


def log_filtered_alarm(
    *,
    source: str,
    external_id: str,
    alarm_type_name: str,
    alarm_level: int | None,
    rule: AlarmFilterRule,
    plate: str = "",
) -> None:
    logger.info(
        "主动安全报警命中过滤规则仍入库(安全管理列表软隐藏): source=%s ext_id=%s plate=%s type=%s level=%s rule_id=%s rule_code=%s",
        source,
        external_id,
        plate or "-",
        alarm_type_name,
        alarm_level if alarm_level is not None else "-",
        rule.id,
        rule.rule_name,
    )

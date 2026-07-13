"""地图规则类别天气限速解析（OBD 监测与规则 API 共用逻辑）。"""
from __future__ import annotations

from typing import Any

from app.models import MapRuleCategory, PrivateMapRule, PrivateMapRuleWeather

WEATHER_TYPE_OPTIONS = [
    {"code": "sunny", "label": "晴"},
    {"code": "cloudy", "label": "多云"},
    {"code": "overcast", "label": "阴"},
    {"code": "rain", "label": "雨"},
    {"code": "snow", "label": "雪"},
    {"code": "fog", "label": "雾"},
    {"code": "wind", "label": "大风"},
    {"code": "other", "label": "其他"},
]


def _normalize_weather_types(values: list[str] | None) -> list[str]:
    allowed = {str(x["code"]) for x in WEATHER_TYPE_OPTIONS}
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        code = str(raw or "").strip().lower()
        if not code or code not in allowed or code in seen:
            continue
        seen.add(code)
        out.append(code)
    if "sunny" not in seen:
        out.insert(0, "sunny")
    return out


def _normalize_weather_speed_limits(values: dict[str, Any] | None, weather_types: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    allowed = set(weather_types)
    for code in allowed:
        raw = (values or {}).get(code)
        try:
            n = int(raw)
        except (TypeError, ValueError):
            n = 0
        out[code] = max(0, min(500, n))
    return out


def weather_text_to_type_code(weather: str) -> str:
    """将实况天气文案映射为规则类别天气编码。"""
    w = (weather or "").strip()
    if not w:
        return "sunny"
    if any(x in w for x in ("雪", "冰", "冻")):
        return "snow"
    if any(x in w for x in ("雾", "霾", "沙", "尘")):
        return "fog"
    if any(x in w for x in ("雨", "阵雨", "毛毛雨")):
        return "rain"
    if "风" in w:
        return "wind"
    if "阴" in w:
        return "overcast"
    if "云" in w:
        return "cloudy"
    if "晴" in w:
        return "sunny"
    return "other"


def resolve_category_speed_limit(
    category: MapRuleCategory,
    weather_type_code: str | None,
    *,
    weather_rule_row: PrivateMapRuleWeather | None = None,
) -> int:
    """按类别天气配置解析生效限速，逻辑与 resolve-speed API 一致。"""
    default_speed = int(category.speed_limit_kmh or 0)
    cur = (weather_type_code or "").strip().lower() or "sunny"
    weather_types = _normalize_weather_types(category.weather_types if isinstance(category.weather_types, list) else [])
    weather_speed_limits = _normalize_weather_speed_limits(
        category.weather_speed_limits if isinstance(category.weather_speed_limits, dict) else {},
        weather_types,
    )
    if cur in weather_types:
        return int(weather_speed_limits.get(cur, default_speed))
    if category.weather_rule_id is not None and cur and weather_rule_row is not None:
        if (weather_rule_row.weather_type_code or "").strip().lower() == cur:
            return int(weather_rule_row.speed_limit_kmh or 0)
    return default_speed


def effective_limit_kmh(
    rule: PrivateMapRule,
    category: MapRuleCategory,
    weather_type_code: str | None = None,
    *,
    weather_rule_row: PrivateMapRuleWeather | None = None,
) -> int:
    """围栏（规则分配）用类别天气限速；折线限速规则自身限速优先，为 0 时回落类别。"""
    code = (rule.rule_type_code or "").strip().lower()
    if code != "speed_rule":
        return resolve_category_speed_limit(category, weather_type_code, weather_rule_row=weather_rule_row)
    limit = int(rule.speed_limit_kmh or 0)
    if limit > 0:
        return limit
    return resolve_category_speed_limit(category, weather_type_code, weather_rule_row=weather_rule_row)

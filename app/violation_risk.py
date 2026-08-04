"""主动安全报警风险等级：以报警类型字典的安全级别为准。"""
from __future__ import annotations

RISK_HIGH = "high"
RISK_MID = "mid"
RISK_LOW = "low"

RISK_LEVEL_LABELS = {
    RISK_HIGH: "高风险",
    RISK_MID: "中风险",
    RISK_LOW: "低风险",
}

_SAFETY_TO_RISK = {
    "高": RISK_HIGH,
    "中": RISK_MID,
    "低": RISK_LOW,
}


def risk_from_safety_level(safety_level: str | None) -> str:
    """报警类型安全级别（高/中/低）→ 风险码 high/mid/low。"""
    return _SAFETY_TO_RISK.get((safety_level or "").strip(), RISK_MID)


def derive_risk_level(violation_type_name: str | None = None) -> str:
    """兼容旧调用：不再按类型名关键字写死，缺省中风险。

    入库/列表请优先用报警类型表 safety_level（见 alarm_type_gate / risk_map）。
    """
    return RISK_MID


def resolve_risk_level(
    *,
    type_name: str | None,
    stored: str | None = None,
    risk_map: dict[str, str] | None = None,
) -> str:
    """优先取类型表映射；传入 risk_map 但类型不存在时默认 mid；否则再用已存值。"""
    name = (type_name or "").strip()
    if risk_map is not None:
        if name and name in risk_map:
            return risk_map[name]
        return RISK_MID
    s = str(stored or "").strip().lower()
    if s in (RISK_HIGH, RISK_MID, RISK_LOW):
        return s
    if "高" in str(stored or ""):
        return RISK_HIGH
    if "中" in str(stored or ""):
        return RISK_MID
    if "低" in str(stored or ""):
        return RISK_LOW
    return RISK_MID


def risk_level_label(level: str | None) -> str:
    return RISK_LEVEL_LABELS.get(str(level or "").strip(), RISK_LEVEL_LABELS[RISK_MID])

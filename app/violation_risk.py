"""主动安全报警风险等级（与前端安全监控规则一致）。"""
from __future__ import annotations

RISK_HIGH = "high"
RISK_MID = "mid"
RISK_LOW = "low"

RISK_LEVEL_LABELS = {
    RISK_HIGH: "高风险",
    RISK_MID: "中风险",
    RISK_LOW: "低风险",
}


def derive_risk_level(violation_type_name: str | None) -> str:
    """超速=高风险；打电话、疲劳=中风险；其它=低风险。"""
    s = str(violation_type_name or "")
    if "超速" in s:
        return RISK_HIGH
    if "打电话" in s or "疲劳" in s:
        return RISK_MID
    return RISK_LOW


def risk_level_label(level: str | None) -> str:
    return RISK_LEVEL_LABELS.get(str(level or "").strip(), RISK_LEVEL_LABELS[RISK_LOW])

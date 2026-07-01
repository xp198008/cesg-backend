"""车牌等业务字符串规范化。"""


def norm_plate(s: str | None) -> str:
    if s is None:
        return ""
    return str(s).replace("\u3000", " ").strip()

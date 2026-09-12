"""登录口令强度：大写、小写、数字、特殊符号，至少 8 位。"""
from __future__ import annotations

import re
import secrets
import string

PASSWORD_HINT = "密码至少8位，须包含大写字母、小写字母、数字和特殊符号"

_WEAK_EXACT = {
    "123456",
    "12345678",
    "123456789",
    "111111",
    "000000",
    "888888",
    "666666",
    "password",
    "admin",
    "admin123",
    "admin123456",
    "admin888",
    "root",
    "root123",
    "qwerty",
    "abc123",
}

_SPECIALS = "!@#$%^*-_+="


def is_weak_password(password: str | None) -> bool:
    raw = (password or "").strip()
    if len(raw) < 8:
        return True
    folded = raw.lower()
    if folded in _WEAK_EXACT:
        return True
    if folded.startswith("admin") and folded[5:].isdigit():
        return True
    if not re.search(r"[A-Z]", raw):
        return True
    if not re.search(r"[a-z]", raw):
        return True
    if not re.search(r"\d", raw):
        return True
    if not re.search(r"[^A-Za-z0-9]", raw):
        return True
    return False


def require_strong_password(password: str | None) -> str:
    raw = (password or "").strip()
    if is_weak_password(raw):
        raise ValueError(PASSWORD_HINT)
    return raw


def generate_login_password(length: int = 10) -> str:
    n = max(8, int(length or 10))
    lower = string.ascii_lowercase
    upper = string.ascii_uppercase
    digit = string.digits
    chars = [
        secrets.choice(lower),
        secrets.choice(upper),
        secrets.choice(digit),
        secrets.choice(_SPECIALS),
    ]
    pool = lower + upper + digit + _SPECIALS
    while len(chars) < n:
        chars.append(secrets.choice(pool))
    secrets.SystemRandom().shuffle(chars)
    return "".join(chars)

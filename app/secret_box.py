"""业务口令落库加密：AES-256-GCM。登录校验仍走 bcrypt，本模块只保护 808 代登所需的可逆副本。"""
from __future__ import annotations

import base64
import hashlib
import logging
import os
import secrets
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logger = logging.getLogger(__name__)

PREFIX = "enc:v1:"
_KEY_FILE = Path(__file__).resolve().parent.parent / "data" / ".cesg_secret_key"
_cached_key: bytes | None = None


def _raw_secret() -> str:
    from app.config import settings

    env = (getattr(settings, "cesg_secret_key", None) or os.getenv("CESG_SECRET_KEY") or "").strip()
    if env:
        return env
    _KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
    if _KEY_FILE.exists():
        stored = _KEY_FILE.read_text(encoding="utf-8").strip()
        if stored:
            return stored
    generated = secrets.token_hex(32)
    _KEY_FILE.write_text(generated, encoding="utf-8")
    try:
        os.chmod(_KEY_FILE, 0o600)
    except OSError:
        pass
    logger.warning("已生成 CESG 密钥文件 %s，丢失后历史代登口令无法解密", _KEY_FILE)
    return generated


def _aes_key() -> bytes:
    global _cached_key
    if _cached_key is None:
        _cached_key = hashlib.sha256(_raw_secret().encode("utf-8")).digest()
    return _cached_key


def is_encrypted(value: str | None) -> bool:
    return bool(value) and str(value).startswith(PREFIX)


def encrypt_secret(plain: str | None) -> str:
    text = (plain or "").strip()
    if not text:
        return ""
    if is_encrypted(text):
        return text
    nonce = os.urandom(12)
    packed = nonce + AESGCM(_aes_key()).encrypt(nonce, text.encode("utf-8"), None)
    return PREFIX + base64.b64encode(packed).decode("ascii")


def decrypt_secret(stored: str | None) -> str:
    raw = (stored or "").strip()
    if not raw:
        return ""
    if not is_encrypted(raw):
        return raw
    try:
        blob = base64.b64decode(raw[len(PREFIX) :], validate=True)
        if len(blob) < 13:
            raise ValueError("ciphertext too short")
        return AESGCM(_aes_key()).decrypt(blob[:12], blob[12:], None).decode("utf-8")
    except Exception as exc:  # noqa: BLE001
        logger.warning("代登口令解密失败，请检查 CESG_SECRET_KEY: %s", exc)
        return ""


def store_proxy_password(user, plain: str | None) -> None:
    enc = encrypt_secret(plain)
    user.password_plain = enc or None


def load_proxy_password(user) -> str:
    return decrypt_secret(getattr(user, "password_plain", None))


async def migrate_legacy_plaintext_passwords() -> int:
    """启动时把历史明文口令改写成 enc:v1: 密文。已加密的跳过。"""
    from sqlalchemy import select

    from app.database import AsyncSessionLocal
    from app.models import SysUser

    async with AsyncSessionLocal() as session:
        users = (await session.execute(select(SysUser))).scalars().all()
        changed = 0
        for user in users:
            raw = (getattr(user, "password_plain", None) or "").strip()
            if raw and not is_encrypted(raw):
                store_proxy_password(user, raw)
                changed += 1
        if changed:
            await session.commit()
        return changed

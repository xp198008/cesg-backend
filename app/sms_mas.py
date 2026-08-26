"""中国移动云 MAS HTTP/HTTPS 短信发送。

配置未维护或错误时不抛致命异常，统一返回无法发送，便于登录验证码等场景降级。
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import random
import re
import string
import time
from dataclasses import dataclass
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import SmsPlatformConfig
from app.timeutil import china_now_naive

logger = logging.getLogger(__name__)

_PHONE_RE = re.compile(r"^1\d{10}$")
_FAIL_MSG = "无法获取短信"

# phone -> {code, expire_at, sent_at}
_code_store: dict[str, dict[str, Any]] = {}
_RATE_SECONDS = 60


@dataclass
class SmsSendResult:
    ok: bool
    message: str
    detail: str | None = None
    msg_group: str | None = None


def _trim(v: Any) -> str:
    return str(v or "").strip()


def config_ready(row: SmsPlatformConfig | None) -> tuple[bool, str]:
    if row is None:
        return False, "未配置短信平台"
    if not bool(getattr(row, "enabled", False)):
        return False, "短信平台未启用"
    if not _trim(row.base_url):
        return False, "未填写接口地址"
    if not _trim(row.ec_name):
        return False, "未填写企业名称 ecName"
    if not _trim(row.ap_id):
        return False, "未填写接口账号 apId"
    if not _trim(row.secret_key):
        return False, "未填写接口密码 secretKey"
    if not _trim(row.sign):
        return False, "未填写签名编码 sign"
    mode = _trim(row.send_mode).lower() or "normal"
    if mode == "template":
        if not _trim(row.template_id):
            return False, "模板模式未填写 templateId"
    else:
        tpl = _trim(row.content_template) or "您的验证码为{code}，5分钟内有效。"
        if "{code}" not in tpl:
            return False, "短信内容模板须包含 {code}"
    return True, "就绪"


def _md5_lower(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _join_url(base: str, path: str) -> str:
    b = base.rstrip("/")
    p = path if path.startswith("/") else f"/{path}"
    return f"{b}{p}"


async def get_sms_config(db: AsyncSession, provider: str = "mas") -> SmsPlatformConfig | None:
    return await db.scalar(
        select(SmsPlatformConfig).where(SmsPlatformConfig.provider == (provider or "mas")).limit(1)
    )


async def send_mas_sms(
    db: AsyncSession,
    *,
    mobiles: str,
    content: str,
    provider: str = "mas",
) -> SmsSendResult:
    """按库内配置发送普通/模板短信；失败只返回 ok=False。"""
    row = await get_sms_config(db, provider)
    ready, reason = config_ready(row)
    if not ready or row is None:
        logger.info("短信未发送：%s", reason)
        return SmsSendResult(ok=False, message=_FAIL_MSG, detail=reason)

    mode = _trim(row.send_mode).lower() or "normal"
    ec_name = _trim(row.ec_name)
    ap_id = _trim(row.ap_id)
    secret_key = _trim(row.secret_key)
    sign = _trim(row.sign)
    add_serial = _trim(row.add_serial)
    mobiles = _trim(mobiles)
    content = _trim(content)

    if mode == "template":
        template_id = _trim(row.template_id)
        # params：验证码作为首个变量；JSON 数组字符串
        params = json.dumps([content], ensure_ascii=False)
        mac_src = f"{ec_name}{ap_id}{secret_key}{template_id}{mobiles}{params}{sign}{add_serial}"
        payload = {
            "ecName": ec_name,
            "apId": ap_id,
            "secretKey": secret_key,  # 官方样例 Base64 报文含此字段
            "templateId": template_id,
            "mobiles": mobiles,
            "params": params,
            "sign": sign,
            "addSerial": add_serial,
            "mac": _md5_lower(mac_src),
        }
        path = _trim(row.template_path) or "/sms/tmpsubmit"
    else:
        mac_src = f"{ec_name}{ap_id}{secret_key}{mobiles}{content}{sign}{add_serial}"
        payload = {
            "ecName": ec_name,
            "apId": ap_id,
            "secretKey": secret_key,  # 官方样例 Base64 报文含此字段
            "mobiles": mobiles,
            "content": content,
            "sign": sign,
            "addSerial": add_serial,
            "mac": _md5_lower(mac_src),
        }
        path = _trim(row.submit_path) or "/sms/submit"

    url = _join_url(_trim(row.base_url), path)
    raw_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    body_b64 = base64.b64encode(raw_json.encode("utf-8")).decode("ascii")

    try:
        async with httpx.AsyncClient(timeout=15.0, verify=False, trust_env=False) as client:
            resp = await client.post(
                url,
                content=body_b64.encode("ascii"),
                headers={"Content-Type": "text/plain; charset=UTF-8"},
            )
        text = (resp.text or "").strip()
        data: dict[str, Any] = {}
        try:
            data = json.loads(text) if text else {}
        except Exception:
            # 少数网关可能再包一层 base64
            try:
                data = json.loads(base64.b64decode(text).decode("utf-8"))
            except Exception:
                data = {}

        rspcod = str(data.get("rspcod") or "")
        success = bool(data.get("success")) or rspcod.lower() == "success"
        msg_group = data.get("msgGroup") or data.get("mgsGroup")
        if resp.status_code < 400 and success:
            return SmsSendResult(
                ok=True,
                message="发送成功",
                detail=rspcod or "success",
                msg_group=str(msg_group) if msg_group else None,
            )
        logger.warning(
            "云 MAS 发送失败 status=%s rspcod=%s body=%s",
            resp.status_code,
            rspcod,
            text[:300],
        )
        return SmsSendResult(
            ok=False,
            message=_FAIL_MSG,
            detail=rspcod or f"http_{resp.status_code}",
        )
    except Exception as exc:
        logger.warning("云 MAS 请求异常: %s", exc)
        return SmsSendResult(ok=False, message=_FAIL_MSG, detail="request_error")


def _gen_code(length: int = 6) -> str:
    return "".join(random.choices(string.digits, k=length))


def _purge_codes(now: float | None = None) -> None:
    now = now if now is not None else time.time()
    dead = [k for k, v in _code_store.items() if float(v.get("expire_at") or 0) < now]
    for k in dead:
        _code_store.pop(k, None)


def _norm_phone(phone: str) -> str:
    s = _trim(phone).replace(" ", "").replace("-", "")
    if s.startswith("+86"):
        s = s[3:]
    elif s.startswith("86") and len(s) == 13 and s[2:3] == "1":
        s = s[2:]
    return s


async def phone_bound_user_exists(db: AsyncSession, phone: str) -> bool:
    from app.models import SysUser

    phone = _norm_phone(phone)
    if not _PHONE_RE.match(phone):
        return False
    uid = await db.scalar(select(SysUser.id).where(SysUser.phone == phone).limit(1))
    return uid is not None


async def send_login_sms_code(db: AsyncSession, phone: str) -> SmsSendResult:
    """发送登录验证码；配置缺失/错误/未绑定时仅返回无法获取短信。"""
    phone = _norm_phone(phone)
    if not _PHONE_RE.match(phone):
        return SmsSendResult(ok=False, message="请输入正确的手机号")

    if not await phone_bound_user_exists(db, phone):
        logger.info("验证码未发送：手机号未绑定系统用户 phone=%s", phone)
        return SmsSendResult(ok=False, message="该手机号未绑定", detail="phone_not_bound")

    row = await get_sms_config(db)
    ready, reason = config_ready(row)
    if not ready or row is None:
        logger.info("验证码未发送：%s", reason)
        return SmsSendResult(ok=False, message=_FAIL_MSG, detail=reason)

    now = time.time()
    _purge_codes(now)
    prev = _code_store.get(phone)
    if prev and now - float(prev.get("sent_at") or 0) < _RATE_SECONDS:
        return SmsSendResult(ok=False, message="发送过于频繁，请稍后再试")

    code = _gen_code(6)
    ttl = int(row.code_ttl_seconds or 300)
    if ttl < 60:
        ttl = 60
    mode = _trim(row.send_mode).lower() or "normal"
    if mode == "template":
        # 模板变量首参为验证码
        content_for_send = code
    else:
        tpl = _trim(row.content_template) or "您的验证码为{code}，5分钟内有效。"
        content_for_send = tpl.replace("{code}", code)

    result = await send_mas_sms(db, mobiles=phone, content=content_for_send)
    if result.ok:
        _code_store[phone] = {
            "code": code,
            "expire_at": now + ttl,
            "sent_at": now,
        }
    return result


def verify_login_sms_code(phone: str, code: str) -> bool:
    phone = _norm_phone(phone)
    code = _trim(code)
    if not phone or not code:
        return False
    now = time.time()
    _purge_codes(now)
    item = _code_store.get(phone)
    if not item:
        return False
    if str(item.get("code")) != code:
        return False
    _code_store.pop(phone, None)
    return True


def config_out(row: SmsPlatformConfig | None, *, mask_secret: bool = False) -> dict[str, Any] | None:
    if row is None:
        return None
    secret = _trim(row.secret_key)
    if mask_secret and secret:
        secret = secret[:2] + "*" * max(0, len(secret) - 4) + secret[-2:] if len(secret) > 4 else "****"
    ready, reason = config_ready(row)
    return {
        "id": row.id,
        "provider": row.provider,
        "enabled": bool(row.enabled),
        "base_url": row.base_url,
        "submit_path": row.submit_path or "/sms/submit",
        "template_path": row.template_path or "/sms/tmpsubmit",
        "ec_name": row.ec_name,
        "ap_id": row.ap_id,
        "secret_key": secret if mask_secret else row.secret_key,
        "sign": row.sign,
        "add_serial": row.add_serial or "",
        "send_mode": row.send_mode or "normal",
        "template_id": row.template_id,
        "content_template": row.content_template or "您的验证码为{code}，5分钟内有效。",
        "code_ttl_seconds": int(row.code_ttl_seconds or 300),
        "remark": row.remark,
        "ready": ready,
        "ready_reason": reason,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def touch_updated(row: SmsPlatformConfig) -> None:
    row.updated_at = china_now_naive()

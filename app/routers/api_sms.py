"""短信平台配置与验证码发送接口。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import SmsPlatformConfig
from app.sms_mas import (
    config_out,
    config_ready,
    get_sms_config,
    send_login_sms_code,
    send_mas_sms,
    touch_updated,
)

router = APIRouter(prefix="/api", tags=["sms"])


class SmsPlatformConfigBody(BaseModel):
    provider: str = "mas"
    enabled: bool = False
    base_url: str | None = None
    submit_path: str | None = "/sms/submit"
    template_path: str | None = "/sms/tmpsubmit"
    ec_name: str | None = None
    ap_id: str | None = None
    secret_key: str | None = None
    sign: str | None = None
    add_serial: str | None = ""
    send_mode: str = "normal"
    template_id: str | None = None
    content_template: str | None = "您的验证码为{code}，5分钟内有效。"
    code_ttl_seconds: int | None = Field(default=300, ge=60, le=3600)
    remark: str | None = None


class SmsSendCodeBody(BaseModel):
    phone: str = Field(..., min_length=11, max_length=20)


class SmsTestSendBody(BaseModel):
    phone: str = Field(..., min_length=11, max_length=20)
    content: str | None = None


@router.get("/sms-api-config")
async def sms_api_config_get(
    provider: str = Query("mas"),
    db: AsyncSession = Depends(get_db),
):
    row = await get_sms_config(db, provider)
    if row is not None:
        await db.refresh(row)
    return {"ok": True, "data": config_out(row)}


@router.put("/sms-api-config")
async def sms_api_config_put(body: SmsPlatformConfigBody, db: AsyncSession = Depends(get_db)):
    provider = (body.provider or "mas").strip() or "mas"
    row = await db.scalar(select(SmsPlatformConfig).where(SmsPlatformConfig.provider == provider).limit(1))
    if row is None:
        row = SmsPlatformConfig(provider=provider)
        db.add(row)
    row.enabled = bool(body.enabled)
    row.base_url = (body.base_url or "").strip() or None
    row.submit_path = (body.submit_path or "").strip() or "/sms/submit"
    row.template_path = (body.template_path or "").strip() or "/sms/tmpsubmit"
    row.ec_name = (body.ec_name or "").strip() or None
    row.ap_id = (body.ap_id or "").strip() or None
    row.secret_key = (body.secret_key or "").strip() or None
    row.sign = (body.sign or "").strip() or None
    row.add_serial = (body.add_serial or "").strip()
    mode = (body.send_mode or "normal").strip().lower()
    row.send_mode = mode if mode in ("normal", "template") else "normal"
    row.template_id = (body.template_id or "").strip() or None
    row.content_template = (
        (body.content_template or "").strip() or "您的验证码为{code}，5分钟内有效。"
    )
    if body.code_ttl_seconds is not None:
        row.code_ttl_seconds = int(body.code_ttl_seconds)
    row.remark = (body.remark or "").strip() or None
    touch_updated(row)
    await db.flush()
    await db.refresh(row)
    ready, reason = config_ready(row)
    return {"ok": True, "data": config_out(row), "ready": ready, "ready_reason": reason}


@router.post("/sms/send-code")
async def sms_send_code(body: SmsSendCodeBody, db: AsyncSession = Depends(get_db)):
    """登录页获取验证码。配置缺失或平台失败时统一提示无法获取短信。"""
    result = await send_login_sms_code(db, body.phone)
    return {
        "ok": result.ok,
        "message": result.message,
        # 运维排错用；前端勿直接展示 detail
        "detail": result.detail if not result.ok else None,
        "msg_group": result.msg_group,
    }


@router.post("/sms-api-config/test-send")
async def sms_api_config_test_send(body: SmsTestSendBody, db: AsyncSession = Depends(get_db)):
    """运维页试发一条（使用当前已保存配置）。"""
    row = await get_sms_config(db)
    ready, reason = config_ready(row)
    if not ready or row is None:
        return {"ok": False, "message": "无法获取短信", "detail": reason}
    content = (body.content or "").strip()
    if not content:
        if (row.send_mode or "normal").lower() == "template":
            content = "000000"
        else:
            tpl = (row.content_template or "您的验证码为{code}，5分钟内有效。").strip()
            content = tpl.replace("{code}", "000000")
    result = await send_mas_sms(db, mobiles=body.phone.strip(), content=content)
    return {
        "ok": result.ok,
        "message": result.message,
        "detail": result.detail,
        "msg_group": result.msg_group,
    }

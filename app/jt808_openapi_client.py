"""新 JT808 OpenAPI 客户端。

接口文档：docs/api.docx。这里仅封装主动安全第一阶段需要的接口：
1200/1210 token、1201 位置、1208 ADAS、1209 DSM。
"""
from __future__ import annotations

import hashlib
import logging
import time
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class Jt808OpenApiError(RuntimeError):
    """新 JT808 OpenAPI 调用失败。"""


def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()  # noqa: S324 - 接口文档要求 MD5 签名


def _encrypted_password(account: str, password: str, already_hashed: bool) -> str:
    p = (password or "").strip()
    if already_hashed:
        return p
    return _md5(_md5(p) + _md5((account or "").strip()))


def _compact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in payload.items() if v is not None and v != ""}


class Jt808OpenApiClient:
    """轻量异步客户端，内部缓存短时 token。"""

    def __init__(self) -> None:
        self._token: str = ""
        self._token_expire_at = 0.0

    def configured(self) -> bool:
        return bool(
            (settings.jt808_openapi_base_url or "").strip()
            and (settings.jt808_openapi_account or "").strip()
            and (settings.jt808_openapi_password or "").strip()
            and (settings.jt808_openapi_apitoken or "").strip()
        )

    async def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        url = (settings.jt808_openapi_base_url or "").strip()
        if not url:
            raise Jt808OpenApiError("未配置 JT808_OPENAPI_BASE_URL")
        try:
            async with httpx.AsyncClient(timeout=float(settings.jt808_openapi_timeout)) as client:
                resp = await client.post(url, json=_compact_payload(payload))
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:  # noqa: BLE001
            raise Jt808OpenApiError(f"JT808 OpenAPI 请求失败: {exc}") from exc
        if not isinstance(data, dict):
            raise Jt808OpenApiError("JT808 OpenAPI 返回格式不是 JSON 对象")
        if int(data.get("code") or 0) != 1:
            raise Jt808OpenApiError(str(data.get("message") or data))
        return data

    async def login(self) -> str:
        account = settings.jt808_openapi_account.strip()
        password = _encrypted_password(account, settings.jt808_openapi_password, settings.jt808_openapi_password_hashed)
        data = await self._post(
            {
                "apicode": 1200,
                "account": account,
                "password": password,
                "apitoken": settings.jt808_openapi_apitoken.strip(),
            }
        )
        token = str(data.get("token") or "").strip()
        if not token:
            raise Jt808OpenApiError("JT808 OpenAPI 未返回 token")
        self._token = token
        # 文档标注 30 分钟有效，这里提前刷新。
        self._token_expire_at = time.time() + 25 * 60
        return token

    async def token(self) -> str:
        if self._token and time.time() < self._token_expire_at:
            return self._token
        return await self.login()

    async def refresh_token(self) -> str:
        if not self._token:
            return await self.login()
        data = await self._post({"apicode": 1210, "lingxtoken": self._token})
        token = str(data.get("token") or "").strip()
        if not token:
            return await self.login()
        self._token = token
        self._token_expire_at = time.time() + 25 * 60
        return token

    async def list_adas_alarms(
        self,
        stime: str,
        etime: str,
        *,
        page: int = 1,
        rows: int = 100,
        device_id: str | None = None,
        alarm_type: str | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            {
                "apicode": 1208,
                "lingxtoken": await self.token(),
                "deviceId": device_id,
                "type": alarm_type,
                "stime": stime,
                "etime": etime,
                "page": page,
                "rows": rows,
            }
        )

    async def list_dsm_alarms(
        self,
        stime: str,
        etime: str,
        *,
        page: int = 1,
        rows: int = 100,
        device_id: str | None = None,
        alarm_type: str | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            {
                "apicode": 1209,
                "lingxtoken": await self.token(),
                "deviceId": device_id,
                "type": alarm_type,
                "stime": stime,
                "etime": etime,
                "page": page,
                "rows": rows,
            }
        )

    async def list_positions(self, device_ids: list[str]) -> dict[str, Any]:
        ids = ",".join([x.strip() for x in device_ids if x and x.strip()])
        if not ids:
            return {"code": 1, "message": "SUCCESS", "data": []}
        return await self._post({"apicode": 1201, "lingxtoken": await self.token(), "deviceId": ids})

    async def list_vehicles(self, *, device_id: str | None = None, text: str | None = None, page: int = 1, rows: int = 20) -> dict[str, Any]:
        return await self._post(
            {
                "apicode": 1211,
                "lingxtoken": await self.token(),
                "deviceId": device_id,
                "text": text,
                "page": page,
                "rows": rows,
            }
        )


jt808_openapi_client = Jt808OpenApiClient()


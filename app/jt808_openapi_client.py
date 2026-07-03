"""JT808 平台 HTTP 客户端（1208 主动安全、1201 定位、1211 车辆等）。

- 自建 8800：apicode 8003 登录，密码算法与前端/lingx 一致。
- 公网 OpenAPI（gb35658）：apicode 1200 + apitoken。
"""
from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Literal

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

AuthMode = Literal["8003", "1200"]


class Jt808OpenApiError(RuntimeError):
    """JT808 平台 API 调用失败。"""


def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()  # noqa: S324 - 接口要求 MD5


def _handler_userid(userid: str) -> str:
    allowed = "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM_"
    return "".join(ch for ch in userid if ch in allowed)


def _lingx8003_password(account: str, password: str, already_hashed: bool) -> str:
    """与 jt808_vehicle / 前端 LoginPage 的 8003 密码编码一致。"""
    p = (password or "").strip()
    if already_hashed or len(p) == 32:
        return p
    return _md5(_md5(p) + _md5(_handler_userid(account)))


def _openapi1200_password(account: str, password: str, already_hashed: bool) -> str:
    p = (password or "").strip()
    if already_hashed:
        return p
    return _md5(_md5(p) + _md5((account or "").strip()))


def _compact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in payload.items() if v is not None and v != ""}


class Jt808OpenApiClient:
    """轻量异步客户端，内部缓存短时 lingxtoken。"""

    def __init__(self) -> None:
        self._token: str = ""
        self._token_expire_at = 0.0

    def _account(self) -> str:
        return (settings.jt808_openapi_account or settings.jt808_admin_account or "").strip()

    def _password(self) -> str:
        return (settings.jt808_openapi_password or settings.jt808_admin_password or "").strip()

    def auth_mode(self) -> AuthMode:
        mode = (settings.jt808_openapi_auth_mode or "").strip().lower()
        if mode in ("8003", "1200"):
            return mode  # type: ignore[return-value]
        url = (settings.jt808_openapi_base_url or "").lower()
        if "gb35658" in url or (settings.jt808_openapi_apitoken or "").strip():
            return "1200"
        return "8003"

    def configured(self) -> bool:
        url = (settings.jt808_openapi_base_url or "").strip()
        account = self._account()
        password = self._password()
        if not url or not account or not password:
            return False
        if self.auth_mode() == "1200":
            return bool((settings.jt808_openapi_apitoken or "").strip())
        return True

    async def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        url = (settings.jt808_openapi_base_url or "").strip()
        if not url:
            raise Jt808OpenApiError("未配置 JT808_OPENAPI_BASE_URL")
        try:
            async with httpx.AsyncClient(timeout=float(settings.jt808_openapi_timeout), trust_env=False) as client:
                resp = await client.post(url, json=_compact_payload(payload))
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:  # noqa: BLE001
            raise Jt808OpenApiError(f"JT808 API 请求失败: {exc}") from exc
        if not isinstance(data, dict):
            raise Jt808OpenApiError("JT808 API 返回格式不是 JSON 对象")
        if int(data.get("code") or 0) != 1:
            raise Jt808OpenApiError(str(data.get("message") or data))
        return data

    async def login(self) -> str:
        account = self._account()
        password = self._password()
        if not account or not password:
            raise Jt808OpenApiError("未配置 JT808 登录账号或密码")

        if self.auth_mode() == "8003":
            enc = _lingx8003_password(account, password, settings.jt808_openapi_password_hashed)
            data = await self._post({"apicode": 8003, "account": account, "password": enc})
        else:
            enc = _openapi1200_password(account, password, settings.jt808_openapi_password_hashed)
            data = await self._post(
                {
                    "apicode": 1200,
                    "account": account,
                    "password": enc,
                    "apitoken": (settings.jt808_openapi_apitoken or "").strip(),
                }
            )

        token = str(data.get("token") or "").strip()
        if not token:
            raise Jt808OpenApiError("JT808 登录未返回 token")
        self._token = token
        self._token_expire_at = time.time() + 25 * 60
        return token

    async def token(self) -> str:
        if self._token and time.time() < self._token_expire_at:
            return self._token
        return await self.login()

    async def refresh_token(self) -> str:
        if not self._token:
            return await self.login()
        try:
            data = await self._post({"apicode": 1210, "lingxtoken": self._token})
            token = str(data.get("token") or "").strip()
            if token:
                self._token = token
                self._token_expire_at = time.time() + 25 * 60
                return token
        except Jt808OpenApiError:
            pass
        return await self.login()

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
        """apicode 1209（DSM）；808 平台已合并至 1208，同步调度不再调用。"""
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

    async def list_vehicles(
        self, *, device_id: str | None = None, text: str | None = None, page: int = 1, rows: int = 20
    ) -> dict[str, Any]:
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

    async def list_vehicle_alarms(
        self,
        stime: str,
        etime: str,
        *,
        page: int = 1,
        rows: int = 1,
        device_id: str | None = None,
    ) -> dict[str, Any]:
        """1207 获取车辆报警数据列表。"""
        payload: dict[str, Any] = {
            "apicode": 1207,
            "lingxtoken": await self.token(),
            "stime": stime,
            "etime": etime,
            "page": page,
            "rows": rows,
        }
        if device_id:
            payload["deviceId"] = device_id
        return await self._post(payload)

    async def list_latest_data(self, *, text: str | None = None) -> dict[str, Any]:
        """1241 最新数据接口（全量最新快照，含 online 字段）。"""
        return await self._post(
            {
                "apicode": 1241,
                "lingxtoken": await self.token(),
                "text": text,
            }
        )


jt808_openapi_client = Jt808OpenApiClient()

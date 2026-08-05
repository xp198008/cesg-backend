"""JT808 平台 HTTP 客户端（1208 主动安全、1201 定位、1211 车辆等）。

- 自建 8800：apicode 8003 登录，密码算法与前端/lingx 一致。
- 公网 OpenAPI（gb35658）：apicode 1200 + apitoken。
- 登录密码从 CESG 库 sys_user.password_plain 读取（与界面改密一致），不使用 .env 静态密码。
"""
from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Literal

import httpx

from app.config import settings
from app.jt808_openapi_credentials import load_service_password_plain, service_openapi_username

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
        return service_openapi_username()

    def auth_mode(self) -> AuthMode:
        mode = (settings.jt808_openapi_auth_mode or "").strip().lower()
        if mode in ("8003", "1200"):
            return mode  # type: ignore[return-value]
        url = (settings.jt808_openapi_base_url or "").lower()
        if "gb35658" in url or (settings.jt808_openapi_apitoken or "").strip():
            return "1200"
        return "8003"

    def configured(self) -> bool:
        """仅检查 URL/账号/公网 token；密码在 login 时从数据库读取。"""
        url = (settings.jt808_openapi_base_url or "").strip()
        account = self._account()
        if not url or not account:
            return False
        if self.auth_mode() == "1200":
            return bool((settings.jt808_openapi_apitoken or "").strip())
        return True

    def invalidate_token(self) -> None:
        self._token = ""
        self._token_expire_at = 0.0

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
        if not account:
            raise Jt808OpenApiError("未配置 JT808 服务账号")
        try:
            password = await load_service_password_plain()
        except RuntimeError as exc:
            raise Jt808OpenApiError(str(exc)) from exc

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
        logger.debug("JT808 OpenAPI 登录成功 account=%s（密码来自数据库）", account)
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

    async def list_travel_records(
        self,
        *,
        device_id: str | list[str],
        stime: str,
        etime: str,
        type: int = 2,
        page: int = 1,
        rows: int = 200,
        min_time: int | None = None,
    ) -> dict[str, Any]:
        """1240 行车/停车记录。type: 1=行车 2=停车。"""
        if isinstance(device_id, (list, tuple)):
            ids = [str(x).strip() for x in device_id if str(x).strip()]
            device_val: str | list[str] = ids if len(ids) != 1 else ids[0]
        else:
            device_val = str(device_id or "").strip()
        payload: dict[str, Any] = {
            "apicode": 1240,
            "lingxtoken": await self.token(),
            "deviceId": device_val,
            "stime": str(stime or "").strip(),
            "etime": str(etime or "").strip(),
            "type": int(type),
            "page": int(page),
            "rows": int(rows),
        }
        if min_time is not None and int(min_time) > 0:
            payload["minTime"] = int(min_time)
        return await self._post(payload)

    async def list_history_stops(
        self,
        *,
        device_id: str,
        stime: str,
        etime: str,
        time_stop_minutes: int = 3,
        speed_stop: int = 10,
    ) -> list[dict[str, Any]]:
        """取停车点 stops[]（与历史回放「停车点」同源）。

        - 优先 OpenAPI **1202**：只传 deviceId，由 808 ``tidsToIds`` 转成数字车 id 再查轨迹
          （切勿同时传 car_id=设备号，否则会绕过转换，历史表查空）。
        - 回退会话接口 **1105**：与 History.vue 一致，传 car_id=设备号/树上的车 id。
        """
        tid = str(device_id or "").strip()
        if not tid:
            return []
        st = str(stime or "").strip()
        et = str(etime or "").strip()
        tstop = str(max(0, int(time_stop_minutes)))
        sstop = str(max(0, int(speed_stop)))
        token = await self.token()

        async def _call(payload: dict[str, Any]) -> dict[str, Any]:
            url = (settings.jt808_openapi_base_url or "").strip()
            if not url:
                raise Jt808OpenApiError("未配置 JT808_OPENAPI_BASE_URL")
            timeout = max(float(settings.jt808_openapi_timeout), 90.0)
            try:
                async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
                    resp = await client.post(url, json=_compact_payload(payload))
                    resp.raise_for_status()
                    data = resp.json()
            except Exception as exc:  # noqa: BLE001
                raise Jt808OpenApiError(f"JT808 停车点请求失败: {exc}") from exc
            if not isinstance(data, dict):
                raise Jt808OpenApiError("JT808 停车点返回非对象")
            if int(data.get("code") or 0) != 1:
                raise Jt808OpenApiError(str(data.get("message") or data.get("msg") or "停车点接口失败"))
            return data

        # 1) 1202：仅 deviceId，交给 tidsToIds
        data = await _call(
            {
                "apicode": 1202,
                "lingxtoken": token,
                "deviceId": tid,
                "stime": st,
                "etime": et,
                "timeStop": tstop,
                "speedStop": sstop,
                "alarm": "false",
                "isaddress": "false",
            }
        )
        stops = data.get("stops")
        if isinstance(stops, list) and stops:
            return [x for x in stops if isinstance(x, dict)]

        # 2) 1105 回退（与页面 History.vue 一致）
        data2 = await _call(
            {
                "apicode": 1105,
                "lingxtoken": token,
                "car_id": tid,
                "stime": st,
                "etime": et,
                "timeStop": tstop,
                "speedStop": sstop,
                "static": "true",
                "alarm": "false",
                "isaddress": "false",
            }
        )
        stops2 = data2.get("stops")
        if not isinstance(stops2, list):
            return []
        return [x for x in stops2 if isinstance(x, dict)]

    async def create_car_alarm(
        self,
        *,
        device_id: str,
        name: str,
        gpstime: str,
        speed: float | int | None = None,
        lat: float | None = None,
        lng: float | None = None,
        mileage: float | int | None = None,
        time_sec: float | int | None = None,
        bjlc: float | int | None = None,
        end_lat: float | None = None,
        end_lng: float | None = None,
        end_speed: float | int | None = None,
        end_gpstime: str | None = None,
        end_mileage: float | int | None = None,
        remark: str | None = None,
    ) -> dict[str, Any]:
        """1303 车辆报警数据添加 → tgps_car_alarm。

        deviceId 传终端号；808 侧会解析为内部 car_id。
        """
        payload: dict[str, Any] = {
            "apicode": 1303,
            "lingxtoken": await self.token(),
            "deviceId": str(device_id or "").strip(),
            "name": str(name or "").strip(),
            "gpstime": str(gpstime or "").strip(),
            "speed": speed,
            "lat": lat,
            "lng": lng,
            "mileage": mileage,
            "time": time_sec,
            "bjlc": bjlc,
            "endLat": end_lat,
            "endLng": end_lng,
            "endSpeed": end_speed,
            "endGpstime": end_gpstime,
            "endMileage": end_mileage,
            "remark": remark,
        }
        return await self._post(payload)

    async def send_text_message(
        self,
        *,
        device_id: str,
        content: str,
        urgent: bool = False,
        display: bool = False,
        voice: bool = True,
        smart: bool = False,
        send_type: str = "instant",
    ) -> dict[str, Any]:
        """1142 文字信息下发。deviceId 可为终端号或平台 car_id。

        默认 voice=1（终端语音播报）；标志位用 0/1，兼容 808 平台解析。
        """
        text = str(content or "").strip()
        if not text:
            raise Jt808OpenApiError("文字下发内容为空")
        payload: dict[str, Any] = {
            "apicode": 1142,
            "lingxtoken": await self.token(),
            "deviceId": str(device_id or "").strip(),
            "content": text[:500],
            "urgent": 1 if urgent else 0,
            "display": 1 if display else 0,
            "voice": 1 if voice else 0,
            "smart": 1 if smart else 0,
            "sendType": (send_type or "instant").strip() or "instant",
        }
        return await self._post(payload)


jt808_openapi_client = Jt808OpenApiClient()

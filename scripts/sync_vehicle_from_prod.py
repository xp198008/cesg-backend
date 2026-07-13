"""从生产 CESG 拉取 7610 车辆，登录 808(8003) 并实际调用 1251 同步，打印完整请求/响应。"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import jt808_vehicle  # noqa: E402
from app.config import settings  # noqa: E402

CESG_BASE = "http://113.207.68.96:8100/api"
JT808_API = settings.jt808_api_base


def _pp(title: str, obj: Any) -> None:
    print(f"\n{'=' * 60}")
    print(title)
    print("=" * 60)
    if isinstance(obj, (dict, list)):
        print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))
    else:
        print(obj)


async def _cesg_get(path: str) -> dict:
    url = f"{CESG_BASE}{path}"
    async with httpx.AsyncClient(timeout=20.0, trust_env=False) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()


async def _find_vehicle(keyword: str) -> tuple[int, dict]:
    """按车牌关键字或 id 定位车辆。"""
    if keyword.isdigit():
        try:
            vid = int(keyword)
            detail = await _cesg_get(f"/vehicle/detail/{vid}")
            if detail.get("ok") and detail.get("data"):
                return vid, detail["data"]
        except httpx.HTTPStatusError as e:
            if e.response.status_code != 404:
                raise
    listed = await _cesg_get(f"/vehicle/list?plate_no={keyword}&page=1&page_size=20")
    items = listed.get("items") or []
    if not items:
        raise RuntimeError(f"生产 CESG 未找到车牌含 {keyword!r} 的车辆")
    vid = int(items[0]["id"])
    detail = await _cesg_get(f"/vehicle/detail/{vid}")
    if not detail.get("ok"):
        raise RuntimeError(f"获取车辆详情失败 id={vid}")
    return vid, detail["data"]


async def _resolve_group_id(company_id: int | None) -> int | None:
    if not company_id:
        return None
    from sqlalchemy import select

    from app.database import AsyncSessionLocal
    from app.models import OrgCompany

    async with AsyncSessionLocal() as s:
        gid = await s.scalar(
            select(OrgCompany.jt808_group_id).where(OrgCompany.id == company_id).limit(1)
        )
        return int(gid) if gid else None


def _remote_to_sync_data(remote: dict, group_id: int | None) -> dict:
    return {
        "id": remote.get("id"),
        "plate_no": remote.get("plate_no"),
        "plate_color": remote.get("plate_color"),
        "vin": remote.get("vin"),
        "engine_no": remote.get("engine_no"),
        "product_model_code": remote.get("product_model_code"),
        "frame_no": remote.get("frame_no"),
        "vehicle_type_code": remote.get("vehicle_type_code"),
        "vehicle_length": remote.get("vehicle_length"),
        "vehicle_width": remote.get("vehicle_width"),
        "vehicle_height": remote.get("vehicle_height"),
        "loaded_weight": remote.get("loaded_weight"),
        "vehicle_payload": remote.get("vehicle_payload"),
        "curb_weight": remote.get("curb_weight"),
        "urea_info": remote.get("urea_info"),
        "channel_count": remote.get("channel_count") or len(remote.get("channels") or []) or 0,
        "channels": remote.get("channels") or [],
        "contact_name": remote.get("contact_name"),
        "contact_phone": remote.get("contact_phone"),
        "owner_name": remote.get("owner_name"),
        "remark": remote.get("remark"),
        "device_no": remote.get("device_no"),
        "sim_no": remote.get("sim_no"),
        "terminal_type": remote.get("terminal_type"),
        "group_id": group_id,
        "driver_name": remote.get("driver_name"),
        "vehicle_category": remote.get("vehicle_category"),
        "vehicle_type": remote.get("vehicle_type"),
        "vehicle_type_ii": remote.get("vehicle_type_ii"),
        "vehicle_usage": remote.get("vehicle_usage"),
        "status": remote.get("status"),
        "brand": remote.get("brand"),
        "model": remote.get("model"),
        "manufacturer": remote.get("manufacturer"),
        "engine_displacement": remote.get("engine_displacement"),
        "fuel_tank_capacity": remote.get("fuel_tank_capacity"),
        "battery_capacity": remote.get("battery_capacity"),
        "range_mileage": remote.get("range_mileage"),
        "battery_no": remote.get("battery_no"),
        "motor_no": remote.get("motor_no"),
        "vehicle_grade": remote.get("vehicle_grade"),
        "route": remote.get("route"),
        "agent": remote.get("agent"),
        "mileage_offset": remote.get("mileage_offset"),
        "mileage_factor": remote.get("mileage_factor"),
        "speed_limit": remote.get("speed_limit"),
        "track_retain_days": remote.get("track_retain_days"),
        "icon_id": remote.get("icon_id"),
        "night_speed_enabled": remote.get("night_speed_enabled"),
        "night_start_time": remote.get("night_start_time"),
        "night_end_time": remote.get("night_end_time"),
        "night_speed_percent": remote.get("night_speed_percent"),
        "plate_login": remote.get("plate_login"),
        "is_connect": remote.get("is_connect"),
        "install_date": remote.get("install_date"),
        "service_start_date": remote.get("service_start_date"),
        "service_end_date": remote.get("service_end_date"),
        "scrap_date": remote.get("scrap_date"),
        "inspect_date": remote.get("inspect_date"),
    }


async def main(keyword: str) -> int:
    _pp("步骤1：从生产 CESG 获取车辆信息", f"GET {CESG_BASE}/vehicle/list?plate_no={keyword}")
    vid, remote = await _find_vehicle(keyword)
    _pp(f"步骤1 结果：CESG vehicle id={vid}", remote)

    company_id = remote.get("company_id")
    group_id = await _resolve_group_id(company_id)
    _pp(
        "步骤1b：解析 808 groupId",
        {
            "company_id": company_id,
            "jt808_group_id": group_id,
            "note": "来自本地 org_company.jt808_group_id（与生产库字段一致）",
        },
    )

    sync_data = _remote_to_sync_data(remote, group_id)
    _pp("步骤2：组装同步用车辆数据", sync_data)

    _pp("步骤3：808 登录 apicode=8003", f"POST {JT808_API}")
    login_body = {
        "language": "zh-CN",
        "apicode": 8003,
        "account": settings.jt808_admin_account,
        "password": jt808_vehicle._encode_password(
            settings.jt808_admin_password, settings.jt808_admin_account
        ),
    }
    _pp("8003 请求体（密码已 MD5）", {**login_body, "password": "<md5>"})
    login_resp = await jt808_vehicle._post(login_body)
    _pp("8003 响应", login_resp)
    token = login_resp.get("token")
    if not token:
        print("\n登录失败，无法继续同步")
        return 1

    payload, err = await jt808_vehicle._build_1251_request(sync_data, token)
    if payload is None:
        print(f"\n无法组装 1251: {err}")
        return 2

    call_body = {k: v for k, v in payload.items() if k not in ("language", "lingxtoken")}
    req_full = {"language": "zh-CN", **call_body, "lingxtoken": token}
    _pp("步骤4：808 车辆同步 apicode=1251 请求", {"url": JT808_API, "method": "POST", "body": req_full})

    ext = call_body.get("ext_json")
    if ext:
        try:
            _pp("ext_json 展开", json.loads(ext))
        except json.JSONDecodeError:
            _pp("ext_json 原文", ext)

    sync_resp = await jt808_vehicle._call(call_body)
    _pp("步骤4：808 车辆同步 1251 响应", sync_resp)

    return 0 if sync_resp.get("code") == 1 else 3


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("keyword", nargs="?", default="7610", help="车牌关键字或 CESG vehicle id")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.keyword)))

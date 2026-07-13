"""预览 CESG 车辆同步到 808 的 1251 请求报文（不强制发送）。"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import jt808_vehicle  # noqa: E402
from app.config import settings  # noqa: E402


async def main(vehicle_id: int, send: bool) -> int:
    data = await jt808_vehicle._load_vehicle(vehicle_id)
    if not data:
        print(f"本地未找到车辆 id={vehicle_id}")
        return 1

    print("=== CESG 本地车辆数据 ===")
    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))

    token = await jt808_vehicle._ensure_token()
    payload, err = await jt808_vehicle._build_1251_request(data, token)
    if payload is None:
        print(f"\n无法组装 1251 报文: {err}")
        return 2

    call_body = {k: v for k, v in payload.items() if k not in ("language", "lingxtoken")}
    print("\n=== 发往 808 的 HTTP 请求 ===")
    print(f"URL: {settings.jt808_api_base}")
    print("Method: POST")
    print("Content-Type: application/json")
    print("\n=== 1251 请求体（lingxtoken 已脱敏）===")
    safe = dict(call_body)
    safe["lingxtoken"] = "<token>"
    print(json.dumps({"language": "zh-CN", **safe}, ensure_ascii=False, indent=2))

    ext = call_body.get("ext_json")
    if ext:
        print("\n=== ext_json 展开 ===")
        try:
            print(json.dumps(json.loads(ext), ensure_ascii=False, indent=2))
        except json.JSONDecodeError:
            print(ext)

    if send:
        print("\n=== 实际调用 808 响应 ===")
        r = await jt808_vehicle._call(call_body)
        print(json.dumps(r, ensure_ascii=False, indent=2, default=str))
    else:
        print("\n（未发送；加 --send 可实际调用 808）")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("vehicle_id", type=int, nargs="?", default=7610)
    parser.add_argument("--send", action="store_true", help="实际 POST 到 808")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.vehicle_id, args.send)))

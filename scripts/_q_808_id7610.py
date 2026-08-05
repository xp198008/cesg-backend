"""查询 808 平台 id=7610 并尝试组装 1251 报文。"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import jt808_vehicle  # noqa: E402


async def main() -> int:
    token = await jt808_vehicle._ensure_token()
    print("808 登录成功，token 前8位:", token[:8] + "...")

    # 1211 按 deviceId 查（空 deviceId 可能返回列表）
    for label, payload in [
        ("1211 deviceId=7610", {"apicode": 1211, "deviceId": "7610", "page": 1, "rows": 5}),
        ("1211 deviceId=0202510007610", {"apicode": 1211, "deviceId": "0202510007610", "page": 1, "rows": 5}),
        ("1211 id=7610 via text empty page1", {"apicode": 1211, "page": 1, "rows": 2000}),
    ]:
        print(f"\n=== {label} ===")
        try:
            r = await jt808_vehicle._call(payload)
            print("code:", r.get("code"), "message:", r.get("message"))
            data = r.get("data") or []
            if isinstance(data, list):
                hits = [x for x in data if str(x.get("id")) == "7610" or "7610" in str(x.get("carno") or "")]
                print("total rows:", len(data), "hits:", len(hits))
                for row in hits[:3]:
                    print(json.dumps(row, ensure_ascii=False, default=str))
            else:
                print(json.dumps(r, ensure_ascii=False, default=str)[:500])
        except Exception as e:
            print("error:", e)

    # MySQL 直连查 id=7610
    print("\n=== MySQL tgps_car id=7610 ===")
    row = await asyncio.to_thread(jt808_vehicle._lookup_car_row_mysql, "", "")
    # direct query by id
    conn = jt808_vehicle._connect_mysql()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("select id,tid,carno,sim from tgps_car where id=%s limit 1", (7610,))
                hit = cur.fetchone()
                print("by id:", hit)
                cur.execute("select id,tid,carno,sim from tgps_car where carno like %s limit 5", ("%7610%",))
                print("by carno like %7610%:", cur.fetchall())
        finally:
            conn.close()
    else:
        print("MySQL 不可用（无 SSH 隧道）")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

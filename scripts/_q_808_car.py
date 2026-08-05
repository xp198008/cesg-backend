import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.jt808_openapi_client import jt808_openapi_client


async def main():
    for text in ("7610", "渝DX7610", "020251000334"):
        r = await jt808_openapi_client.list_vehicles(text=text, page=1, rows=20)
        print("=== 1211 text=", text, "code=", r.get("code"), "===")
        data = r.get("data") or []
        print("rows", len(data))
        for row in data[:5]:
            print(json.dumps(row, ensure_ascii=False, default=str))


asyncio.run(main())

"""删除 808 中不在 CESG 的车辆（1216）。

保留规则：808.tid 能对上 CESG 主设备号（含去前导 0 / 补 12 位）。
其余视为多余：从未进 CESG 的历史车，以及同车牌的旧设备号。

用法（在 backend 目录）：
  python scripts/purge_808_extras.py --dry-run
  python scripts/purge_808_extras.py --apply
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logger = logging.getLogger("purge_808_extras")


def _norm_plate(s: str | None) -> str:
    return (s or "").replace("\u3000", " ").replace(" ", "").strip().upper()


def _variants(dev: str | None) -> set[str]:
    t = (dev or "").strip()
    if not t:
        return set()
    out = {t, t.lstrip("0") or "0"}
    if t.isdigit():
        out.add(t.zfill(12))
    return {x for x in out if x}


async def _cesg_tids() -> set[str]:
    from app.database import AsyncSessionLocal
    from app.models import VehicleDevice

    async with AsyncSessionLocal() as db:
        devices = (
            await db.execute(select(VehicleDevice.device_no).where(VehicleDevice.is_main.is_(True)))
        ).scalars().all()
    tids: set[str] = set()
    for dev in devices:
        tids |= _variants(dev)
    return tids


def _load_808_cars() -> list[dict]:
    from app import jt808_vehicle

    conn = jt808_vehicle._connect_mysql()
    if conn is None:
        raise SystemExit("无法连接 808 MySQL")
    try:
        cur = conn.cursor()
        cur.execute("select id, tid, carno, sim, group_id from tgps_car")
        rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {"id": r[0], "tid": r[1], "carno": r[2], "sim": r[3], "group_id": r[4]}
        for r in rows
    ]


def _classify(cars: list[dict], cesg_tids: set[str]) -> tuple[list[dict], list[dict]]:
    keep: list[dict] = []
    extra: list[dict] = []
    for row in cars:
        if _variants(row.get("tid")) & cesg_tids:
            keep.append(row)
        else:
            extra.append(row)
    return keep, extra


async def run(apply: bool) -> int:
    from app import jt808_vehicle

    cesg_tids = await _cesg_tids()
    cars = _load_808_cars()
    keep, extra = _classify(cars, cesg_tids)
    logger.info("808=%s keep=%s extra=%s", len(cars), len(keep), len(extra))

    report = {
        "mode": "apply" if apply else "dry-run",
        "808": len(cars),
        "keep": len(keep),
        "extra": len(extra),
        "deleted": [],
        "fail": [],
    }
    if apply:
        for i, row in enumerate(extra, start=1):
            tid = (row.get("tid") or "").strip()
            plate = (row.get("carno") or "").strip()
            try:
                ok = await jt808_vehicle.delete_now(tid, plate)
            except Exception as exc:  # noqa: BLE001
                report["fail"].append({"id": row["id"], "tid": tid, "carno": plate, "err": str(exc)})
                logger.warning("删除异常 id=%s tid=%s: %s", row["id"], tid, exc)
                continue
            if ok:
                report["deleted"].append({"id": row["id"], "tid": tid, "carno": plate})
            else:
                report["fail"].append({"id": row["id"], "tid": tid, "carno": plate, "err": "1216失败"})
            if i % 50 == 0 or i == len(extra):
                logger.info("删除进度 %s/%s ok=%s fail=%s", i, len(extra), len(report["deleted"]), len(report["fail"]))

    summary = {
        "mode": report["mode"],
        "808": report["808"],
        "keep": report["keep"],
        "extra": report["extra"],
        "deleted": len(report["deleted"]),
        "fail": len(report["fail"]),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print("==== EXTRA SAMPLE ====")
    for x in extra[:30]:
        print(json.dumps({"id": x["id"], "tid": x["tid"], "carno": x["carno"], "group_id": x["group_id"]}, ensure_ascii=False))
    if report["fail"]:
        print("==== FAIL ====")
        for x in report["fail"][:40]:
            print(json.dumps(x, ensure_ascii=False))
    out = Path("/tmp/808_extra_purge.json") if Path("/tmp").is_dir() else Path("808_extra_purge.json")
    out.write_text(json.dumps({**report, "extra": extra}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print("REPORT", str(out))
    return 0 if not report["fail"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="删除 808 中不在 CESG 的车辆")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    return asyncio.run(run(apply=bool(args.apply)))


if __name__ == "__main__":
    raise SystemExit(main())

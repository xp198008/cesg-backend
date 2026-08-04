"""清空 alarm_type_dict 并按 808 ADAS/DSM/BSD 目录重新灌入。

用法（在 backend 目录）：
  set PYTHONPATH=.
  python scripts/reset_alarm_types_from_jt808.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import delete, func, select  # noqa: E402

from app.database import AsyncSessionLocal, init_models  # noqa: E402
from app.jt808_alarm_sync import jt808_alarm_type_catalog  # noqa: E402
from app.models import AlarmTypeDict  # noqa: E402
from app.timeutil import china_now_naive  # noqa: E402


async def main() -> None:
    await init_models()
    async with AsyncSessionLocal() as db:
        before = await db.scalar(select(func.count()).select_from(AlarmTypeDict)) or 0
        await db.execute(delete(AlarmTypeDict))
        await db.flush()
        catalog = jt808_alarm_type_catalog()
        stamp = china_now_naive().strftime("%Y%m%d%H%M%S")
        for index, name in enumerate(catalog, start=1):
            db.add(
                AlarmTypeDict(
                    type_code=f"AT{stamp}{index:04d}",
                    type_name=name,
                    alarm_level="中级",
                    safety_level="中",
                    min_interval_minutes=15,
                    status="启用",
                    data_source="jt808",
                )
            )
        await db.commit()
        after = await db.scalar(select(func.count()).select_from(AlarmTypeDict)) or 0
        print(f"cleared={before} inserted={after}")


if __name__ == "__main__":
    asyncio.run(main())

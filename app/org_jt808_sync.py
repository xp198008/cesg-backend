"""组织架构 → 808 定时同步。

基础数据 org_company 为准，实时监控树读的是 808 tgps_group。
公司增删改当时会 best-effort 推一次，失败或手工改库后两边会漂。
本定时器对照名称和上级：缺分组就建，名称/上级不一致就 8002 改回去。
不删除 808 多出来的分组。
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

from sqlalchemy import select

from app import jt808_group
from app.config import settings
from app.database import AsyncSessionLocal
from app.models import OrgCompany
from app.scheduler_lock import should_run_schedulers
from app.timeutil import china_now_naive

logger = logging.getLogger(__name__)


def _load_jt808_groups() -> dict[int, dict[str, Any]]:
    import pymysql

    conn = pymysql.connect(
        host=settings.jt808_mysql_host,
        port=int(settings.jt808_mysql_port),
        user=settings.jt808_mysql_user,
        password=settings.jt808_mysql_password,
        database=settings.jt808_mysql_database,
        charset="utf8mb4",
        connect_timeout=min(8.0, settings.jt808_sync_timeout),
        read_timeout=15,
        write_timeout=15,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, fid FROM tgps_group")
            out: dict[int, dict[str, Any]] = {}
            for gid, name, fid in cur.fetchall():
                out[int(gid)] = {
                    "name": (name or "").strip(),
                    "fid": int(fid or 0),
                }
            return out
    finally:
        conn.close()


def _parent_first(rows: list[tuple[int, str, int | None, int | None]]) -> list[tuple[int, str, int | None, int | None]]:
    by_parent: dict[int | None, list[tuple[int, str, int | None, int | None]]] = {}
    ids = {r[0] for r in rows}
    for row in rows:
        pid = row[2] if row[2] in ids else None
        by_parent.setdefault(pid, []).append(row)
    for kids in by_parent.values():
        kids.sort(key=lambda x: x[0])
    out: list[tuple[int, str, int | None, int | None]] = []
    seen: set[int] = set()

    def walk(pid: int | None) -> None:
        for row in by_parent.get(pid, []):
            if row[0] in seen:
                continue
            seen.add(row[0])
            out.append(row)
            walk(row[0])

    walk(None)
    for row in rows:
        if row[0] not in seen:
            out.append(row)
    return out


class OrgJt808SyncScheduler:
    def __init__(self) -> None:
        self._running = False
        self._task: asyncio.Task | None = None
        self._kick = asyncio.Event()
        self._last_run_at: datetime | None = None
        self._last_result: dict[str, Any] | None = None
        self._last_error: str | None = None

    def kick(self) -> None:
        """组织增删改后立刻再对一轮，不用干等到下一分钟。"""
        self._kick.set()

    @property
    def running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    def status(self) -> dict[str, Any]:
        return {
            "enabled": bool(settings.org_jt808_sync_enabled),
            "running": self.running if should_run_schedulers() else bool(settings.org_jt808_sync_enabled),
            "interval_seconds": int(settings.org_jt808_sync_interval_seconds),
            "batch_size": int(settings.org_jt808_sync_batch_size),
            "last_run_at": self._last_run_at.isoformat(sep=" ", timespec="seconds")
            if self._last_run_at
            else None,
            "last_result": self._last_result,
            "last_error": self._last_error,
        }

    def start(self, *, force: bool = False) -> None:
        if not force and not bool(settings.org_jt808_sync_enabled):
            logger.info("组织 808 同步调度未启用（org_jt808_sync_enabled=False）")
            return
        if self.running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="org-jt808-sync")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_once(self) -> dict[str, Any]:
        if not settings.jt808_sync_enabled:
            payload = {"skipped": True, "reason": "jt808_sync_disabled"}
            self._last_run_at = china_now_naive()
            self._last_result = payload
            return payload

        async with AsyncSessionLocal() as db:
            rows = list(
                (
                    await db.execute(
                        select(
                            OrgCompany.id,
                            OrgCompany.name,
                            OrgCompany.parent_id,
                            OrgCompany.jt808_group_id,
                        ).order_by(OrgCompany.id.asc())
                    )
                ).all()
            )

        companies = [
            (int(oid), (name or "").strip(), int(pid) if pid is not None else None, int(gid) if gid is not None else None)
            for oid, name, pid, gid in rows
        ]
        order = _parent_first(companies)
        try:
            groups = await asyncio.to_thread(_load_jt808_groups)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"读取 808 分组失败: {exc}") from exc

        gid_of: dict[int, int | None] = {oid: gid for oid, _n, _p, gid in companies}
        bound = {int(gid) for gid in gid_of.values() if gid}

        created = 0
        bound_existed = 0
        edited = 0
        skipped = 0
        errors: list[str] = []
        ops = 0
        batch = max(1, int(settings.org_jt808_sync_batch_size))

        for oid, name, pid, _gid in order:
            if ops >= batch:
                break
            if not name:
                skipped += 1
                continue
            if pid is None:
                parent_gid = 0
            else:
                parent_gid = gid_of.get(pid)
                if not parent_gid:
                    skipped += 1
                    continue
                parent_gid = int(parent_gid)

            current_gid = gid_of.get(oid)
            if not current_gid:
                match = next(
                    (
                        gid
                        for gid, info in groups.items()
                        if gid not in bound
                        and info["name"] == name
                        and int(info["fid"]) == parent_gid
                    ),
                    None,
                )
                if match is not None:
                    await jt808_group._backfill_group_id(oid, int(match))
                    gid_of[oid] = int(match)
                    bound.add(int(match))
                    bound_existed += 1
                    ops += 1
                    continue
                new_gid = await jt808_group.add_group(name, parent_gid)
                if new_gid:
                    await jt808_group._backfill_group_id(oid, int(new_gid))
                    gid_of[oid] = int(new_gid)
                    bound.add(int(new_gid))
                    groups[int(new_gid)] = {"name": name, "fid": parent_gid}
                    created += 1
                    ops += 1
                else:
                    errors.append(f"{name}#{oid}: 新建 808 分组失败")
                continue

            info = groups.get(int(current_gid))
            if not info:
                new_gid = await jt808_group.add_group(name, parent_gid)
                if new_gid:
                    await jt808_group._backfill_group_id(oid, int(new_gid))
                    gid_of[oid] = int(new_gid)
                    bound.add(int(new_gid))
                    groups[int(new_gid)] = {"name": name, "fid": parent_gid}
                    created += 1
                    ops += 1
                else:
                    errors.append(f"{name}#{oid}: 绑定分组已不存在且重建失败")
                continue

            if info["name"] == name and int(info["fid"]) == parent_gid:
                continue
            ok = await jt808_group.edit_group(int(current_gid), name, parent_gid)
            if ok:
                info["name"] = name
                info["fid"] = parent_gid
                edited += 1
                ops += 1
            else:
                errors.append(f"{name}#{oid}: 改分组失败 gid={current_gid}")

        payload = {
            "org_total": len(companies),
            "created": created,
            "bound_existed": bound_existed,
            "edited": edited,
            "skipped": skipped,
            "errors": errors[:20],
        }
        self._last_run_at = china_now_naive()
        self._last_result = payload
        self._last_error = errors[0] if errors else None
        if created or bound_existed or edited or errors:
            logger.info(
                "组织 808 同步：新建%s 回绑%s 改上级/名称%s 跳过%s 失败%s",
                created,
                bound_existed,
                edited,
                skipped,
                len(errors),
            )
        else:
            logger.info("组织 808 同步：已对齐 %s 家", len(companies))
        return payload

    async def _loop(self) -> None:
        logger.info(
            "组织 808 同步调度已启动，间隔 %ss，每轮最多 %s 条；基础数据改组织会立刻再对一轮",
            int(settings.org_jt808_sync_interval_seconds),
            int(settings.org_jt808_sync_batch_size),
        )
        while self._running:
            try:
                await self.run_once()
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("组织 808 同步执行失败: %s", exc)
            timeout = max(15, int(settings.org_jt808_sync_interval_seconds))
            try:
                await asyncio.wait_for(self._kick.wait(), timeout=timeout)
                self._kick.clear()
            except asyncio.TimeoutError:
                pass


org_jt808_sync_scheduler = OrgJt808SyncScheduler()

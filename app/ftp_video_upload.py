"""历史录像上传：手动唤醒、后台上传、进度和断点续传。

录像目录：/home/huanwei/jt808/jt808_html/JT1078_FTP/{车辆ID}/
手动唤醒按 JT/T 1078：向终端 SIM 发唤醒短信，车辆上线后再查录像。
上传任务写在库里，页面退出后仍由后台盯着 FTP 文件大小；长时间不增长就再次下发 0x9206，
终端按已有文件续传。文件达到设备申报的大小后标记完成，同一时段不再重复下发。
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.jt808_openapi_client import Jt808OpenApiError, jt808_openapi_client
from app.models import FtpUploadTask
from app.sms_mas import send_mas_sms
from app.timeutil import china_now_naive

logger = logging.getLogger(__name__)

_WAKE_TEXT = "WAKEUP"
_WAKE_GAP = timedelta(seconds=90)
_STALL = timedelta(seconds=40)
_SLOW_STALL = timedelta(seconds=180)
_FAST_RESUME = 6
_BATCH_GAP = timedelta(seconds=5)
_MAX_ACTIVE_CARS = 2
_CLICK_GAP = timedelta(seconds=15)
_MAX_ITEMS = 4000
_last_wake: dict[str, datetime] = {}
_dispatch_locks: dict[str, asyncio.Lock] = {}
_HOLD_TTL = timedelta(seconds=30)
_hold_ready = False
_HOLD_LABEL = {
    "live": "实时监控占用，暂停上传",
    "playback": "历史回放占用，暂停上传",
    "voice": "对讲或监听占用，暂停上传",
    "search": "正在查询录像，暂停上传",
}


def _digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _compact_time(value: str) -> str:
    return _digits(value)[:14]


def _as_dt(compact: str) -> datetime | None:
    text = _compact_time(compact)
    if len(text) != 14:
        return None
    try:
        return datetime.strptime(text, "%Y%m%d%H%M%S")
    except ValueError:
        return None


def _acc_on(car_id: str) -> bool:
    from app.obd_mileage_daily import _connect_jt808

    conn = _connect_jt808(read_timeout=8)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT acc FROM tgps_car WHERE id=%s LIMIT 1", (car_id,))
            row = cur.fetchone()
            return str(row[0] if row else "1") == "1"
    except Exception:
        return True
    finally:
        conn.close()


async def _ensure_hold_table(db: AsyncSession) -> None:
    global _hold_ready
    if _hold_ready:
        return
    await db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS ftp_media_hold (
                car_id VARCHAR(32) NOT NULL,
                source VARCHAR(16) NOT NULL,
                slot VARCHAR(32) NOT NULL DEFAULT '',
                expires_at DATETIME NOT NULL,
                PRIMARY KEY (car_id, source, slot)
            )
            """
        )
    )
    await db.commit()
    _hold_ready = True


async def set_media_hold(car_id: str, source: str, slot: str, active: bool) -> None:
    cid = str(car_id or "").strip()
    kind = str(source or "").strip()
    mark = str(slot or "").strip()[:32]
    if not cid.isdigit() or kind not in _HOLD_LABEL:
        return
    previously = await media_hold_message(cid) if active else ""
    async with AsyncSessionLocal() as db:
        await _ensure_hold_table(db)
        if active:
            expires = china_now_naive() + _HOLD_TTL
            await db.execute(
                text(
                    """
                    INSERT INTO ftp_media_hold (car_id, source, slot, expires_at)
                    VALUES (:car, :source, :slot, :expires)
                    ON DUPLICATE KEY UPDATE expires_at = VALUES(expires_at)
                    """
                ),
                {"car": cid, "source": kind, "slot": mark, "expires": expires},
            )
        else:
            await db.execute(
                text(
                    "DELETE FROM ftp_media_hold WHERE car_id=:car AND source=:source AND slot=:slot"
                ),
                {"car": cid, "source": kind, "slot": mark},
            )
        await db.commit()
    if active and not previously:
        try:
            await cancel_inflight_upload(cid)
        except Exception:
            logger.exception("打断录像上传失败 car=%s", cid)


async def media_hold_message(car_id: str) -> str:
    cid = str(car_id or "").strip()
    if not cid.isdigit():
        return ""
    now = china_now_naive()
    async with AsyncSessionLocal() as db:
        await _ensure_hold_table(db)
        await db.execute(text("DELETE FROM ftp_media_hold WHERE expires_at < :now"), {"now": now})
        row = (
            await db.execute(
                text(
                    """
                    SELECT source FROM ftp_media_hold
                    WHERE car_id=:car AND expires_at >= :now
                    ORDER BY expires_at DESC LIMIT 1
                    """
                ),
                {"car": cid, "now": now},
            )
        ).first()
        await db.commit()
    if row is None:
        return ""
    return _HOLD_LABEL.get(str(row[0]), "终端忙，暂停上传")


def _unescape_jt808(packet: bytes) -> bytes:
    out = bytearray()
    index = 0
    while index < len(packet):
        if packet[index] == 0x7D and index + 1 < len(packet) and packet[index + 1] in (0x01, 0x02):
            out.append(0x7D if packet[index + 1] == 0x01 else 0x7E)
            index += 2
            continue
        out.append(packet[index])
        index += 1
    return bytes(out)


def _sn_from_cmd(hex_text: str) -> int:
    digits = re.sub(r"[^0-9A-Fa-f]", "", hex_text or "")
    if len(digits) < 24 or len(digits) % 2:
        return 0
    frame = _unescape_jt808(bytes.fromhex(digits))
    if len(frame) < 13 or frame[0] != 0x7E:
        return 0
    props = int.from_bytes(frame[3:5], "big")
    cursor = 5
    phone_len = 6
    if props & 0x4000:
        cursor += 1
        phone_len = 10
    cursor += phone_len
    if cursor + 2 > len(frame) - 1:
        return 0
    return int.from_bytes(frame[cursor:cursor + 2], "big")


def _latest_9206_sn(car_id: str) -> int:
    from app.obd_mileage_daily import _connect_jt808

    conn = _connect_jt808(read_timeout=8)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT cmd_str FROM tgps_car_cmd WHERE car_id=%s AND cmd_id='9206' ORDER BY ts DESC LIMIT 1",
                (car_id,),
            )
            row = cur.fetchone()
            return _sn_from_cmd(str(row[0] or "")) if row else 0
    except Exception:
        logger.info("未读到上传指令流水号 car=%s", car_id)
        return 0
    finally:
        conn.close()


def _build_9207(phone_hex: str, is_version: bool, reply_sn: int) -> tuple[str, int]:
    """0x9207 上传控制：2=取消。打断后由调度在空闲时重新下发 0x9206。"""
    body = int(reply_sn).to_bytes(2, "big") + bytes([2])
    phone = bytes.fromhex(phone_hex)
    msg_sn = 7
    props = len(body)
    if is_version:
        props |= 0x4000
    frame = bytearray()
    frame += (0x9207).to_bytes(2, "big")
    frame += props.to_bytes(2, "big")
    if is_version:
        frame += bytes([1])
    frame += phone
    frame += msg_sn.to_bytes(2, "big")
    frame += body
    checksum = 0
    for value in frame:
        checksum ^= value
    frame.append(checksum)
    escaped = bytearray([0x7E])
    for value in frame:
        if value == 0x7D:
            escaped += bytes([0x7D, 0x01])
        elif value == 0x7E:
            escaped += bytes([0x7D, 0x02])
        else:
            escaped.append(value)
    escaped.append(0x7E)
    return escaped.hex().upper(), msg_sn


async def terminal_linked(car_id: str) -> bool:
    cid = str(car_id or "").strip()
    if not cid.isdigit():
        return False
    from app.obd_speed_monitor import _new_redis

    redis = _new_redis()
    try:
        raw = await redis.get(cid)
    finally:
        await redis.aclose()
    if not raw:
        return False
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return False
    return bool(str(data.get("JT808ServerId") or "").strip())


async def cancel_inflight_upload(car_id: str) -> None:
    """实时监控、回放、对讲或查目录开始时，取消终端当前这一段上传。"""
    from app.obd_speed_monitor import _new_redis

    cid = str(car_id or "").strip()
    redis = _new_redis()
    try:
        raw = await redis.get(cid)
        if not raw:
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        server_id = str(data.get("JT808ServerId") or "").strip()
        if not server_id:
            return
        version = data.get("isVersion")
        is_version = version is True or str(version).lower() == "true"
        width = 20 if is_version else 12
        phone = re.sub(r"\D", "", str(data.get("tid") or "")).zfill(width)[-width:]
        if len(phone) != width:
            return
        reply_sn = await asyncio.to_thread(_latest_9206_sn, cid)
        packet, msg_sn = _build_9207(phone, is_version, reply_sn)
        tid = str(data.get("tid") or "").lstrip("0") or "0"
        payload = json.dumps(
            {"tid": tid, "cmd": packet, "msgId": 0x9207, "msgSn": msg_sn},
            ensure_ascii=False,
        )
        await redis.lpush(server_id, payload)
    finally:
        await redis.aclose()
    async with AsyncSessionLocal() as db:
        row = (
            await db.execute(
                select(FtpUploadTask).where(
                    FtpUploadTask.car_id == cid,
                    FtpUploadTask.status.in_(("queued", "uploading")),
                ).order_by(FtpUploadTask.id.asc()).limit(1)
            )
        ).scalar_one_or_none()
        if row is not None:
            row.message = "已打断上传，暂停上传"
            row.updated_at = china_now_naive()
            await db.commit()
    logger.info("已下发取消上传 car=%s", cid)


def _lookup_sim(car_id: str) -> str:
    from app.obd_mileage_daily import _connect_jt808

    conn = _connect_jt808(read_timeout=15)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sim FROM tgps_car WHERE id=%s OR tid=%s LIMIT 1",
                (car_id, car_id),
            )
            row = cur.fetchone()
            return str(row[0] or "").strip() if row else ""
    finally:
        conn.close()


def _scan_car_files(car_id: str) -> list[dict[str, Any]]:
    from app.routers.api_media import _iter_ftp_files

    return list(_iter_ftp_files(car_id))


def _file_start(item: dict[str, Any]) -> datetime | None:
    day = _digits(str(item.get("day") or ""))[:8]
    start = str(item.get("start") or "")
    if len(day) != 8 or len(start) < 8:
        return None
    return _as_dt(day + start.replace(":", ""))


def _best_file(files: list[dict[str, Any]], channel: int, stime: str) -> dict[str, Any] | None:
    """文件名里的开始时间要对上这一段，避免邻段把同一个文件认成自己的。"""
    start = _as_dt(stime)
    if start is None:
        return None
    best: dict[str, Any] | None = None
    best_gap: float | None = None
    for item in files:
        if item.get("channel") is None or int(item["channel"]) != int(channel):
            continue
        stamp = _file_start(item)
        if stamp is None:
            continue
        gap = abs((stamp - start).total_seconds())
        if gap <= 30 and (best_gap is None or gap < best_gap):
            best = item
            best_gap = gap
    return best


def _coverage(files: list[dict[str, Any]], channel: int, stime: str, etime: str, expected: int) -> tuple[int, str, bool]:
    item = _best_file(files, channel, stime)
    if item is None:
        return 0, "", False
    size = int(item.get("size") or 0)
    name = str(item.get("name") or "")
    done = expected > 0 and size >= int(expected * 0.98)
    return size, name, done


def _task_out(row: FtpUploadTask) -> dict[str, Any]:
    return {
        "id": row.id,
        "car_id": row.car_id,
        "channel": row.channel,
        "stime": row.stime,
        "etime": row.etime,
        "expected_bytes": int(row.expected_bytes or 0),
        "status": row.status,
        "file_name": row.file_name or "",
        "bytes_size": int(row.bytes_size or 0),
        "percent": int(row.percent or 0),
        "resume_count": int(row.resume_count or 0),
        "message": row.message or "",
    }


async def wake_vehicle(db: AsyncSession, car_id: str) -> dict[str, Any]:
    cid = str(car_id or "").strip()
    if not cid.isdigit():
        return {"ok": False, "message": "无效车辆"}
    now = china_now_naive()
    prev = _last_wake.get(cid)
    if prev and now - prev < _WAKE_GAP:
        return {"ok": True, "message": "唤醒短信刚刚已发送，正在等待车辆上线", "debounced": True}
    sim = await asyncio.to_thread(_lookup_sim, cid)
    mobile = _digits(sim)
    if mobile.startswith("8986") or len(mobile) >= 15:
        return {"ok": False, "message": "车辆资料里的卡号是 SIM 卡序列号，不是手机号，无法发送唤醒短信"}
    if mobile.startswith("86") and len(mobile) > 11:
        mobile = mobile[2:]
    if len(mobile) != 11 or not mobile.startswith("1"):
        return {"ok": False, "message": "该车辆没有可接收短信的手机号，无法手动唤醒"}
    result = await send_mas_sms(db, mobiles=mobile, content=_WAKE_TEXT)
    if not result.ok:
        logger.info("手动唤醒短信未发出 car=%s detail=%s", cid, result.detail)
        return {"ok": False, "message": "手动唤醒短信发送失败"}
    _last_wake[cid] = now
    return {"ok": True, "message": "已发送手动唤醒，正在等待车辆上线"}


def _pending_batch(pending: list[FtpUploadTask]) -> list[FtpUploadTask]:
    """同一通道里首尾相接的未完成段，合成一次 0x9206。设备仍按自己的小文件上传。"""
    if not pending:
        return []
    ordered = sorted(pending, key=lambda row: (int(row.channel or 0), str(row.stime or ""), int(row.id or 0)))
    head = ordered[0]
    batch = [head]
    prev_end = _as_dt(head.etime)
    day = str(head.stime or "")[:8]
    for row in ordered[1:]:
        if int(row.channel or 0) != int(head.channel or 0):
            break
        if str(row.stime or "")[:8] != day:
            break
        start = _as_dt(row.stime)
        end = _as_dt(row.etime)
        if prev_end is None or start is None or end is None or start - prev_end > _BATCH_GAP:
            break
        batch.append(row)
        prev_end = end
    return batch


async def _dispatch(row: FtpUploadTask, etime: str | None = None) -> str:
    try:
        await jt808_openapi_client.upload_history_video(
            device_id=row.car_id,
            channel=str(row.channel),
            stime=row.stime,
            etime=etime or row.etime,
            media_type=row.media_type or "0",
            stream_type=row.stream_type or "0",
            storage_type=row.storage_type or "0",
        )
    except Jt808OpenApiError as exc:
        return str(exc)[:180] or "上传指令下发失败"
    return ""


async def start_uploads(db: AsyncSession, car_id: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """只入库排队。指令由运维页所在的那一个调度下发，避免用户连点把网页进程打满。"""
    cid = str(car_id or "").strip()
    if not cid.isdigit():
        return []
    if len(items) > _MAX_ITEMS:
        items = items[:_MAX_ITEMS]
    now = china_now_naive()
    hot = (
        await db.execute(
            text(
                "SELECT COUNT(*), MAX(updated_at) FROM ftp_upload_task "
                "WHERE car_id=:cid AND status IN ('queued','uploading')"
            ),
            {"cid": cid},
        )
    ).one()
    pending_n = int(hot[0] or 0)
    latest = hot[1]
    if pending_n and latest is not None and now - latest < _CLICK_GAP:
        return await _tasks_for_items(db, cid, items)
    files = await asyncio.to_thread(_scan_car_files, cid)
    out: list[dict[str, Any]] = []
    for raw in items:
        channel = int(raw.get("channel") or 0)
        stime = _compact_time(str(raw.get("stime") or ""))
        etime = _compact_time(str(raw.get("etime") or ""))
        if channel <= 0 or len(stime) != 14 or len(etime) != 14:
            continue
        expected = max(0, int(raw.get("length") or raw.get("expected_bytes") or 0))
        size, name, done = _coverage(files, channel, stime, etime, expected)
        existing = (
            await db.execute(
                select(FtpUploadTask).where(
                    FtpUploadTask.car_id == cid,
                    FtpUploadTask.channel == channel,
                    FtpUploadTask.stime == stime,
                    FtpUploadTask.etime == etime,
                ).limit(1)
            )
        ).scalar_one_or_none()
        if done or (existing is not None and existing.status == "done"):
            if existing is None:
                existing = FtpUploadTask(
                    car_id=cid,
                    channel=channel,
                    stime=stime,
                    etime=etime,
                    media_type=str(raw.get("type") or "0"),
                    stream_type=str(raw.get("mltype") or "0"),
                    storage_type=str(raw.get("zcqtype") or "0"),
                    expected_bytes=expected,
                    status="done",
                    file_name=name,
                    bytes_size=size,
                    percent=100,
                    message="文件已存在",
                    created_at=now,
                    updated_at=now,
                )
                db.add(existing)
            else:
                existing.status = "done"
                existing.percent = 100
                existing.bytes_size = max(int(existing.bytes_size or 0), size)
                existing.file_name = name or existing.file_name
                existing.message = "文件已存在"
                existing.updated_at = now
            await db.flush()
            out.append(_task_out(existing))
            continue
        if existing is None:
            existing = FtpUploadTask(
                car_id=cid,
                channel=channel,
                stime=stime,
                etime=etime,
                media_type=str(raw.get("type") or "0"),
                stream_type=str(raw.get("mltype") or "0"),
                storage_type=str(raw.get("zcqtype") or "0"),
                expected_bytes=expected,
                status="queued",
                bytes_size=size,
                percent=0 if expected <= 0 else min(99, int(size * 100 / expected)),
                message="排队等待",
                created_at=now,
                updated_at=now,
            )
            db.add(existing)
            await db.flush()
        elif existing.status in ("queued", "uploading"):
            pass
        else:
            existing.status = "queued"
            existing.message = "排队等待"
            existing.updated_at = now
        await db.flush()
        out.append(_task_out(existing))
    await db.commit()
    return out


async def _tasks_for_items(db: AsyncSession, car_id: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = (
        await db.execute(select(FtpUploadTask).where(FtpUploadTask.car_id == car_id))
    ).scalars().all()
    by_key = {(int(row.channel), row.stime, row.etime): row for row in rows}
    out = []
    for raw in items:
        try:
            channel = int(raw.get("channel") or 0)
        except (TypeError, ValueError):
            continue
        key = (channel, _compact_time(str(raw.get("stime") or "")), _compact_time(str(raw.get("etime") or "")))
        row = by_key.get(key)
        if row is not None:
            out.append(_task_out(row))
    return out


def schedule_dispatch(car_id: str) -> None:
    """下发 0x9206 不占着页面请求。同一辆车按顺序下发，避免一次堵死。"""
    lock = _dispatch_locks.setdefault(car_id, asyncio.Lock())
    try:
        asyncio.get_running_loop().create_task(_dispatch_car(car_id, lock))
    except RuntimeError:
        return


async def _dispatch_car(car_id: str, lock: asyncio.Lock) -> None:
    """同一辆车一次下发当前通道里接得上的整段时间，不再一分钟一条指令。"""
    async with lock:
        async with AsyncSessionLocal() as db:
            rows = (
                await db.execute(
                    select(FtpUploadTask).where(
                        FtpUploadTask.car_id == car_id,
                        FtpUploadTask.status.in_(("queued", "uploading")),
                    ).order_by(FtpUploadTask.id.asc())
                )
            ).scalars().all()
            batch = _pending_batch(list(rows))
            if not batch or batch[0].last_dispatch_at is not None:
                return
            busy = await media_hold_message(car_id)
            now = china_now_naive()
            if busy:
                batch[0].message = busy
                batch[0].updated_at = now
                await db.commit()
                return
            err = await _dispatch(batch[0], batch[-1].etime)
            for row in batch:
                row.last_dispatch_at = now
                row.updated_at = now
                if row is batch[0]:
                    row.status = "uploading"
                    row.message = err or "正在上传"
                else:
                    row.status = "queued"
                    row.message = "排队等待"
            await db.commit()


async def list_uploads(db: AsyncSession, car_id: str, day: str = "") -> list[dict[str, Any]]:
    cid = str(car_id or "").strip()
    if not cid.isdigit():
        return []
    rows = (
        await db.execute(
            select(FtpUploadTask).where(FtpUploadTask.car_id == cid).order_by(FtpUploadTask.id.desc())
        )
    ).scalars().all()
    want = _digits(day)[:8]
    out = []
    for row in rows:
        if want and not str(row.stime or "").startswith(want):
            continue
        out.append(_task_out(row))
    return out


async def refresh_uploading() -> None:
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(FtpUploadTask).where(
                    FtpUploadTask.status.in_(("uploading", "queued", "done"))
                ).order_by(FtpUploadTask.id.asc())
            )
        ).scalars().all()
        if not rows:
            return
        now = china_now_naive()
        groups: dict[str, list[FtpUploadTask]] = {}
        for row in rows:
            groups.setdefault(row.car_id, []).append(row)
        ordered_groups = sorted(
            groups.items(),
            key=lambda item: (
                0 if any(r.last_dispatch_at and r.status in ("queued", "uploading") for r in item[1]) else 1,
                item[0],
            ),
        )
        active_cars = 0
        for car_id, car_rows in ordered_groups:
            files = await asyncio.to_thread(_scan_car_files, car_id)
            pending: list[FtpUploadTask] = []
            for row in car_rows:
                size, name, done = _coverage(
                    files, row.channel, row.stime, row.etime, int(row.expected_bytes or 0)
                )
                if size > int(row.bytes_size or 0):
                    row.bytes_size = size
                    row.last_growth_at = now
                if name:
                    row.file_name = name
                expected = int(row.expected_bytes or 0)
                growth_at = row.last_growth_at or row.created_at or now
                settled = expected <= 0 and size > 0 and now - growth_at >= _SLOW_STALL
                # 文件已经停住、续传几次仍不增长，就收下现有文件，改传下一段
                stalled_file = (
                    size > 0
                    and now - growth_at >= _SLOW_STALL
                    and int(row.resume_count or 0) >= 2
                )
                if size > 0 and (done or settled or stalled_file):
                    row.status = "done"
                    row.percent = 100
                    row.bytes_size = size
                    row.file_name = name or row.file_name
                    row.message = "上传完成"
                    row.updated_at = now
                    continue
                if size <= 0 and row.status == "done":
                    row.status = "queued"
                    row.percent = 0
                    row.bytes_size = 0
                    row.file_name = ""
                    row.last_growth_at = None
                    row.message = "排队等待"
                    row.updated_at = now
                elif expected > 0 and size > 0:
                    row.percent = min(99, int(size * 100 / expected))
                    row.bytes_size = size
                pending.append(row)
            if not pending:
                continue
            batch = _pending_batch(pending)
            head = batch[0]
            for row in pending:
                if row is head:
                    continue
                if row.status != "queued" or row.message != "排队等待":
                    row.status = "queued"
                    row.message = "排队等待"
                    row.updated_at = now
            head.status = "uploading"
            window = _SLOW_STALL if int(head.resume_count or 0) >= _FAST_RESUME else _STALL
            peers = [
                row for row in car_rows
                if int(row.channel or 0) == int(head.channel or 0)
                and str(row.stime or "")[:8] == str(head.stime or "")[:8]
            ]
            growth_times = [row.last_growth_at for row in peers if row.last_growth_at]
            latest_growth = max(growth_times) if growth_times else None
            dispatch_times = [row.last_dispatch_at for row in batch if row.last_dispatch_at]
            latest_dispatch = max(dispatch_times) if dispatch_times else None
            grew = any(row.last_growth_at == now for row in peers)
            stalled = latest_growth is None or now - latest_growth >= window
            quiet = latest_dispatch is None or now - latest_dispatch >= window
            acc_on = await asyncio.to_thread(_acc_on, car_id)
            if not acc_on:
                head.message = "车辆熄火，唤醒后再继续上传"
                head.updated_at = now
                continue
            busy = await media_hold_message(car_id)
            if busy:
                head.message = busy
                head.updated_at = now
                continue
            # 这一批还在落文件就不要再下发。停住超过窗口，才从还没完成的第一段续传。
            # 同时只让有限辆车占着指令，其余在调度里排队，避免用户连点把终端打满。
            leaving_hold = "暂停上传" in (head.message or "")
            in_flight = latest_dispatch is not None and (grew or not (stalled and quiet)) and not leaving_hold
            if in_flight:
                active_cars += 1
                head.message = "正在上传"
            elif not grew and (latest_dispatch is None or leaving_hold or (stalled and quiet)):
                if active_cars >= _MAX_ACTIVE_CARS:
                    head.message = f"调度排队，同时最多上传{_MAX_ACTIVE_CARS}辆车"
                else:
                    resent = latest_dispatch is not None
                    err = await _dispatch(head, batch[-1].etime)
                    for row in batch:
                        row.last_dispatch_at = now
                    active_cars += 1
                    if resent:
                        head.resume_count = int(head.resume_count or 0) + 1
                        head.message = err or "网络中断，正在断点续传"
                    else:
                        head.message = err or "正在上传"
            else:
                head.message = "正在上传"
            head.updated_at = now
        await db.commit()


class FtpUploadScheduler:
    def __init__(self) -> None:
        self._running = False
        self._task: asyncio.Task | None = None
        self._last_run_at = ""
        self._last_error = ""

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="ftp-upload-scheduler")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def status(self) -> dict[str, Any]:
        return {
            "running": self._running,
            "interval_seconds": 5,
            "max_active_cars": _MAX_ACTIVE_CARS,
            "click_gap_seconds": int(_CLICK_GAP.total_seconds()),
            "last_run_at": self._last_run_at,
            "last_error": self._last_error,
        }

    async def _loop(self) -> None:
        while self._running:
            started = china_now_naive()
            try:
                await refresh_uploading()
                self._last_error = ""
            except Exception as exc:
                self._last_error = str(exc)[:180]
                logger.exception("录像上传进度刷新失败")
            self._last_run_at = started.strftime("%Y-%m-%d %H:%M:%S")
            await asyncio.sleep(5)


async def upload_queue_snapshot() -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                text(
                    "SELECT status, COUNT(*), COUNT(DISTINCT car_id) "
                    "FROM ftp_upload_task GROUP BY status"
                )
            )
        ).all()
    counts = {str(status): {"tasks": int(n or 0), "cars": int(cars or 0)} for status, n, cars in rows}
    return {"running": ftp_upload_scheduler.status(), "counts": counts}


ftp_upload_scheduler = FtpUploadScheduler()

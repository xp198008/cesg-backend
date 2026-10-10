"""808 主动安全证据媒体代理（HTTPS 页面同源访问，不依赖 gb35658）。"""



from __future__ import annotations



import httpx

from fastapi import APIRouter, HTTPException

from fastapi.responses import Response



from app.media_url import jt808_media_origin



router = APIRouter(prefix="/api/media", tags=["media"])





@router.get("/adas/{path:path}")

async def proxy_adas_media(path: str) -> Response:

    rel = (path or "").lstrip("/")

    if not rel or ".." in rel.split("/"):

        raise HTTPException(status_code=400, detail="无效路径")



    upstream = f"{jt808_media_origin()}/ADAS_FILE/{rel}"

    try:

        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:

            resp = await client.get(upstream)

    except httpx.HTTPError as exc:

        raise HTTPException(status_code=502, detail=f"808 媒体拉取失败: {exc}") from exc



    if resp.status_code >= 400:

        raise HTTPException(status_code=resp.status_code, detail="媒体不存在")



    content_type = (resp.headers.get("content-type") or "application/octet-stream").split(";")[0].strip()

    headers = {"Cache-Control": "public, max-age=3600"}

    accept_ranges = resp.headers.get("accept-ranges")

    if accept_ranges:

        headers["Accept-Ranges"] = accept_ranges

    content_length = resp.headers.get("content-length")

    if content_length:

        headers["Content-Length"] = content_length



    return Response(content=resp.content, media_type=content_type, headers=headers)


import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path

from fastapi import Query
from fastapi.responses import FileResponse

_FTP_ROOT = Path(os.environ.get("JT1078_FTP_ROOT", "/home/huanwei/jt808/jt808_html/JT1078_FTP"))
_TR_NAME = re.compile(r"^TR-(\d{8})-(\d{6})-CH(\d+)\.", re.I)
_CH_NAME = re.compile(r"^ch(\d+)_(\d{12})_(\d{12})\.", re.I)
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,180}$")
_VIDEO_EXT = {".avi", ".mp4", ".flv", ".h264", ".bin"}


def _ftp_car_dir(car_id: str) -> Path:
    cid = str(car_id or "").strip()
    if not cid.isdigit() or len(cid) > 12:
        raise HTTPException(status_code=400, detail="无效车辆")
    root = _FTP_ROOT.resolve()
    folder = (root / cid).resolve()
    if folder.parent != root:
        raise HTTPException(status_code=400, detail="无效车辆")
    return folder


def _file_meta(name: str, mtime: float) -> dict:
    matched = _TR_NAME.match(name)
    if matched:
        hms = matched.group(2)
        return {
            "day": matched.group(1),
            "start": f"{hms[0:2]}:{hms[2:4]}:{hms[4:6]}",
            "channel": int(matched.group(3)),
        }
    matched = _CH_NAME.match(name)
    if matched:
        start = matched.group(2)
        hms = start[6:]
        return {
            "day": "20" + start[:6],
            "start": f"{hms[0:2]}:{hms[2:4]}:{hms[4:6]}",
            "channel": int(matched.group(1)),
        }
    from datetime import datetime
    stamp = datetime.fromtimestamp(mtime)
    return {"day": stamp.strftime("%Y%m%d"), "start": stamp.strftime("%H:%M:%S"), "channel": None}


def _iter_ftp_files(car_id: str):
    folder = _ftp_car_dir(car_id)
    if not folder.is_dir():
        return
    for path in folder.iterdir():
        if not path.is_file():
            continue
        name = path.name
        if not _SAFE_NAME.match(name) or path.suffix.lower() not in _VIDEO_EXT:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        meta = _file_meta(name, stat.st_mtime)
        yield {
            "name": name,
            "size": stat.st_size,
            "day": meta["day"],
            "start": meta["start"],
            "channel": meta["channel"],
        }


def _day_text(value: str) -> str:
    day = re.sub(r"\D", "", value or "")[:8]
    if len(day) != 8:
        raise HTTPException(status_code=400, detail="无效日期")
    return day


def _catalog_path(car_id: str, day: str) -> Path:
    return _ftp_car_dir(car_id) / f"catalog-{day}.xml"


def _read_catalog(car_id: str, day: str) -> list[dict]:
    path = _catalog_path(car_id, day)
    if not path.is_file():
        return []
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return []
    items = []
    for node in root.findall("item"):
        stime = re.sub(r"\D", "", node.get("stime") or "")[:14]
        etime = re.sub(r"\D", "", node.get("etime") or "")[:14]
        try:
            channel = int(node.get("channel") or "0")
        except ValueError:
            continue
        if channel <= 0 or len(stime) != 14 or len(etime) != 14:
            continue
        try:
            length = int(node.get("length") or "0")
        except ValueError:
            length = 0
        items.append({
            "channel": channel,
            "stime": stime,
            "etime": etime,
            "type": node.get("type") or "0",
            "mltype": node.get("mltype") or "0",
            "zcqtype": node.get("zcqtype") or "0",
            "length": max(0, length),
            "alarm": (node.get("alarm") or "") == "1",
        })
    return items


def _write_catalog(car_id: str, day: str, items: list[dict]) -> None:
    folder = _ftp_car_dir(car_id)
    folder.mkdir(parents=True, exist_ok=True)
    root = ET.Element("catalog", {"car": str(car_id), "date": day})
    for item in items[:4000]:
        stime = re.sub(r"\D", "", str(item.get("stime") or ""))[:14]
        etime = re.sub(r"\D", "", str(item.get("etime") or ""))[:14]
        try:
            channel = int(item.get("channel") or 0)
        except (TypeError, ValueError):
            continue
        if channel <= 0 or len(stime) != 14 or len(etime) != 14:
            continue
        try:
            length = max(0, int(item.get("length") or 0))
        except (TypeError, ValueError):
            length = 0
        ET.SubElement(root, "item", {
            "channel": str(channel),
            "stime": stime,
            "etime": etime,
            "type": str(item.get("type") if item.get("type") is not None else "0"),
            "mltype": str(item.get("mltype") if item.get("mltype") is not None else "0"),
            "zcqtype": str(item.get("zcqtype") if item.get("zcqtype") is not None else "0"),
            "length": str(length),
            "alarm": "1" if item.get("alarm") else "0",
        })
    path = _catalog_path(car_id, day)
    tmp = path.with_suffix(".xml.tmp")
    ET.ElementTree(root).write(tmp, encoding="utf-8", xml_declaration=True)
    tmp.replace(path)


@router.get("/ftp/days")
def ftp_video_days(car_id: str = Query(...)) -> dict:
    days = sorted({item["day"] for item in _iter_ftp_files(car_id) if item["day"]})
    return {"car_id": str(car_id).strip(), "days": days}


def _ftp_file_still_uploading(item: dict, tasks: list[dict]) -> bool:
    """断点续传或还没传完的文件不进下载列表。"""
    from app.ftp_video_upload import _best_file

    name = str(item.get("name") or "")
    for task in tasks:
        if str(task.get("status") or "") == "done":
            continue
        if name and name == str(task.get("file_name") or ""):
            return True
        try:
            channel = int(task.get("channel") or 0)
        except (TypeError, ValueError):
            continue
        stime = str(task.get("stime") or "")
        if channel > 0 and _best_file([item], channel, stime) is not None:
            return True
    return False


@router.get("/ftp/files")
async def ftp_video_files(car_id: str = Query(...), date: str = Query(...)) -> dict:
    day = re.sub(r"\D", "", date or "")[:8]
    if len(day) != 8:
        raise HTTPException(status_code=400, detail="无效日期")
    files = [item for item in _iter_ftp_files(car_id) if item["day"] == day]
    from app.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        tasks = await list_uploads(db, car_id, day)
    files = [item for item in files if not _ftp_file_still_uploading(item, tasks)]
    files.sort(key=lambda item: (item["channel"] or 0, item["start"] or "", item["name"]))
    return {"car_id": str(car_id).strip(), "date": day, "files": files}


@router.get("/ftp/file")
def ftp_video_file(car_id: str = Query(...), name: str = Query(...)) -> FileResponse:
    filename = Path(str(name or "")).name
    if filename != str(name or "") or not _SAFE_NAME.match(filename):
        raise HTTPException(status_code=400, detail="无效文件")
    if Path(filename).suffix.lower() not in _VIDEO_EXT:
        raise HTTPException(status_code=400, detail="无效文件")
    folder = _ftp_car_dir(car_id)
    path = (folder / filename).resolve()
    if path.parent != folder or not path.is_file():
        raise HTTPException(status_code=404, detail="录像不存在")
    return FileResponse(path, filename=filename, media_type="application/octet-stream")


_PLAY_CACHE = Path("/home/huanwei/cesg/cache/ftp-play")
_play_locks: dict[str, object] = {}
_play_locks_guard = None


def _play_lock(key: str):
    import threading

    global _play_locks_guard
    if _play_locks_guard is None:
        _play_locks_guard = threading.Lock()
    with _play_locks_guard:
        lock = _play_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _play_locks[key] = lock
        return lock


def _browser_mp4(car_id: str, source: Path) -> Path:
    """AVI 里是 H.264，浏览器不能直接播。转封装成 mp4 后给页面播放，不重新编码。"""
    import subprocess

    if source.suffix.lower() == ".mp4":
        return source
    target_dir = _PLAY_CACHE / str(car_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{source.stem}.mp4"
    try:
        fresh = target.is_file() and target.stat().st_size > 0 and target.stat().st_mtime >= source.stat().st_mtime
    except OSError:
        fresh = False
    if fresh:
        return target
    lock = _play_lock(f"{car_id}/{source.name}")
    with lock:
        try:
            fresh = target.is_file() and target.stat().st_size > 0 and target.stat().st_mtime >= source.stat().st_mtime
        except OSError:
            fresh = False
        if fresh:
            return target
        part = target.with_suffix(".part.mp4")
        try:
            proc = subprocess.run(
                [
                    "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                    "-i", str(source),
                    "-c", "copy",
                    "-f", "mp4",
                    "-movflags", "+faststart",
                    str(part),
                ],
                capture_output=True,
                timeout=180,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            part.unlink(missing_ok=True)
            raise HTTPException(status_code=500, detail="录像转播放格式失败") from exc
        if proc.returncode != 0 or not part.is_file() or part.stat().st_size <= 0:
            part.unlink(missing_ok=True)
            raise HTTPException(status_code=500, detail="录像转播放格式失败")
        part.replace(target)
    return target


@router.get("/ftp/play")
def ftp_video_play(car_id: str = Query(...), name: str = Query(...)) -> FileResponse:
    filename = Path(str(name or "")).name
    if filename != str(name or "") or not _SAFE_NAME.match(filename):
        raise HTTPException(status_code=400, detail="无效文件")
    if Path(filename).suffix.lower() not in _VIDEO_EXT:
        raise HTTPException(status_code=400, detail="无效文件")
    folder = _ftp_car_dir(car_id)
    path = (folder / filename).resolve()
    if path.parent != folder or not path.is_file():
        raise HTTPException(status_code=404, detail="录像不存在")
    playable = _browser_mp4(str(car_id).strip(), path)
    return FileResponse(playable, media_type="video/mp4", content_disposition_type="inline")


from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.ftp_video_upload import list_uploads, set_media_hold, start_uploads, terminal_linked, wake_vehicle
from fastapi import Depends


class FtpWakeBody(BaseModel):
    car_id: str


class FtpUploadItem(BaseModel):
    channel: int
    stime: str
    etime: str
    type: str | int = 0
    mltype: str | int = 0
    zcqtype: str | int = 0
    length: int = 0


class FtpUploadBody(BaseModel):
    car_id: str
    items: list[FtpUploadItem] = Field(default_factory=list)


class FtpBusyBody(BaseModel):
    car_id: str
    source: str = "live"
    slot: str = ""
    active: bool = True


class CatalogItem(BaseModel):
    channel: int
    stime: str
    etime: str
    type: str | int = 0
    mltype: str | int = 0
    zcqtype: str | int = 0
    length: int = 0
    alarm: bool = False

    @field_validator("stime", "etime", "type", "mltype", "zcqtype", mode="before")
    @classmethod
    def _as_text(cls, value):
        if value is None or value == "":
            return "0"
        return str(value).strip()

    @field_validator("channel", "length", mode="before")
    @classmethod
    def _as_int(cls, value):
        if value is None or value == "":
            return 0
        return int(float(value))

    @field_validator("alarm", mode="before")
    @classmethod
    def _as_bool(cls, value):
        if isinstance(value, str):
            return value.strip().lower() not in ("", "0", "false")
        return bool(value)


class CatalogBody(BaseModel):
    car_id: str
    date: str
    items: list[CatalogItem] = Field(default_factory=list)

    @field_validator("car_id", "date", mode="before")
    @classmethod
    def _id_text(cls, value):
        return str(value or "").strip()


@router.post("/ftp/wake")
async def ftp_wake(body: FtpWakeBody, db: AsyncSession = Depends(get_db)) -> dict:
    return await wake_vehicle(db, body.car_id)


@router.post("/ftp/busy")
async def ftp_busy(body: FtpBusyBody) -> dict:
    await set_media_hold(body.car_id, body.source, body.slot, body.active)
    return {"ok": True}


@router.get("/ftp/link")
async def ftp_link(car_id: str = Query(...)) -> dict:
    cid = str(car_id or "").strip()
    return {"car_id": cid, "online": await terminal_linked(cid)}


@router.get("/ftp/catalog")
def ftp_catalog(car_id: str = Query(...), date: str = Query(...)) -> dict:
    day = _day_text(date)
    cid = str(car_id).strip()
    saved = _read_catalog(cid, day)
    if saved:
        return {"car_id": cid, "date": day, "source": "xml", "items": saved}
    return {"car_id": cid, "date": day, "source": "none", "items": []}


@router.post("/ftp/catalog")
def ftp_catalog_save(body: CatalogBody) -> dict:
    day = _day_text(body.date)
    cid = str(body.car_id).strip()
    _write_catalog(cid, day, [item.model_dump() for item in body.items])
    return {"ok": True, "car_id": cid, "date": day}


@router.post("/ftp/upload")
async def ftp_upload(body: FtpUploadBody, db: AsyncSession = Depends(get_db)) -> dict:
    tasks = await start_uploads(db, body.car_id, [item.model_dump() for item in body.items])
    return {"car_id": str(body.car_id).strip(), "tasks": tasks}


@router.get("/ftp/tasks")
async def ftp_tasks(
    car_id: str = Query(...),
    date: str = Query(""),
    db: AsyncSession = Depends(get_db),
) -> dict:
    tasks = await list_uploads(db, car_id, date)
    return {"car_id": str(car_id).strip(), "tasks": tasks}


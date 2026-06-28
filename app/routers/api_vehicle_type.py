"""车辆类型维护：基础数据本地 CRUD。"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal, get_db
from app.models import Vehicle, VehicleTypeDict

router = APIRouter(prefix="/api/vehicle-type", tags=["vehicle-type"])

_ICON_DIR = Path(__file__).resolve().parents[2] / "data" / "vehicle_type_icons"
_ICON_DIR.mkdir(parents=True, exist_ok=True)
_ALLOWED_ICON_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
_ICON_SIZE = 72

DEFAULT_VEHICLE_TYPES = [
    {
        "type_code": "HW2026001",
        "type_name": "隧道清洗车",
        "icon_url": "./images/carIcon/qingjie.png",
        "spec": "18吨纯电动",
        "site": "城东环卫作业站",
    },
    {
        "type_code": "HW2026001",
        "type_name": "扫地车",
        "icon_url": "./images/carIcon/xisao.png",
        "spec": "18吨纯电动",
        "site": "城东环卫作业站",
    },
    {
        "type_code": "HW2026001",
        "type_name": "拉臂钩车",
        "icon_url": "./images/carIcon/laji.png",
        "spec": "18吨纯电动",
        "site": "城东环卫作业站",
    },
    {
        "type_code": "HW2026001",
        "type_name": "后压车",
        "icon_url": "./images/carIcon/yasuo.png",
        "spec": "12吨道路后压车",
        "site": "城东环卫作业站",
    },
    {
        "type_code": "HW2026001",
        "type_name": "风炮车",
        "icon_url": "./images/carIcon/xiwu.png",
        "spec": "封闭式风炮车",
        "site": "城东环卫作业站",
    },
]


class VehicleTypeCreateIn(BaseModel):
    type_name: str = Field(..., min_length=1, max_length=64)
    spec: str = Field(..., min_length=1, max_length=256)
    site: str | None = Field(None, max_length=128)
    icon_url: str | None = Field(None, max_length=256)


class VehicleTypeUpdateIn(BaseModel):
    type_name: str | None = Field(None, min_length=1, max_length=64)
    spec: str | None = Field(None, max_length=256)
    site: str | None = Field(None, max_length=128)
    icon_url: str | None = Field(None, max_length=256)


def _gen_type_code() -> str:
    return f"HW{datetime.now().strftime('%Y%m%d%H%M%S')}"


def _png_size(content: bytes) -> tuple[int, int]:
    if len(content) < 24 or content[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError("invalid png")
    width = int.from_bytes(content[16:20], "big")
    height = int.from_bytes(content[20:24], "big")
    return width, height


def _jpeg_size(content: bytes) -> tuple[int, int]:
    if len(content) < 4 or content[:2] != b"\xff\xd8":
        raise ValueError("invalid jpeg")
    index = 2
    while index + 1 < len(content):
        while index < len(content) and content[index] == 0xFF:
            index += 1
        if index >= len(content):
            break
        marker = content[index]
        index += 1
        if marker in (0xD8, 0xD9):
            continue
        if index + 1 >= len(content):
            break
        segment_len = int.from_bytes(content[index : index + 2], "big")
        if segment_len < 2 or index + segment_len > len(content):
            break
        if marker in (
            0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF,
        ):
            height = int.from_bytes(content[index + 3 : index + 5], "big")
            width = int.from_bytes(content[index + 5 : index + 7], "big")
            return width, height
        index += segment_len
    raise ValueError("invalid jpeg")


def _webp_size(content: bytes) -> tuple[int, int]:
    if len(content) < 30 or content[:4] != b"RIFF" or content[8:12] != b"WEBP":
        raise ValueError("invalid webp")
    chunk = content[12:16]
    if chunk == b"VP8X":
        width = 1 + int.from_bytes(content[24:27], "little")
        height = 1 + int.from_bytes(content[27:30], "little")
        return width, height
    if chunk == b"VP8 " and len(content) >= 30:
        width = int.from_bytes(content[26:28], "little") & 0x3FFF
        height = int.from_bytes(content[28:30], "little") & 0x3FFF
        return width, height
    if chunk == b"VP8L" and len(content) >= 25:
        bits = int.from_bytes(content[21:25], "little")
        width = (bits & 0x3FFF) + 1
        height = ((bits >> 14) & 0x3FFF) + 1
        return width, height
    raise ValueError("invalid webp")


def _image_size(content: bytes, suffix: str) -> tuple[int, int]:
    if suffix in {".jpg", ".jpeg"}:
        return _jpeg_size(content)
    if suffix == ".png":
        return _png_size(content)
    if suffix == ".webp":
        return _webp_size(content)
    raise ValueError("unsupported")


def _ensure_icon_format(content: bytes, suffix: str) -> None:
    if suffix in {".jpg", ".jpeg"} and not content.startswith(b"\xff\xd8"):
        raise HTTPException(status_code=400, detail="图片格式无效，请上传 jpg 或 jpeg 文件")
    if suffix == ".png" and not content.startswith(b"\x89PNG\r\n\x1a\n"):
        raise HTTPException(status_code=400, detail="图片格式无效，请上传 png 文件")
    if suffix == ".webp" and (len(content) < 12 or content[:4] != b"RIFF" or content[8:12] != b"WEBP"):
        raise HTTPException(status_code=400, detail="图片格式无效，请上传 webp 文件")


def _ensure_icon_size(content: bytes, suffix: str) -> None:
    _ensure_icon_format(content, suffix)
    try:
        width, height = _image_size(content, suffix)
    except ValueError:
        raise HTTPException(status_code=400, detail="无法识别图片尺寸，请上传有效的 jpg、png 或 webp 图片")
    if width != _ICON_SIZE or height != _ICON_SIZE:
        raise HTTPException(status_code=400, detail=f"车型图标尺寸必须为 {_ICON_SIZE}×{_ICON_SIZE} 像素")


def _row_out(row: VehicleTypeDict, vehicle_count: int = 0) -> dict:
    return {
        "id": row.id,
        "type_code": row.type_code,
        "type_name": row.type_name,
        "icon_url": row.icon_url,
        "spec": row.spec,
        "site": row.site,
        "sort_order": row.sort_order,
        "vehicle_count": vehicle_count,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


async def _vehicle_counts_by_type(db: AsyncSession, type_names: list[str]) -> dict[str, int]:
    names = [name.strip() for name in type_names if name and name.strip()]
    if not names:
        return {}
    rows = (
        await db.execute(
            select(Vehicle.vehicle_type, func.count())
            .where(Vehicle.vehicle_type.in_(names))
            .group_by(Vehicle.vehicle_type)
        )
    ).all()
    return {name: int(count) for name, count in rows if name}


async def _vehicle_count_for_type(db: AsyncSession, type_name: str | None) -> int:
    if not type_name or not type_name.strip():
        return 0
    return int(
        await db.scalar(
            select(func.count())
            .select_from(Vehicle)
            .where(Vehicle.vehicle_type == type_name.strip())
        )
        or 0
    )


async def _ensure_unique_type_name(
    db: AsyncSession,
    type_name: str,
    exclude_id: int | None = None,
) -> None:
    name = type_name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="请输入车型名称")
    stmt = select(VehicleTypeDict.id).where(VehicleTypeDict.type_name == name)
    if exclude_id is not None:
        stmt = stmt.where(VehicleTypeDict.id != exclude_id)
    exists = await db.scalar(stmt.limit(1))
    if exists is not None:
        raise HTTPException(status_code=400, detail="车辆类型名称已存在，请更换后重试")


def _normalize_required_text(value: str | None, field_label: str) -> str:
    text = (value or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail=f"请输入{field_label}")
    return text


async def ensure_default_vehicle_types() -> None:
    """空库首次启动时写入页面原有 5 条车辆类型记录。"""
    async with AsyncSessionLocal() as db:
        n = await db.scalar(select(func.count()).select_from(VehicleTypeDict))
        if n and n > 0:
            return
        for index, item in enumerate(DEFAULT_VEHICLE_TYPES, start=1):
            db.add(VehicleTypeDict(sort_order=index, **item))
        await db.commit()


@router.get("/list")
async def vehicle_type_list(
    type_name: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(VehicleTypeDict)
    if type_name and type_name.strip():
        stmt = stmt.where(VehicleTypeDict.type_name.ilike(f"%{type_name.strip()}%"))
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (
        await db.execute(
            stmt.order_by(VehicleTypeDict.sort_order.asc(), VehicleTypeDict.id.asc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).scalars().all()
    counts = await _vehicle_counts_by_type(db, [x.type_name for x in rows])
    return {
        "total": total,
        "items": [_row_out(x, counts.get(x.type_name, 0)) for x in rows],
        "page": page,
        "page_size": page_size,
    }


@router.get("/type-options")
async def vehicle_type_options(db: AsyncSession = Depends(get_db)):
    rows = (
        await db.execute(
            select(VehicleTypeDict.type_name)
            .where(VehicleTypeDict.type_name.isnot(None), VehicleTypeDict.type_name != "")
            .group_by(VehicleTypeDict.type_name)
            .order_by(func.min(VehicleTypeDict.sort_order).asc(), VehicleTypeDict.type_name.asc())
        )
    ).scalars().all()
    return {"ok": True, "items": [{"label": name, "value": name} for name in rows]}


@router.get("/{tid}")
async def vehicle_type_get(tid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleTypeDict).where(VehicleTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    vehicle_count = await _vehicle_count_for_type(db, row.type_name)
    return {"ok": True, "data": _row_out(row, vehicle_count)}


@router.post("")
async def vehicle_type_create(body: VehicleTypeCreateIn, db: AsyncSession = Depends(get_db)):
    type_name = _normalize_required_text(body.type_name, "车型名称")
    spec = _normalize_required_text(body.spec, "车型规格")
    await _ensure_unique_type_name(db, type_name)
    max_order = await db.scalar(select(func.max(VehicleTypeDict.sort_order))) or 0
    row = VehicleTypeDict(
        type_code=_gen_type_code(),
        type_name=type_name,
        spec=spec,
        site=(body.site or "").strip() or None,
        icon_url=(body.icon_url or "").strip() or "./images/carIcon/xiwu.png",
        sort_order=max_order + 1,
    )
    db.add(row)
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.post("/icon-upload")
async def vehicle_type_icon_upload(file: UploadFile = File(...)):
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in _ALLOWED_ICON_EXTS:
        raise HTTPException(status_code=400, detail="仅支持 jpg、jpeg、png、webp 图片")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="上传文件为空")
    if len(content) > 2 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="图片不能超过 2MB")
    _ensure_icon_size(content, suffix)

    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid4().hex}{suffix}"
    target = _ICON_DIR / filename
    target.write_bytes(content)
    if not target.exists():
        raise HTTPException(status_code=500, detail="图片保存失败")
    return {
        "ok": True,
        "url": f"/cmmedia/vehicle-type-icons/{filename}",
        "filename": filename,
    }


@router.patch("/{tid}")
async def vehicle_type_update(tid: int, body: VehicleTypeUpdateIn, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleTypeDict).where(VehicleTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    if body.type_name is not None:
        type_name = _normalize_required_text(body.type_name, "车型名称")
        await _ensure_unique_type_name(db, type_name, exclude_id=tid)
        row.type_name = type_name
    if body.spec is not None:
        row.spec = _normalize_required_text(body.spec, "车型规格")
    if body.site is not None:
        row.site = body.site.strip() or None
    if body.icon_url is not None:
        row.icon_url = body.icon_url.strip() or None
    await db.flush()
    await db.refresh(row)
    return {"ok": True, "data": _row_out(row)}


@router.delete("/{tid}")
async def vehicle_type_delete(tid: int, db: AsyncSession = Depends(get_db)):
    row = await db.scalar(select(VehicleTypeDict).where(VehicleTypeDict.id == tid).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    await db.delete(row)
    await db.flush()
    return {"ok": True}

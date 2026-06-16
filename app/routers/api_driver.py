"""司机基础信息 API — 与公司信息 org_company 关联，供基础数据司机信息页 CRUD。"""
from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Driver, OrgCompany

router = APIRouter(prefix="/api/driver", tags=["driver"])


def _parse_date(raw: str | None) -> date | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail="日期格式须为 YYYY-MM-DD")


def _row_out(d: Driver, company_name: str | None) -> dict:
    return {
        "id": d.id,
        "name": d.name,
        "company_id": d.company_id,
        "company_name": company_name or "—",
        "gender": d.gender,
        "id_card": d.id_card,
        "phone": d.phone,
        "birth_date": d.birth_date.isoformat() if d.birth_date else None,
        "driver_license_no": d.driver_license_no,
        "remark": d.remark,
        "created_at": d.created_at.isoformat() if d.created_at else None,
        "updated_at": d.updated_at.isoformat() if d.updated_at else None,
    }


class DriverCreateIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    company_id: int = Field(..., ge=1)
    gender: str | None = Field(None, max_length=8)
    id_card: str | None = Field(None, max_length=32)
    phone: str | None = Field(None, max_length=32)
    birth_date: str | None = None
    driver_license_no: str | None = Field(None, max_length=64)

    @field_validator("id_card")
    @classmethod
    def strip_id_card(cls, v: str | None) -> str | None:
        if v is None:
            return None
        s = v.strip()
        return s or None


class DriverUpdateIn(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=64)
    company_id: int | None = Field(None, ge=1)
    gender: str | None = Field(None, max_length=8)
    id_card: str | None = Field(None, max_length=32)
    phone: str | None = Field(None, max_length=32)
    birth_date: str | None = None
    driver_license_no: str | None = Field(None, max_length=64)


class DriverBatchDeleteIn(BaseModel):
    ids: list[int] = Field(..., min_length=1)


async def _ensure_company(db: AsyncSession, company_id: int) -> None:
    cid = await db.scalar(select(OrgCompany.id).where(OrgCompany.id == company_id).limit(1))
    if cid is None:
        raise HTTPException(status_code=400, detail="所属公司不存在")


@router.post("/batch-delete")
async def driver_batch_delete(body: DriverBatchDeleteIn, db: AsyncSession = Depends(get_db)):
    ids = list({i for i in body.ids if i and i > 0})
    if not ids:
        raise HTTPException(status_code=400, detail="请选择要删除的记录")
    await db.execute(delete(Driver).where(Driver.id.in_(ids)))
    await db.commit()
    return {"ok": True, "deleted": len(ids)}


@router.get("/list")
async def driver_list(
    name: str | None = Query(None),
    company_id: int | None = Query(None, ge=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    conds = []
    if name and name.strip():
        conds.append(Driver.name.ilike(f"%{name.strip()}%"))
    if company_id is not None:
        conds.append(Driver.company_id == company_id)

    count_stmt = select(func.count()).select_from(Driver)
    if conds:
        count_stmt = count_stmt.where(*conds)
    total = (await db.execute(count_stmt)).scalar() or 0

    q = select(Driver, OrgCompany.name.label("company_name")).outerjoin(
        OrgCompany, OrgCompany.id == Driver.company_id
    )
    if conds:
        q = q.where(*conds)
    q = q.order_by(Driver.id.desc()).offset((page - 1) * page_size).limit(page_size)
    rows = (await db.execute(q)).all()
    items = [_row_out(d, cn) for d, cn in rows]
    return {"total": total, "items": items, "page": page, "page_size": page_size}


@router.get("/options")
async def driver_options_for_company(
    company_id: int = Query(..., ge=1),
    name: str | None = Query(None),
    limit: int = Query(500, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    await _ensure_company(db, company_id)
    q = select(Driver.id, Driver.name).where(Driver.company_id == company_id)
    if name and name.strip():
        q = q.where(Driver.name.ilike(f"%{name.strip()}%"))
    q = q.order_by(Driver.name.asc()).limit(limit)
    rows = (await db.execute(q)).all()
    return {"items": [{"id": row[0], "name": row[1] or ""} for row in rows]}


@router.get("/{did}")
async def driver_get(did: int, db: AsyncSession = Depends(get_db)):
    r = await db.execute(
        select(Driver, OrgCompany.name.label("company_name"))
        .outerjoin(OrgCompany, OrgCompany.id == Driver.company_id)
        .where(Driver.id == did)
    )
    row = r.first()
    if row is None:
        raise HTTPException(status_code=404, detail="司机不存在")
    d, cn = row
    return {"ok": True, "data": _row_out(d, cn)}


@router.post("")
async def driver_create(body: DriverCreateIn, db: AsyncSession = Depends(get_db)):
    await _ensure_company(db, body.company_id)
    bd = _parse_date(body.birth_date)
    row = Driver(
        name=body.name.strip(),
        company_id=body.company_id,
        gender=(body.gender.strip() if body.gender else None) or None,
        id_card=body.id_card,
        phone=(body.phone.strip() if body.phone else None) or None,
        birth_date=bd,
        driver_license_no=(body.driver_license_no.strip() if body.driver_license_no else None) or None,
    )
    db.add(row)
    await db.flush()
    await db.commit()
    await db.refresh(row)
    cn = await db.scalar(select(OrgCompany.name).where(OrgCompany.id == row.company_id).limit(1))
    return {"ok": True, "data": _row_out(row, cn)}


@router.put("/{did}")
async def driver_update(did: int, body: DriverUpdateIn, db: AsyncSession = Depends(get_db)):
    row = (await db.execute(select(Driver).where(Driver.id == did))).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="司机不存在")
    patch = body.model_dump(exclude_unset=True)
    if "company_id" in patch and patch["company_id"] is not None:
        await _ensure_company(db, patch["company_id"])
        row.company_id = patch["company_id"]
    if "name" in patch and patch["name"] is not None:
        row.name = patch["name"].strip()
    if "gender" in patch:
        g = patch["gender"]
        row.gender = (g.strip() if isinstance(g, str) else None) or None
    if "id_card" in patch:
        ic = patch["id_card"]
        row.id_card = ic.strip() if isinstance(ic, str) and ic.strip() else None
    if "phone" in patch:
        ph = patch["phone"]
        row.phone = ph.strip() if isinstance(ph, str) and ph.strip() else None
    if "birth_date" in patch:
        raw_bd = patch["birth_date"]
        if raw_bd is None or (isinstance(raw_bd, str) and not raw_bd.strip()):
            row.birth_date = None
        else:
            row.birth_date = _parse_date(raw_bd if isinstance(raw_bd, str) else str(raw_bd))
    if "driver_license_no" in patch:
        dl = patch["driver_license_no"]
        row.driver_license_no = dl.strip() if isinstance(dl, str) and dl.strip() else None
    await db.commit()
    await db.refresh(row)
    cn = await db.scalar(select(OrgCompany.name).where(OrgCompany.id == row.company_id).limit(1))
    return {"ok": True, "data": _row_out(row, cn)}


@router.delete("/{did}")
async def driver_delete(did: int, db: AsyncSession = Depends(get_db)):
    r = await db.execute(select(Driver.id).where(Driver.id == did).limit(1))
    if r.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="司机不存在")
    await db.execute(delete(Driver).where(Driver.id == did))
    await db.commit()
    return {"ok": True}

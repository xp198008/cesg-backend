"""知识图谱文件管理 API — 按用户所属公司隔离，16 分类本地目录与 AI 知识库 category 对齐。"""
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai_datasets import resolve_ai_company, resolve_dataset_id
from app.database import get_db
from app.knowledge_storage import (
    ALLOWED_EXTS,
    MAX_UPLOAD_BYTES,
    build_catalog,
    category_dir,
    category_name,
    list_files_in_category,
    safe_filename,
)
from app.models import OrgCompany, SysUser

router = APIRouter(prefix="/api/knowledge", tags=["knowledge"])


async def _resolve_user_org_name(db: AsyncSession, user_id: str | None) -> str:
    if not user_id or not str(user_id).isdigit():
        return ""
    uid = int(user_id)
    row = await db.scalar(select(SysUser).where(SysUser.id == uid).limit(1))
    if row is None or row.org_id is None:
        return ""
    org = await db.scalar(select(OrgCompany).where(OrgCompany.id == row.org_id).limit(1))
    return (org.name if org else "") or ""


async def _knowledge_scope(
    db: AsyncSession,
    x_user_id: str | None,
) -> tuple[str, str, str | None, int | None]:
    """返回 (company, company_key, dataset_id, user_id)。"""
    from app.knowledge_storage import company_dir_key

    uid: int | None = int(x_user_id) if x_user_id and str(x_user_id).isdigit() else None
    org_name = await _resolve_user_org_name(db, x_user_id)
    company = resolve_ai_company(org_name)
    company_key = company_dir_key(company)
    dataset_id = resolve_dataset_id(company)
    return company, company_key, dataset_id, uid


@router.get("/catalog")
async def knowledge_catalog(
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    """按当前用户所属公司，返回 16 类及各类文件列表（本地目录扫描，不调 AI）。"""
    company, company_key, dataset_id, user_id = await _knowledge_scope(db, x_user_id)
    data = build_catalog(company, company_key, dataset_id)
    data["user_id"] = user_id
    return data


@router.get("/categories")
async def list_categories(
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    """16 个分类及本公司各类文件数。"""
    company, company_key, dataset_id, user_id = await _knowledge_scope(db, x_user_id)
    categories = []
    for item in build_catalog(company, company_key, dataset_id)["categories"]:
        categories.append({
            "id": item["id"],
            "name": item["name"],
            "file_count": item["file_count"],
        })
    return {
        "company": company,
        "dataset_id": dataset_id,
        "user_id": user_id,
        "categories": categories,
    }


@router.get("/files")
async def list_files(
    category_id: int,
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    """本公司指定分类下的文件列表。"""
    _, company_key, _, user_id = await _knowledge_scope(db, x_user_id)
    return {
        "user_id": user_id,
        "category_id": category_id,
        "category_name": category_name(category_id),
        "files": list_files_in_category(company_key, category_id),
    }


@router.post("/upload")
async def upload_files(
    category_id: int = Form(...),
    files: list[UploadFile] = File(...),
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    """上传文件到本公司指定分类目录；重名自动加时间戳后缀。"""
    company, company_key, _, user_id = await _knowledge_scope(db, x_user_id)
    d = category_dir(company_key, category_id)
    saved = []
    for f in files:
        name = safe_filename(f.filename)
        suffix = Path(name).suffix.lower()
        if suffix not in ALLOWED_EXTS:
            raise HTTPException(status_code=400, detail=f"不支持的文件类型：{name}")
        content = await f.read()
        if len(content) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=400, detail=f"文件过大（>100MB）：{name}")
        target = d / name
        if target.exists():
            stamp = datetime.now().strftime("%Y%m%d%H%M%S")
            target = d / f"{Path(name).stem}_{stamp}{suffix}"
        target.write_bytes(content)
        saved.append(target.name)
    return {
        "saved": saved,
        "count": len(saved),
        "company": company,
        "category_id": category_id,
        "category_name": category_name(category_id),
        "user_id": user_id,
    }


@router.get("/download/{category_id}/{filename}")
async def download_file(
    category_id: int,
    filename: str,
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    _, company_key, _, _ = await _knowledge_scope(db, x_user_id)
    d = category_dir(company_key, category_id)
    name = safe_filename(filename)
    target = d / name
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="文件不存在")
    return FileResponse(target, filename=name)


@router.delete("/files/{category_id}/{filename}")
async def delete_file(
    category_id: int,
    filename: str,
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    _, company_key, _, _ = await _knowledge_scope(db, x_user_id)
    d = category_dir(company_key, category_id)
    name = safe_filename(filename)
    target = d / name
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="文件不存在")
    target.unlink()
    return {"ok": True}

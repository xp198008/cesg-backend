"""知识图谱文件管理 API — 按用户所属公司隔离，16 分类本地目录与 AI 知识库 category 对齐。"""
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent_worker_client import AgentWorkerError, agent_worker_client
from app.ai_datasets import resolve_ai_company, resolve_dataset_id
from app.database import get_db
from app.knowledge_storage import (
    ALLOWED_EXTS,
    MAX_UPLOAD_BYTES,
    build_catalog,
    category_dir,
    category_name,
    file_sha256,
    find_local_duplicates,
    iter_company_files,
    list_files_in_category,
    safe_filename,
    unique_target_path,
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
        "dataset_name": company,
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


def _truthy_form(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _ai_duplicate_hits(ai_items: list[dict], *, filename: str, sha256: str) -> list[dict]:
    want_name = safe_filename(filename).casefold()
    want_hash = (sha256 or "").strip().lower()
    hits: list[dict] = []
    for it in ai_items:
        remote_name = str(it.get("name") or "")
        remote_hash = str(it.get("content_hash") or "").strip().lower()
        same_hash = bool(want_hash and remote_hash and remote_hash == want_hash)
        same_name = bool(remote_name) and remote_name.casefold() == want_name
        if not (same_hash or same_name):
            continue
        hits.append(
            {
                "name": remote_name,
                "sha256": remote_hash,
                "size": it.get("file_size"),
                "reason": "content" if same_hash else "filename",
                "source": "ai",
                "status": it.get("status"),
            }
        )
    return hits


async def _load_ai_docs(company: str) -> list[dict]:
    if not agent_worker_client.configured():
        return []
    try:
        data = await agent_worker_client.list_all_documents(dataset_name=company)
    except Exception:
        return []
    items = data.get("items") if isinstance(data, dict) else None
    return [it for it in (items or []) if isinstance(it, dict)]


@router.get("/ai-remote")
async def knowledge_ai_remote(
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    """列出 AI 侧本公司已学习文档（元数据，不含原文）。"""
    company, _, dataset_id, user_id = await _knowledge_scope(db, x_user_id)
    items = await _load_ai_docs(company)
    return {
        "company": company,
        "dataset_id": dataset_id,
        "dataset_name": company,
        "user_id": user_id,
        "total": len(items),
        "items": items,
        "can_download": True,
    }


@router.post("/sync-from-ai")
async def knowledge_sync_from_ai(
    category_id: int = Form(...),
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    """从 AI 知识库拉取本公司已学习原文，写入当前分类；内容哈希相同则跳过。"""
    if not agent_worker_client.configured():
        raise HTTPException(status_code=503, detail="AI 接口未配置或未启用")
    company, company_key, _, user_id = await _knowledge_scope(db, x_user_id)
    d = category_dir(company_key, category_id)
    existing_hashes = {row["sha256"] for row in iter_company_files(company_key)}
    items = await _load_ai_docs(company)
    pulled: list[str] = []
    skipped: list[dict] = []
    failed: list[dict] = []
    for it in items:
        if str(it.get("status") or "") not in ("", "available"):
            skipped.append({"name": it.get("name"), "reason": f"状态 {it.get('status')} 未就绪"})
            continue
        remote_hash = str(it.get("content_hash") or "").strip().lower()
        remote_name = str(it.get("name") or "document.bin")
        if remote_hash and remote_hash in existing_hashes:
            skipped.append({"name": remote_name, "reason": "本地已有相同内容"})
            continue
        try:
            blob = await agent_worker_client.download_document_file(
                str(it.get("id") or ""),
                fallback_name=remote_name,
            )
        except AgentWorkerError as exc:
            failed.append({"name": remote_name, "reason": str(exc)})
            continue
        content = blob.get("content") or b""
        if not content:
            failed.append({"name": remote_name, "reason": "空文件"})
            continue
        digest = file_sha256(content)
        if digest in existing_hashes:
            skipped.append({"name": remote_name, "reason": "本地已有相同内容"})
            continue
        name = safe_filename(str(blob.get("filename") or remote_name))
        if len(content) > MAX_UPLOAD_BYTES:
            failed.append({"name": name, "reason": "文件过大"})
            continue
        target = unique_target_path(d, name)
        target.write_bytes(content)
        existing_hashes.add(digest)
        pulled.append(target.name)
    return {
        "ok": True,
        "company": company,
        "category_id": category_id,
        "category_name": category_name(category_id),
        "user_id": user_id,
        "pulled": pulled,
        "pulled_count": len(pulled),
        "skipped": skipped,
        "skipped_count": len(skipped),
        "failed": failed,
        "failed_count": len(failed),
        "remote_total": len(items),
    }


@router.post("/upload")
async def upload_files(
    category_id: int = Form(...),
    files: list[UploadFile] = File(...),
    force: str = Form("0"),
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    """上传到本公司分类。内容或文件名与本地/AI 已有文档相同则提示，除非 force=1。"""
    company, company_key, _, user_id = await _knowledge_scope(db, x_user_id)
    d = category_dir(company_key, category_id)
    forced = _truthy_form(force)
    pending: list[tuple[str, bytes, str]] = []
    duplicates: list[dict] = []
    ai_items = await _load_ai_docs(company)
    for f in files:
        name = safe_filename(f.filename)
        suffix = Path(name).suffix.lower()
        if suffix not in ALLOWED_EXTS:
            raise HTTPException(status_code=400, detail=f"不支持的文件类型：{name}")
        content = await f.read()
        if len(content) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=400, detail=f"文件过大（>100MB）：{name}")
        digest = file_sha256(content)
        hits = find_local_duplicates(company_key, filename=name, sha256=digest)
        hits.extend(_ai_duplicate_hits(ai_items, filename=name, sha256=digest))
        if hits:
            duplicates.append(
                {
                    "name": name,
                    "sha256": digest,
                    "matches": hits,
                }
            )
        pending.append((name, content, digest))
    if duplicates and not forced:
        names = "、".join(item["name"] for item in duplicates)
        return {
            "ok": False,
            "need_confirm": True,
            "message": f"该公司知识库已有相同内容或同名文件（{names}），建议不要重复上传",
            "duplicates": duplicates,
            "saved": [],
            "count": 0,
            "company": company,
            "category_id": category_id,
            "category_name": category_name(category_id),
            "user_id": user_id,
        }
    saved = []
    for name, content, _digest in pending:
        target = unique_target_path(d, name)
        target.write_bytes(content)
        saved.append(target.name)
    return {
        "ok": True,
        "need_confirm": False,
        "saved": saved,
        "count": len(saved),
        "duplicates": duplicates if forced else [],
        "company": company,
        "category_id": category_id,
        "category_name": category_name(category_id),
        "user_id": user_id,
    }


def _request_user_id(request: Request, x_user_id: str | None) -> str | None:
    """优先请求头，其次登录会话（window.open 下不了自定义头）。"""
    if x_user_id and str(x_user_id).strip():
        return str(x_user_id).strip()
    sid = getattr(request.state, "user_id", None)
    return str(sid) if sid is not None else None


@router.get("/download/{category_id}/{filename}")
async def download_file(
    request: Request,
    category_id: int,
    filename: str,
    db: AsyncSession = Depends(get_db),
    x_user_id: str | None = Header(None, alias="X-User-Id"),
):
    _, company_key, _, _ = await _knowledge_scope(db, _request_user_id(request, x_user_id))
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

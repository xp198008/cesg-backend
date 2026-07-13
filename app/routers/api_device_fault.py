"""设备/人工报障合并列表、处理与单据上传。"""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.device_fault_service import (
    device_fault_receipt_safe_ext,
    device_fault_receipts_root,
    gen_receipt_stored_name,
    get_jt_device_fault_by_id,
    get_jt_device_fault_receipt_by_id,
    get_merged_device_manual_fault_list,
    insert_jt_device_fault_receipt,
    jt_device_fault_receipt_eligible,
    list_jt_device_fault_receipts,
    resolve_device_fault_receipt_file_path,
    update_jt_device_fault_handle,
)

router = APIRouter(prefix="/api/device-fault", tags=["device-fault"])

_JT_FAULT_RECEIPT_MAX_BYTES = 10 * 1024 * 1024


class JtDeviceFaultHandleBody(BaseModel):
    handle_status: str
    handler_remark: str | None = None
    handler_name: str | None = None
    auditor_name: str | None = None
    audit_remark: str | None = None


@router.get("/list")
async def list_device_faults(
    terminal_id: str | None = None,
    terminal_id_contains: str | None = None,
    plate_no_contains: str | None = None,
    biz_no: str | None = None,
    biz_no_contains: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    handle_status: str | None = None,
    receipt_eligible_only: bool = Query(False),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows, total = await get_merged_device_manual_fault_list(
        db,
        terminal_id=terminal_id,
        terminal_id_contains=terminal_id_contains,
        plate_no_contains=plate_no_contains,
        biz_no_contains=(biz_no_contains or biz_no or "").strip() or None,
        start_time=start_time,
        end_time=end_time,
        handle_status=handle_status,
        receipt_eligible_only=receipt_eligible_only,
        page=page,
        page_size=page_size,
    )
    return {"ok": True, "items": rows, "data": rows, "total": total, "page": page, "page_size": page_size}


@router.get("/receipts/list")
async def api_list_device_fault_receipts(
    fault_id: int | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows, total = await list_jt_device_fault_receipts(db, fault_id=fault_id, page=page, page_size=page_size)
    return {"ok": True, "items": rows, "total": total}


@router.get("/receipts/{receipt_id}/download")
async def download_device_fault_receipt_file(receipt_id: int, db: AsyncSession = Depends(get_db)):
    meta = await get_jt_device_fault_receipt_by_id(db, receipt_id)
    if not meta:
        raise HTTPException(status_code=404, detail="单据不存在")
    p = resolve_device_fault_receipt_file_path(int(meta["fault_id"]), str(meta["stored_name"]))
    if p is None:
        raise HTTPException(status_code=404, detail="文件不存在")
    return FileResponse(
        path=p,
        filename=str(meta.get("original_name") or Path(p).name),
        media_type="application/octet-stream",
    )


@router.get("/{fault_id}")
async def get_device_fault_detail(fault_id: int, db: AsyncSession = Depends(get_db)):
    row = await get_jt_device_fault_by_id(db, fault_id)
    if not row:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True, "data": row}


@router.put("/{fault_id}/handle")
async def handle_device_fault(
    fault_id: int,
    body: JtDeviceFaultHandleBody,
    db: AsyncSession = Depends(get_db),
):
    ok, err = await update_jt_device_fault_handle(
        db,
        fault_id,
        handle_status=body.handle_status,
        handler_remark=body.handler_remark,
        handler_name=body.handler_name,
        auditor_name=body.auditor_name,
        audit_remark=body.audit_remark,
    )
    if not ok:
        raise HTTPException(status_code=400, detail=err or "更新失败")
    return {"ok": True}


@router.post("/{fault_id}/receipts")
async def upload_device_fault_receipts(
    fault_id: int,
    files: list[UploadFile] = File(...),
    uploader_name: str | None = Form(None),
    db: AsyncSession = Depends(get_db),
):
    row = await get_jt_device_fault_by_id(db, fault_id)
    if not row:
        raise HTTPException(status_code=404, detail="报障记录不存在")
    if not jt_device_fault_receipt_eligible(row.get("handle_status")):
        raise HTTPException(status_code=400, detail="仅审核已通过的报障可上传单据")
    if not files:
        raise HTTPException(status_code=400, detail="请选择至少一个文件")

    root = device_fault_receipts_root()
    fault_dir = root / str(fault_id)
    fault_dir.mkdir(parents=True, exist_ok=True)
    biz = row.get("biz_no") or ""
    uname = (uploader_name or "").strip()[:64] or None
    saved: list[dict] = []

    for uf in files:
        ext = device_fault_receipt_safe_ext(uf.filename)
        if not ext:
            raise HTTPException(status_code=400, detail=f"不支持的文件类型: {uf.filename}")
        orig = (uf.filename or "file").replace("\\", "/").split("/")[-1][:255]
        content = await uf.read()
        if len(content) > _JT_FAULT_RECEIPT_MAX_BYTES:
            raise HTTPException(status_code=400, detail=f"文件 {orig} 超过 10MB")
        stored = gen_receipt_stored_name(ext)
        dest = fault_dir / stored
        dest.write_bytes(content)
        rid = await insert_jt_device_fault_receipt(
            db,
            fault_id=fault_id,
            biz_no=biz,
            stored_name=stored,
            original_name=orig,
            file_size=len(content),
            mime_type=uf.content_type,
            uploader_name=uname,
        )
        one = await get_jt_device_fault_receipt_by_id(db, rid)
        if one:
            saved.append(one)
    await db.commit()
    return {"ok": True, "saved": saved}

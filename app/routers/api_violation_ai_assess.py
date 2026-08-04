"""安全报警自动 AI 评估调度管理接口。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.violation_ai_assessment_scheduler import violation_ai_assessment_scheduler

router = APIRouter(tags=["violation-ai-assess"])


@router.get("/api/violation-ai-assess/status")
async def violation_ai_assess_status():
    return {"ok": True, "scheduler": await violation_ai_assessment_scheduler.status()}


@router.post("/api/violation-ai-assess/run-once")
async def violation_ai_assess_run_once():
    try:
        result = await violation_ai_assessment_scheduler.run_once()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "result": result.as_dict(), "scheduler": await violation_ai_assessment_scheduler.status()}


@router.post("/api/violation-ai-assess/start")
async def violation_ai_assess_start():
    violation_ai_assessment_scheduler.start()
    return {"ok": True, "scheduler": await violation_ai_assessment_scheduler.status()}


@router.post("/api/violation-ai-assess/stop")
async def violation_ai_assess_stop():
    await violation_ai_assessment_scheduler.stop()
    return {"ok": True, "scheduler": await violation_ai_assessment_scheduler.status()}

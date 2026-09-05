"""安全报警待处理记录：自动调用 AI 评估并落库罚单建议。

固定行为（无额外配置）：
- 始终从最新记录开始（id 从大到小）
- 一条评估完成（成功/跳过/失败）后立刻处理下一条，不等待
- 仅当当前没有候选时短暂歇一下，避免空转打满数据库

筛选条件：
- status = 待处理
- 非 OBD 超速
- 报警类型过滤后可见
- 尚未 AI 评估
- 有图片或视频证据

复用 ``run_violation_ai_assessment``（与处理弹窗同一套规则）。
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time
from typing import Any

from fastapi import HTTPException
from sqlalchemy import and_, func, or_, select

from app.agent_worker_client import agent_worker_client
from app.agent_worker_config import cached_runtime
from app.alarm_type_gate import load_disabled_alarm_type_names
from app.database import AsyncSessionLocal
from app.models import VehicleViolation, ViolationAiAssessment
from app.timeutil import china_now_naive, china_today
from app.violation_ai_assessment import (
    AiRefusalError,
    backfill_ai_suggested_false_alarms,
    backfill_reset_refused_assessments,
    run_violation_ai_assessment,
)
from app.violation_filters import (
    violation_list_visibility,
    violation_non_obd_has_media_clause,
)

logger = logging.getLogger(__name__)

# 固定参数：不读配置文件
_USER_ID = "cesg_ai_scheduler"
_BATCH_SIZE = 1
_IDLE_SLEEP_WHEN_EMPTY_SEC = 10  # 仅「没有候选」时歇一下，有活干则连续跑
_DEFER_NO_EVIDENCE_SEC = 1800
_DEFER_ERROR_SEC = 300
_DEFER_REFUSAL_SEC = 90  # 拒答后稍后再捞，避免一直占着最新队列
_REFUSAL_OUTER_ATTEMPTS = 2  # 定时器侧整单再问几轮（每轮内部 chat 还会自重试）
_REFUSAL_OUTER_DELAY_SEC = 2
_STARTUP_DELAY_SEC = 5


@dataclass
class AiAssessRoundResult:
    scanned: int = 0
    assessed: int = 0
    cached: int = 0
    skipped_no_evidence: int = 0
    skipped_other: int = 0
    auto_false_alarm: int = 0
    refused: int = 0
    errors: int = 0
    detail: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "scanned": self.scanned,
            "assessed": self.assessed,
            "cached": self.cached,
            "skipped_no_evidence": self.skipped_no_evidence,
            "skipped_other": self.skipped_other,
            "auto_false_alarm": self.auto_false_alarm,
            "refused": self.refused,
            "errors": self.errors,
            "detail": self.detail[:20],
            "error": self.error,
        }


def _candidate_where(disabled_names: list[str], defer_ids: list[int]):
    clauses = [
        VehicleViolation.status == "待处理",
        or_(VehicleViolation.ai_queried.is_(False), VehicleViolation.ai_queried.is_(None)),
        ViolationAiAssessment.id.is_(None),
        violation_list_visibility(disabled_names),
        violation_non_obd_has_media_clause(),
    ]
    if defer_ids:
        clauses.append(VehicleViolation.id.notin_(defer_ids))
    return and_(*clauses)


async def count_today_assessed(db) -> int:
    start = datetime.combine(china_today(), dt_time.min)
    return int(
        await db.scalar(
            select(func.count())
            .select_from(ViolationAiAssessment)
            .where(ViolationAiAssessment.created_at >= start)
        )
        or 0
    )


async def count_pending_unassessed(db) -> int:
    disabled = await load_disabled_alarm_type_names(db)
    stmt = (
        select(func.count())
        .select_from(VehicleViolation)
        .outerjoin(
            ViolationAiAssessment,
            ViolationAiAssessment.violation_id == VehicleViolation.id,
        )
        .where(_candidate_where(disabled, []))
    )
    return int(await db.scalar(stmt) or 0)


async def fetch_candidate_ids(db, *, limit: int, defer_ids: list[int]) -> list[int]:
    """始终取最新：id 从大到小。"""
    disabled = await load_disabled_alarm_type_names(db)
    stmt = (
        select(VehicleViolation.id)
        .outerjoin(
            ViolationAiAssessment,
            ViolationAiAssessment.violation_id == VehicleViolation.id,
        )
        .where(_candidate_where(disabled, defer_ids))
        .order_by(VehicleViolation.id.desc())
        .limit(max(1, int(limit)))
    )
    rows = (await db.execute(stmt)).scalars().all()
    return [int(x) for x in rows if x is not None]


async def list_recent_assessed(db, *, limit: int = 30) -> list[dict[str, Any]]:
    stmt = (
        select(ViolationAiAssessment, VehicleViolation)
        .join(VehicleViolation, VehicleViolation.id == ViolationAiAssessment.violation_id)
        .order_by(ViolationAiAssessment.created_at.desc(), ViolationAiAssessment.id.desc())
        .limit(max(1, min(100, int(limit))))
    )
    rows = (await db.execute(stmt)).all()
    out: list[dict[str, Any]] = []
    for assessment, violation in rows:
        created = assessment.created_at
        vtime = getattr(violation, "violation_time", None)
        out.append(
            {
                "violation_id": violation.id,
                "biz_no": (violation.biz_no or "").strip(),
                "plate_no": (violation.plate_no or "").strip(),
                "alarm_type": (assessment.alarm_type_name or violation.violation_type_name or "").strip(),
                "status": (violation.status or "").strip(),
                "ticket_process_type": (assessment.ticket_process_type or "").strip(),
                "ticket_amount": assessment.ticket_amount,
                "assessed_at": created.strftime("%Y-%m-%d %H:%M:%S") if created else None,
                "violation_time": vtime.strftime("%Y-%m-%d %H:%M:%S") if vtime else None,
                "has_evaluation": bool((assessment.evaluation_text or "").strip()),
            }
        )
    return out


async def run_violation_ai_assess_once() -> AiAssessRoundResult:
    result = AiAssessRoundResult()
    if not agent_worker_client.configured():
        result.error = "AI 接口未配置或未启用"
        return result

    defer_ids = violation_ai_assessment_scheduler.active_defer_ids()
    async with AsyncSessionLocal() as db:
        try:
            ids = await fetch_candidate_ids(db, limit=_BATCH_SIZE, defer_ids=defer_ids)
        except Exception as exc:  # noqa: BLE001
            result.error = f"查询候选失败: {exc}"
            logger.warning(result.error)
            return result

    result.scanned = len(ids)
    if not ids:
        return result

    for vid in ids:
        item: dict[str, Any] = {"id": vid}
        out: dict[str, Any] | None = None
        last_refusal: str | None = None
        try:
            async with AsyncSessionLocal() as db:
                meta = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == vid).limit(1))
                if meta is not None:
                    item["plate_no"] = (meta.plate_no or "").strip()
                    item["alarm_type"] = (meta.violation_type_name or "").strip()

            for attempt in range(1, _REFUSAL_OUTER_ATTEMPTS + 1):
                try:
                    async with AsyncSessionLocal() as db:
                        out = await run_violation_ai_assessment(
                            db,
                            violation_id=vid,
                            user_id=_USER_ID,
                            force=False,
                        )
                        await db.commit()
                    last_refusal = None
                    break
                except AiRefusalError as exc:
                    last_refusal = str(exc)
                    logger.warning(
                        "自动 AI 评估拒答 id=%s attempt=%s/%s: %s",
                        vid,
                        attempt,
                        _REFUSAL_OUTER_ATTEMPTS,
                        (last_refusal or "")[:120],
                    )
                    if attempt < _REFUSAL_OUTER_ATTEMPTS:
                        await asyncio.sleep(_REFUSAL_OUTER_DELAY_SEC)

            if last_refusal is not None:
                result.refused += 1
                item["result"] = "refused"
                item["reason"] = last_refusal[:200]
                violation_ai_assessment_scheduler.defer(vid, seconds=_DEFER_REFUSAL_SEC)
            elif out is None:
                result.errors += 1
                item["result"] = "error"
                item["reason"] = "无返回"
                violation_ai_assessment_scheduler.defer(vid, seconds=_DEFER_ERROR_SEC)
            elif out.get("cached"):
                result.cached += 1
                item["result"] = "cached"
            elif out.get("skipped"):
                reason = str(out.get("skip_reason") or "skipped")
                item["result"] = "skipped"
                item["reason"] = reason
                if "证据" in reason:
                    result.skipped_no_evidence += 1
                else:
                    result.skipped_other += 1
                violation_ai_assessment_scheduler.defer(vid, seconds=_DEFER_NO_EVIDENCE_SEC)
            else:
                result.assessed += 1
                item["result"] = "assessed"
                item["ticket_process_type"] = (out.get("assessment") or {}).get("ticket_process_type") or ""
                if out.get("auto_false_alarm"):
                    item["auto_false_alarm"] = True
                    result.auto_false_alarm += 1
        except HTTPException as exc:
            result.errors += 1
            item["result"] = "error"
            item["reason"] = str(exc.detail)
            violation_ai_assessment_scheduler.defer(vid, seconds=_DEFER_ERROR_SEC)
            logger.warning("自动 AI 评估 HTTP 失败 id=%s: %s", vid, exc.detail)
        except Exception as exc:  # noqa: BLE001
            result.errors += 1
            item["result"] = "error"
            item["reason"] = str(exc)
            violation_ai_assessment_scheduler.defer(vid, seconds=_DEFER_ERROR_SEC)
            logger.warning("自动 AI 评估失败 id=%s: %s", vid, exc)
        result.detail.append(item)

    if result.assessed:
        logger.info(
            "自动 AI 评估：本轮成功 %s 条（扫描 %s，跳过无证据 %s，错误 %s）",
            result.assessed,
            result.scanned,
            result.skipped_no_evidence,
            result.errors,
        )
    return result


class ViolationAiAssessmentScheduler:
    """独立后台调度：最新待处理 → 调模型 → 落库 → 立刻下一条。"""

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_result: dict[str, Any] | None = None
        self._last_error: str | None = None
        self._last_run_at: datetime | None = None
        self._defer_until: dict[int, float] = {}

    @property
    def running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    def active_defer_ids(self) -> list[int]:
        now = time.time()
        self._defer_until = {k: v for k, v in self._defer_until.items() if v > now}
        return list(self._defer_until.keys())

    def defer(self, violation_id: int, seconds: int = _DEFER_NO_EVIDENCE_SEC) -> None:
        self._defer_until[int(violation_id)] = time.time() + max(60, int(seconds))

    async def status(self) -> dict[str, Any]:
        today_db = 0
        pending = 0
        recent: list[dict[str, Any]] = []
        try:
            async with AsyncSessionLocal() as db:
                today_db = await count_today_assessed(db)
                pending = await count_pending_unassessed(db)
                recent = await list_recent_assessed(db, limit=30)
        except Exception as exc:  # noqa: BLE001
            logger.warning("AI 评估状态统计失败: %s", exc)

        return {
            "running": self.running,
            "enabled": True,
            "interval_seconds": 0,
            "batch_size": _BATCH_SIZE,
            "order": "newest",
            "order_label": "优先最新（id 从大到小）；一条完成后立刻下一条",
            "user_id": _USER_ID,
            "defer_seconds": _DEFER_NO_EVIDENCE_SEC,
            "deferred_count": len(self.active_defer_ids()),
            "agent_worker_configured": agent_worker_client.configured(),
            "agent_worker_base_url": str(cached_runtime().get("base_url") or ""),
            "last_run_at": self._last_run_at.isoformat(sep=" ", timespec="seconds") if self._last_run_at else None,
            "last_result": self._last_result,
            "last_error": self._last_error,
            "today_assessed_db": today_db,
            "pending_unassessed_estimate": pending,
            "recent_assessed": recent,
            "rules": (
                "待处理 + 非OBD超速 + 报警类型过滤可见 + 未AI评估 + 有图/视频证据；"
                "从最新开始；一条返回并落库后立刻处理下一条；"
                "若助手拒答则当场多问几轮，仍失败则短延后重试且不落库。"
            ),
        }

    def start(self, **_kwargs) -> None:
        if self.running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="violation-ai-assess")

    async def stop(self, **_kwargs) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def run_once(self) -> AiAssessRoundResult:
        result = await run_violation_ai_assess_once()
        self._last_run_at = china_now_naive()
        self._last_result = result.as_dict()
        self._last_error = result.error
        return result

    async def _loop(self) -> None:
        logger.info("安全报警自动 AI 评估已启动：最新优先，完成后立刻下一条")
        await asyncio.sleep(_STARTUP_DELAY_SEC)
        # 历史：评估已建议误报但 status 仍为待处理 → 一次性回填
        try:
            async with AsyncSessionLocal() as db:
                n = await backfill_ai_suggested_false_alarms(db)
                await db.commit()
            if n:
                logger.info("启动回填：AI建议误报→误报 %s 条", n)
        except Exception as exc:  # noqa: BLE001
            logger.warning("启动回填 AI 误报失败: %s", exc)
        # 历史：评估内容为助手拒答 → 清评估并重置未查询，供定时器重评
        try:
            async with AsyncSessionLocal() as db:
                n = await backfill_reset_refused_assessments(db)
                await db.commit()
            if n:
                logger.info("启动回捞：拒答评估已重置 %s 条", n)
        except Exception as exc:  # noqa: BLE001
            logger.warning("启动回捞拒答评估失败: %s", exc)
        while self._running:
            try:
                result = await self.run_once()
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("安全报警自动 AI 评估执行失败: %s", exc)
                # 异常时稍歇，避免死循环刷日志
                await asyncio.sleep(2)
                continue
            # 有候选并已处理完：立刻下一条；没有候选才歇一下
            if result.scanned <= 0:
                await asyncio.sleep(_IDLE_SLEEP_WHEN_EMPTY_SEC)


violation_ai_assessment_scheduler = ViolationAiAssessmentScheduler()


async def _standalone_main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    scheduler = violation_ai_assessment_scheduler
    scheduler.start()
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        await scheduler.stop()


if __name__ == "__main__":
    asyncio.run(_standalone_main())

"""主动安全报警 AI 评估：收集可用图片（0~3 张）与视频（可选），咨询 Agent Worker 并持久化。"""
from __future__ import annotations

import base64
import json
import logging
import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent_worker_client import AgentWorkerError, agent_worker_client
from app.ai_datasets import match_ai_company
from app.media_url import extract_adas_relative_path, jt808_media_origin
from app.config import settings
from app.database import AsyncSessionLocal
from app.models import OrgCompany, Vehicle, VehicleViolation, ViolationAiAssessment

logger = logging.getLogger(__name__)

_BACKEND_ROOT = Path(__file__).resolve().parent.parent
_MAX_IMAGE_BYTES = 8 * 1024 * 1024
_MAX_VIDEO_BYTES = 80 * 1024 * 1024
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}


def _json_loads(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except Exception:
        return fallback


def _assessment_out(row: ViolationAiAssessment | None) -> dict[str, Any] | None:
    if row is None:
        return None
    rules = _json_loads(row.violated_rules_json, [])
    if not isinstance(rules, list):
        rules = []
    return {
        "id": row.id,
        "violation_id": row.violation_id,
        "session_id": row.session_id,
        "evaluation_text": row.evaluation_text or "",
        "ticket_process_type": row.ticket_process_type or "",
        "ticket_amount": row.ticket_amount,
        "ticket_basis": row.ticket_basis or "",
        "ticket_suggestion_text": row.ticket_suggestion_text or "",
        "evidence_valid": row.evidence_valid,
        "system_judgment_correct": row.system_judgment_correct,
        "violated_rules": rules,
        "video_analysis_text": row.video_analysis_text or "",
        "company_name": row.company_name or "",
        "alarm_type_name": row.alarm_type_name or "",
        "image_count": row.image_count or 0,
        "has_video": bool(row.has_video),
        "created_at": row.created_at.strftime("%Y-%m-%d %H:%M:%S") if row.created_at else None,
    }


def _extract_url(item: Any) -> str:
    if isinstance(item, str):
        return item.strip()
    if isinstance(item, dict):
        for key in ("url", "wfsl", "path", "src"):
            val = item.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
    return ""


def _resolve_fetch_url(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        rel = extract_adas_relative_path(s)
        if rel:
            return f"{jt808_media_origin()}/ADAS_FILE/{rel}"
        return s
    if s.startswith("/cmapi/media/adas/") or s.startswith("/api/media/adas/"):
        rel = s.split("/media/adas/", 1)[-1].lstrip("/")
        return f"{jt808_media_origin()}/ADAS_FILE/{rel}" if rel else ""
    rel = extract_adas_relative_path(s)
    if rel and "ADAS_FILE" in s.upper():
        return f"{jt808_media_origin()}/ADAS_FILE/{rel}"
    if s.startswith("/cmmedia/"):
        s = s.replace("/cmmedia/", "/media/", 1)
    if s.startswith("/cmapi/"):
        s = s.replace("/cmapi/", "/", 1)
    if s.startswith("/media/"):
        return f"http://127.0.0.1:{settings.app_port}{s}"
    if s.startswith("/"):
        return f"http://127.0.0.1:{settings.app_port}{s}"
    return s


def _local_media_path(url: str) -> Path | None:
    for prefix in ("/media/", "media/"):
        if url.startswith(prefix):
            rel = url[len(prefix) :].lstrip("/")
            return (_BACKEND_ROOT / "data" / rel).resolve()
    if "/media/" in url:
        rel = url.split("/media/", 1)[1]
        return (_BACKEND_ROOT / "data" / rel).resolve()
    return None


async def _download_media(url: str) -> tuple[bytes, str]:
    resolved = _resolve_fetch_url(url)
    if not resolved:
        raise ValueError("空 URL")

    local = _local_media_path(resolved)
    if local and local.exists() and local.is_file():
        data = local.read_bytes()
        ext = local.suffix.lower()
        mime = "image/jpeg" if ext in {".jpg", ".jpeg"} else f"application/octet-stream"
        if ext == ".png":
            mime = "image/png"
        elif ext == ".webp":
            mime = "image/webp"
        elif ext in {".mp4", ".mov", ".avi", ".mkv", ".flv"}:
            mime = "video/mp4"
        return data, mime

    async with httpx.AsyncClient(timeout=agent_worker_client._video_timeout(), follow_redirects=True) as client:
        resp = await client.get(resolved)
        resp.raise_for_status()
        content_type = (resp.headers.get("content-type") or "application/octet-stream").split(";")[0].strip()
        return resp.content, content_type


def _gather_media_refs(row: VehicleViolation) -> tuple[list[str], str | None]:
    image_urls: list[str] = []
    video_url: str | None = None

    snapshots = _json_loads(row.stream_snapshot_refs, [])
    if isinstance(snapshots, list):
        for item in snapshots[:3]:
            if len(image_urls) >= 3:
                break
            if isinstance(item, str) and item.strip():
                image_urls.append(f"/media/violation-snapshots/{item.strip().lstrip('/')}")
            elif isinstance(item, dict):
                u = _extract_url(item)
                if u:
                    image_urls.append(u)

    evidence = _json_loads(row.ttx_evidence_refs, {})
    if isinstance(evidence, dict):
        imgs = evidence.get("images") if isinstance(evidence.get("images"), list) else []
        vids = evidence.get("videos") if isinstance(evidence.get("videos"), list) else []
        for item in imgs:
            if len(image_urls) >= 3:
                break
            u = _extract_url(item)
            if u:
                image_urls.append(u)
        if vids:
            video_url = _extract_url(vids[0]) or None

    return image_urls[:3], video_url


def _image_data_uri(data: bytes, mime: str) -> str:
    mt = (mime or "image/jpeg").split(";")[0].strip() or "image/jpeg"
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:{mt};base64,{encoded}"


def _skip_response(*, reason: str, ai_queried: bool = False, assessment: ViolationAiAssessment | None = None) -> dict[str, Any]:
    return {
        "ok": True,
        "skipped": True,
        "skip_reason": reason,
        "cached": False,
        "ai_queried": ai_queried,
        "assessment": _assessment_out(assessment),
    }


def _build_prompt(
    *,
    alarm_type: str,
    plate_no: str,
    violation_time: str,
    biz_no: str,
    video_analysis: str,
    image_count: int,
    has_video: bool,
) -> str:
    evidence_parts: list[str] = []
    if image_count > 0:
        evidence_parts.append(f"{image_count} 张图片")
    if has_video:
        evidence_parts.append("1 段车载视频")
    if evidence_parts:
        evidence_desc = "已附上：" + "、".join(evidence_parts)
    else:
        evidence_desc = "当前无图片或视频附件，请仅依据报警信息作参考性说明"

    video_section = ""
    if has_video or (video_analysis or "").strip():
        video_section = f"【视频预分析】\n{(video_analysis or '').strip() or '（无）'}\n\n"

    return (
        f"你是环卫集团主动安全违章分析助手。系统对本条报警的初步判定如下：\n"
        f"- 报警编号：{biz_no or '—'}\n"
        f"- 车牌号：{plate_no or '—'}\n"
        f"- 报警时间：{violation_time or '—'}\n"
        f"- 系统报警类型：{alarm_type or '—'}\n\n"
        f"{evidence_desc}。\n\n"
        f"{video_section}"
        "请回答：\n"
        "1. 证据资料是否属实？能否支撑该报警类型？\n"
        "2. 系统初步判断（报警类型）是否正确？\n"
        "3. 如属实，违反了公司哪些规章制度？请引用具体制度条款。\n"
        "4. 请给出罚单建议（类型、金额如适用、依据）。\n"
        "   注意：ticket_suggestion.basis（罚单依据）必须是简短摘要，**不超过 10 个汉字**，"
        "例如「吸烟违章」「未系安全带」「超速行驶」等，不要写长句或整段法规原文。\n\n"
        "回复末尾务必附带 JSON（不要省略字段）：\n"
        "```json\n"
        "{\n"
        '  "evidence_valid": true,\n'
        '  "system_judgment_correct": true,\n'
        '  "evaluation_summary": "综合评估说明",\n'
        '  "violated_rules": ["制度名称及条款"],\n'
        '  "ticket_suggestion": {\n'
        '    "process_type": "警告",\n'
        '    "amount": 0,\n'
        '    "basis": "不超过10字的简短依据"\n'
        "  }\n"
        "}\n"
        "```"
    )


def _extract_json_block(text: str) -> dict[str, Any] | None:
    raw = text or ""
    fenced = re.search(r"```json\s*(\{.*?\})\s*```", raw, flags=re.S | re.I)
    if fenced:
        try:
            return json.loads(fenced.group(1))
        except json.JSONDecodeError:
            pass
    brace = re.search(r"\{[\s\S]*\"ticket_suggestion\"[\s\S]*\}", raw)
    if brace:
        try:
            return json.loads(brace.group(0))
        except json.JSONDecodeError:
            pass
    return None


def _compose_evaluation_text(parsed: dict[str, Any] | None, full_text: str, video_text: str) -> str:
    lines: list[str] = []
    if parsed:
        summary = str(parsed.get("evaluation_summary") or "").strip()
        if summary:
            lines.append(summary)
        ev = parsed.get("evidence_valid")
        sj = parsed.get("system_judgment_correct")
        if ev is not None:
            lines.append(f"证据是否属实：{'是' if ev else '否'}")
        if sj is not None:
            lines.append(f"系统判断是否正确：{'是' if sj else '否'}")
        rules = parsed.get("violated_rules")
        if isinstance(rules, list) and rules:
            lines.append("违反规章制度：")
            lines.extend(f"- {str(r)}" for r in rules if str(r).strip())
    body = strip_fenced_json(full_text).strip()
    if body and not lines:
        lines.append(body)
    if video_text.strip():
        lines.append("\n【视频预分析】\n" + video_text.strip())
    return "\n".join(lines).strip() or body or "AI 未返回有效评估内容。"


def strip_fenced_json(text: str) -> str:
    return re.sub(r"```json[\s\S]*?```", "", text or "", flags=re.I).strip()


def _summarize_basis(text: str, *, max_len: int = 10) -> str:
    """罚单依据摘要，限制在 max_len 个字符以内（按 Unicode 计，中文一字算一字）。"""
    s = re.sub(r"\s+", "", (text or "").strip())
    if not s:
        return ""
    if len(s) <= max_len:
        return s
    return s[:max_len]


def _ticket_from_parsed(parsed: dict[str, Any] | None, full_text: str) -> dict[str, Any]:
    ticket = parsed.get("ticket_suggestion") if isinstance(parsed, dict) else None
    if not isinstance(ticket, dict):
        ticket = {}
    process_type = str(ticket.get("process_type") or "警告").strip() or "警告"
    amount_raw = ticket.get("amount")
    try:
        amount = float(amount_raw) if amount_raw is not None else 0.0
    except (TypeError, ValueError):
        amount = 0.0
    basis = _summarize_basis(str(ticket.get("basis") or ""))
    if not basis and isinstance(parsed, dict):
        rules = parsed.get("violated_rules")
        if isinstance(rules, list) and rules:
            basis = _summarize_basis(str(rules[0]))
    if not basis:
        summary = str(parsed.get("evaluation_summary") or "").strip() if isinstance(parsed, dict) else ""
        basis = _summarize_basis(summary)
    suggestion_text = f"罚单类型：{process_type}\n罚单金额：{amount if process_type == '罚款' else '-'}\n罚单依据：{basis}"
    return {
        "process_type": process_type,
        "amount": amount,
        "basis": basis,
        "suggestion_text": suggestion_text,
    }


async def _resolve_company_for_violation(db: AsyncSession, row: VehicleViolation) -> str:
    """按 报警记录 → 车辆 → 机构树逐级向上 匹配 AI 知识库公司名，匹配不到才用默认值。

    报警记录的 company_id 往往是叶子机构（如「本部车队」），需沿 parent_id 向上
    找到能对应知识库的真实公司（如 益渝公司）。
    """
    company_id = row.company_id
    if not company_id and row.vehicle_id:
        company_id = await db.scalar(
            select(Vehicle.company_id).where(Vehicle.id == int(row.vehicle_id)).limit(1)
        )
    plate = (row.plate_no or "").strip()
    if not company_id and plate:
        company_id = await db.scalar(
            select(Vehicle.company_id).where(Vehicle.plate_no == plate).limit(1)
        )

    seen: set[int] = set()
    while company_id and int(company_id) not in seen:
        seen.add(int(company_id))
        org = await db.scalar(select(OrgCompany).where(OrgCompany.id == int(company_id)).limit(1))
        if org is None:
            break
        matched = match_ai_company(org.name) or match_ai_company(org.short_name)
        if matched:
            return matched
        company_id = org.parent_id

    return (settings.agent_worker_default_company or "三峰城服").strip()


async def _download_image_blocks(image_urls: list[str]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for url in image_urls:
        try:
            data, mime = await _download_media(url)
            if len(data) > _MAX_IMAGE_BYTES:
                logger.warning("图片 %s 超过大小限制，已跳过", url)
                continue
            blocks.append({"type": "image", "image_url": _image_data_uri(data, mime)})
        except Exception as exc:
            logger.warning("下载图片失败 %s: %s", url, exc)
    return blocks


async def _video_preanalysis(
    video_url: str,
    *,
    user_id: str,
    company: str,
    violation_id: int,
) -> tuple[str, bool]:
    """返回 (视频预分析文本, 是否成功拿到视频)。"""
    video_downloaded = False
    try:
        video_data, video_mime = await _download_media(video_url)
        if len(video_data) > _MAX_VIDEO_BYTES:
            raise ValueError("视频文件过大")
        video_downloaded = True
        ext = Path(_extract_url(video_url)).suffix.lower() or ".mp4"
        v_result = await agent_worker_client.analyze_video_violation(
            user_id=user_id,
            company=company,
            filename=f"evidence{ext}",
            content=video_data,
            content_type=video_mime,
            session_id=f"violation_{violation_id}",
        )
        parts = [
            f"结论：{v_result.get('conclusion')}" if v_result.get("conclusion") else "",
            f"违章详情：{v_result.get('violation_detail')}" if v_result.get("violation_detail") else "",
            f"分析：{v_result.get('analysis')}" if v_result.get("analysis") else "",
        ]
        return "\n".join(p for p in parts if p).strip(), True
    except Exception as exc:
        logger.warning("视频 AI 分析失败: %s", exc)
        if video_downloaded:
            return f"视频分析未完成：{exc}", True
        return "", False


async def _save_assessment(
    db: AsyncSession,
    row: VehicleViolation,
    existing: ViolationAiAssessment | None,
    *,
    session_id: str,
    full_text: str,
    video_analysis_text: str,
    company: str,
    image_count: int,
    has_video: bool,
) -> ViolationAiAssessment:
    parsed = _extract_json_block(full_text)
    ticket_info = _ticket_from_parsed(parsed, full_text)
    evaluation_text = _compose_evaluation_text(parsed, full_text, video_analysis_text)

    evidence_valid = parsed.get("evidence_valid") if isinstance(parsed, dict) else None
    system_ok = parsed.get("system_judgment_correct") if isinstance(parsed, dict) else None
    violated_rules = parsed.get("violated_rules") if isinstance(parsed, dict) else []
    if not isinstance(violated_rules, list):
        violated_rules = []

    if existing is None:
        existing = ViolationAiAssessment(violation_id=row.id)
        db.add(existing)

    existing.session_id = session_id
    existing.evaluation_text = evaluation_text
    existing.ticket_process_type = ticket_info["process_type"]
    existing.ticket_amount = ticket_info["amount"]
    existing.ticket_basis = ticket_info["basis"]
    existing.ticket_suggestion_text = ticket_info["suggestion_text"]
    existing.evidence_valid = evidence_valid if isinstance(evidence_valid, bool) else None
    existing.system_judgment_correct = system_ok if isinstance(system_ok, bool) else None
    existing.violated_rules_json = json.dumps(violated_rules, ensure_ascii=False)
    existing.video_analysis_text = video_analysis_text
    existing.raw_response_text = full_text
    existing.company_name = company
    existing.alarm_type_name = row.violation_type_name or ""
    existing.image_count = image_count
    existing.has_video = has_video

    row.ai_queried = True
    await db.flush()
    await db.refresh(existing)
    return existing


async def get_violation_ai_assessment(db: AsyncSession, violation_id: int) -> dict[str, Any]:
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    assessment = await db.scalar(
        select(ViolationAiAssessment).where(ViolationAiAssessment.violation_id == violation_id).limit(1)
    )
    return {
        "ok": True,
        "ai_queried": bool(getattr(row, "ai_queried", False)),
        "assessment": _assessment_out(assessment),
    }


async def run_violation_ai_assessment(
    db: AsyncSession,
    *,
    violation_id: int,
    user_id: str,
    force: bool = False,
) -> dict[str, Any]:
    if not agent_worker_client.configured():
        raise HTTPException(status_code=503, detail="Agent Worker 未配置")

    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")

    existing = await db.scalar(
        select(ViolationAiAssessment).where(ViolationAiAssessment.violation_id == violation_id).limit(1)
    )
    if bool(getattr(row, "ai_queried", False)) and existing and not force:
        return {
            "ok": True,
            "cached": True,
            "ai_queried": True,
            "assessment": _assessment_out(existing),
        }

    company = await _resolve_company_for_violation(db, row)

    image_urls, video_url = _gather_media_refs(row)
    if not image_urls and not video_url:
        return _skip_response(reason="暂无图片或视频证据，已跳过 AI 分析")

    image_blocks = await _download_image_blocks(image_urls)

    video_analysis_text = ""
    has_video = False
    if video_url:
        video_analysis_text, has_video = await _video_preanalysis(
            video_url, user_id=user_id, company=company, violation_id=violation_id
        )

    if not image_blocks and not has_video:
        return _skip_response(reason="证据下载失败或全部为空，已跳过 AI 分析")

    session_id = f"violation_assess_{violation_id}"
    prompt = _build_prompt(
        alarm_type=row.violation_type_name or "",
        plate_no=row.plate_no or "",
        violation_time=row.violation_time.strftime("%Y-%m-%d %H:%M:%S") if row.violation_time else "",
        biz_no=row.biz_no or "",
        video_analysis=video_analysis_text,
        image_count=len(image_blocks),
        has_video=has_video,
    )
    content_blocks: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
    content_blocks.extend(image_blocks)

    try:
        full_text = await agent_worker_client.chat_collect_text(
            user_id=user_id,
            company=company,
            session_id=session_id,
            input_messages=[{"role": "user", "content": content_blocks}],
        )
    except AgentWorkerError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    existing = await _save_assessment(
        db,
        row,
        existing,
        session_id=session_id,
        full_text=full_text,
        video_analysis_text=video_analysis_text,
        company=company,
        image_count=len(image_blocks),
        has_video=has_video,
    )

    return {
        "ok": True,
        "cached": False,
        "ai_queried": True,
        "assessment": _assessment_out(existing),
    }


def _sse(payload: dict[str, Any]) -> bytes:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")


async def stream_violation_ai_assessment(
    *,
    violation_id: int,
    user_id: str,
    force: bool = False,
) -> AsyncIterator[bytes]:
    """SSE 流式版 AI 评估：边生成边推送文本增量，完成后落库并推送最终结果。

    自管数据库会话（StreamingResponse 场景下不依赖 get_db）。
    """
    if not agent_worker_client.configured():
        yield _sse({"object": "error", "message": "Agent Worker 未配置（AGENT_WORKER_BASE_URL）"})
        return

    async with AsyncSessionLocal() as db:
        try:
            row = await db.scalar(
                select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1)
            )
            if row is None:
                yield _sse({"object": "error", "message": "记录不存在"})
                return

            existing = await db.scalar(
                select(ViolationAiAssessment)
                .where(ViolationAiAssessment.violation_id == violation_id)
                .limit(1)
            )
            if bool(getattr(row, "ai_queried", False)) and existing and not force:
                yield _sse(
                    {
                        "object": "assessment",
                        "cached": True,
                        "ai_queried": True,
                        "assessment": _assessment_out(existing),
                    }
                )
                return

            company = await _resolve_company_for_violation(db, row)
            yield _sse(
                {
                    "object": "status",
                    "stage": "company",
                    "company": company,
                    "message": f"已按「{company}」的规章制度进行分析…",
                }
            )

            image_urls, video_url = _gather_media_refs(row)
            if not image_urls and not video_url:
                yield _sse({"object": "skip", "reason": "暂无图片或视频证据，已跳过 AI 分析"})
                return

            yield _sse({"object": "status", "stage": "download", "message": "正在下载图片/视频证据…"})
            image_blocks = await _download_image_blocks(image_urls)

            video_analysis_text = ""
            has_video = False
            if video_url:
                yield _sse({"object": "status", "stage": "video", "message": "正在进行视频违章预分析…"})
                video_analysis_text, has_video = await _video_preanalysis(
                    video_url, user_id=user_id, company=company, violation_id=violation_id
                )

            if not image_blocks and not has_video:
                yield _sse({"object": "skip", "reason": "证据下载失败或全部为空，已跳过 AI 分析"})
                return

            session_id = f"violation_assess_{violation_id}"
            prompt = _build_prompt(
                alarm_type=row.violation_type_name or "",
                plate_no=row.plate_no or "",
                violation_time=row.violation_time.strftime("%Y-%m-%d %H:%M:%S") if row.violation_time else "",
                biz_no=row.biz_no or "",
                video_analysis=video_analysis_text,
                image_count=len(image_blocks),
                has_video=has_video,
            )
            content_blocks: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
            content_blocks.extend(image_blocks)

            yield _sse({"object": "status", "stage": "chat", "message": "AI 正在评估证据并生成罚单建议…"})

            buffer = ""
            parts: list[str] = []
            async for chunk in agent_worker_client.chat_stream(
                user_id=user_id,
                company=company,
                session_id=session_id,
                input_messages=[{"role": "user", "content": content_blocks}],
                stream=True,
            ):
                buffer += chunk.decode("utf-8", "replace")
                while "\n\n" in buffer:
                    block, buffer = buffer.split("\n\n", 1)
                    line = next(
                        (ln.strip() for ln in block.split("\n") if ln.strip().startswith("data:")),
                        "",
                    )
                    if not line:
                        continue
                    try:
                        evj = json.loads(line[5:].strip())
                    except json.JSONDecodeError:
                        continue
                    if (
                        evj.get("object") == "content"
                        and evj.get("type") == "text"
                        and evj.get("delta")
                        and evj.get("text")
                    ):
                        parts.append(str(evj["text"]))
                        yield _sse({"object": "content", "type": "text", "delta": True, "text": evj["text"]})

            full_text = "".join(parts)
            existing = await _save_assessment(
                db,
                row,
                existing,
                session_id=session_id,
                full_text=full_text,
                video_analysis_text=video_analysis_text,
                company=company,
                image_count=len(image_blocks),
                has_video=has_video,
            )
            await db.commit()
            yield _sse(
                {
                    "object": "assessment",
                    "cached": False,
                    "ai_queried": True,
                    "assessment": _assessment_out(existing),
                }
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("流式 AI 评估失败 violation_id=%s", violation_id)
            await db.rollback()
            yield _sse({"object": "error", "message": str(exc)})

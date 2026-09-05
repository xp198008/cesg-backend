"""主动安全报警 AI 评估。

一律走 Agent Worker ``POST /api/video/violation``（SSE 违章判定）：
有视频传 ``file``，没有视频则把抓拍图作为 ``images`` 一次提交。
"""
from __future__ import annotations

import asyncio
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

from app.agent_worker_client import AgentWorkerError, agent_worker_client, video_complete_failed
from app.agent_worker_config import cached_default_company
from app.ai_datasets import match_ai_company
from app.media_url import extract_adas_relative_path, jt808_media_origin
from app.config import settings
from app.database import AsyncSessionLocal
from app.models import OrgCompany, Vehicle, VehicleViolation, ViolationAiAssessment
from app.timeutil import china_now_naive
from app.violation_filters import violation_row_is_page_visible

_AI_AUTO_FALSE_ALARM_HANDLER = "AI自动评估"
_AI_AUTO_FALSE_ALARM_REMARK = "AI评估建议为误报，系统自动处理"
# 助手拒答时同一条再问几轮（换 session，避免会话上下文污染）
_AI_CHAT_MAX_ATTEMPTS = 3
_AI_CHAT_RETRY_DELAY_SEC = 2.0

logger = logging.getLogger(__name__)


class AiRefusalError(RuntimeError):
    """Agent Worker 返回「超出服务范围」类拒答，评估未落库，可供定时器稍后重试。"""

_BACKEND_ROOT = Path(__file__).resolve().parent.parent
_MAX_IMAGE_BYTES = 10 * 1024 * 1024
_MAX_VIDEO_BYTES = 80 * 1024 * 1024
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}
# 罚单建议三选一（无违章对应「误报」；有违章在「罚款/警告」中择一）
_DISPOSITION_TYPES = ("罚款", "警告", "误报")
# 整段丢弃（含 recommend 畸形写法）
_AI_DISCARD_TAGS = (
    "recommend|attachment|image|chart|violation_detail|plugin_call|tool_call"
)
_AI_KEEP_INNER_TAGS = "violation|violation_type"
_AI_ALL_TAGS = f"{_AI_DISCARD_TAGS}|{_AI_KEEP_INNER_TAGS}"

_AI_DISCARD_BLOCK_RE = re.compile(
    rf"<({_AI_DISCARD_TAGS})\b[\s\S]*?</\1\s*>",
    re.I,
)
# <recommend ...>/recommend> 或未规范闭合
_AI_DISCARD_MALFORMED_RE = re.compile(
    rf"<({_AI_DISCARD_TAGS})\b[\s\S]*?/?\s*\1\s*>",
    re.I,
)
_AI_DISCARD_SELF_RE = re.compile(
    rf"<({_AI_DISCARD_TAGS})\b[^>]*/\s*>",
    re.I,
)
_VIOLATION_XML_BLOCK_RE = re.compile(
    rf"<({_AI_KEEP_INNER_TAGS})\b[^>]*>([\s\S]*?)</\1\s*>",
    re.I,
)
_AI_LOOSE_TAG_RE = re.compile(rf"</?\s*(?:{_AI_ALL_TAGS})\b[^>]*>", re.I)
_AI_ORPHAN_CLOSER_RE = re.compile(rf"/?\s*(?:{_AI_DISCARD_TAGS})\s*>", re.I)
_AI_PARTIAL_TAG_RE = re.compile(rf"<(?:{_AI_ALL_TAGS})\b[^>]*$", re.I)
_MD_BOLD_RE = re.compile(r"\*\*([^*]+)\*\*")
_MD_HEADING_RE = re.compile(r"^#{1,6}\s+", re.M)
_AI_DECOR_RE = re.compile(r"[✅❌☑️✔️√]")


def _keep_violation_inner(match: re.Match[str]) -> str:
    inner = (match.group(2) or "").strip()
    if not inner or inner in ("无", "未发现违章行为"):
        return ""
    return inner


def sanitize_ai_display_text(text: str | None) -> str:
    """去掉 recommend/violation 等结构化标签与 ** / ✅ 等装饰，供评估文案展示。"""
    s = str(text or "")
    s = _AI_DISCARD_BLOCK_RE.sub("", s)
    s = _AI_DISCARD_MALFORMED_RE.sub("", s)
    s = _AI_DISCARD_SELF_RE.sub("", s)
    s = _VIOLATION_XML_BLOCK_RE.sub(_keep_violation_inner, s)
    s = _AI_LOOSE_TAG_RE.sub("", s)
    s = _AI_ORPHAN_CLOSER_RE.sub("", s)
    s = _AI_PARTIAL_TAG_RE.sub("", s)
    s = _MD_HEADING_RE.sub("", s)
    s = _MD_BOLD_RE.sub(r"\1", s)
    s = s.replace("**", "")
    s = _AI_DECOR_RE.sub("", s)
    lines = []
    for ln in s.splitlines():
        cleaned = re.sub(r"[ \t]{2,}", " ", ln).strip()
        if cleaned == "未发现违章行为":
            continue
        lines.append(cleaned)
    s = "\n".join(lines)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


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


def _fmt_coord(value: Any, *, digits: int = 6) -> str:
    if value is None or value == "":
        return ""
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value).strip()


def _alarm_context_lines(row: VehicleViolation) -> list[str]:
    """业务库报警上下文：车牌/终端/坐标/地址等，供模型判断，无需从画面识别。"""
    violation_time = ""
    if getattr(row, "violation_time", None):
        try:
            violation_time = row.violation_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            violation_time = str(row.violation_time)
    lat = _fmt_coord(getattr(row, "lat", None))
    lng = _fmt_coord(getattr(row, "lng", None))
    if lat and lng:
        coord_text = f"{lng}, {lat}"  # 经度, 纬度
    elif lng:
        coord_text = f"经度 {lng}"
    elif lat:
        coord_text = f"纬度 {lat}"
    else:
        coord_text = ""

    lines = [
        f"- 报警编号：{(row.biz_no or '').strip() or '—'}",
        f"- 车牌号：{(row.plate_no or '').strip() or '—'}",
        f"- 终端号：{(row.terminal_id or '').strip() or '—'}",
        f"- 所属公司：{(row.company_name or '').strip() or '—'}",
        f"- 报警时间：{violation_time or '—'}",
        f"- 系统报警类型：{(row.violation_type_name or '').strip() or '—'}",
        f"- 风险等级：{(row.risk_level or '').strip() or '—'}",
        f"- 经纬度：{coord_text or '—'}",
        f"- 违章地址：{(row.address or '').strip() or '—'}",
    ]
    weather = (getattr(row, "weather", None) or "").strip()
    if weather:
        lines.append(f"- 天气：{weather}")
    rule_name = (getattr(row, "private_rule_name", None) or "").strip()
    if rule_name:
        lines.append(f"- 关联规则：{rule_name}")
    category = (getattr(row, "rule_category_name", None) or "").strip()
    if category:
        lines.append(f"- 规则类别：{category}")
    return lines


def _alarm_context_block(row: VehicleViolation) -> str:
    return "\n".join(_alarm_context_lines(row))


def _alarm_extra_form_fields(row: VehicleViolation) -> dict[str, str]:
    """传给视频违章接口的可选业务字段（Worker 不识别会忽略）。"""
    fields: dict[str, str] = {}
    mapping = {
        "plate_no": (row.plate_no or "").strip(),
        "terminal_id": (row.terminal_id or "").strip(),
        "alarm_type": (row.violation_type_name or "").strip(),
        "address": (row.address or "").strip(),
        "biz_no": (row.biz_no or "").strip(),
        "company_name": (row.company_name or "").strip(),
        "risk_level": (row.risk_level or "").strip(),
    }
    lat = _fmt_coord(getattr(row, "lat", None))
    lng = _fmt_coord(getattr(row, "lng", None))
    if lat:
        mapping["lat"] = lat
    if lng:
        mapping["lng"] = lng
    if getattr(row, "violation_time", None):
        try:
            mapping["violation_time"] = row.violation_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    for key, value in mapping.items():
        if value:
            fields[key] = value
    return fields


def _build_prompt(
    row: VehicleViolation,
    *,
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
        "请基于本公司车辆主动安全报警数据与证据，做合规规章制度解答（属于车辆数据查询与制度问答范围）。\n"
        "本条报警业务字段如下（车牌/终端/坐标/地址以系统字段为准，无需 OCR 车牌）：\n"
        f"{_alarm_context_block(row)}\n\n"
        "【约束】\n"
        "- 证据看不清车牌时，仍须按报警类型、地址/坐标与画面行为作答，不得只回复无法识别。\n"
        "- 请引用公司规章制度条款；若证据不足或不构成违章，明确说明。\n\n"
        f"{evidence_desc}。\n\n"
        f"{video_section}"
        "请按下列问题作答：\n"
        "1. 证据是否足以支撑该报警类型？\n"
        "2. 系统报警类型判断是否正确？\n"
        "3. 如属实，违反了哪些规章制度（写出制度名称及条款）？\n"
        "4. 合规处理建议 process_type 只能是「罚款」「警告」「误报」三选一"
        "（证据不足/未构成违章→误报；构成违章从轻→警告；需经济处罚→罚款）。\n"
        "   ticket_suggestion.basis 须写全违章行为结论，如「驾驶途中抽烟；闭眼疲劳」。\n\n"
        "回复末尾务必附带 JSON：\n"
        "```json\n"
        "{\n"
        '  "evidence_valid": true,\n'
        '  "system_judgment_correct": true,\n'
        '  "evaluation_summary": "综合评估说明",\n'
        '  "violated_rules": ["制度名称及条款"],\n'
        '  "ticket_suggestion": {\n'
        '    "process_type": "警告",\n'
        '    "amount": 0,\n'
        '    "basis": "驾驶途中抽烟；闭眼疲劳"\n'
        "  }\n"
        "}\n"
        "```\n"
    )


def _normalize_disposition(raw: Any) -> str | None:
    """归一化为 罚款/警告/误报；无法识别则返回 None。"""
    text = str(raw or "").strip()
    if not text:
        return None
    if text in _DISPOSITION_TYPES:
        return text
    for name in _DISPOSITION_TYPES:
        if name in text:
            return name
    lower = text.lower()
    if "false" in lower or "误报" in text or "无违章" in text:
        return "误报"
    if "罚" in text:
        return "罚款"
    if "警告" in text or "警示" in text:
        return "警告"
    return None


def _build_disposition_prompt(
    row: VehicleViolation,
    *,
    video_eval_text: str,
    video_violation: bool | None,
) -> str:
    """在视频判定结果上自动追加「罚款/警告/误报」三选一，便于直接写入罚单建议。"""
    hint = "未知"
    if video_violation is True:
        hint = "视频接口判定为「存在违章」"
    elif video_violation is False:
        hint = "视频接口判定为「未发现违章」"
    return (
        "请基于本公司车辆主动安全报警与视频判定结果，做合规规章制度解答，"
        "并给出合规处理建议（属于车辆数据查询与制度问答范围）。\n\n"
        f"{_alarm_context_block(row)}\n"
        f"- 视频接口结论提示：{hint}\n\n"
        "【约束】车牌/终端/坐标/地址以系统字段为准；视频写「无法识别车牌」时仍须完成三选一。\n\n"
        f"【视频判定详情】\n{(video_eval_text or '（无）').strip()}\n\n"
        "请只输出处理建议：在「罚款」「警告」「误报」中三选一。\n"
        "- 未发现违章 / 证据不足 / 系统误判 → 误报\n"
        "- 构成违章但从轻 → 警告\n"
        "- 构成违章且需经济处罚 → 罚款（并给金额）\n\n"
        "回复末尾附带 JSON：\n"
        "```json\n"
        "{\n"
        '  "ticket_suggestion": {\n'
        '    "process_type": "误报",\n'
        '    "amount": 0,\n'
        '    "basis": "驾驶途中抽烟；闭眼疲劳"\n'
        "  }\n"
        "}\n"
        "```\n"
        "process_type 只能是罚款/警告/误报；basis 写全违章行为。"
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
    body = sanitize_ai_display_text(strip_fenced_json(full_text))
    if body and not lines:
        lines.append(body)
    video_clean = sanitize_ai_display_text(video_text)
    if video_clean:
        lines.append("\n【视频预分析】\n" + video_clean)
    return sanitize_ai_display_text("\n".join(lines)) or body or "AI 未返回有效评估内容。"


def strip_fenced_json(text: str) -> str:
    return re.sub(r"```json[\s\S]*?```", "", text or "", flags=re.I).strip()


def _summarize_basis(text: str, *, max_len: int = 64) -> str:
    """罚单依据：去掉空白后保留完整结论，仅在超长时截断。"""
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
    process_type = _normalize_disposition(ticket.get("process_type")) or "警告"
    amount_raw = ticket.get("amount")
    try:
        amount = float(amount_raw) if amount_raw is not None else 0.0
    except (TypeError, ValueError):
        amount = 0.0
    if process_type != "罚款":
        amount = 0.0
    basis = _summarize_basis(str(ticket.get("basis") or ""))
    if not basis and isinstance(parsed, dict):
        rules = parsed.get("violated_rules")
        if isinstance(rules, list) and rules:
            basis = _summarize_basis(str(rules[0]))
    if not basis:
        summary = str(parsed.get("evaluation_summary") or "").strip() if isinstance(parsed, dict) else ""
        basis = _summarize_basis(summary)
    if process_type == "误报" and not basis:
        basis = "无违章"
    suggestion_text = (
        f"罚单类型：{process_type}\n"
        f"罚单金额：{amount if process_type == '罚款' else '-'}\n"
        f"罚单依据：{basis}"
    )
    return {
        "process_type": process_type,
        "amount": amount,
        "basis": basis,
        "suggestion_text": suggestion_text,
    }


def _looks_like_ai_refusal(text: str) -> bool:
    s = (text or "").strip()
    if not s:
        return True
    markers = (
        "服务范围",
        "无法为您提供",
        "超出了我的",
        "仅支持车辆数据",
        "敏感信息",
        "车联网专属智能助手",
        "抱歉，我是",
    )
    return any(m in s for m in markers)


def _video_result_is_refusal(v_result: dict[str, Any]) -> bool:
    for key in ("conclusion", "analysis", "violation_detail", "message", "detail"):
        val = str(v_result.get(key) or "").strip()
        if not val:
            continue
        if _looks_like_ai_refusal(val):
            return True
    return False


def _with_compliance_retry_preamble(input_messages: list[dict[str, Any]], *, attempt: int) -> list[dict[str, Any]]:
    """拒答重试时加合规问答前缀，降低被判「超服务范围」的概率。"""
    if attempt <= 1:
        return input_messages
    preamble = (
        "说明：本请求属于本公司车辆数据查询与合规规章制度解答，"
        "请根据报警字段与证据作制度问答，不要拒答。"
    )
    out: list[dict[str, Any]] = []
    for msg in input_messages:
        role = msg.get("role") or "user"
        content = msg.get("content")
        if isinstance(content, list):
            blocks = list(content)
            if blocks and isinstance(blocks[0], dict) and blocks[0].get("type") == "text":
                first = dict(blocks[0])
                first["text"] = preamble + "\n\n" + str(first.get("text") or "")
                blocks[0] = first
            else:
                blocks.insert(0, {"type": "text", "text": preamble})
            out.append({"role": role, "content": blocks})
        else:
            out.append({"role": role, "content": preamble + "\n\n" + str(content or "")})
    return out or input_messages


async def _chat_collect_with_retries(
    *,
    user_id: str,
    company: str,
    session_id: str,
    input_messages: list[dict[str, Any]],
    max_attempts: int = _AI_CHAT_MAX_ATTEMPTS,
    delay_sec: float = _AI_CHAT_RETRY_DELAY_SEC,
    purpose: str = "chat",
) -> str:
    """调用 /api/chat；若返回能力范围拒答则换 session 再问，仍失败则抛 AiRefusalError。"""
    last_text = ""
    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        sid = session_id if attempt == 1 else f"{session_id}_r{attempt}"
        msgs = _with_compliance_retry_preamble(input_messages, attempt=attempt)
        try:
            full_text = await agent_worker_client.chat_collect_text(
                user_id=user_id,
                company=company,
                session_id=sid,
                input_messages=msgs,
            )
        except AgentWorkerError as exc:
            last_text = str(exc)
            logger.warning("%s 第 %s/%s 次调用失败: %s", purpose, attempt, attempts, exc)
            if attempt >= attempts:
                raise
            await asyncio.sleep(delay_sec)
            continue
        if not _looks_like_ai_refusal(full_text):
            if attempt > 1:
                logger.info("%s 第 %s 次重试成功 session=%s", purpose, attempt, sid)
            return full_text
        last_text = full_text
        logger.warning(
            "%s 被助手拒答（%s/%s）session=%s preview=%s",
            purpose,
            attempt,
            attempts,
            sid,
            (full_text or "")[:80],
        )
        if attempt < attempts:
            await asyncio.sleep(delay_sec)
    raise AiRefusalError(last_text or f"{purpose} 多次拒答")


async def _classify_disposition_via_chat(
    *,
    user_id: str,
    company: str,
    session_id: str,
    row: VehicleViolation,
    video_eval_text: str,
    video_violation: bool | None,
) -> dict[str, Any] | None:
    """在视频判定后追加「罚款/警告/误报」三选一问答，直接得到可展示的建议。"""
    # 视频已明确无违章时不再多问一轮，直接误报
    if video_violation is False:
        return {
            "process_type": "误报",
            "amount": 0.0,
            "basis": "无违章",
            "suggestion_text": "罚单类型：误报\n罚单金额：-\n罚单依据：无违章",
        }
    prompt = _build_disposition_prompt(
        row,
        video_eval_text=video_eval_text,
        video_violation=video_violation,
    )
    try:
        full_text = await _chat_collect_with_retries(
            user_id=user_id,
            company=company,
            session_id=f"{session_id}_disposition",
            input_messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}],
            purpose="处罚建议三选一",
        )
    except (AgentWorkerError, AiRefusalError) as exc:
        logger.warning("处罚建议三选一最终失败，改用视频结论兜底: %s", exc)
        return None
    parsed = _extract_json_block(full_text)
    if parsed is None:
        guessed = _normalize_disposition(full_text)
        if not guessed:
            return None
        basis = "无违章" if guessed == "误报" else ""
        return {
            "process_type": guessed,
            "amount": 0.0,
            "basis": basis,
            "suggestion_text": f"罚单类型：{guessed}\n罚单金额：-\n罚单依据：{basis}",
        }
    ticket = _ticket_from_parsed(parsed, full_text)
    return ticket


def _apply_ticket_info(existing: ViolationAiAssessment, ticket_info: dict[str, Any]) -> None:
    existing.ticket_process_type = ticket_info.get("process_type") or existing.ticket_process_type
    existing.ticket_amount = ticket_info.get("amount") if ticket_info.get("process_type") == "罚款" else 0.0
    existing.ticket_basis = ticket_info.get("basis") or existing.ticket_basis
    existing.ticket_suggestion_text = ticket_info.get("suggestion_text") or existing.ticket_suggestion_text


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

    return cached_default_company()


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


def _evaluation_from_video_result(v_result: dict[str, Any]) -> str:
    """把 /api/video/violation 的 complete 结果拼成对话展示文案。"""
    lines: list[str] = []
    conclusion = sanitize_ai_display_text(v_result.get("conclusion"))
    vtype = str(v_result.get("violation_type") or "").strip()
    detail = sanitize_ai_display_text(v_result.get("violation_detail"))
    analysis = sanitize_ai_display_text(v_result.get("analysis"))
    is_v = v_result.get("violation")
    if is_v is not None:
        lines.append(f"是否违章：{'是' if bool(is_v) else '否'}")
    if conclusion:
        lines.append(conclusion)
    if vtype and vtype != "无":
        lines.append(f"违章类型：{vtype}")
    if detail:
        lines.append(detail)
    if analysis and analysis not in (conclusion, detail):
        # 原始 LLM 输出可能很长，仍保留供人工核对
        lines.append(analysis)
    return sanitize_ai_display_text("\n".join(lines)) or "视频违章判定未返回有效内容。"


def _video_has_violation(v_result: dict[str, Any]) -> bool | None:
    """从视频接口 complete 结果解析是否违章；无法判断时返回 None。"""
    if video_complete_failed(v_result):
        return None
    vtype = str(v_result.get("violation_type") or "").strip()
    if vtype == "无":
        vtype = ""
    raw_v = v_result.get("violation")
    if isinstance(raw_v, str):
        low = raw_v.strip().lower()
        if low in ("1", "true", "yes", "是"):
            return True
        if low in ("0", "false", "no", "否"):
            return False
        is_v = bool(raw_v.strip())
    elif raw_v is None:
        is_v = None
    else:
        is_v = bool(raw_v)
    if is_v is True:
        return True
    if is_v is False:
        return False
    if vtype:
        return True
    if vtype == "" and "violation_type" in v_result:
        return False
    return None


def _basis_from_video_result(v_result: dict[str, Any]) -> str:
    """从视频判定结果提取简短罚单依据（优先违章类型，其次结论正文）。"""
    vtype = str(v_result.get("violation_type") or "").strip()
    if vtype == "无":
        vtype = ""
    if vtype:
        return _summarize_basis(vtype)
    conclusion = sanitize_ai_display_text(v_result.get("conclusion")) or ""
    conclusion = re.sub(r"^是否违章[：:].*$", "", conclusion, flags=re.M).strip()
    conclusion = re.sub(r"^存在违章行为\s*", "", conclusion).strip()
    conclusion = re.sub(r"^未发现违章行为\s*", "", conclusion).strip()
    return _summarize_basis(conclusion)


def _ticket_suggestion_text(process_type: str, amount: float, basis: str) -> str:
    return (
        f"罚单类型：{process_type}\n"
        f"罚单金额：{amount if process_type == '罚款' else '-'}\n"
        f"罚单依据：{basis or '暂无建议'}"
    )


def _ticket_from_video_result(v_result: dict[str, Any]) -> dict[str, Any]:
    """视频违章接口结果 → 罚单建议兜底（三选一 chat 失败时使用）。

    有违章：默认「警告」；无违章：默认「误报」。
    """
    is_v = _video_has_violation(v_result)
    if is_v is True:
        process_type = "警告"
        basis = _basis_from_video_result(v_result)
    else:
        process_type = "误报"
        basis = _summarize_basis("无违章")
    return {
        "process_type": process_type,
        "amount": 0.0,
        "basis": basis,
        "suggestion_text": _ticket_suggestion_text(process_type, 0.0, basis),
    }


async def _resolve_ticket_after_video(
    *,
    user_id: str,
    company: str,
    session_id: str,
    row: VehicleViolation,
    v_result: dict[str, Any],
) -> dict[str, Any]:
    """视频判定成功后再用 /api/chat 问罚款/警告/误报；失败则用视频结论兜底。"""
    eval_text = _evaluation_from_video_result(v_result)
    fallback = _ticket_from_video_result(v_result)
    ticket = await _classify_disposition_via_chat(
        user_id=user_id,
        company=company,
        session_id=session_id,
        row=row,
        video_eval_text=eval_text,
        video_violation=_video_has_violation(v_result),
    )
    if ticket is None:
        return fallback
    if not str(ticket.get("basis") or "").strip():
        ticket["basis"] = fallback.get("basis") or ""
        ticket["suggestion_text"] = _ticket_suggestion_text(
            str(ticket.get("process_type") or "警告"),
            float(ticket.get("amount") or 0),
            str(ticket.get("basis") or ""),
        )
    return ticket


def _maybe_auto_false_alarm(row: VehicleViolation, process_type: str | None) -> bool:
    """AI 建议为误报且记录仍为待处理时，自动落库为误报。"""
    if (process_type or "").strip() != "误报":
        return False
    if (row.status or "").strip() != "待处理":
        return False
    row.status = "误报"
    row.pre_audit_kind = "false_alarm"
    row.handler_name = _AI_AUTO_FALSE_ALARM_HANDLER
    row.handler_remark = _AI_AUTO_FALSE_ALARM_REMARK
    row.handled_at = china_now_naive()
    logger.info(
        "AI评估建议误报，自动落库 status=误报 violation_id=%s plate=%s",
        getattr(row, "id", None),
        (row.plate_no or "").strip(),
    )
    return True


async def backfill_ai_suggested_false_alarms(db: AsyncSession) -> int:
    """将「AI 已建议误报但仍为待处理」的历史记录一次性落库为误报。"""
    rows = (
        await db.execute(
            select(VehicleViolation, ViolationAiAssessment)
            .join(ViolationAiAssessment, ViolationAiAssessment.violation_id == VehicleViolation.id)
            .where(
                VehicleViolation.status == "待处理",
                ViolationAiAssessment.ticket_process_type == "误报",
            )
        )
    ).all()
    n = 0
    for row, assessment in rows:
        if _maybe_auto_false_alarm(row, assessment.ticket_process_type):
            n += 1
    if n:
        await db.flush()
        logger.info("回填 AI 建议误报 → 状态误报：%s 条", n)
    return n


def _assessment_looks_like_refusal(assessment: ViolationAiAssessment) -> bool:
    """历史落库内容是否为助手拒答话术（应清掉重评）。"""
    chunks = (
        assessment.evaluation_text,
        assessment.raw_response_text,
        assessment.video_analysis_text,
        assessment.ticket_suggestion_text,
        assessment.ticket_basis,
    )
    for chunk in chunks:
        text = (chunk or "").strip()
        if not text:
            continue
        if _looks_like_ai_refusal(text):
            return True
    return False


async def backfill_reset_refused_assessments(db: AsyncSession) -> int:
    """清除「待处理 + 评估内容为拒答」的历史，重置为未评估，供定时器重新调度。"""
    rows = (
        await db.execute(
            select(VehicleViolation, ViolationAiAssessment)
            .join(ViolationAiAssessment, ViolationAiAssessment.violation_id == VehicleViolation.id)
            .where(VehicleViolation.status == "待处理")
        )
    ).all()
    n = 0
    for row, assessment in rows:
        if not _assessment_looks_like_refusal(assessment):
            continue
        await db.delete(assessment)
        row.ai_queried = False
        n += 1
        logger.info(
            "回捞拒答评估：清除后重评 violation_id=%s biz_no=%s plate=%s",
            row.id,
            (row.biz_no or "").strip(),
            (row.plate_no or "").strip(),
        )
    if n:
        await db.flush()
        logger.info("回捞拒答评估：已重置 %s 条待处理记录，等待定时评估", n)
    return n


def _persist_assessment_fields(
    existing: ViolationAiAssessment,
    row: VehicleViolation,
    *,
    session_id: str,
    evaluation_text: str,
    ticket_info: dict[str, Any],
    video_analysis_text: str,
    raw_response_text: str,
    company: str,
    image_count: int,
    has_video: bool,
    evidence_valid: bool | None,
    system_ok: bool | None,
    violated_rules: list[Any],
) -> bool:
    existing.session_id = session_id
    existing.evaluation_text = evaluation_text
    existing.ticket_process_type = ticket_info["process_type"]
    existing.ticket_amount = ticket_info["amount"]
    existing.ticket_basis = ticket_info["basis"]
    existing.ticket_suggestion_text = ticket_info["suggestion_text"]
    existing.evidence_valid = evidence_valid
    existing.system_judgment_correct = system_ok
    existing.violated_rules_json = json.dumps(violated_rules, ensure_ascii=False)
    existing.video_analysis_text = video_analysis_text
    existing.raw_response_text = raw_response_text
    existing.company_name = company
    existing.alarm_type_name = row.violation_type_name or ""
    existing.image_count = image_count
    existing.has_video = has_video
    row.ai_queried = True
    return _maybe_auto_false_alarm(row, ticket_info.get("process_type"))


async def _save_assessment_from_video(
    db: AsyncSession,
    row: VehicleViolation,
    existing: ViolationAiAssessment | None,
    *,
    session_id: str,
    v_result: dict[str, Any],
    company: str,
    image_count: int = 0,
    ticket_info: dict[str, Any] | None = None,
) -> tuple[ViolationAiAssessment, bool]:
    if existing is None:
        existing = ViolationAiAssessment(violation_id=row.id)
        db.add(existing)
    evaluation_text = _evaluation_from_video_result(v_result)
    if ticket_info is None:
        ticket_info = _ticket_from_video_result(v_result)
    is_v = _video_has_violation(v_result)
    vtype = str(v_result.get("violation_type") or "").strip()
    rules: list[Any] = [vtype] if is_v is True and vtype and vtype != "无" else []
    auto_false = _persist_assessment_fields(
        existing,
        row,
        session_id=session_id,
        evaluation_text=evaluation_text,
        ticket_info=ticket_info,
        video_analysis_text=evaluation_text,
        raw_response_text=json.dumps(v_result, ensure_ascii=False)[:8000],
        company=company,
        image_count=image_count,
        has_video=True,
        evidence_valid=True,
        system_ok=is_v if is_v is not None else None,
        violated_rules=rules,
    )
    await db.flush()
    await db.refresh(existing)
    return existing, auto_false


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
) -> tuple[ViolationAiAssessment, bool]:
    """chat 回退路径落库（仅无视频、仅图片时使用）。"""
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

    auto_false = _persist_assessment_fields(
        existing,
        row,
        session_id=session_id,
        evaluation_text=evaluation_text,
        ticket_info=ticket_info,
        video_analysis_text=video_analysis_text,
        raw_response_text=full_text,
        company=company,
        image_count=image_count,
        has_video=has_video,
        evidence_valid=evidence_valid if isinstance(evidence_valid, bool) else None,
        system_ok=system_ok if isinstance(system_ok, bool) else None,
        violated_rules=violated_rules,
    )
    await db.flush()
    await db.refresh(existing)
    return existing, auto_false


async def get_violation_ai_assessment(db: AsyncSession, violation_id: int) -> dict[str, Any]:
    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None or not violation_row_is_page_visible(row):
        raise HTTPException(status_code=404, detail="记录不存在")
    assessment = await db.scalar(
        select(ViolationAiAssessment).where(ViolationAiAssessment.violation_id == violation_id).limit(1)
    )
    auto_false = False
    if assessment is not None:
        auto_false = _maybe_auto_false_alarm(row, assessment.ticket_process_type)
        if auto_false:
            await db.flush()
            await db.refresh(row)
    return {
        "ok": True,
        "ai_queried": bool(getattr(row, "ai_queried", False)),
        "auto_false_alarm": auto_false,
        "status": (row.status or "").strip(),
        "assessment": _assessment_out(assessment),
    }


async def _download_video_bytes(video_url: str) -> tuple[bytes, str, str]:
    """下载视频，返回 (bytes, mime, filename_ext)。"""
    video_data, video_mime = await _download_media(video_url)
    if len(video_data) > _MAX_VIDEO_BYTES:
        raise ValueError("视频文件过大")
    ext = Path(_extract_url(video_url)).suffix.lower() or ".mp4"
    return video_data, video_mime, ext


def _image_ext_and_mime(url: str, mime: str) -> tuple[str, str]:
    ext = Path((url or "").split("?", 1)[0]).suffix.lower()
    if ext not in _IMAGE_EXTS:
        ext = ".jpg"
    mt = (mime or "").split(";")[0].strip().lower()
    if not mt.startswith("image/"):
        mt = {
            ".png": "image/png",
            ".webp": "image/webp",
            ".bmp": "image/bmp",
            ".gif": "image/gif",
        }.get(ext, "image/jpeg")
    return ext, mt


async def _download_image_file(image_url: str) -> tuple[bytes, str, str]:
    """下载抓拍图，返回 (bytes, mime, filename_ext)，供 /api/video/violation 的 images 字段。"""
    data, mime = await _download_media(image_url)
    if not data:
        raise ValueError("图片内容为空")
    if len(data) > _MAX_IMAGE_BYTES:
        raise ValueError("图片文件过大")
    ext, mime = _image_ext_and_mime(image_url, mime)
    return data, mime, ext


async def _collect_violation_evidence(
    image_urls: list[str],
    video_url: str | None,
) -> dict[str, Any]:
    """下载视频（可选）和最多 9 张抓拍图；判定时视频走 file，图片走 images。"""
    video: dict[str, Any] | None = None
    images: list[dict[str, Any]] = []
    if video_url:
        try:
            data, mime, ext = await _download_video_bytes(video_url)
            if data:
                video = {
                    "kind": "video",
                    "data": data,
                    "mime": mime,
                    "ext": ext,
                    "filename": f"evidence{ext}",
                }
        except Exception as exc:
            logger.warning("下载视频失败，将改用抓拍图调用 video/violation: %s", exc)
    for idx, url in enumerate(image_urls[:9], start=1):
        try:
            data, mime, ext = await _download_image_file(url)
            images.append(
                {
                    "filename": f"image_{idx}{ext}",
                    "content": data,
                    "content_type": mime,
                }
            )
        except Exception as exc:
            logger.warning("下载图片失败 %s: %s", url, exc)
    return {"video": video, "images": images}


def _violation_attempts(evidence: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """视频优先走 file；失败或无视频时，抓拍图一次按 images 提交。"""
    attempts: list[tuple[str, dict[str, Any]]] = []
    video = evidence.get("video")
    images = evidence.get("images") or []
    if video:
        attempts.append(
            (
                "video",
                {
                    "filename": video["filename"],
                    "content": video["data"],
                    "content_type": video["mime"],
                    "images": None,
                },
            )
        )
    if images:
        attempts.append(
            (
                "image",
                {
                    "filename": None,
                    "content": None,
                    "content_type": None,
                    "images": images,
                },
            )
        )
    return attempts


def _alarm_type_display(row: VehicleViolation) -> str:
    from app.jt808_alarm_sync import _strip_alarm_level_suffix

    raw = (row.violation_type_name or "").strip()
    return _strip_alarm_level_suffix(raw) or raw or "主动安全报警"


def _build_text_rules_prompt(row: VehicleViolation) -> str:
    """纯文本规章制度问答（不带抓拍图，避免被判敏感信息拒答）。"""
    alarm = _alarm_type_display(row)
    return (
        "请做合规规章制度解答（车辆主动安全报警业务，属于车辆数据查询与制度问答范围）。\n"
        f"{_alarm_context_block(row)}\n\n"
        f"请查询本公司规章制度中与「{alarm}」相关的条款，并回答：\n"
        "1. 该报警一般如何认定？\n"
        "2. 制度上的处理原则是什么？\n"
        "3. 综合本条业务字段，给出 process_type：只能是「罚款」「警告」「误报」三选一"
        "（证据不足或无法确认事实→警告并注明需人工复核；明显误报→误报；需经济处罚→罚款）。\n"
        "   basis 写清依据摘要。\n\n"
        "回复末尾附带 JSON：\n"
        "```json\n"
        "{\n"
        '  "evidence_valid": null,\n'
        '  "system_judgment_correct": null,\n'
        '  "evaluation_summary": "制度问答结论",\n'
        '  "violated_rules": ["制度名称及条款"],\n'
        '  "ticket_suggestion": {\n'
        '    "process_type": "警告",\n'
        '    "amount": 0,\n'
        f'    "basis": "{alarm}"\n'
        "  }\n"
        "}\n"
        "```\n"
    )


async def _run_text_rules_fallback_assessment(
    db: AsyncSession,
    row: VehicleViolation,
    existing: ViolationAiAssessment | None,
    *,
    user_id: str,
    company: str,
    image_count: int = 0,
) -> tuple[ViolationAiAssessment, bool]:
    """图片被拒答后的兜底：不传图，只做规章制度文本问答并落库。"""
    session_id = f"violation_assess_{row.id}_rules"
    prompt = _build_text_rules_prompt(row)
    full_text = await _chat_collect_with_retries(
        user_id=user_id,
        company=company,
        session_id=session_id,
        input_messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        purpose="制度文本问答",
        max_attempts=2,
    )
    note = (
        "【说明】抓拍图片分析被助手拒答（可能含敏感画面），"
        "已改为不传图的合规规章制度问答，请结合证据人工复核。\n\n"
    )
    return await _save_assessment(
        db,
        row,
        existing,
        session_id=session_id,
        full_text=note + (full_text or ""),
        video_analysis_text="",
        company=company,
        image_count=image_count,
        has_video=False,
    )


async def _run_local_refusal_fallback_assessment(
    db: AsyncSession,
    row: VehicleViolation,
    existing: ViolationAiAssessment | None,
    *,
    company: str,
    image_count: int = 0,
    reason: str = "",
) -> tuple[ViolationAiAssessment, bool]:
    """Worker 多次拒答时的兜底：落「误报」，并完整保留助手返回原文。"""
    alarm = _alarm_type_display(row)
    session_id = f"violation_assess_{row.id}_local"
    refuse_text = (reason or "").strip() or "（助手未返回正文）"
    basis = "助手拒答"
    ticket_info = {
        "process_type": "误报",
        "amount": 0.0,
        "basis": basis,
        "suggestion_text": _ticket_suggestion_text("误报", 0.0, basis),
    }
    evaluation = (
        "【说明】车联网助手对本条多次返回服务范围拒答，系统按误报处理。\n"
        f"- 报警类型：{alarm}\n"
        f"- 车牌：{(row.plate_no or '').strip() or '—'}\n"
        f"- 处理：误报\n\n"
        "【AI 返回原文】\n"
        f"{refuse_text}\n"
    )

    if existing is None:
        existing = ViolationAiAssessment(violation_id=row.id)
        db.add(existing)
    auto_false = _persist_assessment_fields(
        existing,
        row,
        session_id=session_id,
        evaluation_text=evaluation,
        ticket_info=ticket_info,
        video_analysis_text="",
        raw_response_text=refuse_text[:8000],
        company=company,
        image_count=image_count,
        has_video=False,
        evidence_valid=False,
        system_ok=False,
        violated_rules=[],
    )
    await db.flush()
    await db.refresh(existing)
    logger.warning(
        "AI多次拒答，已落库误报并保留原文 violation_id=%s plate=%s auto_false=%s",
        row.id,
        (row.plate_no or "").strip(),
        auto_false,
    )
    return existing, auto_false


async def _run_chat_fallback_assessment(
    db: AsyncSession,
    row: VehicleViolation,
    existing: ViolationAiAssessment | None,
    *,
    user_id: str,
    company: str,
    image_blocks: list[dict[str, Any]],
) -> tuple[ViolationAiAssessment, bool]:
    """无视频仅有图片时回退 /api/chat。

    1) 带图评估 → 2) 拒答则无图制度问答 → 3) 仍拒答则本地落库（警告），保证调度不卡死。
    """
    session_id = f"violation_assess_{row.id}"
    prompt = _build_prompt(
        row,
        video_analysis="",
        image_count=len(image_blocks),
        has_video=False,
    )
    content_blocks: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
    content_blocks.extend(image_blocks)
    last_refuse = ""
    try:
        full_text = await _chat_collect_with_retries(
            user_id=user_id,
            company=company,
            session_id=session_id,
            input_messages=[{"role": "user", "content": content_blocks}],
            purpose="图片评估",
            max_attempts=2,
        )
        return await _save_assessment(
            db,
            row,
            existing,
            session_id=session_id,
            full_text=full_text,
            video_analysis_text="",
            company=company,
            image_count=len(image_blocks),
            has_video=False,
        )
    except AiRefusalError as exc:
        last_refuse = str(exc)
        logger.warning("图片评估拒答，改走无图制度问答 violation_id=%s", row.id)

    try:
        return await _run_text_rules_fallback_assessment(
            db,
            row,
            existing,
            user_id=user_id,
            company=company,
            image_count=len(image_blocks),
        )
    except AiRefusalError as exc:
        last_refuse = str(exc) or last_refuse
        logger.warning("制度问答仍拒答，改本地落库 violation_id=%s", row.id)
        return await _run_local_refusal_fallback_assessment(
            db,
            row,
            existing,
            company=company,
            image_count=len(image_blocks),
            reason=last_refuse,
        )


async def run_violation_ai_assessment(
    db: AsyncSession,
    *,
    violation_id: int,
    user_id: str,
    force: bool = False,
) -> dict[str, Any]:
    """主动安全 AI 评估：一律走 Worker /api/video/violation（无视频则传抓拍图）。"""
    if not agent_worker_client.configured():
        raise HTTPException(status_code=503, detail="Agent Worker 未配置")

    row = await db.scalar(select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1))
    if row is None or not violation_row_is_page_visible(row):
        raise HTTPException(status_code=404, detail="记录不存在")

    existing = await db.scalar(
        select(ViolationAiAssessment).where(ViolationAiAssessment.violation_id == violation_id).limit(1)
    )
    if bool(getattr(row, "ai_queried", False)) and existing and not force:
        auto_false = _maybe_auto_false_alarm(row, existing.ticket_process_type)
        if auto_false:
            await db.flush()
            await db.refresh(row)
        return {
            "ok": True,
            "cached": True,
            "ai_queried": True,
            "auto_false_alarm": auto_false,
            "status": (row.status or "").strip(),
            "assessment": _assessment_out(existing),
        }

    company = await _resolve_company_for_violation(db, row)
    image_urls, video_url = _gather_media_refs(row)
    if not image_urls and not video_url:
        return _skip_response(reason="暂无图片或视频证据，已跳过 AI 分析")

    session_id = f"violation_assess_{violation_id}"
    evidence = await _collect_violation_evidence(image_urls, video_url)
    attempts = _violation_attempts(evidence)
    if not attempts:
        return _skip_response(reason="证据下载失败或全部为空，已跳过 AI 分析")

    last_error = ""
    for kind, kwargs in attempts:
        try:
            v_result = await agent_worker_client.analyze_video_violation(
                user_id=user_id,
                company=company,
                session_id=session_id,
                extra_fields=_alarm_extra_form_fields(row),
                **kwargs,
            )
            if video_complete_failed(v_result) or _video_result_is_refusal(v_result):
                last_error = "违章判定失败或拒答"
                logger.warning("video/violation 拒答/失败 kind=%s violation_id=%s", kind, violation_id)
                continue
            ticket_info = await _resolve_ticket_after_video(
                user_id=user_id,
                company=company,
                session_id=session_id,
                row=row,
                v_result=v_result,
            )
            existing, auto_false = await _save_assessment_from_video(
                db,
                row,
                existing,
                session_id=session_id,
                v_result=v_result,
                company=company,
                image_count=len(image_urls),
                ticket_info=ticket_info,
            )
            return {
                "ok": True,
                "cached": False,
                "ai_queried": True,
                "source": "video_violation",
                "auto_false_alarm": auto_false,
                "status": (row.status or "").strip(),
                "assessment": _assessment_out(existing),
            }
        except AiRefusalError as exc:
            last_error = str(exc)
            logger.warning("video/violation 拒答 kind=%s: %s", kind, exc)
        except Exception as exc:
            last_error = str(exc)
            logger.warning("video/violation 失败 kind=%s: %s", kind, exc)

    raise HTTPException(status_code=502, detail=f"违章判定失败：{last_error or '未知错误'}")


def _sse(payload: dict[str, Any]) -> bytes:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")


async def stream_violation_ai_assessment(
    *,
    violation_id: int,
    user_id: str,
    force: bool = False,
) -> AsyncIterator[bytes]:
    """SSE 流式 AI 评估。

    一律走 Worker ``POST /api/video/violation``（SSE）：有视频传视频，无视频传抓拍图。
    """
    if not agent_worker_client.configured():
        yield _sse({"object": "error", "message": "AI 接口未配置或未启用"})
        return

    async with AsyncSessionLocal() as db:
        try:
            row = await db.scalar(
                select(VehicleViolation).where(VehicleViolation.id == violation_id).limit(1)
            )
            if row is None or not violation_row_is_page_visible(row):
                yield _sse({"object": "error", "message": "记录不存在"})
                return

            existing = await db.scalar(
                select(ViolationAiAssessment)
                .where(ViolationAiAssessment.violation_id == violation_id)
                .limit(1)
            )
            if bool(getattr(row, "ai_queried", False)) and existing and not force:
                auto_false = _maybe_auto_false_alarm(row, existing.ticket_process_type)
                if auto_false:
                    await db.commit()
                    await db.refresh(row)
                yield _sse(
                    {
                        "object": "assessment",
                        "cached": True,
                        "ai_queried": True,
                        "auto_false_alarm": auto_false,
                        "status": (row.status or "").strip(),
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

            session_id = f"violation_assess_{violation_id}"
            yield _sse(
                {
                    "object": "status",
                    "stage": "download",
                    "message": "正在下载证据（视频优先，无视频则用抓拍图）…",
                }
            )
            evidence = await _collect_violation_evidence(image_urls, video_url)
            attempts = _violation_attempts(evidence)
            if not attempts:
                yield _sse({"object": "skip", "reason": "证据下载失败或全部为空，已跳过 AI 分析"})
                return

            last_error = ""
            for kind, kwargs in attempts:
                kind_label = "视频" if kind == "video" else "抓拍图"
                yield _sse(
                    {
                        "object": "status",
                        "stage": "video",
                        "message": f"正在调用违章判定接口（{kind_label}）…",
                    }
                )
                text_parts: list[str] = []
                v_result: dict[str, Any] | None = None
                try:
                    async for ev in agent_worker_client.analyze_video_violation_stream(
                        user_id=user_id,
                        company=company,
                        session_id=session_id,
                        extra_fields=_alarm_extra_form_fields(row),
                        **kwargs,
                    ):
                        obj = str(ev.get("object") or "")
                        if obj == "delta":
                            if ev.get("type") == "text" and ev.get("text"):
                                text_parts.append(str(ev["text"]))
                                yield _sse(
                                    {
                                        "object": "content",
                                        "type": "text",
                                        "delta": True,
                                        "text": ev["text"],
                                    }
                                )
                            elif ev.get("type") == "tool_call":
                                name = str(ev.get("name") or "tool")
                                status = str(ev.get("status") or "")
                                tip = f"[{name} {status}]".strip()
                                yield _sse(
                                    {
                                        "object": "status",
                                        "stage": "video",
                                        "message": tip,
                                    }
                                )
                        elif obj == "complete":
                            if video_complete_failed(ev):
                                raise AgentWorkerError(
                                    str(ev.get("analysis") or ev.get("conclusion") or "违章判定分析失败")
                                )
                            v_result = dict(ev)
                            v_result.pop("object", None)
                        elif obj == "error":
                            raise AgentWorkerError(
                                str(ev.get("detail") or ev.get("message") or "违章判定失败")
                            )
                except AgentWorkerError as exc:
                    last_error = str(exc)
                    logger.warning("video/violation SSE 失败 kind=%s: %s", kind, exc)
                    v_result = None
                    yield _sse(
                        {
                            "object": "status",
                            "stage": "video",
                            "message": f"{kind_label}判定失败：{exc}",
                        }
                    )

                if v_result is not None and _video_result_is_refusal(v_result):
                    last_error = "违章判定返回拒答文案"
                    logger.warning("流式 video/violation 拒答 kind=%s", kind)
                    v_result = None
                    yield _sse(
                        {
                            "object": "status",
                            "stage": "video",
                            "message": f"{kind_label}判定被拒答",
                        }
                    )

                if v_result is None:
                    continue

                eval_text = _evaluation_from_video_result(v_result)
                if not text_parts and eval_text:
                    yield _sse(
                        {
                            "object": "content",
                            "type": "text",
                            "delta": True,
                            "text": eval_text,
                        }
                    )
                if _video_has_violation(v_result) is not False:
                    yield _sse(
                        {
                            "object": "status",
                            "stage": "disposition",
                            "message": "正在判定处罚建议（罚款 / 警告 / 误报）…",
                        }
                    )
                ticket_info = await _resolve_ticket_after_video(
                    user_id=user_id,
                    company=company,
                    session_id=session_id,
                    row=row,
                    v_result=v_result,
                )
                existing, auto_false = await _save_assessment_from_video(
                    db,
                    row,
                    existing,
                    session_id=session_id,
                    v_result=v_result,
                    company=company,
                    image_count=len(image_urls),
                    ticket_info=ticket_info,
                )
                await db.commit()
                yield _sse(
                    {
                        "object": "assessment",
                        "cached": False,
                        "ai_queried": True,
                        "source": "video_violation",
                        "auto_false_alarm": auto_false,
                        "status": (row.status or "").strip(),
                        "assessment": _assessment_out(existing),
                    }
                )
                return

            yield _sse({"object": "error", "message": f"违章判定失败：{last_error or '未知错误'}"})
        except Exception as exc:  # noqa: BLE001
            logger.exception("流式 AI 评估失败 violation_id=%s", violation_id)
            await db.rollback()
            yield _sse({"object": "error", "message": str(exc)})

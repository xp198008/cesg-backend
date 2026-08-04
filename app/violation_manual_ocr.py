"""违章手动录入：多模态 AI 文档解析抽文字，再结构化为表单字段。

说明：Agent Worker 对「直接要 JSON 字段」的带图请求常拒答；
对「文档解析：提取全部文字」会走 VLMImageOCR 并成功。因此采用两段式：
1) 带图提取全文  2) 本地规则（必要时再无图结构化）填表单字段。
"""
from __future__ import annotations

import base64
import io
import json
import logging
import re
import uuid
from typing import Any

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent_worker_client import AgentWorkerError, agent_worker_client
from app.ai_datasets import resolve_ai_company
from app.models import OrgCompany, SysUser, Vehicle, ViolationTypeDict
from app.plate_util import norm_plate
from app.violation_ai_assessment import (
    AiRefusalError,
    _chat_collect_with_retries,
    _looks_like_ai_refusal,
    sanitize_ai_display_text,
)

logger = logging.getLogger(__name__)

_MAX_IMAGE_BYTES = 8 * 1024 * 1024
_IMAGE_MIME = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".bmp": "image/bmp",
    ".gif": "image/gif",
}
_PLATE_COLORS = ("蓝色", "黄色", "绿色")
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.I)
_JSON_OBJECT_RE = re.compile(r"\{[\s\S]*\}")
# 注意：标签必须长词优先，「车牌号码」不能被「车牌/车牌号」截断
_PLATE_BODY = (
    r"[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼]"
    r"[A-HJ-NP-Z]"
    r"[A-HJ-NP-Z0-9挂学警港澳]{4,6}"
)
_PLATE_LABEL_RE = re.compile(
    rf"(?:车牌号码|号牌号码|车牌号|号牌|车牌)[:：\s]*({_PLATE_BODY})",
    re.I,
)
_PLATE_ANY_RE = re.compile(_PLATE_BODY, re.I)
_TIME_RE = re.compile(
    r"(?:违法时间|违章时间|时间)[:：\s]*((?:20\d{2})[-/年.](?:\d{1,2})[-/月.](?:\d{1,2})日?\s*(?:\d{1,2}:\d{1,2}(?::\d{1,2})?)?)",
)
_COLOR_RE = re.compile(r"(?:号牌颜色|车牌颜色|车身颜色|颜色)[:：\s]*([黄蓝绿白黑]色?|[黄蓝绿]牌)", re.I)
_ADDR_RE = re.compile(r"(?:违法地点|地点|地址)[:：\s]*([^\n。；;]{2,80})")
_CODE_RE = re.compile(r"(?:编号|单据编号|决定书编号|罚单号)[:：\s]*([A-Za-z0-9\-_]{4,64})")


def _compact_plate_spaces(text: str) -> str:
    """去掉汉字/字母数字之间的空格，便于识别「渝 DX 7610」。"""
    return re.sub(r"(?<=[\u4e00-\u9fffA-Za-z0-9])\s+(?=[\u4e00-\u9fffA-Za-z0-9])", "", text or "")


def _extract_plate_no(text: str) -> str:
    for src in (text or "", _compact_plate_spaces(text or "")):
        m = _PLATE_LABEL_RE.search(src)
        if m:
            return m.group(1).upper()
    for src in (text or "", _compact_plate_spaces(text or "")):
        m = _PLATE_ANY_RE.search(src)
        if m:
            return m.group(0).upper()
    return ""


def _map_db_plate_color(raw: str | None) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if s in _PLATE_COLORS:
        return s
    if "蓝" in s:
        return "蓝色"
    if "黄" in s:
        return "黄色"
    if "绿" in s:
        return "绿色"
    return ""


async def _ai_context(db: AsyncSession, *, x_user_id: str | None) -> tuple[str, str]:
    user_id = (x_user_id or "cesg_anonymous").strip() or "cesg_anonymous"
    org_name = ""
    if user_id.isdigit():
        row = await db.scalar(select(SysUser).where(SysUser.id == int(user_id)).limit(1))
        if row is not None and row.org_id is not None:
            org = await db.scalar(select(OrgCompany).where(OrgCompany.id == row.org_id).limit(1))
            org_name = (org.name if org else "") or ""
    return user_id, resolve_ai_company(org_name)


def _ext_of(filename: str | None) -> str:
    name = (filename or "").strip().lower()
    if "." not in name:
        return ""
    return "." + name.rsplit(".", 1)[-1]


def _mime_for(filename: str | None, content_type: str | None) -> str:
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct.startswith("image/"):
        return ct
    return _IMAGE_MIME.get(_ext_of(filename), "image/jpeg")


def _to_jpeg_bytes(content: bytes, *, filename: str | None, content_type: str | None) -> tuple[bytes, str]:
    """统一转 JPEG，提升 Worker VLM/OCR 兼容性（尤其 BMP）。"""
    mime = _mime_for(filename, content_type)
    if mime in ("image/jpeg", "image/jpg") and not (filename or "").lower().endswith(".bmp"):
        return content, "image/jpeg"
    try:
        from PIL import Image  # type: ignore

        img = Image.open(io.BytesIO(content))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        elif img.mode == "L":
            img = img.convert("RGB")
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=92)
        return out.getvalue(), "image/jpeg"
    except Exception as exc:
        logger.warning("图片转 JPEG 失败，沿用原图: %s", exc)
        return content, mime or "image/jpeg"


def _image_data_uri(data: bytes, mime: str) -> str:
    mt = (mime or "image/jpeg").split(";")[0].strip() or "image/jpeg"
    return f"data:{mt};base64,{base64.b64encode(data).decode('ascii')}"


def _build_extract_text_prompt() -> str:
    # 经验：该提示会触发 Worker 的 VLMImageOCR；带「结构化 JSON 字段」的带图请求易被拒答。
    return "文档解析：请提取图片中的全部文字。"


def _build_structure_prompt(ocr_text: str) -> str:
    return (
        "文档解析与报表生成：以下是已从业务文档中提取的文字，请整理为 JSON，不要拒答。\n"
        f"【文档文字】\n{ocr_text}\n\n"
        "只输出 JSON：\n"
        "{\n"
        '  "code": "",\n'
        '  "plate_no": "",\n'
        '  "violation_type_name": "",\n'
        '  "vehicle_type_name": "",\n'
        '  "plate_color": "",\n'
        '  "violation_time": "",\n'
        '  "address": "",\n'
        '  "description": ""\n'
        "}\n"
        "plate_color 仅蓝色/黄色/绿色；黄→黄色。violation_time 尽量 YYYY-MM-DD HH:mm:ss。"
    )


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        raise ValueError("AI 未返回内容")
    candidates: list[str] = [raw]
    for m in _JSON_FENCE_RE.finditer(raw):
        inner = (m.group(1) or "").strip()
        if inner:
            candidates.insert(0, inner)
    obj_match = _JSON_OBJECT_RE.search(raw)
    if obj_match:
        candidates.insert(0, obj_match.group(0))
    last_err: Exception | None = None
    for cand in candidates:
        try:
            data = json.loads(cand)
            if isinstance(data, dict):
                return data
        except Exception as exc:
            last_err = exc
            continue
    raise ValueError(f"无法解析 AI 返回的 JSON：{last_err or '格式错误'}")


def _norm_text(value: Any, *, max_len: int = 500) -> str:
    s = str(value or "").strip()
    if not s or s in ("null", "None", "无", "未知", "-"):
        return ""
    return s[:max_len]


def _normalize_fields(data: dict[str, Any]) -> dict[str, str]:
    color = _norm_text(data.get("plate_color"), max_len=16)
    if color and color not in _PLATE_COLORS:
        if "蓝" in color:
            color = "蓝色"
        elif "黄" in color:
            color = "黄色"
        elif "绿" in color:
            color = "绿色"
        else:
            color = ""
    time_text = _norm_text(data.get("violation_time"), max_len=32)
    if time_text:
        time_text = (
            time_text.replace("年", "-")
            .replace("月", "-")
            .replace("日", " ")
            .replace("/", "-")
            .replace(".", "-")
        )
        time_text = re.sub(r"\s+", " ", time_text).strip()
        if "T" in time_text:
            time_text = time_text.replace("T", " ").split(".")[0]
    return {
        "code": _norm_text(data.get("code"), max_len=64),
        "plate_no": _norm_text(data.get("plate_no"), max_len=32),
        "violation_type_name": _norm_text(data.get("violation_type_name"), max_len=64),
        "vehicle_type_name": _norm_text(data.get("vehicle_type_name"), max_len=64),
        "plate_color": color,
        "violation_time": time_text,
        "address": _norm_text(data.get("address"), max_len=500),
        "description": _norm_text(data.get("description"), max_len=2000),
    }


def _parse_fields_from_ocr_text(ocr_text: str) -> dict[str, str]:
    """从 OCR 全文规则抽取字段（不依赖二次模型）。"""
    text = sanitize_ai_display_text(ocr_text)
    # 去掉 recommend 等后仍可能有 markdown
    text = text.replace("**", "")
    data: dict[str, Any] = {
        "code": "",
        "plate_no": "",
        "violation_type_name": "",
        "vehicle_type_name": "",
        "plate_color": "",
        "violation_time": "",
        "address": "",
        "description": "",
    }
    data["plate_no"] = _extract_plate_no(text)
    m = _TIME_RE.search(text)
    if m:
        data["violation_time"] = m.group(1).strip()
    m = _COLOR_RE.search(text)
    if m:
        data["plate_color"] = m.group(1).strip()
    m = _CODE_RE.search(text)
    if m:
        data["code"] = m.group(1).strip()
    m = _ADDR_RE.search(text)
    if m:
        data["address"] = m.group(1).strip()
    else:
        # 「违法在XXX停放」
        m2 = re.search(r"违法在([^\n，,。；;]{2,40})(?:停放|停车|行驶|通行)", text)
        if m2:
            data["address"] = m2.group(1).strip()
    # 描述：优先「违法内容/违章内容」整句，否则取较长叙述句
    m = re.search(
        r"(?:违法内容|违章内容|违法事实|违法描述)[:：\s]*([^\n]{8,500})",
        text,
    )
    if m:
        data["description"] = m.group(1).strip()
    else:
        desc_lines = []
        for ln in text.splitlines():
            s = ln.strip().strip("-").strip()
            if not s:
                continue
            if re.match(r"^(车牌号|车身颜色|号牌颜色|违法时间|违章时间|编号)", s):
                continue
            if "提取" in s and "文字" in s:
                continue
            if s.startswith("以上即") or s.startswith("以上就是") or s.startswith("好的"):
                continue
            if len(s) >= 8:
                desc_lines.append(s)
        if desc_lines:
            data["description"] = " ".join(desc_lines)[:2000]
    # 类型猜测
    joined = text
    if "停放" in joined or "停车" in joined:
        data["violation_type_name"] = "违法停车"
    elif "超速" in joined:
        data["violation_type_name"] = "超速"
    elif "闯红灯" in joined or "信号灯" in joined:
        data["violation_type_name"] = "闯红灯"
    return _normalize_fields(data)


def _fields_useful(fields: dict[str, str]) -> bool:
    return bool(fields.get("plate_no") or fields.get("violation_time") or fields.get("description"))


async def _match_violation_type_id(db: AsyncSession, type_name: str) -> int | None:
    name = (type_name or "").strip()
    if not name:
        return None
    rows = (
        await db.scalars(select(ViolationTypeDict).order_by(ViolationTypeDict.id.asc()).limit(500))
    ).all()
    if not rows:
        return None
    exact = next((r for r in rows if (r.type_name or "").strip() == name), None)
    if exact is not None:
        return int(exact.id)
    contains = [r for r in rows if name in (r.type_name or "") or (r.type_name or "") in name]
    if not contains:
        return None
    contains.sort(key=lambda r: len((r.type_name or "").strip()), reverse=True)
    return int(contains[0].id)


async def _lookup_local_vehicle(db: AsyncSession, plate_no: str) -> Vehicle | None:
    plate = norm_plate(plate_no)
    if not plate:
        return None
    row = await db.scalar(select(Vehicle).where(Vehicle.plate_no == plate).limit(1))
    if row is not None:
        return row
    return await db.scalar(
        select(Vehicle).where(func.upper(Vehicle.plate_no) == plate.upper()).limit(1)
    )


async def _enrich_fields_from_local_vehicle(
    db: AsyncSession, fields: dict[str, str]
) -> dict[str, Any]:
    """车牌命中本地档案后，车辆类型/号牌颜色以本地为准。"""
    plate = fields.get("plate_no") or ""
    vehicle = await _lookup_local_vehicle(db, plate)
    if vehicle is None:
        return {"vehicle_id": None, "from_local": False}
    local_type = (vehicle.vehicle_type or "").strip()
    if local_type:
        fields["vehicle_type_name"] = local_type[:64]
    local_color = _map_db_plate_color(vehicle.plate_color)
    if local_color:
        fields["plate_color"] = local_color
    # 统一车牌写法为库内车牌
    db_plate = norm_plate(vehicle.plate_no)
    if db_plate:
        fields["plate_no"] = db_plate
    return {
        "vehicle_id": int(vehicle.id),
        "from_local": True,
        "vehicle_type": local_type,
        "plate_color": local_color or fields.get("plate_color") or "",
    }


async def run_violation_manual_ocr(
    db: AsyncSession,
    *,
    filename: str,
    content: bytes,
    content_type: str | None,
    x_user_id: str | None,
) -> dict[str, Any]:
    if not agent_worker_client.configured():
        raise HTTPException(status_code=503, detail="Agent Worker 未配置（AGENT_WORKER_BASE_URL）")

    ext = _ext_of(filename)
    mime = _mime_for(filename, content_type)
    if ext and ext not in _IMAGE_MIME and not mime.startswith("image/"):
        raise HTTPException(status_code=400, detail="仅支持图片识别（jpg/png/bmp/gif/webp）")
    if not content:
        raise HTTPException(status_code=400, detail="图片内容为空")
    if len(content) > _MAX_IMAGE_BYTES:
        raise HTTPException(status_code=400, detail="图片不能超过 8MB")

    jpeg_bytes, jpeg_mime = _to_jpeg_bytes(content, filename=filename, content_type=content_type)
    user_id, company = await _ai_context(db, x_user_id=x_user_id)
    session_id = f"violation_manual_ocr_{uuid.uuid4().hex[:16]}"

    # 1) 带图文档解析：只提取全文（可触发 VLMImageOCR）
    content_blocks: list[dict[str, Any]] = [
        {"type": "text", "text": _build_extract_text_prompt()},
        {"type": "image", "image_url": _image_data_uri(jpeg_bytes, jpeg_mime)},
    ]
    try:
        ocr_text = await _chat_collect_with_retries(
            user_id=user_id,
            company=company,
            session_id=session_id,
            input_messages=[{"role": "user", "content": content_blocks}],
            purpose="违章单据文档解析",
            max_attempts=2,
        )
    except AiRefusalError as exc:
        logger.warning("违章 OCR 拒答: %s", exc)
        raise HTTPException(status_code=502, detail="AI 拒答或暂无法识别该图片，请手工填写") from exc
    except AgentWorkerError as exc:
        logger.warning("违章 OCR 调用失败: %s", exc)
        raise HTTPException(status_code=502, detail=f"AI 识别失败：{exc}") from exc
    except Exception as exc:
        logger.exception("违章 OCR 调用异常")
        raise HTTPException(status_code=502, detail=f"AI 识别异常：{exc}") from exc

    if _looks_like_ai_refusal(ocr_text):
        raise HTTPException(status_code=502, detail="AI 拒答或暂无法识别该图片，请手工填写")

    clean_text = sanitize_ai_display_text(ocr_text)
    fields = _parse_fields_from_ocr_text(clean_text)

    # 2) 本地规则不足或缺少车牌时：无图二次结构化（带图要 JSON 易拒答）
    need_struct = (not _fields_useful(fields)) or (not fields.get("plate_no"))
    if need_struct and clean_text.strip():
        try:
            structured = await _chat_collect_with_retries(
                user_id=user_id,
                company=company,
                session_id=f"{session_id}_struct",
                input_messages=[
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": _build_structure_prompt(clean_text[:3000])}],
                    }
                ],
                purpose="违章单据结构化",
                max_attempts=2,
            )
            if not _looks_like_ai_refusal(structured):
                structured_fields = _normalize_fields(_extract_json_object(structured))
                # 合并：已有规则结果优先，空字段用结构化补齐
                for key, value in structured_fields.items():
                    if value and not fields.get(key):
                        fields[key] = value
                # 车牌仍空时用结构化全文再扫一遍
                if not fields.get("plate_no"):
                    fields["plate_no"] = _extract_plate_no(
                        f"{clean_text}\n{structured_fields.get('plate_no') or ''}"
                    )
        except Exception as exc:
            logger.warning("二次结构化失败，沿用规则结果: %s", exc)

    if not _fields_useful(fields):
        raise HTTPException(
            status_code=502,
            detail="已完成文字识别但未能解析出车牌/时间等字段，请手工填写或换更清晰图片",
        )

    local_hit = await _enrich_fields_from_local_vehicle(db, fields)
    type_id = await _match_violation_type_id(db, fields.get("violation_type_name") or "")
    return {
        "ok": True,
        "fields": fields,
        "matched": {
            "violation_type_dict_id": type_id,
            "vehicle_id": local_hit.get("vehicle_id"),
            "from_local": bool(local_hit.get("from_local")),
        },
        "raw_preview": (clean_text or "")[:500],
        "company": company,
        "session_id": session_id,
    }
